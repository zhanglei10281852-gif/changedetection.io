#!/usr/bin/env python3
"""
End-to-end guarantees for the joint publication of watch.json + processor config
through the actual edit page and REST API entry points.

- Both files of one save converge to one generation (UI edit, GET API, restart)
- A stale base version is rejected from the API (If-Match) and the edit form
- Any publish failure returns an explicit error, leaves the old complete config
  on disk, and does not queue an immediate recheck
- First-time creation persists the processor settings together with watch.json
"""

import json
import os

from flask import url_for

from changedetectionio.store import ChangeDetectionStore
from changedetectionio.store.config_transaction import WATCH_CONFIG_VERSION_KEY


def _watch_json(datastore, uuid):
    path = os.path.join(datastore.data['watching'][uuid].data_dir, 'watch.json')
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def _restock_json(datastore, uuid):
    path = os.path.join(datastore.data['watching'][uuid].data_dir, 'restock_diff.json')
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def _drain_queue():
    from changedetectionio.flask_app import update_q
    while not update_q.empty():
        try:
            update_q.get_nowait()
        except Exception:
            break


def test_api_put_joint_publish_and_if_match_conflict(client, live_server):
    datastore = client.application.config.get('DATASTORE')
    api_key = live_server.app.config['DATASTORE'].data['settings']['application'].get('api_access_token')
    headers = {'content-type': 'application/json', 'x-api-key': api_key}

    res = client.post(
        url_for("createwatch"),
        data=json.dumps({
            'url': 'http://example.com/',
            'processor': 'restock_diff',
            'processor_config_restock_diff': {'follow_price_changes': False},
        }),
        headers=headers,
    )
    assert res.status_code == 201, res.data
    uuid = res.json['uuid']

    # The first generation exists on BOTH files
    watch_disk = _watch_json(datastore, uuid)
    restock_disk = _restock_json(datastore, uuid)
    assert watch_disk[WATCH_CONFIG_VERSION_KEY] >= 1
    assert restock_disk['__config_version__'] == watch_disk[WATCH_CONFIG_VERSION_KEY]
    assert restock_disk['restock_diff']['follow_price_changes'] is False
    # No transaction debris
    debris = [n for n in os.listdir(datastore.data['watching'][uuid].data_dir) if n.startswith('.cfgtx-')]
    assert debris == []

    version = watch_disk[WATCH_CONFIG_VERSION_KEY]

    # A stale If-Match is rejected with 409, cannot clobber the newer save
    res = client.put(
        url_for("watch", uuid=uuid),
        data=json.dumps({'title': 'Late stale request'}),
        headers={**headers, 'If-Match': str(version - 1)},
    )
    assert res.status_code == 409, res.data
    assert _watch_json(datastore, uuid).get('title') != 'Late stale request'

    # Matching If-Match publishes
    res = client.put(
        url_for("watch", uuid=uuid),
        data=json.dumps({
            'title': 'Joint title',
            'processor_config_restock_diff': {'in_stock_processing': 'all_changes'},
        }),
        headers={**headers, 'If-Match': str(version)},
    )
    assert res.status_code == 200, res.data

    # GET API converges to the same new generation
    res = client.get(url_for("watch", uuid=uuid), headers={'x-api-key': api_key})
    assert res.status_code == 200
    data = res.json
    assert data['title'] == 'Joint title'
    assert data[WATCH_CONFIG_VERSION_KEY] == version + 1
    assert data['processor_config_restock_diff']['in_stock_processing'] == 'all_changes'


def test_api_put_failure_keeps_old_config_and_does_not_queue(client, live_server, monkeypatch):
    datastore = client.application.config.get('DATASTORE')
    api_key = live_server.app.config['DATASTORE'].data['settings']['application'].get('api_access_token')

    uuid = datastore.add_watch(url='http://example.com/', extras={'processor': 'restock_diff'})
    datastore.publish_watch_update(
        uuid, processor_config={'restock_diff': {'in_stock_processing': 'in_stock_only'}}
    )

    before = _watch_json(datastore, uuid)
    before_restock = _restock_json(datastore, uuid)
    _drain_queue()

    def explode(*args, **kwargs):
        raise OSError("simulated disk failure during joint publish")

    monkeypatch.setattr(datastore, 'publish_watch_update', explode)

    res = client.put(
        url_for("watch", uuid=uuid),
        data=json.dumps({
            'title': 'Should not persist',
            'processor_config_restock_diff': {'in_stock_processing': 'off'},
        }),
        headers={'content-type': 'application/json', 'x-api-key': api_key},
    )
    assert res.status_code == 500, res.data

    # Old COMPLETE config still on disk
    assert _watch_json(datastore, uuid)['title'] == before['title']
    assert _restock_json(datastore, uuid)['restock_diff'] == before_restock['restock_diff']
    debris = [n for n in os.listdir(datastore.data['watching'][uuid].data_dir) if n.startswith('.cfgtx-')]
    assert debris == []

    # No immediate check was queued by the failed save
    from changedetectionio.flask_app import update_q
    assert update_q.empty(), "a failed save must not queue an immediate recheck"


def test_edit_page_stale_form_version_is_rejected(client, live_server):
    datastore = client.application.config.get('DATASTORE')
    uuid = datastore.add_watch(url='http://example.com/', extras={'title': 'Keep me'})

    # A newer joint save wins before the stale form posts
    datastore.publish_watch_update(uuid, watch_updates={'title': 'Newer saved title'})

    res = client.post(
        url_for("ui.ui_edit.edit_page", uuid=uuid),
        data={
            'config_version': '0',
            'url': 'http://example.com/',
            'title': 'Stale overwrite',
            'time_between_check-hours': '2',
            'time_between_check-minutes': '30',
            'fetch_backend': 'html_requests',
            'method': 'GET',
            'ignore_text': '',
        },
        follow_redirects=True,
    )
    assert res.status_code == 200
    assert b"another tab or request" in res.data, "a stale edit save should show a clear conflict error"
    assert _watch_json(datastore, uuid)['title'] == 'Newer saved title'


def test_edit_page_joint_publish_survives_restart(client, live_server):
    datastore = client.application.config.get('DATASTORE')
    uuid = datastore.add_watch(url='http://example.com/', extras={'processor': 'restock_diff'})

    res = client.post(
        url_for("ui.ui_edit.edit_page", uuid=uuid),
        data={
            'config_version': '0',
            'url': 'http://example.com/',
            'title': 'UI joint title',
            'time_between_check-hours': '2',
            'time_between_check-minutes': '30',
            'fetch_backend': 'html_requests',
            'method': 'GET',
            'ignore_text': '',
            'processor_config_restock_diff-in_stock_processing': 'all_changes',
            'processor_config_restock_diff-follow_price_changes': 'y',
            'processor_config_restock_diff-price_change_min': '42.5',
            'processor_config_restock_diff-price_change_max': '',
            'processor_config_restock_diff-price_change_threshold_percent': '',
        },
        follow_redirects=True,
    )
    assert res.status_code == 200
    assert b"Updated watch" in res.data

    version = _watch_json(datastore, uuid)[WATCH_CONFIG_VERSION_KEY]
    restock = _restock_json(datastore, uuid)
    assert restock['__config_version__'] == version
    assert restock['restock_diff']['price_change_min'] == 42.5
    assert restock['restock_diff']['in_stock_processing'] == 'all_changes'

    # The edit page GET converges on the same committed settings
    res = client.get(url_for("ui.ui_edit.edit_page", uuid=uuid))
    assert res.status_code == 200
    assert b'value="42.5"' in res.data, "edit form must prefill the jointly saved threshold"

    # Switch processor away and back - the dedicated settings must survive
    res = client.post(
        url_for("ui.ui_edit.edit_page", uuid=uuid),
        data={
            'config_version': str(version),
            'url': 'http://example.com/',
            'title': 'UI joint title',
            'time_between_check-hours': '2',
            'time_between_check-minutes': '30',
            'fetch_backend': 'html_requests',
            'method': 'GET',
            'processor': 'text_json_diff',
            'ignore_text': '',
        },
        follow_redirects=True,
    )
    assert b"Updated watch" in res.data
    # restock_diff.json is still on disk (dedicated settings are per-processor)
    assert _restock_json(datastore, uuid)['restock_diff']['price_change_min'] == 42.5

    # Restart: one consistent generation visible
    datastore2 = ChangeDetectionStore(datastore_path=datastore.datastore_path,
                                    include_default_watches=False)
    datastore2.stop_thread = True
    watch = datastore2.data['watching'][uuid]
    assert watch['title'] == 'UI joint title'
    assert watch[WATCH_CONFIG_VERSION_KEY] >= version
    cfg = datastore2.read_watch_processor_config(uuid, 'restock_diff.json')
    assert cfg['restock_diff']['price_change_min'] == 42.5
    debris = [n for n in os.listdir(watch.data_dir) if n.startswith('.cfgtx-')]
    assert debris == []
