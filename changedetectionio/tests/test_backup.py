#!/usr/bin/env python3

import io
import json
import os
import re
import time
from zipfile import ZipFile, ZIP_DEFLATED

from flask import url_for

from changedetectionio.model import Tag, Watch

from .util import live_server_setup, set_original_response, wait_for_all_checks


def test_backup(client, live_server, measure_memory_usage, datastore_path):
    set_original_response(datastore_path=datastore_path)


    # Add our URL to the import page
    res = client.post(
        url_for("imports.import_page"),
        data={"urls": url_for('test_endpoint', _external=True)+"?somechar=őőőőőőőő"},
        follow_redirects=True
    )

    assert b"1 Imported" in res.data
    wait_for_all_checks(client)

    # Launch the thread in the background to create the backup
    res = client.post(
        url_for("backups.request_backup"),
        follow_redirects=True
    )
    time.sleep(4)

    res = client.get(
        url_for("backups.create"),
        follow_redirects=True
    )
    # Can see the download link to the backup
    assert b'<a href="/backups/download/changedetection-backup-20' in res.data
    assert b'Remove backups' in res.data

    # Get the latest one
    res = client.get(
        url_for("backups.download_backup", filename="latest"),
        follow_redirects=True
    )

    # Should get the right zip content type
    assert res.content_type == "application/zip"

    # Should be PK/ZIP stream
    assert res.data.count(b'PK') >= 2

    backup = ZipFile(io.BytesIO(res.data))
    l = backup.namelist()

    # Check for UUID-based txt files (history, snapshot, and last-checksum)
    uuid4hex_txt = re.compile('^[a-f0-9]{8}-?[a-f0-9]{4}-?4[a-f0-9]{3}-?[89ab][a-f0-9]{3}-?[a-f0-9]{12}.*txt', re.I)
    txt_files = list(filter(uuid4hex_txt.match, l))
    # Should be three txt files in the archive (history, snapshot, and last-checksum)
    assert len(txt_files) == 3

    # Check for watch.json files (new format)
    uuid4hex_json = re.compile('^[a-f0-9]{8}-?[a-f0-9]{4}-?4[a-f0-9]{3}-?[89ab][a-f0-9]{3}-?[a-f0-9]{12}/watch\.json$', re.I)
    json_files = list(filter(uuid4hex_json.match, l))
    # Should be one watch.json file in the archive (the imported watch)
    assert len(json_files) == 1, f"Expected 1 watch.json file, found {len(json_files)}: {json_files}"

    # Check for changedetection.json (settings file)
    assert 'changedetection.json' in l, "changedetection.json should be in backup"

    # secret.txt must never be included — it contains the Flask session key
    assert 'secret.txt' not in l, "secret.txt (Flask session key) must not be included in backup"

    # Get the latest one
    res = client.post(
        url_for("backups.remove_backups"),
        follow_redirects=True
    )

    assert b'No backups found.' in res.data


def test_watch_data_package_download(client, live_server, measure_memory_usage, datastore_path):
    """Test downloading a single watch's data as a zip package"""

    set_original_response(datastore_path=datastore_path)

    uuid = client.application.config.get('DATASTORE').add_watch(url=url_for('test_endpoint', _external=True))
    tag_uuid = client.application.config.get('DATASTORE').add_tag(title="Tasty backup tag")
    tag_uuid2 = client.application.config.get('DATASTORE').add_tag(title="Tasty backup tag number two")
    client.post(url_for("ui.form_watch_checknow"), follow_redirects=True)

    wait_for_all_checks(client)

    # Download the watch data package
    res = client.get(url_for("ui.ui_edit.watch_get_data_package", uuid=uuid))

    # Should get the right zip content type
    assert res.content_type == "application/zip"

    # Should be PK/ZIP stream (PKzip header)
    assert res.data[:2] == b'PK', "File should start with PK (PKzip header)"
    assert res.data.count(b'PK') >= 2, "Should have multiple PK markers (zip file structure)"

    # Verify zip contents
    backup = ZipFile(io.BytesIO(res.data))
    files = backup.namelist()

    # Should have files in a UUID directory
    assert any(uuid in f for f in files), f"Files should be in UUID directory: {files}"

    # Should contain watch.json
    watch_json_path = f"{uuid}/watch.json"
    assert watch_json_path in files, f"Should contain watch.json, got: {files}"

    # Should contain history/snapshot files
    uuid4hex_txt = re.compile(f'^{re.escape(uuid)}/.*\\.txt', re.I)
    txt_files = list(filter(uuid4hex_txt.match, files))
    assert len(txt_files) > 0, f"Should have at least one .txt file (history/snapshot), got: {files}"


def test_backup_restore(client, live_server, measure_memory_usage, datastore_path):
    """Test that a full backup zip can be restored — watches and tags survive a round-trip."""

    set_original_response(datastore_path=datastore_path)

    datastore = live_server.app.config['DATASTORE']
    watch_url = url_for('test_endpoint', _external=True)

    # Set up: one watch and two tags
    uuid = datastore.add_watch(url=watch_url)
    tag_uuid = datastore.add_tag(title="Tasty backup tag")
    tag_uuid2 = datastore.add_tag(title="Tasty backup tag number two")

    client.post(url_for("ui.form_watch_checknow"), follow_redirects=True)
    wait_for_all_checks(client)

    # Create a full backup
    client.post(url_for("backups.request_backup"), follow_redirects=True)
    time.sleep(4)

    # Download the latest backup zip
    res = client.get(url_for("backups.download_backup", filename="latest"), follow_redirects=True)
    assert res.content_type == "application/zip"
    zip_data = res.data

    # Confirm the zip contains both watch.json and tag.json entries
    backup = ZipFile(io.BytesIO(zip_data))
    names = backup.namelist()
    assert f"{uuid}/watch.json" in names, f"watch.json missing from backup: {names}"
    assert f"{tag_uuid}/tag.json" in names, f"tag.json for tag 1 missing from backup: {names}"
    assert f"{tag_uuid2}/tag.json" in names, f"tag.json for tag 2 missing from backup: {names}"

    # --- Wipe everything ---
    datastore.delete('all')
    client.post(url_for("tags.delete_all"), follow_redirects=True)

    assert uuid not in datastore.data['watching'], "Watch should be gone after delete"
    assert tag_uuid not in datastore.data['settings']['application']['tags'], "Tag 1 should be gone after delete"
    assert tag_uuid2 not in datastore.data['settings']['application']['tags'], "Tag 2 should be gone after delete"

    # --- Restore from the backup zip ---
    res = client.post(
        url_for("backups.restore.backups_restore_start"),
        data={
            'zip_file': (io.BytesIO(zip_data), 'backup.zip'),
            'include_groups': 'y',
            'include_groups_replace_existing': 'y',
            'include_watches': 'y',
            'include_watches_replace_existing': 'y',
        },
        content_type='multipart/form-data',
        follow_redirects=True
    )
    assert res.status_code == 200

    # Wait for the thread to finish
    time.sleep(2)

    # --- Watch checks ---
    restored_watch = datastore.data['watching'].get(uuid)
    assert restored_watch is not None, f"Watch {uuid} not found after restore"
    assert restored_watch['url'] == watch_url, "Restored watch URL does not match"
    assert isinstance(restored_watch, Watch.model), \
        f"Watch not properly rehydrated, got {type(restored_watch)}"
    assert restored_watch.history_n >= 1, \
        f"Restored watch should have at least 1 history entry, got {restored_watch.history_n}"

    # --- Tag checks ---
    restored_tags = datastore.data['settings']['application']['tags']

    restored_tag = restored_tags.get(tag_uuid)
    assert restored_tag is not None, f"Tag {tag_uuid} not found after restore"
    assert restored_tag['title'] == "Tasty backup tag", "Restored tag 1 title does not match"
    assert isinstance(restored_tag, Tag.model), \
        f"Tag 1 not properly rehydrated, got {type(restored_tag)}"

    restored_tag2 = restored_tags.get(tag_uuid2)
    assert restored_tag2 is not None, f"Tag {tag_uuid2} not found after restore"
    assert restored_tag2['title'] == "Tasty backup tag number two", "Restored tag 2 title does not match"
    assert isinstance(restored_tag2, Tag.model), \
        f"Tag 2 not properly rehydrated, got {type(restored_tag2)}"


def test_backup_restore_zip_slip_rejected(client, live_server, measure_memory_usage, datastore_path):
    """Zip Slip path traversal entries in a restore zip must be rejected."""
    import pytest
    from changedetectionio.blueprint.backups.restore import import_from_zip

    # Build a zip with a path traversal entry that would escape the extraction dir
    malicious_zip = io.BytesIO()
    with ZipFile(malicious_zip, 'w') as zf:
        zf.writestr("../escaped.txt", "ATTACKER-CONTROLLED")
    malicious_zip.seek(0)

    datastore = live_server.app.config['DATASTORE']

    with pytest.raises(ValueError, match="Zip Slip"):
        import_from_zip(
            zip_stream=malicious_zip,
            datastore=datastore,
            include_groups=True,
            include_groups_replace=True,
            include_watches=True,
            include_watches_replace=True,
        )


def test_backup_restore_zip_bomb_rejected(client, live_server, measure_memory_usage, datastore_path):
    """A zip whose total uncompressed size exceeds the limit must be rejected.

    The guard reads file_size from the zip central-directory metadata — no
    actual decompression happens, so this test is fast and uses minimal RAM.
    100 KB of zeros compresses to ~100 bytes; monkeypatching the limit to
    50 KB is enough to trigger the check without creating any large files.
    """
    import pytest
    import changedetectionio.blueprint.backups.restore as restore_mod
    from changedetectionio.blueprint.backups.restore import import_from_zip

    # ~100 KB of zeros → deflate compresses to ~100 bytes, but file_size metadata = 100 KB
    bomb_zip = io.BytesIO()
    with ZipFile(bomb_zip, 'w', compression=ZIP_DEFLATED) as zf:
        zf.writestr("data.txt", b"\x00" * (100 * 1024))
    bomb_zip.seek(0)

    datastore = live_server.app.config['DATASTORE']
    original_limit = restore_mod._MAX_DECOMPRESSED_BYTES
    try:
        restore_mod._MAX_DECOMPRESSED_BYTES = 50 * 1024  # 50 KB limit for this test
        with pytest.raises(ValueError, match="decompressed size"):
            import_from_zip(
                zip_stream=bomb_zip,
                datastore=datastore,
                include_groups=True,
                include_groups_replace=True,
                include_watches=True,
                include_watches_replace=True,
            )
    finally:
        restore_mod._MAX_DECOMPRESSED_BYTES = original_limit


class _FakeWatch(dict):
    """Minimal stand-in for a Watch.model object: data_dir + dict access."""
    def __init__(self, data_dir, **kw):
        super().__init__(**kw)
        self.data_dir = data_dir


def _write_json(path, data):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f)


def test_backup_rejects_torn_history_and_retries(tmp_path):
    """An index referencing a snapshot that vanished (trim/half commit) must not
    be published; the candidate is removed and the next request succeeds."""
    import pytest

    from changedetectionio.blueprint.backups import BackupBuildError, create_backup

    datastore_dir = str(tmp_path)
    uuid = "11111111-1111-4111-8111-111111111111"
    watch_dir = os.path.join(datastore_dir, uuid)
    os.makedirs(watch_dir)
    snapshot_name = "abcdef.txt"

    _write_json(os.path.join(watch_dir, "watch.json"), {
        "uuid": uuid,
        "url": "https://example.com/",
        "processor": "text_json_diff",
        "last_checked": 1700000000,
        "tags": [],
    })
    # Index points at a snapshot that is not on disk (trimmed mid-backup/raced)
    with open(os.path.join(watch_dir, "history.txt"), "w", encoding="utf-8") as f:
        f.write(f"1700000000,{snapshot_name}\n")

    watches = {uuid: _FakeWatch(watch_dir, url="https://example.com/", tags=[])}

    with pytest.raises(BackupBuildError):
        create_backup(datastore_path=datastore_dir, watches=watches, tags={})

    # No published archive, no candidate leftovers
    assert not [n for n in os.listdir(datastore_dir) if n.endswith(".zip")]
    assert not [n for n in os.listdir(datastore_dir) if n.startswith(".backup-candidate-")]

    # The check boundary becomes complete (snapshot appears): retry succeeds
    with open(os.path.join(watch_dir, snapshot_name), "w", encoding="utf-8") as f:
        f.write("snapshot body")
    backup_path = create_backup(datastore_path=datastore_dir, watches=watches, tags={})
    assert os.path.isfile(backup_path)

    with ZipFile(backup_path) as zf:
        names = set(zf.namelist())
        assert f"{uuid}/watch.json" in names
        assert f"{uuid}/history.txt" in names
        assert f"{uuid}/{snapshot_name}" in names


def test_backup_cleans_stale_candidates_but_keeps_completed_backups(tmp_path):
    """Interrupted candidates are removed; already published *.zip backups survive."""
    from changedetectionio.blueprint.backups import CANDIDATE_DIR_PREFIX, create_backup

    datastore_dir = str(tmp_path)

    old_backup = os.path.join(datastore_dir, "changedetection-backup-20000101000000.zip")
    with ZipFile(old_backup, "w") as zf:
        zf.writestr("changedetection.json", "{}")

    # New-scheme candidate directory (process died while building)
    dead_dir = os.path.join(datastore_dir, CANDIDATE_DIR_PREFIX + "1234-deadbeef")
    os.makedirs(dead_dir)
    with open(os.path.join(dead_dir, "half.zip"), "w") as f:
        f.write("partial")
    # Old-scheme leftover
    with open(os.path.join(datastore_dir, "changedetection-backup-20000101010101.tmp"), "w") as f:
        f.write("legacy partial")

    # Empty instance (no watch dirs) - background creation must still work
    create_backup(datastore_path=datastore_dir, watches={}, tags={})

    assert os.path.isfile(old_backup), "a completed old backup must remain downloadable"
    leftovers = [n for n in os.listdir(datastore_dir)
                 if n.startswith(CANDIDATE_DIR_PREFIX) or n.endswith(".tmp")]
    assert leftovers == [], f"unfinished candidates must be cleaned: {leftovers}"
    published = [n for n in os.listdir(datastore_dir)
                 if n.startswith("changedetection-backup-") and n.endswith(".zip")]
    assert len(published) == 2


def test_backup_is_a_consistent_watch_boundary(tmp_path):
    """All files of one watch land in the archive; scratch files never do; the
    URL list only references the archived watch."""
    from changedetectionio.blueprint.backups import create_backup

    datastore_dir = str(tmp_path)
    uuid = "22222222-2222-4222-a222-222222222222"
    tag_uuid = "33333333-3333-4333-b333-333333333333"
    watch_dir = os.path.join(datastore_dir, uuid)
    tag_dir = os.path.join(datastore_dir, tag_uuid)
    os.makedirs(watch_dir)
    os.makedirs(tag_dir)

    _write_json(os.path.join(datastore_dir, "changedetection.json"), {
        "settings": {"application": {"tags": {}}}
    })
    _write_json(os.path.join(watch_dir, "watch.json"), {
        "uuid": uuid,
        "url": "https://example.com/űrlap?x=ő",
        "processor": "restock_diff",
        "last_checked": 1700000005,
        "tags": [tag_uuid],
    })
    _write_json(os.path.join(watch_dir, "restock_diff.json"), {"in_stock_only": True})
    with open(os.path.join(watch_dir, "history-restock_diff.txt"), "w", encoding="utf-8") as f:
        f.write("1700000005,snap-5.txt.br\n")
    with open(os.path.join(watch_dir, "snap-5.txt.br"), "wb") as f:
        f.write(b"compressed-snapshot")
    # Writer scratch files must never enter the archive
    with open(os.path.join(watch_dir, "tmpXYZ"), "w") as f:
        f.write("scratch")
    with open(os.path.join(watch_dir, "json-xyz.tmp"), "w") as f:
        f.write("scratch")
    _write_json(os.path.join(tag_dir, "tag.json"), {"uuid": tag_uuid, "title": "Gruppe 🏷️"})

    watches = {uuid: _FakeWatch(watch_dir,
                                url="https://example.com/űrlap?x=ő",
                                tags=[tag_uuid])}
    tags = {tag_uuid: _FakeWatch(tag_dir, title="Gruppe 🏷️")}

    backup_path = create_backup(datastore_path=datastore_dir, watches=watches, tags=tags)
    with ZipFile(backup_path) as zf:
        names = set(zf.namelist())

    assert f"{uuid}/watch.json" in names
    assert f"{uuid}/restock_diff.json" in names
    assert f"{uuid}/history-restock_diff.txt" in names
    assert f"{uuid}/snap-5.txt.br" in names
    assert f"{tag_uuid}/tag.json" in names
    assert "changedetection.json" in names
    assert f"{uuid}/tmpXYZ" not in names
    assert f"{uuid}/json-xyz.tmp" not in names
    assert "secret.txt" not in names

    with ZipFile(backup_path) as zf:
        assert "example.com/űrlap" in zf.read("url-list.txt").decode("utf-8")
        assert tag_uuid in zf.read("url-list-with-tags.txt").decode("utf-8")


def test_backup_watch_without_committed_watch_json_deferred(tmp_path):
    """A half-created watch directory (no watch.json yet) waits for the next
    archive instead of being published as a partial entry."""
    from changedetectionio.blueprint.backups import create_backup

    datastore_dir = str(tmp_path)
    uuid = "44444444-4444-4444-8444-444444444444"
    watch_dir = os.path.join(datastore_dir, uuid)
    os.makedirs(watch_dir)
    with open(os.path.join(watch_dir, "last-screenshot.png"), "wb") as f:
        f.write(b"\x89PNG-fake")

    watches = {uuid: _FakeWatch(watch_dir, url="https://example.com/new", tags=[])}
    backup_path = create_backup(datastore_path=datastore_dir, watches=watches, tags={})

    with ZipFile(backup_path) as zf:
        names = set(zf.namelist())
    assert f"{uuid}/last-screenshot.png" not in names
    assert f"{uuid}/watch.json" not in names
    # The URL list must not reference a watch missing from the archive
    assert "https://example.com/new" not in zf_read(backup_path, "url-list.txt")


def zf_read(zip_path, member):
    with ZipFile(zip_path) as zf:
        return zf.read(member).decode("utf-8")


def test_backup_with_extended_utf8_urls_and_tags(datastore_path):
    """Test create_backup handles extended UTF-8 characters in URLs and tags without encoding errors (issue #1805)."""
    import os
    import tempfile
    from unittest.mock import MagicMock

    from changedetectionio.blueprint.backups import create_backup

    utf8_url = "https://example.com/products/café-öl-日本語-🎉?query=test#section"
    utf8_tag_titles = ["Größe", "日本語タグ", "🏷️special"]
    tag_uuids = [
        "aaaaaaaa-0000-4000-8000-000000000001",
        "aaaaaaaa-0000-4000-8000-000000000002",
        "aaaaaaaa-0000-4000-8000-000000000003",
    ]

    with tempfile.TemporaryDirectory() as tmp_dir:
        # A committed watch boundary: its own data directory with watch.json
        watch_data_dir = os.path.join(tmp_dir, "test-uuid-1")
        os.makedirs(watch_data_dir)
        with open(os.path.join(watch_data_dir, "watch.json"), "w", encoding="utf-8") as f:
            json.dump({"uuid": "test-uuid-1", "url": utf8_url, "tags": tag_uuids}, f)

        watch_mock = MagicMock()
        watch_mock.data_dir = watch_data_dir
        watch_mock.__getitem__.side_effect = lambda key: utf8_url if key == "url" else None
        watch_mock.get.side_effect = lambda key, default=None: (
            utf8_url if key == "url" else (tag_uuids if key == "tags" else default)
        )

        tag_mocks = {}
        for tag_uuid, title in zip(tag_uuids, utf8_tag_titles, strict=True):
            tag_dir = os.path.join(tmp_dir, tag_uuid)
            os.makedirs(tag_dir)
            with open(os.path.join(tag_dir, "tag.json"), "w", encoding="utf-8") as f:
                json.dump({"uuid": tag_uuid, "title": title}, f, ensure_ascii=False)
            tag_mock = MagicMock()
            tag_mock.data_dir = tag_dir
            tag_mock.get.side_effect = lambda key, default=None, t=title: t if key == "title" else default
            tag_mocks[tag_uuid] = tag_mock

        watches = {"test-uuid-1": watch_mock}
        # Should create backup without UnicodeEncodeError
        backup_path = create_backup(datastore_path=tmp_dir, watches=watches, tags=tag_mocks)

        url_list_path = os.path.join(tmp_dir, "url-list.txt")
        url_list_tags_path = os.path.join(tmp_dir, "url-list-with-tags.txt")

        assert os.path.isfile(url_list_path)
        assert os.path.isfile(url_list_tags_path)

        with open(url_list_path, encoding="utf-8") as f:
            content = f.read()
            assert "café-öl-日本語-🎉" in content

        with open(url_list_tags_path, encoding="utf-8") as f:
            content = f.read()
            assert "café-öl-日本語-🎉" in content
            for tag_uuid in tag_uuids:
                assert tag_uuid in content

        # Group titles (UTF-8) survive into the archive as well
        with ZipFile(backup_path) as zf:
            for tag_uuid, title in zip(tag_uuids, utf8_tag_titles, strict=True):
                assert title in zf.read(f"{tag_uuid}/tag.json").decode("utf-8")
