#!/usr/bin/env python3
"""
Unit tests for the joint publication of watch.json + {processor}.json.

Covers:
- Both files land as one generation (same version, no transaction debris)
- Processor settings merge and per-processor retention when switching processors
- Stale optimistic version is rejected
- Write/close/publish failure rolls disk AND memory back to the old complete config
- Startup recovery: prepared marker rolls back, committed marker completes the commit
- Readers never observe a prepared (half-published) generation
- Concurrent saves can never let a late request overwrite a newer success
"""

import glob
import json
import os
import tempfile
import threading
import unittest
from unittest import mock

from changedetectionio.store import ChangeDetectionStore
from changedetectionio.store import config_transaction as ctx
from changedetectionio.store.config_transaction import (
    WATCH_CONFIG_VERSION_KEY,
    PROCESSOR_CONFIG_VERSION_KEY,
    StaleConfigVersionError,
    ConfigPublishError,
    _TX_PREFIX,
    _MARKER_TEMPLATE,
    _CANDIDATE_TEMPLATE,
)


class JointPublishTestBase(unittest.TestCase):
    def setUp(self):
        self.datastore_path = tempfile.mkdtemp()
        self.store = ChangeDetectionStore(datastore_path=self.datastore_path,
                                         include_default_watches=False)
        self.uuid = self.store.add_watch(url='https://example.com/',
                                        extras={'processor': 'restock_diff',
                                                'title': 'Original'})
        self.watch = self.store.data['watching'][self.uuid]
        self.data_dir = self.watch.data_dir

    def tearDown(self):
        self.store.stop_thread = True

    def _read(self, name):
        with open(os.path.join(self.data_dir, name), 'rb') as f:
            return json.loads(f.read())

    def _debris(self):
        return [os.path.basename(p) for p in glob.glob(os.path.join(self.data_dir, _TX_PREFIX + '*'))]


class TestJointPublishHappyPath(JointPublishTestBase):

    def test_both_files_publish_together_at_one_generation(self):
        version = self.store.publish_watch_update(
            self.uuid,
            watch_updates={'title': 'New title'},
            processor_config={'restock_diff': {'in_stock_processing': 'all_changes',
                                             'follow_price_changes': False}},
        )
        self.assertEqual(version, 1)

        watch_disk = self._read('watch.json')
        restock_disk = self._read('restock_diff.json')

        # One shared generation
        self.assertEqual(watch_disk[WATCH_CONFIG_VERSION_KEY], 1)
        self.assertEqual(restock_disk.pop(PROCESSOR_CONFIG_VERSION_KEY), 1)
        self.assertEqual(restock_disk['restock_diff'],
                         {'in_stock_processing': 'all_changes',
                          'follow_price_changes': False})

        # In-memory state matches
        self.assertEqual(self.watch[WATCH_CONFIG_VERSION_KEY], 1)
        self.assertEqual(self.watch['title'], 'New title')

        # Candidates/backups/markers are all cleaned up
        self.assertEqual(self._debris(), [])

    def test_processor_config_optional_rewrites_only_watch_json(self):
        self.store.publish_watch_update(self.uuid, watch_updates={'title': 'No proc part'})
        self.assertFalse(os.path.exists(os.path.join(self.data_dir, 'restock_diff.json')))
        self.assertEqual(self._debris(), [])

    def test_restart_sees_exactly_one_consistent_generation(self):
        from changedetectionio.store import ChangeDetectionStore

        self.store.publish_watch_update(
            self.uuid,
            watch_updates={'title': 'Persisted'},
            processor_config={'restock_diff': {'in_stock_processing': 'off'}},
        )

        reloaded = ChangeDetectionStore(datastore_path=self.datastore_path,
                                       include_default_watches=False)
        reloaded.stop_thread = True
        watch = reloaded.data['watching'][self.uuid]
        self.assertEqual(watch['title'], 'Persisted')
        self.assertEqual(watch[WATCH_CONFIG_VERSION_KEY], 1)

        restock = reloaded.read_watch_processor_config(self.uuid, 'restock_diff.json')
        self.assertEqual(restock['restock_diff']['in_stock_processing'], 'off')
        self.assertEqual(self._debris(), [])


class TestProcessorSwitchingAndMerge(JointPublishTestBase):

    def test_each_processor_keeps_its_own_settings_when_switching(self):
        # Restock settings
        self.store.publish_watch_update(
            self.uuid,
            processor_config={'restock_diff': {'in_stock_processing': 'all_changes'}},
        )
        # Switch to the text processor and save (no dedicated config in this request)
        self.store.publish_watch_update(self.uuid,
                                       watch_updates={'processor': 'text_json_diff'})
        # restock_diff.json must survive untouched
        restock = self._read('restock_diff.json')
        self.assertEqual(restock['restock_diff']['in_stock_processing'], 'all_changes')
        self.assertFalse(os.path.exists(os.path.join(self.data_dir, 'text_json_diff.json')))

        # Switch back to restock and merge one more key
        self.store.publish_watch_update(
            self.uuid,
            watch_updates={'processor': 'restock_diff'},
            processor_config={'restock_diff': {'follow_price_changes': False}},
        )
        restock = self.store.read_watch_processor_config(self.uuid, 'restock_diff.json')
        # Wholesale nested replacement semantics: new nested dict wins...
        self.assertEqual(restock['restock_diff'], {'follow_price_changes': False})

    def test_flat_processor_keys_still_merge(self):
        # The legacy API shape stored flat keys directly in the processor file
        self.store.publish_watch_update(
            self.uuid,
            processor_config={'price_threshold': 10.0},
        )
        self.store.publish_watch_update(
            self.uuid,
            processor_config={'in_stock_only': True},
        )
        cfg = self.store.read_watch_processor_config(self.uuid, 'restock_diff.json')
        self.assertEqual(cfg.get('price_threshold'), 10.0)
        self.assertEqual(cfg.get('in_stock_only'), True)


class TestStaleVersion(JointPublishTestBase):

    def test_expected_version_must_match_current(self):
        self.store.publish_watch_update(self.uuid, watch_updates={'title': 'v1'})
        with self.assertRaises(StaleConfigVersionError):
            self.store.publish_watch_update(self.uuid,
                                          watch_updates={'title': 'stale'},
                                          expected_version=0)
        # Newer success is intact
        self.assertEqual(self.watch['title'], 'v1')
        self.assertEqual(self._read('watch.json')['title'], 'v1')

    def test_concurrent_saves_cannot_let_a_late_request_overwrite(self):
        barrier = threading.Barrier(2)
        results = []

        def save(title):
            barrier.wait()
            try:
                self.store.publish_watch_update(
                    self.uuid,
                    watch_updates={'title': title},
                    expected_version=0,
                )
                results.append('ok')
            except StaleConfigVersionError:
                results.append('stale')
            except Exception as e:  # pragma: no cover - surfaces real bugs
                results.append(f'error:{e!r}')

        threads = [threading.Thread(target=save, args=('A-title',)),
                   threading.Thread(target=save, args=('B-title',))]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(sorted(results), ['ok', 'stale'])
        self.assertEqual(self._read('watch.json')[WATCH_CONFIG_VERSION_KEY], 1)
        self.assertEqual(self._debris(), [])


class TestFailureRollback(JointPublishTestBase):

    def test_failure_after_first_rename_restores_old_complete_config(self):
        # Establish a complete generation 1 first
        self.store.publish_watch_update(
            self.uuid,
            watch_updates={'title': 'Generation one'},
            processor_config={'restock_diff': {'in_stock_processing': 'in_stock_only'}},
        )

        real_replace = os.replace

        def fail_on_watch_final(src, dst):
            if os.path.basename(str(dst)) == 'watch.json':
                raise OSError("simulated rename failure (quota / permission)")
            return real_replace(src, dst)

        # Suppress the immediate in-process rollback so we can exercise STARTUP
        # recovery of the exact "process died between the two writes" state.
        original_rollback = ctx._rollback
        ctx._rollback = lambda *a, **k: (_ for _ in ()).throw(
            OSError("rollback suppressed to simulate process death"))

        with mock.patch('changedetectionio.store.config_transaction.os.replace',
                        side_effect=fail_on_watch_final):
            with self.assertRaises(ConfigPublishError):
                self.store.publish_watch_update(
                    self.uuid,
                    watch_updates={'title': 'Generation two'},
                    processor_config={'restock_diff': {'in_stock_processing': 'off'}},
                )

        ctx._rollback = original_rollback

        # Disk is currently mixed: processor file already at generation 2, marker
        # still "prepared", watch.json still generation 1.
        marker = glob.glob(os.path.join(self.data_dir, _TX_PREFIX + 'm-*.json'))
        self.assertEqual(len(marker), 1)

        # Startup must deterministically roll back, never treat this as success.
        ctx.recover_config_transaction(self.data_dir)

        watch_disk = self._read('watch.json')
        restock_disk = self._read('restock_diff.json')
        self.assertEqual(watch_disk['title'], 'Generation one')
        self.assertEqual(watch_disk[WATCH_CONFIG_VERSION_KEY], 1)
        self.assertEqual(restock_disk['restock_diff']['in_stock_processing'], 'in_stock_only')
        self.assertEqual(restock_disk[PROCESSOR_CONFIG_VERSION_KEY], 1)
        self.assertEqual(self._debris(), [])

    def test_failure_rolls_back_live_in_memory_object_too(self):
        real_replace = os.replace
        calls = {'n': 0}

        def fail_once_on_watch_final(src, dst):
            # Fail exactly at the watch.json PUBLISH rename; the best-effort
            # rollback renames (which run after that point) must still work.
            if os.path.basename(str(dst)) == 'watch.json':
                calls['n'] += 1
                if calls['n'] == 1:
                    raise OSError("simulated failure")
            return real_replace(src, dst)

        with mock.patch('changedetectionio.store.config_transaction.os.replace',
                        side_effect=fail_once_on_watch_final):
            with self.assertRaises(ConfigPublishError):
                self.store.publish_watch_update(
                    self.uuid,
                    watch_updates={'title': 'DO NOT KEEP'},
                    processor_config={'restock_diff': {'in_stock_processing': 'off'}},
                )

        # The in-process immediate rollback restored memory as well - worker keeps
        # running the old complete configuration (read through the store, since
        # rollback replaces the live object).
        live = self.store.data['watching'][self.uuid]
        self.assertEqual(live['title'], 'Original')
        self.assertEqual(live[WATCH_CONFIG_VERSION_KEY], 0)
        self.assertEqual(self._read('watch.json')['title'], 'Original')
        self.assertEqual(self._debris(), [])


class TestStartupRecovery(unittest.TestCase):

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        os.makedirs(os.path.join(self.dir, 'watch-uuid'), exist_ok=True)
        self.data_dir = os.path.join(self.dir, 'watch-uuid')

    def _write(self, name, payload):
        with open(os.path.join(self.data_dir, name), 'w', encoding='utf-8') as f:
            json.dump(payload, f)

    def test_committed_marker_completes_the_publish(self):
        generation = 9
        # Old finals
        self._write('watch.json', {'title': 'old', WATCH_CONFIG_VERSION_KEY: generation - 1})
        self._write('restock_diff.json',
                    {'restock_diff': {'in_stock_processing': 'in_stock_only'},
                     PROCESSOR_CONFIG_VERSION_KEY: generation - 1})
        # Candidates that never got renamed (watch part missing its replace)
        cand_watch = os.path.basename(
            ctx._write_candidate(self.data_dir, 0, generation,
                                ctx._dumps({'title': 'new', WATCH_CONFIG_VERSION_KEY: generation})))
        cand_proc = os.path.basename(
            ctx._write_candidate(self.data_dir, 1, generation,
                                ctx._dumps({'restock_diff': {'in_stock_processing': 'all_changes'},
                                           PROCESSOR_CONFIG_VERSION_KEY: generation})))
        marker = {
            'generation': generation,
            'state': ctx.STATE_COMMITTED,
            'parts': [
                {'final': 'watch.json', 'candidate': cand_watch, 'backup': None,
                 'version_key': WATCH_CONFIG_VERSION_KEY},
                {'final': 'restock_diff.json', 'candidate': cand_proc, 'backup': None,
                 'version_key': PROCESSOR_CONFIG_VERSION_KEY},
            ],
        }
        marker_path = os.path.join(self.data_dir,
                                  _MARKER_TEMPLATE.format(generation=generation))
        with open(marker_path, 'w', encoding='utf-8') as f:
            json.dump(marker, f)

        ctx.recover_config_transaction(self.data_dir)

        with open(os.path.join(self.data_dir, 'watch.json'), 'r', encoding='utf-8') as f:
            self.assertEqual(json.load(f)['title'], 'new')
        with open(os.path.join(self.data_dir, 'restock_diff.json'), 'r', encoding='utf-8') as f:
            self.assertEqual(json.load(f)['restock_diff']['in_stock_processing'], 'all_changes')
        self.assertEqual([n for n in os.listdir(self.data_dir) if n.startswith(_TX_PREFIX)], [])

    def test_stray_candidates_without_marker_are_swept(self):
        stray = os.path.join(self.data_dir,
                            _CANDIDATE_TEMPLATE.format(index=0, generation=3))
        with open(stray, 'w', encoding='utf-8') as f:
            f.write('partial')
        self._write('watch.json', {'title': 'untouched'})

        ctx.recover_config_transaction(self.data_dir)

        self.assertFalse(os.path.exists(stray))
        with open(os.path.join(self.data_dir, 'watch.json'), 'r', encoding='utf-8') as f:
            self.assertEqual(json.load(f)['title'], 'untouched')


class TestReadFencing(JointPublishTestBase):

    def test_reader_gets_previous_generation_while_prepared(self):
        # Existing complete generation
        self.store.publish_watch_update(
            self.uuid,
            watch_updates={'title': 'old title'},
            processor_config={'restock_diff': {'in_stock_processing': 'in_stock_only'}},
        )

        # Hand-craft a PREPARED marker pointing to a new candidate + old backup
        generation = 2

        import shutil
        proc_final = os.path.join(self.data_dir, 'restock_diff.json')
        proc_backup = os.path.join(self.data_dir, '.cfgtx-b-1-2')
        shutil.copyfile(proc_final, proc_backup)
        cand = os.path.basename(ctx._write_candidate(
            self.data_dir, 1, generation,
            ctx._dumps({'restock_diff': {'in_stock_processing': 'off'},
                       PROCESSOR_CONFIG_VERSION_KEY: generation})))
        marker = {'generation': generation, 'state': ctx.STATE_PREPARED,
                  'parts': [{'final': 'restock_diff.json', 'candidate': cand,
                             'backup': os.path.basename(proc_backup),
                             'version_key': PROCESSOR_CONFIG_VERSION_KEY}]}
        with open(os.path.join(self.data_dir, _MARKER_TEMPLATE.format(generation=generation)),
                  'w', encoding='utf-8') as f:
            json.dump(marker, f)

        # Readers see the backup (old complete generation), not the candidate
        seen = self.store.read_watch_processor_config(self.uuid, 'restock_diff.json')
        self.assertEqual(seen['restock_diff']['in_stock_processing'], 'in_stock_only')


if __name__ == '__main__':
    unittest.main()
