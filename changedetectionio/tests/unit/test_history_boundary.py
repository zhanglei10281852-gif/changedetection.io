"""
Tests for the history-clear "boundary" semantics.

Every clear (single UI action, bulk list action, REST DELETE, processor switch,
global clear-all job) establishes a durable ``history_clear_epoch`` boundary:

* write-backs (snapshots, screenshots, fetched pages, error state) from a check
  claimed before the clear are discarded;
* queued change notifications from before the clear are never delivered;
* the first check produced by normal scheduling AFTER the clear is allowed to
  build the new first snapshot;
* the global job is persistent, resumable and never re-clears a watch that
  already got its boundary.

These tests build an isolated in-process datastore (no live server) so they run
identically on every platform.
"""
import json
import os
import shutil
import tempfile
import time
import unittest

from changedetectionio import worker_pool
from changedetectionio.model import Watch
from changedetectionio.queue_handlers import NotificationQueue
from changedetectionio.store import ChangeDetectionStore


class HistoryBoundaryTestBase(unittest.TestCase):
    def setUp(self):
        self.datastore_path = tempfile.mkdtemp()
        self.store = ChangeDetectionStore(
            datastore_path=self.datastore_path,
            include_default_watches=False,
        )

    def tearDown(self):
        self.store.stop_thread = True
        time.sleep(0.05)
        shutil.rmtree(self.datastore_path, ignore_errors=True)

    def _wait_for_job(self, manager, tries=200):
        for _ in range(tries):
            st = manager.status()
            if st['status'] in ('completed', 'completed_with_errors'):
                return st
            time.sleep(0.02)
        raise AssertionError(f"Job did not finish: {manager.status()}")


class TestWatchModelBoundary(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.mock_datastore = {'settings': {'application': {}}, 'watching': {}}

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _watch(self):
        return Watch.model(datastore_path=self.tmp,
                           __datastore=self.mock_datastore,
                           default={'url': 'https://example.com'})

    def test_stale_check_writes_are_dropped(self):
        watch = self._watch()
        watch.ensure_data_dir_exists()

        # A pre-clear check writes a snapshot
        watch.save_history_blob("v1", 1000, "snap1")
        self.assertEqual(len(watch.history), 1)

        result = watch.clear_watch(wait_for_in_flight=False)
        self.assertTrue(result['ok'])
        self.assertEqual(result['epoch'], 1)
        self.assertEqual(len(watch.history), 0)

        # Writes tagged with the claimed (old) epoch must not cross the boundary
        self.assertIsNone(watch.save_history_blob("old", 2000, "old", check_epoch=0))
        self.assertIsNone(watch.save_history_blob("old", 2001, "old", check_epoch=0))
        self.assertEqual(len(watch.history), 0)

        watch.save_screenshot(b"stale-png", check_epoch=0)
        self.assertFalse(os.path.isfile(os.path.join(watch.data_dir, "last-screenshot.png")))
        watch.save_error_text("stale error", check_epoch=0)
        self.assertFalse(os.path.isfile(os.path.join(watch.data_dir, "last-error.txt")))

        # The first post-boundary check (new epoch) IS allowed: new first snapshot
        watch.save_history_blob("new first", 3000, "new", check_epoch=1)
        self.assertEqual(len(watch.history), 1)
        # Untagged writes (UI/imports/tests) are always allowed
        watch.save_error_text("manual", check_epoch=None)
        self.assertTrue(os.path.isfile(os.path.join(watch.data_dir, "last-error.txt")))

    def test_repeated_clears_advance_epoch_and_preserve_config(self):
        watch = self._watch()
        watch['paused'] = True
        watch.ensure_data_dir_exists()
        cfg = os.path.join(watch.data_dir, "text_json_diff.json")
        with open(cfg, 'w') as f:
            f.write('{"include_filters": ["body"]}')
        watch.save_history_blob("v1", 1000, "s1")

        r1 = watch.clear_watch(wait_for_in_flight=False)
        r2 = watch.clear_watch(wait_for_in_flight=False)
        self.assertEqual((r1['epoch'], r2['epoch']), (1, 2))
        self.assertEqual(watch.history_epoch, 2)
        self.assertIs(watch.get('paused'), True)
        self.assertTrue(os.path.isfile(cfg))
        self.assertEqual(len(watch.history), 0)


class TestStoreBoundary(HistoryBoundaryTestBase):
    def test_clear_returns_diagnosable_result_and_gates_updates(self):
        uuid = self.store.add_watch(url="https://example.com")
        watch = self.store.data['watching'][uuid]
        watch.save_history_blob("v1", 1000, "s1")

        result = self.store.clear_watch_history(uuid, wait_for_in_flight=False)
        self.assertTrue(result['ok'])
        self.assertIsNone(result['error'])
        self.assertTrue(result['converged'])
        self.assertEqual(result['epoch'], 1)
        self.assertEqual(watch.history_n, 0)

        # A stale check's state write-back is dropped entirely
        self.store.update_watch(uuid, {'last_checked': 123, 'last_error': 'boom'}, check_epoch=0)
        self.assertEqual(int(watch['last_checked']), 0)
        self.assertFalse(watch['last_error'])

        # New generation update lands normally
        self.store.update_watch(uuid, {'last_checked': 456}, check_epoch=1)
        self.assertEqual(int(watch['last_checked']), 456)

        # Untagged update (UI) lands normally
        self.store.update_watch(uuid, {'page_title': 'hi'})
        self.assertEqual(watch['page_title'], 'hi')

        # The boundary survived on disk
        with open(os.path.join(watch.data_dir, "watch.json")) as f:
            self.assertEqual(json.load(f)['history_clear_epoch'], 1)

    def test_clear_while_check_is_running_reports_non_convergence(self):
        uuid = self.store.add_watch(url="https://example.com")
        self.assertTrue(worker_pool.claim_uuid_for_processing(uuid, "test-worker"))
        try:
            result = self.store.clear_watch_history(uuid, convergence_timeout=0.2)
        finally:
            worker_pool.release_uuid_from_processing(uuid, "test-worker")

        # Boundary still established safely via the epoch guard
        self.assertTrue(result['ok'])
        self.assertFalse(result['converged'])
        self.assertEqual(self.store.data['watching'][uuid].history_epoch, 1)


class TestNotificationBoundary(HistoryBoundaryTestBase):
    def test_notification_queue_drops_pre_boundary_items(self):
        uuid = self.store.add_watch(url="https://example.com")

        q = NotificationQueue()
        q.set_datastore(self.store)

        old = {'uuid': uuid, 'watch_history_epoch': 0}
        new = {'uuid': uuid, 'watch_history_epoch': 1}
        manual = {'uuid': uuid}  # no epoch tag: test/manual notification

        self.assertTrue(q.put(old))  # epoch 0 == current 0: accepted
        self.assertEqual(q.qsize(), 1)
        q.get()

        self.store.clear_watch_history(uuid, wait_for_in_flight=False)

        self.assertFalse(q.put(old))  # queued BEFORE the clear: dropped
        self.assertEqual(q.qsize(), 0)
        self.assertTrue(q.put(new))  # post-boundary: delivered
        self.assertTrue(q.put(manual))  # untagged: delivered

    def test_notification_service_stamps_and_filters_epoch(self):
        from changedetectionio.notification_service import NotificationService
        uuid = self.store.add_watch(url="https://example.com")
        self.store.update_watch(uuid, {'notification_urls': ['json://example']})

        q = NotificationQueue()
        q.set_datastore(self.store)
        svc = NotificationService(self.store, q)

        # A step-failure notification carrying the old epoch is rejected after clear
        self.store.clear_watch_history(uuid, wait_for_in_flight=False)
        self.assertFalse(svc.send_step_failure_notification(uuid, 1, check_epoch=0))
        self.assertEqual(q.qsize(), 0)

        # Current epoch is tagged on the queued item
        self.assertTrue(svc.send_step_failure_notification(uuid, 1, check_epoch=1))
        item = q.get()
        self.assertEqual(item['watch_history_epoch'], 1)


class TestWorkerArtifactHelpers(HistoryBoundaryTestBase):
    def test_cleanup_error_artifacts_respects_boundary(self):
        from changedetectionio.worker import cleanup_error_artifacts
        uuid = self.store.add_watch(url="https://example.com")
        watch = self.store.data['watching'][uuid]
        watch.ensure_data_dir_exists()
        err = os.path.join(watch.data_dir, "last-error.txt")

        self.store.clear_watch_history(uuid, wait_for_in_flight=False)
        # A post-boundary check produced an error file
        with open(err, 'w') as f:
            f.write("new boundary error")

        # A stale (pre-boundary) successful check must not delete the new file
        cleanup_error_artifacts(uuid, self.store, check_epoch=0)
        self.assertTrue(os.path.isfile(err))

        # A current-generation check may clean it
        cleanup_error_artifacts(uuid, self.store, check_epoch=1)
        self.assertFalse(os.path.isfile(err))

    def test_fetcher_check_generation_is_current(self):
        from changedetectionio.content_fetchers.base import Fetcher
        uuid = self.store.add_watch(url="https://example.com")

        f = Fetcher()
        # Interactive fetch: no epoch, always current
        self.assertTrue(f.check_generation_is_current())

        f.check_epoch = 0
        f.datastore_ref = self.store
        f.check_watch_uuid = uuid
        self.assertTrue(f.check_generation_is_current())
        self.store.clear_watch_history(uuid, wait_for_in_flight=False)
        self.assertFalse(f.check_generation_is_current())  # boundary moved
        f.check_epoch = 1
        self.assertTrue(f.check_generation_is_current())


class TestRestockWatchBoundary(HistoryBoundaryTestBase):
    def test_restock_clear_returns_boundary_and_resets_restock_state(self):
        uuid = self.store.add_watch(url="https://example.com",
                                    extras={'processor': 'restock_diff'})
        watch = self.store.data['watching'][uuid]
        from changedetectionio.processors.restock_diff import Watch as RestockWatch
        self.assertIsInstance(watch, RestockWatch)

        watch['restock']['in_stock'] = True
        watch.commit()
        watch.save_history_blob("price 100", 1000, "s1")

        result = self.store.clear_watch_history(uuid, wait_for_in_flight=False)
        # The override must keep the base boundary contract (diagnosable dict)
        self.assertTrue(result['ok'])
        self.assertEqual(result['epoch'], 1)
        self.assertEqual(watch.history_n, 0)
        self.assertEqual(watch.history_epoch, 1)
        # Restock runtime state is reset AND persisted
        self.assertFalse(watch['restock'].get('in_stock'))
        with open(os.path.join(watch.data_dir, "watch.json")) as f:
            on_disk = json.load(f)
        self.assertEqual(on_disk['history_clear_epoch'], 1)
        self.assertFalse(on_disk['restock'].get('in_stock'))


class TestGlobalClearHistoryJob(HistoryBoundaryTestBase):
    def test_global_job_clears_all_current_watches(self):
        u1 = self.store.add_watch(url="https://example.com")
        u2 = self.store.add_watch(url="https://example.org")
        for u in (u1, u2):
            self.store.data['watching'][u].save_history_blob("x", 1000, "s")

        manager = self.store.get_clear_history_job_manager()
        action, state = manager.start_or_resume()
        self.assertEqual(action, 'started')
        self.assertEqual(state['status'], 'running')

        st = self._wait_for_job(manager)
        self.assertEqual(st['status'], 'completed')
        self.assertEqual((st['total'], st['cleared'], st['failed']), (2, 2, 0))
        self.assertEqual(self.store.data['watching'][u1].history_epoch, 1)
        self.assertEqual(self.store.data['watching'][u2].history_epoch, 1)

        # A second explicit request starts a fresh run over the current watch set
        action, _ = manager.start_or_resume()
        self.assertEqual(action, 'started')
        st = self._wait_for_job(manager)
        self.assertEqual(st['status'], 'completed')
        self.assertEqual(self.store.data['watching'][u1].history_epoch, 2)

    def test_global_job_resumes_without_reclearing_done_watches(self):
        u1 = self.store.add_watch(url="https://example.com")
        u2 = self.store.add_watch(url="https://example.org")

        manager = self.store.get_clear_history_job_manager()
        # u1 was cleared at epoch 1 (boundary done); u2 never ran - simulate a crash
        self.store.data['watching'][u1]['history_clear_epoch'] = 1
        self.store.data['watching'][u1].commit()
        forced = {
            'job_id': '11111111-2222-3333-4444-555555555555',
            'status': 'running',
            'created_at': time.time() - 5,
            'updated_at': time.time() - 5,
            'finished_at': None,
            'convergence_timeout': 1.0,
            'items': [
                {'uuid': u1, 'status': 'cleared', 'epoch': 1, 'error': None},
                {'uuid': u2, 'status': 'pending', 'epoch': None, 'error': None},
            ],
        }
        with open(manager.path, 'w') as f:
            json.dump(forced, f)

        # A genuine first snapshot AFTER u1's boundary
        w1 = self.store.data['watching'][u1]
        w1.save_history_blob("post-boundary first", 9000, "post")

        recovered = manager.recover_interrupted()
        self.assertEqual(recovered['status'], 'interrupted')
        action, _ = manager.start_or_resume()
        self.assertEqual(action, 'resumed')

        st = self._wait_for_job(manager)
        self.assertEqual(st['status'], 'completed')
        items = {i['uuid']: i for i in st['items']}
        self.assertEqual(items[u2]['status'], 'cleared')
        # u1 must NOT have been re-cleared: epoch and new snapshot survive
        self.assertEqual(self.store.data['watching'][u1].history_epoch, 1)
        self.assertEqual(w1.history_n, 1)

    def test_stale_running_state_self_heals_and_gone_watches_finish_job(self):
        u1 = self.store.add_watch(url="https://example.com")
        manager = self.store.get_clear_history_job_manager()

        # Job file says running but no thread exists (process restarted), and the
        # pending watch has since been deleted.
        forced = {
            'job_id': 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee',
            'status': 'running',
            'created_at': time.time(), 'updated_at': time.time(), 'finished_at': None,
            'convergence_timeout': 1.0,
            'items': [{'uuid': u1, 'status': 'pending', 'epoch': None, 'error': None}],
        }
        with open(manager.path, 'w') as f:
            json.dump(forced, f)
        self.store.delete(u1)

        st = manager.status()
        self.assertEqual(st['status'], 'interrupted')  # never stuck showing "running"

        action, _ = manager.start_or_resume()
        self.assertEqual(action, 'resumed')
        st = self._wait_for_job(manager)
        self.assertEqual(st['status'], 'completed')
        self.assertEqual(st['gone'], 1)


if __name__ == '__main__':
    unittest.main()
