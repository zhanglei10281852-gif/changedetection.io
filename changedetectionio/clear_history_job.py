#!/usr/bin/env python3

"""
Persistent, resumable "clear history for ALL watches" background job.

Why this exists
---------------
The old implementation spawned a fire-and-forget daemon thread over an
in-memory snapshot of watch UUIDs. If the process exited mid-run the job was
simply gone: some watches had a fresh history boundary, some did not, there was
no way to tell which, and re-running cleared history (and therefore new first
snapshots) for watches that had already been done.

This manager persists job state to ``<datastore>/clear-all-history-job.json``:

* The UUID set is captured once when the job is created.
* Every watch is recorded as ``cleared`` / ``failed`` / ``gone`` and the file is
  fsynced after each watch, so a crash leaves an exact resume marker.
* Watches already marked ``cleared`` are NEVER touched again. A retry therefore
  cannot delete history created after their boundary.
* On startup a job left in ``running`` by a dead process is marked
  ``interrupted`` and can be resumed (it is auto-resumed by the app) instead of
  displaying "still running" forever.

The per-watch work goes through the same ChangeDetectionStore.clear_watch_history()
chokepoint as every other entry point, so the boundary semantics are identical.
"""

import json
import os
import tempfile
import threading
import time
import uuid as uuid_builder

from loguru import logger

JOB_FILENAME = 'clear-all-history-job.json'

STATUS_RUNNING = 'running'
STATUS_INTERRUPTED = 'interrupted'
STATUS_COMPLETED = 'completed'
STATUS_COMPLETED_WITH_ERRORS = 'completed_with_errors'

ITEM_PENDING = 'pending'
ITEM_CLEARED = 'cleared'
ITEM_FAILED = 'failed'
ITEM_GONE = 'gone'

_TERMINAL_STATUSES = (STATUS_COMPLETED, STATUS_COMPLETED_WITH_ERRORS)
# Items that count as "done" and must never be retried
_DONE_ITEM_STATUSES = (ITEM_CLEARED, ITEM_GONE)


class ClearAllHistoryJobManager:
    def __init__(self, datastore):
        self.datastore = datastore
        self.path = os.path.join(datastore.datastore_path, JOB_FILENAME)
        self._lock = threading.RLock()
        self._thread = None

    # ------------------------------------------------------------------ state
    def _load(self):
        """Load the persisted job state, or None when absent/unreadable."""
        if not os.path.isfile(self.path):
            return None
        try:
            with open(self.path, encoding='utf-8') as f:
                state = json.load(f)
            # Basic shape validation - a corrupt file must not wedge clearing forever
            if not isinstance(state, dict) or 'items' not in state:
                raise ValueError("job state has unexpected shape")
            return state
        except Exception as e:
            logger.error(f"Could not read clear-all-history job state {self.path}: {e}")
            return None

    def _save(self, state):
        """Atomic, durable state write (temp file + fsync + replace)."""
        os.makedirs(self.datastore.datastore_path, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(prefix='.clear-all-history-job.', suffix='.tmp',
                                        dir=self.datastore.datastore_path)
        try:
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                json.dump(state, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, self.path)
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

    @staticmethod
    def _counts(state):
        items = state.get('items', [])
        cleared = sum(1 for i in items if i.get('status') == ITEM_CLEARED)
        failed = sum(1 for i in items if i.get('status') == ITEM_FAILED)
        gone = sum(1 for i in items if i.get('status') == ITEM_GONE)
        pending = sum(1 for i in items if i.get('status') == ITEM_PENDING)
        return {'total': len(items), 'cleared': cleared, 'failed': failed,
                'gone': gone, 'pending': pending}

    def _public_state(self, state):
        """A snapshot safe to hand to the UI/API, with live thread info."""
        if state is None:
            return None
        out = dict(state)
        out.update(self._counts(state))
        with self._lock:
            out['thread_alive'] = bool(self._thread and self._thread.is_alive())
        return out

    def status(self):
        """Current/last job state for the status page/endpoint.

        Also self-heals a ``running`` file whose worker thread is dead (process
        restart) into ``interrupted``, so the UI can never show "running" for a
        job nobody is executing.
        """
        with self._lock:
            state = self._load()
            if state and state.get('status') == STATUS_RUNNING and not (
                    self._thread and self._thread.is_alive()):
                state = self._mark_interrupted(state)
            return self._public_state(state)

    def _mark_interrupted(self, state):
        if state.get('status') != STATUS_RUNNING:
            return state
        state['status'] = STATUS_INTERRUPTED
        state['updated_at'] = time.time()
        state['interrupted_note'] = 'No running worker found for this job (process restarted?)'
        try:
            self._save(state)
        except Exception as e:
            logger.error(f"Could not persist interrupted clear-all-history job state: {e}")
        return state

    def recover_interrupted(self):
        """Flip a stale ``running`` job to ``interrupted`` after a process restart.

        Returns the recovered state, or None when there was nothing to recover.
        """
        with self._lock:
            state = self._load()
            if state and state.get('status') == STATUS_RUNNING and not (
                    self._thread and self._thread.is_alive()):
                logger.warning("Found an interrupted 'clear all history' job from a previous process run")
                return self._public_state(self._mark_interrupted(state))
            return None

    # ------------------------------------------------------------------- run
    def start_or_resume(self):
        """Start a new job, or resume an interrupted/failed/completed-with-errors one.

        Returns ``(action, state)`` where action is one of:
        ``started``, ``resumed``, ``already_running``, ``nothing_to_resume``.
        A cleanly-completed job is treated as "start a fresh run" (an explicit
        new user request), snapshotting the current watch list again.
        """
        with self._lock:
            state = self._load()

            if state and state.get('status') == STATUS_RUNNING:
                if self._thread and self._thread.is_alive():
                    return 'already_running', self._public_state(state)
                state = self._mark_interrupted(state)

            if state and state.get('status') in (STATUS_INTERRUPTED, STATUS_COMPLETED_WITH_ERRORS):
                pending = [i for i in state.get('items', []) if i.get('status') == ITEM_PENDING
                           or i.get('status') == ITEM_FAILED]
                if pending:
                    # Re-arm before spawning: the worker only processes 'running'.
                    state['status'] = STATUS_RUNNING
                    state['updated_at'] = time.time()
                    state.pop('finished_at', None)
                    state.pop('interrupted_note', None)
                    self._save(state)
                    self._spawn(state)
                    return 'resumed', self._public_state(state)
                # Everything was actually done (e.g. failures were for watches
                # deleted meanwhile) - finalize and let a fresh run be created below.
                logger.info("Clear-all-history job has nothing left to retry; finalizing")

            # New job: snapshot the watch set ONCE at request time. Watches added
            # later are not part of this run; deleted watches are handled per-item.
            watch_uuids = list(self.datastore.data['watching'].keys())
            state = {
                'job_id': str(uuid_builder.uuid4()),
                'status': STATUS_RUNNING,
                'created_at': time.time(),
                'updated_at': time.time(),
                'finished_at': None,
                'convergence_timeout': float(os.getenv('CLEAR_HISTORY_CONVERGE_TIMEOUT', '30')),
                'items': [{'uuid': u, 'status': ITEM_PENDING, 'epoch': None, 'error': None}
                          for u in watch_uuids],
            }
            self._save(state)
            self._spawn(state)
            return 'started', self._public_state(state)

    def _spawn(self, state):
        job_id = state['job_id']
        self._thread = threading.Thread(
            target=self._run, args=(job_id,), daemon=True,
            name=f"ClearAllHistory-{job_id[:8]}"
        )
        self._thread.start()

    def _run(self, job_id):
        try:
            self._run_job(job_id)
        except Exception as e:
            logger.exception(f"Clear-all-history job {job_id} crashed: {e}")
            with self._lock:
                state = self._load()
                if state and state.get('job_id') == job_id and state.get('status') == STATUS_RUNNING:
                    self._mark_interrupted(state)

    def _run_job(self, job_id):
        while True:
            with self._lock:
                state = self._load()
                if not state or state.get('job_id') != job_id:
                    return
                if state.get('status') != STATUS_RUNNING:
                    return
                item = next((i for i in state['items']
                             if i.get('status') in (ITEM_PENDING, ITEM_FAILED)), None)
                if item is None:
                    # All items resolved - finalize exactly once.
                    counts = self._counts(state)
                    state['status'] = (STATUS_COMPLETED_WITH_ERRORS if counts['failed']
                                       else STATUS_COMPLETED)
                    state['finished_at'] = time.time()
                    state['updated_at'] = time.time()
                    self._save(state)
                    logger.info(
                        f"Clear-all-history job {job_id} finished: {counts['cleared']} cleared, "
                        f"{counts['gone']} gone, {counts['failed']} failed"
                    )
                    return

            item_status, item_epoch, error = self._clear_one_item(job_id, state, item)

            # Persist per watch BEFORE moving on - this is the resume marker.
            with self._lock:
                state = self._load()
                if not state or state.get('job_id') != job_id:
                    return
                target = next((i for i in state['items'] if i.get('uuid') == item['uuid']), None)
                if target is not None:
                    # A 'cleared'/'gone' result is final; never overwrite it with
                    # a failure from a duplicated worker pass.
                    if target.get('status') not in _DONE_ITEM_STATUSES or item_status in _DONE_ITEM_STATUSES:
                        target['status'] = item_status
                        target['epoch'] = item_epoch
                        target['error'] = error
                        target['finished_at'] = time.time()
                    state['updated_at'] = time.time()
                    try:
                        self._save(state)
                    except Exception as e:
                        logger.error(
                            f"Clear-all-history job {job_id}: state persist failed for {item['uuid']}: {e}"
                        )

    def _clear_one_item(self, job_id, state, item):
        """Run the clear for one pending/failed item.

        Returns (item_status, epoch, error_message).
        """
        uuid = item['uuid']
        logger.info(f"Clear-all-history job {job_id}: clearing {uuid}")
        try:
            watch = self.datastore.data['watching'].get(uuid)
            if watch is None:
                return ITEM_GONE, None, None

            result = self.datastore.clear_watch_history(
                uuid,
                convergence_timeout=state.get('convergence_timeout'),
            )
            problems = []
            if result.get('error'):
                problems.append(result['error'])
            if result.get('file_errors'):
                problems.extend(result['file_errors'])
            if not result.get('converged', True):
                problems.append('in-flight check did not converge before timeout; epoch guard applied')
            if problems:
                return ITEM_FAILED, result.get('epoch'), '; '.join(str(p) for p in problems)
            return ITEM_CLEARED, result.get('epoch'), None
        except Exception as e:
            logger.error(f"Clear-all-history job {job_id}: error clearing {uuid}: {e}")
            return ITEM_FAILED, item.get('epoch'), str(e)
