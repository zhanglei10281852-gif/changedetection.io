"""
Joint, crash-safe publication of a watch's two configuration files.

A single watch edit can change BOTH:

  - ``watch.json``           - the main watch configuration
  - ``{processor}.json``     - the current processor's dedicated settings
                               (e.g. restock_diff.json threshold settings)

Writing one file and then the other as two independent "fire and forget" saves is
not atomic: if the second write fails (disk full, quota, truncation by a crash
mid-write) the entry point used to report success anyway, and after a restart the
worker ran with a new main config paired with OLD processor settings - or with a
truncated processor JSON that silently parsed as "no settings" (defaults).

This module implements a small two-phase commit on top of atomic file renames:

1. STAGE  - every participating file is written to a durable CANDIDATE temp file
            (fsynced + closed). If a previous final file exists, an exact BACKUP
            copy of it is parked next to it. Nothing visible changes.
2. PREPARE - a commit marker (``.cfgtx-m-<generation>.json``) is written last,
            describing candidates/backups and the generation. Readers can now
            tell a transaction is in flight.
3. PUBLISH - candidates are renamed over the final names with os.replace()
            (processor config first, watch.json last).
4. COMMIT  - the marker is flipped to state="committed", after which candidates
            and backups are deleted.

Crash / kill -9 recovery (see recover_config_transactions) is deterministic:

  - marker state="committed" -> finish publishing any part whose final file does
    not yet carry the marker generation, then clean up (recover the commit)
  - marker state="prepared"  -> every replaced final is restored from its
    backup, candidates/marker are deleted (roll back); finals that were never
    replaced simply stay untouched
  - no marker, stray files   -> pure staging debris, delete it

Readers never see a mixed generation while a "prepared" marker is present:
load_committed_json() serves the backup of the in-flight part, i.e. the last
complete configuration.

All files live inside the watch's own data directory, so every rename is
same-filesystem and atomic on POSIX and Windows.
"""

import glob
import json
import os
import shutil
import tempfile
import threading

from loguru import logger

try:
    import orjson

    def _dumps(data_dict):
        return orjson.dumps(data_dict, option=orjson.OPT_INDENT_2)

    def _loads(raw):
        return orjson.loads(raw)

except ImportError:
    def _dumps(data_dict):
        return json.dumps(data_dict, indent=2, ensure_ascii=False).encode('utf-8')

    def _loads(raw):
        return json.loads(raw.decode('utf-8'))

# Version of the joint configuration. Stored on the watch model (watch.json) and
# inside every committed processor config file (reserved, underscore-prefixed key).
WATCH_CONFIG_VERSION_KEY = 'config_version'
PROCESSOR_CONFIG_VERSION_KEY = '__config_version__'

STATE_PREPARED = 'prepared'
STATE_COMMITTED = 'committed'

# All transaction debris shares this prefix inside the watch data directory.
_TX_PREFIX = '.cfgtx-'
_MARKER_TEMPLATE = _TX_PREFIX + 'm-{generation}.json'
_CANDIDATE_TEMPLATE = _TX_PREFIX + 'c-{index}-{generation}'
_BACKUP_TEMPLATE = _TX_PREFIX + 'b-{index}-{generation}'

# Serializes transactions for the SAME watch across threads inside one process.
# Keyed by (datastore_path, watch_uuid); multi-process datastores are unsupported
# elsewhere in the application as well.
_locks_guard = threading.Lock()
_tx_locks: dict = {}


def watch_config_lock(datastore_path, watch_uuid):
    """Return the process-wide reentrant lock guarding one watch directory."""
    key = (os.path.realpath(datastore_path), str(watch_uuid))
    with _locks_guard:
        lock = _tx_locks.get(key)
        if lock is None:
            lock = threading.RLock()
            _tx_locks[key] = lock
        return lock


class ConfigTransactionError(Exception):
    """Base class for joint configuration publication failures."""


class StaleConfigVersionError(ConfigTransactionError):
    """The save was based on an older configuration generation.

    Raised when the caller-supplied expected version no longer matches the
    watch's current config_version - a concurrent newer save won.
    """

    def __init__(self, current_version, expected_version):
        self.current_version = current_version
        self.expected_version = expected_version
        super().__init__(
            f"Watch configuration changed since it was loaded "
            f"(current generation: {current_version}, submitted: {expected_version}). "
            f"Reload the watch and try again."
        )


class ConfigPublishError(ConfigTransactionError):
    """Staging or publishing one of the configuration files failed."""


# ----------------------------------------------------------------------------
# Low level file helpers
# ----------------------------------------------------------------------------

def _safe_part_name(filename):
    """Reject any filename that is not a bare file inside the watch directory."""
    if not filename or filename in ('.', '..'):
        return False
    if filename != os.path.basename(filename) or '/' in filename or '\\' in filename:
        return False
    return True


def _write_bytes_fsync(file_path, data):
    """Write bytes to an exact path, fsync, close. Caller chose the name."""
    with open(file_path, 'wb') as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())


def _copy_backup(final_path, backup_path):
    """Byte-copy an existing final file to its backup and fsync it."""
    with open(final_path, 'rb') as src:
        data = src.read()
    _write_bytes_fsync(backup_path, data)


def _write_candidate(data_dir, index, generation, payload):
    """Stage payload at a deterministic candidate path (same directory)."""
    candidate_path = os.path.join(
        data_dir, _CANDIDATE_TEMPLATE.format(index=index, generation=generation)
    )
    _write_bytes_fsync(candidate_path, payload)
    return candidate_path


def _find_markers(data_dir):
    """Return parsed marker dicts for every transaction marker in a dir."""
    markers = []
    for marker_path in sorted(glob.glob(os.path.join(data_dir, _TX_PREFIX + 'm-*.json'))):
        try:
            with open(marker_path, 'rb') as f:
                marker = _loads(f.read())
            if isinstance(marker, dict) and marker.get('generation') is not None:
                marker['__path'] = marker_path
                markers.append(marker)
        except (OSError, ValueError) as e:
            logger.warning(f"Unreadable config transaction marker {marker_path}: {e}")
    return markers


def _remove_quiet(path):
    if not path:
        return
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass
    except OSError as e:
        logger.warning(f"Could not remove transaction file {path}: {e}")


def _cleanup_parts(data_dir, parts, marker_path=None):
    for part in parts:
        for key in ('candidate', 'backup'):
            name = part.get(key)
            if name:
                _remove_quiet(os.path.join(data_dir, os.path.basename(name)))
    if marker_path:
        _remove_quiet(marker_path)


def _final_has_generation(data_dir, part, generation):
    """True when the part's final file parses and carries `generation`."""
    final_path = os.path.join(data_dir, part['final'])
    try:
        with open(final_path, 'rb') as f:
            data = _loads(f.read())
        return isinstance(data, dict) and data.get(part['version_key']) == generation
    except (OSError, ValueError):
        return False


def _finish_commit(data_dir, marker, marker_path):
    """Replace any final file that is not yet at the marker generation."""
    for part in marker.get('parts', []):
        candidate = part.get('candidate')
        if not candidate:
            continue
        candidate_path = os.path.join(data_dir, os.path.basename(candidate))
        final_path = os.path.join(data_dir, part['final'])
        if not _final_has_generation(data_dir, part, marker['generation']):
            os.replace(candidate_path, final_path)
    _cleanup_parts(data_dir, marker.get('parts', []), marker_path)


def _rollback(data_dir, marker, marker_path):
    """Restore every replaced final from its backup, then discard debris."""
    failed_parts = set()
    for index, part in enumerate(marker.get('parts', [])):
        backup = part.get('backup')
        if not backup:
            continue
        backup_path = os.path.join(data_dir, os.path.basename(backup))
        final_path = os.path.join(data_dir, part['final'])
        try:
            # The backup only exists for parts that changed; replacing is a no-op
            # byte-wise when the final file was never switched to its candidate.
            if os.path.exists(backup_path):
                os.replace(backup_path, final_path)
        except OSError as e:
            failed_parts.add(index)
            logger.error(
                f"Rollback failed for {part.get('final')} in {data_dir}: {e}; "
                f"backup retained at {backup_path}"
            )

    # Candidates always go. Backups go when their restore replace consumed them;
    # a backup for a part whose restore raised is retained for the next startup
    # attempt / manual inspection.
    for index, part in enumerate(marker.get('parts', [])):
        candidate = part.get('candidate')
        if candidate:
            _remove_quiet(os.path.join(data_dir, os.path.basename(candidate)))
        backup = part.get('backup')
        if backup and index not in failed_parts:
            _remove_quiet(os.path.join(data_dir, os.path.basename(backup)))
    _remove_quiet(marker_path)


def recover_config_transaction(data_dir):
    """Deterministically resolve one watch directory's interrupted transaction.

    Called during datastore startup for every watch directory. Safe to call
    repeatedly (idempotent).
    """
    if not os.path.isdir(data_dir):
        return

    markers = _find_markers(data_dir)
    for marker in markers:
        marker_path = marker['__path']
        generation = marker.get('generation')
        state = marker.get('state')
        try:
            if state == STATE_COMMITTED:
                logger.info(f"Recovering interrupted config commit (generation {generation}) in {data_dir}")
                _finish_commit(data_dir, marker, marker_path)
            else:
                logger.warning(
                    f"Rolling back incomplete config publication (generation {generation}) in {data_dir}"
                )
                _rollback(data_dir, marker, marker_path)
        except OSError as e:
            # Leave everything in place for the next startup attempt rather than
            # guessing - mixed generations must never be treated as success.
            logger.error(f"Config transaction recovery error in {data_dir}: {e}")
            return

    # No marker (crash before PREPARE, or after a completed cleanup): staging
    # debris is safe to delete, finals were never touched.
    for debris in glob.glob(os.path.join(data_dir, _TX_PREFIX + '*')):
        _remove_quiet(debris)


def recover_all_config_transactions(datastore_path):
    """Run recovery over every immediate subdirectory of the datastore."""
    if not datastore_path or not os.path.isdir(datastore_path):
        return
    try:
        entries = os.scandir(datastore_path)
    except FileNotFoundError:
        return
    with entries:
        for entry in entries:
            try:
                if entry.is_dir() and glob.glob(os.path.join(entry.path, _TX_PREFIX + '*')):
                    recover_config_transaction(entry.path)
            except OSError:
                continue


# ----------------------------------------------------------------------------
# Publication
# ----------------------------------------------------------------------------

def joint_publish(data_dir, parts, generation):
    """Publish several JSON config files as one configuration generation.

    Args:
        data_dir: watch data directory (all files live in it)
        parts: ordered list of dicts:
               {"final": "watch.json", "payload": dict,
                "version_key": "config_version", "max_size_mb": 10}
               The LAST part should be watch.json (published last).
        generation: integer generation embedded into every payload

    Raises:
        ConfigPublishError on any staging/rename failure after best-effort
        rollback. On raise, the on-disk finals hold the previous complete
        configuration (or are restored to it).
    """
    os.makedirs(data_dir, exist_ok=True)

    # Another interrupted transaction from a crashed process cannot be resolved
    # safely by guessing - finish/roll it back first.
    if _find_markers(data_dir):
        recover_config_transaction(data_dir)
        if _find_markers(data_dir):
            raise ConfigPublishError(
                f"An unresolved config transaction marker remains in {data_dir}; "
                f"refusing to publish generation {generation}"
            )

    staged = []
    marker_path = os.path.join(data_dir, _MARKER_TEMPLATE.format(generation=generation))
    try:
        # --- Phase 1: stage candidates + backups (nothing visible yet) ---
        for index, part in enumerate(parts):
            final_name = part['final']
            if not _safe_part_name(final_name):
                raise ConfigPublishError(f"Unsafe config file name: {final_name!r}")

            payload = dict(part['payload'])
            payload[part['version_key']] = generation
            data = _dumps(payload)

            max_size = part.get('max_size_mb', 1) * 1024 * 1024
            if len(data) > max_size:
                raise ConfigPublishError(
                    f"{final_name} data is unexpectedly large: "
                    f"{len(data) / 1024 / 1024:.2f}MB (max {part.get('max_size_mb', 1)}MB)"
                )

            final_path = os.path.join(data_dir, final_name)
            staged_part = {
                'final': final_name,
                'version_key': part['version_key'],
                'candidate': None,
                'backup': None,
            }

            # Park an exact copy of the previous final so PREPARED can roll back.
            if os.path.exists(final_path):
                backup_path = os.path.join(
                    data_dir, _BACKUP_TEMPLATE.format(index=index, generation=generation)
                )
                _copy_backup(final_path, backup_path)
                staged_part['backup'] = os.path.basename(backup_path)

            staged_part['candidate'] = os.path.basename(
                _write_candidate(data_dir, index, generation, data)
            )
            staged.append(staged_part)

        # --- Phase 2: commit marker makes the transaction discoverable ---
        marker = {
            'generation': generation,
            'state': STATE_PREPARED,
            'parts': [{k: v for k, v in p.items() if k in ('final', 'version_key', 'candidate', 'backup')}
                      for p in staged],
        }
        marker_tmp = marker_path + '.tmp'
        _write_bytes_fsync(marker_tmp, _dumps(marker))
        os.replace(marker_tmp, marker_path)

        # --- Phase 3: publish. Processor parts first, watch.json LAST. ---
        for part in staged:
            os.replace(
                os.path.join(data_dir, part['candidate']),
                os.path.join(data_dir, part['final'])
            )

        # --- Phase 4: declare committed and clean up ---
        marker['state'] = STATE_COMMITTED
        marker_tmp = marker_path + '.tmp'
        _write_bytes_fsync(marker_tmp, _dumps(marker))
        os.replace(marker_tmp, marker_path)
        _cleanup_parts(data_dir, staged, marker_path)

    except Exception as e:
        # Try to make the live on-disk state immediately consistent; startup
        # recovery remains the authoritative deterministic fallback.
        try:
            if os.path.exists(marker_path):
                _rollback(data_dir,
                         {'parts': staged, 'generation': generation},
                         marker_path)
            else:
                _cleanup_parts(data_dir, staged)
        except Exception as cleanup_error:
            logger.error(f"Error while rolling back failed config transaction in {data_dir}: {cleanup_error}")

        if isinstance(e, ConfigTransactionError):
            raise
        raise ConfigPublishError(f"Failed to jointly publish watch configuration: {e}") from e


# ----------------------------------------------------------------------------
# Transaction-aware reads
# ----------------------------------------------------------------------------

def load_committed_json(data_dir, filename):
    """Load a committed JSON config, hiding in-flight (prepared) generations.

    While a transaction is PREPARED but not yet COMMITTED, finals may be a mix
    of two generations. Serve that part's backup (the last complete version)
    instead. Returns {} when the file does not exist.

    Note: startup recovery runs before watches load, so under normal operation
    no marker is present; this only covers the microsecond publish window and
    external readers.
    """
    if not _safe_part_name(filename):
        logger.error(f"Refusing unsafe config file name: {filename!r}")
        return {}

    source_path = os.path.join(data_dir, filename)
    try:
        for marker in _find_markers(data_dir):
            if marker.get('state') != STATE_PREPARED:
                continue
            for part in marker.get('parts', []):
                if part.get('final') == filename and part.get('backup'):
                    backup_path = os.path.join(data_dir, os.path.basename(part['backup']))
                    if os.path.exists(backup_path):
                        source_path = backup_path
        if not os.path.isfile(source_path):
            return {}
        with open(source_path, 'rb') as f:
            data = _loads(f.read())
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError) as e:
        logger.warning(f"Failed to read committed watch config {filename}: {e}")
        return {}


def strip_transaction_keys(config_dict):
    """Drop reserved transaction metadata from a loaded processor config."""
    if not isinstance(config_dict, dict):
        return {}
    return {k: v for k, v in config_dict.items() if not k.startswith('__')}
