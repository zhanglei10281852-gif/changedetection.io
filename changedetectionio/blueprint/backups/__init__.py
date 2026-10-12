import datetime
import glob
import json
import os
import re
import secrets
import shutil
import threading
import time

from flask import Blueprint, abort, flash, redirect, render_template, send_from_directory, url_for
from flask_babel import gettext
from loguru import logger

from changedetectionio.flask_app import login_optionally_required
from changedetectionio.store import ChangeDetectionStore

BACKUP_FILENAME_FORMAT = "changedetection-backup-{}.zip"

# A backup is built inside a private candidate directory and only the finished
# zip is atomically renamed into the datastore root (os.replace). Anything left
# behind under this prefix (or carrying the old ".tmp" suffix) is therefore an
# unfinished candidate of a thread/process that died before publishing - it is
# never a valid download and is safe to remove.
CANDIDATE_DIR_PREFIX = ".backup-candidate-"
LEGACY_CANDIDATE_GLOB = "changedetection-backup-*.tmp"

# How often a per-watch "stable read" may fail (files changed while copied)
# before the whole backup attempt is abandoned instead of publishing a torn
# archive. Checks completing while we run simply land in the next archive.
MAX_BUILD_ATTEMPTS = 4

# history.txt for text_json_diff, history-{processor}.txt for the other processors
_HISTORY_INDEX_RE = re.compile(r'^history(-[A-Za-z0-9_-]+)?\.txt$')

# Scratch files the writers keep inside an entity directory while doing atomic
# saves (tempfile.mkstemp(prefix='json-', suffix='.tmp'), NamedTemporaryFile()
# which uses a "tmp..." name, any explicit *.tmp). They are never a committed
# state and must never enter an archive.
_SCRATCH_SUFFIX = '.tmp'
_SCRATCH_PREFIXES = ('tmp', 'json-')


class BackupBuildError(Exception):
    """The archive could not represent a consistent completed state."""


class BackupUnstableError(BackupBuildError):
    """Files changed (or vanished) while being copied - the attempt must be retried."""


def _is_scratch_file(name):
    return name.endswith(_SCRATCH_SUFFIX) or name.startswith(_SCRATCH_PREFIXES)


def _scan_entity_files(data_dir):
    """Return {filename: (inode, size, mtime_ns)} for the regular files of one entity dir.

    The fingerprint tuple lets a later scan prove that nothing in the directory
    was replaced, appended to, deleted or created while we read it - every
    writer here commits via os.replace() (new inode/mtime) or an in-place
    rewrite (size/mtime change), both of which move the fingerprint.
    """
    entries = {}
    try:
        with os.scandir(data_dir) as it:
            for de in it:
                name = de.name
                if _is_scratch_file(name):
                    continue
                fingerprint = _fingerprint_dir_entry(de, data_dir)
                if fingerprint is not None:
                    entries[name] = fingerprint
    except FileNotFoundError as e:
        raise BackupUnstableError(f"Entity directory {data_dir} disappeared while scanning") from e
    return entries


def _fingerprint_dir_entry(de, data_dir):
    """(inode, size, mtime_ns) for a regular file entry, None for other types."""
    try:
        # Snapshots/configs are always regular files; never follow links.
        if not de.is_file(follow_symlinks=False):
            return None
        st = de.stat(follow_symlinks=False)
    except FileNotFoundError as e:
        raise BackupUnstableError(f"{de.name!r} vanished while scanning {data_dir}") from e
    except OSError as e:
        raise BackupBuildError(f"Could not stat {de.name!r} in {data_dir}: {e}") from e
    return st.st_ino, st.st_size, st.st_mtime_ns


def _stable_archive_entity(zip_obj, data_dir, arc_prefix, config_filename,
                           compress_type, compresslevel):
    """Stream one entity directory into the zip only while it is quiescent.

    scan -> archive -> scan: if the two fingerprints differ, a check/trim/edit
    was committing while we read and any mixture of old/new files is possible,
    so the whole backup attempt is retried. Large files (screenshots, snapshot
    blobs) are streamed straight into the zip; only the small config/history
    files are buffered so they can be validated.

    Returns (config_meta_or_None, {filename: buffered bytes}, set_of_arcnames).
    Entities whose config commit has not landed yet (no watch.json/tag.json)
    return None config and are deferred to the next archive.
    """
    before = _scan_entity_files(data_dir)

    # Config missing means the entity commit has not landed - write nothing and
    # defer the whole entity to the next archive (a retry picks it up once the
    # watch.json/tag.json appears).
    if config_filename not in before:
        return None, {}, set()

    buffered = _archive_entity_files(
        zip_obj, data_dir, arc_prefix, before, config_filename, compress_type, compresslevel
    )
    after = _scan_entity_files(data_dir)
    if before != after:
        raise BackupUnstableError(f"{data_dir} changed while being copied")

    arcnames = {f"{arc_prefix}/{name}" for name in before}
    try:
        meta = json.loads(buffered[config_filename].decode('utf-8'))
    except (UnicodeDecodeError, ValueError) as e:
        raise BackupBuildError(f"{arc_prefix}/{config_filename} is not valid JSON: {e}") from e
    return meta, buffered, arcnames


def _archive_entity_files(zip_obj, data_dir, arc_prefix, manifest, config_filename,
                          compress_type, compresslevel):
    """Write all files of one entity (per its manifest) into the open zip."""
    buffered = {}
    for name in manifest:
        arcname = f"{arc_prefix}/{name}"
        source = os.path.join(data_dir, name)
        if name == config_filename or _HISTORY_INDEX_RE.match(name):
            buffered[name] = _read_small_file(source, data_dir)
            zip_obj.writestr(arcname, buffered[name],
                             compress_type=compress_type, compresslevel=compresslevel)
        else:
            _stream_large_file(zip_obj, source, arcname, data_dir, compress_type, compresslevel)
    return buffered


def _read_small_file(source, data_dir):
    try:
        with open(source, 'rb') as f:
            return f.read()
    except FileNotFoundError as e:
        raise BackupUnstableError(f"{os.path.basename(source)!r} disappeared while reading {data_dir}") from e
    except OSError as e:
        # A read error must never silently drop the file - fail the candidate.
        raise BackupBuildError(f"Could not read {os.path.basename(source)!r} from {data_dir}: {e}") from e


def _stream_large_file(zip_obj, source, arcname, data_dir, compress_type, compresslevel):
    try:
        zip_obj.write(source, arcname=arcname, compress_type=compress_type, compresslevel=compresslevel)
    except FileNotFoundError as e:
        raise BackupUnstableError(f"{os.path.basename(source)!r} disappeared while archiving {data_dir}") from e
    except OSError as e:
        raise BackupBuildError(f"Could not archive {os.path.basename(source)!r} from {data_dir}: {e}") from e


def _stable_read_single_file(filepath):
    """Read a root config file only while it is not being atomically replaced."""
    try:
        st1 = os.stat(filepath)
        with open(filepath, 'rb') as f:
            data = f.read()
        st2 = os.stat(filepath)
    except FileNotFoundError as e:
        raise BackupUnstableError(f"{filepath} disappeared while being read") from e
    except OSError as e:
        raise BackupBuildError(f"Could not read {filepath}: {e}") from e
    if (st1.st_ino, st1.st_size, st1.st_mtime_ns) != (st2.st_ino, st2.st_size, st2.st_mtime_ns):
        raise BackupUnstableError(f"{filepath} changed while being read")
    return data


def _parse_history_index(raw):
    """Parse a history index ('{epoch},{snapshot filename}' per line).

    Mirrors Watch.history parsing: basename() the referenced file, skip junk
    lines. Returns [(key, snapshot_basename), ...] in index order.
    """
    refs = []
    for line in raw.decode('utf-8', errors='replace').splitlines():
        line = line.strip()
        if not line or ',' not in line:
            continue
        key, value = line.split(',', 1)
        key = key.strip()
        snapshot = os.path.basename(value.strip())
        if key and snapshot:
            refs.append((key, snapshot))
    return refs


def _materialize_mapping(mapping):
    """Snapshot a live watches/tags dict to [(uuid, obj), ...].

    The passed-in dicts are mutated by worker/UI threads, so a plain
    list(mapping.items()) can raise mid-iteration; retry briefly instead of
    publishing an archive built from a half-seen set of entities.
    """
    if not mapping:
        return []
    last_error = None
    for _ in range(6):
        try:
            return list(mapping.items())
        except RuntimeError as e:
            # "dictionary changed size during iteration"
            last_error = e
            time.sleep(0.05)
    raise BackupBuildError(f"Entity set kept changing while taking a snapshot: {last_error}")


def cleanup_stale_backup_candidates(datastore_path):
    """Remove unfinished backup candidates left by a crashed thread/process.

    Finished backups are 'changedetection-backup-<digits>.zip' and are never
    touched here; only candidate directories (new scheme) and '*.tmp' files
    (older scheme) are removed. Safe to run at startup and before each backup.
    """
    base = os.path.abspath(datastore_path)
    removed = 0
    removed += _remove_legacy_candidates(base)

    try:
        names = os.listdir(base)
    except OSError as e:
        logger.error(f"Could not scan {base} for stale backup candidates: {e}")
        return removed

    for name in names:
        if name.startswith(CANDIDATE_DIR_PREFIX):
            removed += _remove_candidate(base, name)
    return removed


def _remove_legacy_candidates(base):
    removed = 0
    for legacy in glob.glob(os.path.join(base, LEGACY_CANDIDATE_GLOB)):
        try:
            os.unlink(legacy)
            removed += 1
            logger.warning(f"Removed unfinished backup candidate {legacy}")
        except OSError as e:
            logger.error(f"Could not remove stale backup candidate {legacy}: {e}")
    return removed


def _remove_candidate(base, name):
    candidate = os.path.realpath(os.path.join(base, name))
    if os.path.dirname(candidate) != base:
        return 0
    try:
        if os.path.isdir(candidate) and not os.path.islink(candidate):
            shutil.rmtree(candidate)
        else:
            os.unlink(candidate)
        logger.warning(f"Removed unfinished backup candidate {candidate}")
        return 1
    except OSError as e:
        logger.error(f"Could not remove stale backup candidate {candidate}: {e}")
        return 0


def _build_backup_attempt(datastore_path, watches, tags, final_zip_name, candidate_dir):
    """One attempt at building a complete, internally consistent archive.

    Raises BackupBuildError/BackupUnstableError on any problem; never publishes
    on failure. On success the finished zip is atomically renamed into place.
    """
    import zipfile

    candidate_zip = os.path.join(candidate_dir, final_zip_name)
    if os.path.exists(candidate_zip):
        os.unlink(candidate_zip)

    included_watches = []
    included_tag_uuids = set()
    expected_arcnames = set()

    with zipfile.ZipFile(candidate_zip, "w",
                         compression=zipfile.ZIP_DEFLATED,
                         compresslevel=8) as zip_obj:
        settings_data = _archive_root_settings(zip_obj, datastore_path, expected_arcnames)
        included_tag_uuids.update(
            _archive_groups(zip_obj, tags, expected_arcnames, zipfile.ZIP_DEFLATED, 8)
        )
        included_watches = _archive_watches(
            zip_obj, watches, expected_arcnames, zipfile.ZIP_DEFLATED, 8
        )

        _validate_references(settings_data, included_watches, included_tag_uuids)
        _write_and_archive_url_lists(zip_obj, datastore_path, included_watches, expected_arcnames)

    _verify_candidate_zip(candidate_zip, expected_arcnames)

    # Atomic publish - same filesystem, single rename. Only now does the
    # archive become visible for download/'latest'.
    final_path = os.path.join(datastore_path, final_zip_name)
    os.replace(candidate_zip, final_path)
    return final_path


def _archive_root_settings(zip_obj, datastore_path, expected_arcnames):
    """Archive changedetection.json (active settings) and legacy url-watches.json.

    Returns the parsed active settings dict (changedetection.json), or None.
    The legacy file is a frozen snapshot and is included but not cross-checked.
    """
    settings_data = None
    for root_name in ("changedetection.json", "url-watches.json"):
        root_path = os.path.join(datastore_path, root_name)
        if not os.path.isfile(root_path):
            continue
        data = _stable_read_single_file(root_path)
        try:
            parsed = json.loads(data.decode('utf-8'))
        except (UnicodeDecodeError, ValueError) as e:
            raise BackupBuildError(f"{root_name} is not valid JSON, refusing to back it up: {e}") from e
        if root_name == "changedetection.json":
            # The current writer always saves tags as {}; anything referenced
            # here must exist as an archived group (validated below).
            settings_data = parsed
        zip_obj.writestr(root_name, data, compress_type=zip_obj.compression, compresslevel=8)
        expected_arcnames.add(root_name)
    return settings_data


def _archive_groups(zip_obj, tags, expected_arcnames, compress_type, compresslevel):
    """Archive every committed {uuid}/ tag directory; returns included tag UUIDs."""
    included = set()
    for uuid, tag in _materialize_mapping(tags):
        data_dir = getattr(tag, 'data_dir', None)
        if not data_dir or not os.path.isdir(data_dir):
            # Deleted (or never committed) - simply absent from this boundary.
            continue
        tag_meta, _buffered, arcnames = _stable_archive_entity(
            zip_obj, data_dir, uuid, "tag.json", compress_type, compresslevel
        )
        if tag_meta is None:
            # tag.json commit has not landed yet; wait for the next archive.
            continue
        expected_arcnames.update(arcnames)
        included.add(uuid)
        title = tag_meta.get('title') if isinstance(tag_meta, dict) else uuid
        logger.debug(f"Added group '{title}' ({uuid}) to backup")
    return included


def _archive_watches(zip_obj, watches, expected_arcnames, compress_type, compresslevel):
    """Archive every committed {uuid}/ watch directory; returns watch records."""
    included = []
    for uuid, watch in _materialize_mapping(watches):
        data_dir = getattr(watch, 'data_dir', None)
        if not data_dir or not os.path.isdir(data_dir):
            # Deleted between the request and the copy - nothing to include.
            continue
        watch_meta, buffered, arcnames = _stable_archive_entity(
            zip_obj, data_dir, uuid, "watch.json", compress_type, compresslevel
        )
        if watch_meta is None:
            # add_watch() creates the directory and in-memory entry just before
            # committing watch.json; that watch belongs to the next archive,
            # never a half-created one.
            logger.debug(f"Watch {uuid} has no committed watch.json yet, deferring to next backup")
            continue
        _validate_watch_history(uuid, watch_meta, buffered, arcnames)
        expected_arcnames.update(arcnames)
        included.append((uuid, watch_meta, watch))
    return included


def _validate_watch_history(uuid, watch_meta, buffered, arcnames):
    """History indexes must reference snapshots in the same archive, and the
    index must belong to the same completed check as watch.json."""
    processor = watch_meta.get('processor') or 'text_json_diff'
    primary_index = 'history.txt' if processor == 'text_json_diff' else f'history-{processor}.txt'
    newest_history_key = None
    on_disk_names = {arc.split('/', 1)[1] for arc in arcnames}
    for name, raw in buffered.items():
        if not _HISTORY_INDEX_RE.match(name):
            continue
        refs = _parse_history_index(raw)
        for _key, snapshot_name in refs:
            if snapshot_name not in on_disk_names:
                raise BackupBuildError(
                    f"{uuid}/{name} references snapshot {snapshot_name!r} which is missing "
                    f"from the data directory (trimmed, half-written or raced) - refusing to "
                    f"publish an archive that cannot be restored"
                )
        if name == primary_index and refs:
            newest_history_key = refs[-1][0]

    # Whenever a snapshot was recorded, the check that recorded it also set
    # last_checked to its fetch timestamp (+1s on a same-second key collision).
    # A newer index paired with an older watch.json is exactly the torn commit
    # we must not publish.
    last_checked = watch_meta.get('last_checked')
    try:
        if newest_history_key is not None and last_checked and \
                int(newest_history_key) > int(last_checked) + 2:
            raise BackupBuildError(
                f"Watch {uuid} history index points at {newest_history_key} but "
                f"watch.json last_checked is {last_checked} - config and history come "
                f"from different check states"
            )
    except (TypeError, ValueError) as e:
        # Non-numeric values cannot be cross-checked; reference validation still applied.
        raise BackupBuildError(f"Watch {uuid} has non-numeric history/last_checked values") from e


def _validate_references(settings_data, included_watches, included_tag_uuids):
    """Settings, groups and watches may not reference objects outside the archive."""
    if isinstance(settings_data, dict):
        app_settings = (settings_data.get('settings', {}) or {}).get('application', {})
        settings_tags = app_settings.get('tags') or {}
        if isinstance(settings_tags, dict):
            for tag_uuid in settings_tags:
                if tag_uuid not in included_tag_uuids:
                    raise BackupBuildError(
                        f"changedetection.json references group {tag_uuid} which is not part of "
                        f"the archive"
                    )

    for uuid, watch_meta, _watch in included_watches:
        for tag_uuid in (watch_meta.get('tags') or []):
            if tag_uuid not in included_tag_uuids:
                raise BackupBuildError(
                    f"Watch {uuid} references group {tag_uuid} which is not part of the archive"
                )


def _write_and_archive_url_lists(zip_obj, datastore_path, included_watches, expected_arcnames):
    """Generate url-list files for exactly the watches included in the archive."""
    list_file = "url-list.txt"
    list_with_tags_file = "url-list-with-tags.txt"
    list_lines = []
    list_with_tags_lines = []
    for uuid, watch_meta, watch in included_watches:
        url, tags_value = _watch_url_and_tags(watch_meta, watch)
        list_lines.append(f"{url}\r\n")
        list_with_tags_lines.append(f"{url} {tags_value}\r\n")

    list_path = os.path.join(datastore_path, list_file)
    list_tags_path = os.path.join(datastore_path, list_with_tags_file)
    with open(list_path, "w", encoding="utf-8") as f:
        f.writelines(list_lines)
    with open(list_tags_path, "w", encoding="utf-8") as f:
        f.writelines(list_with_tags_lines)

    for list_name in (list_file, list_with_tags_file):
        zip_obj.write(os.path.join(datastore_path, list_name),
                      arcname=list_name,
                      compress_type=zip_obj.compression,
                      compresslevel=8)
        expected_arcnames.add(list_name)


def _watch_url_and_tags(watch_meta, watch):
    """URL/tags for the URL list - prefer the archived watch.json bytes."""
    if watch_meta and watch_meta.get('url'):
        return watch_meta['url'], watch_meta.get('tags', [])
    # Fallback for unusual layouts (watch.json missing url field)
    url = (watch.get('url') if hasattr(watch, 'get') else None) or watch['url']
    tags_value = watch.get('tags', []) if hasattr(watch, 'get') else []
    return url, tags_value


def _verify_candidate_zip(candidate_zip, expected_arcnames):
    """Reopen the finished zip and confirm every staged entry made it in."""
    import zipfile
    with zipfile.ZipFile(candidate_zip, 'r') as check_zip:
        archived = set(check_zip.namelist())
    missing = expected_arcnames - archived
    if missing:
        raise BackupBuildError(f"Backup archive is missing entries after writing: {sorted(missing)[:10]}")


def _unique_backup_name(datastore_path):
    """Backup filename whose published path does not exist yet (digits only)."""
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    name = BACKUP_FILENAME_FORMAT.format(timestamp)
    for _ in range(5):
        if not os.path.exists(os.path.join(datastore_path, name)):
            return name
        # Two backups in the same second would otherwise silently overwrite the
        # older completed one.
        time.sleep(1.05)
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        name = BACKUP_FILENAME_FORMAT.format(timestamp)
    raise BackupBuildError("Could not allocate a unique backup filename")


def create_backup(datastore_path, watches: dict, tags: dict = None):
    """Build and publish one backup zip representing a complete check boundary.

    Never publishes on failure: the candidate directory is removed and the
    exception is raised/logged, so requesting another backup works again.
    Checks that complete while the backup runs are deferred to the next archive.
    """
    logger.debug("Creating backup...")

    # A previous thread/process may have died mid-build; reclaim its candidates
    # (completed *.zip backups are left untouched).
    cleanup_stale_backup_candidates(datastore_path)

    final_zip_name = _unique_backup_name(datastore_path)
    candidate_dir = os.path.join(
        datastore_path,
        f"{CANDIDATE_DIR_PREFIX}{os.getpid()}-{secrets.token_hex(4)}"
    )
    os.makedirs(candidate_dir, exist_ok=False)

    try:
        last_error = _attempt_backup_builds(datastore_path, watches, tags, final_zip_name, candidate_dir)
        if last_error is not None:
            logger.error(f"Backup could not be made consistent after {MAX_BUILD_ATTEMPTS} attempts, "
                         f"no backup was published: {last_error}")
            raise BackupBuildError(f"Backup abandoned: {last_error}")
        return os.path.join(datastore_path, final_zip_name)
    finally:
        # Candidate directory (and any partial zip inside) must always be gone.
        shutil.rmtree(candidate_dir, ignore_errors=True)


def _attempt_backup_builds(datastore_path, watches, tags, final_zip_name, candidate_dir):
    last_error = None
    for attempt in range(1, MAX_BUILD_ATTEMPTS + 1):
        try:
            final_path = _build_backup_attempt(
                datastore_path, watches, tags or {}, final_zip_name, candidate_dir
            )
            logger.success(f"Backup published: {final_path}")
            return None
        except BackupUnstableError as e:
            last_error = e
            logger.warning(f"Backup attempt {attempt}/{MAX_BUILD_ATTEMPTS} saw a concurrent write "
                           f"({e}); retrying with a fresh snapshot")
            time.sleep(0.05 * attempt)
        except BackupBuildError as e:
            last_error = e
            logger.warning(f"Backup attempt {attempt}/{MAX_BUILD_ATTEMPTS} failed consistency "
                           f"checks ({e}); retrying")
            time.sleep(0.05 * attempt)
    return last_error


def run_backup_in_background(datastore_path, watches, tags=None):
    """Thread entry point: build a backup, never let an exception escape the thread.

    A failed backup has already cleaned its candidate; logging it here is enough
    (the next request simply starts a fresh, clean build).
    """
    try:
        create_backup(datastore_path, watches, tags=tags)
    except BackupBuildError as e:
        logger.error(f"Background backup failed and no backup was published: {e}")
    except Exception:
        logger.exception("Background backup failed with an unexpected error and no backup was published")


def find_backups(datastore_path):
    """Published backups (newest first) with filename, size in MB and ctime."""
    backup_filepath = os.path.join(datastore_path, BACKUP_FILENAME_FORMAT.format("*"))
    backup_info = []
    for backup in glob.glob(backup_filepath):
        size = os.path.getsize(backup) / (1024 * 1024)
        backup_info.append({
            'filename': os.path.basename(backup),
            'filesize': f"{size:.2f}",
            'creation_time': os.path.getctime(backup)
        })
    backup_info.sort(key=lambda x: x['creation_time'], reverse=True)
    return backup_info


def _serve_backup_download(datastore_path, filename):
    """Validate the requested filename (resolving 'latest') and send the zip."""
    filename = filename.strip()
    backup_filename_regex = BACKUP_FILENAME_FORMAT.format(r"\d+")

    # Resolve 'latest' before any validation so checks run against the real name.
    if filename == 'latest':
        backups = find_backups(datastore_path)
        if not backups:
            abort(404)
        filename = backups[0]['filename']

    if not re.match(r"^" + backup_filename_regex + "$", filename):
        abort(400)  # Bad Request if the filename doesn't match the pattern

    base = os.path.abspath(datastore_path)
    full_path = os.path.join(base, filename)
    if not full_path.startswith(base + os.sep):
        abort(404)

    logger.debug(f"Backup download request for '{full_path}'")
    return send_from_directory(base, filename, as_attachment=True)


def construct_blueprint(datastore: ChangeDetectionStore):
    from .restore import construct_restore_blueprint

    backups_blueprint = Blueprint('backups', __name__, template_folder="templates")
    backups_blueprint.register_blueprint(construct_restore_blueprint(datastore))
    backup_threads = []

    # Startup recovery: an interrupted process/thread can never finish its
    # candidate, so remove leftovers before anyone can request a new backup.
    # Already completed *.zip backups remain available for download/restore.
    _cleanup_at_startup(datastore)

    @backups_blueprint.route("/request-backup", methods=['POST'])
    @login_optionally_required
    def request_backup():
        backup_threads[:] = [t for t in backup_threads if t.is_alive()]
        if any(thread.is_alive() for thread in backup_threads):
            flash(gettext("A backup is already running, check back in a few minutes"), "error")
            return redirect(url_for('backups.create'))

        if len(find_backups(datastore.datastore_path)) > int(os.getenv("MAX_NUMBER_BACKUPS", 100)):
            flash(gettext("Maximum number of backups reached, please remove some"), "error")
            return redirect(url_for('backups.create'))

        # With immediate persistence, all data is already saved
        zip_thread = threading.Thread(
            target=run_backup_in_background,
            args=(datastore.datastore_path, datastore.data.get("watching")),
            kwargs={'tags': datastore.data['settings']['application'].get('tags', {})},
            daemon=True,
            name="BackupCreator"
        )
        zip_thread.start()
        backup_threads.append(zip_thread)
        flash(gettext("Backup building in background, check back in a few minutes."))

        return redirect(url_for('backups.create'))

    @backups_blueprint.route("/download/<string:filename>", methods=['GET'])
    @login_optionally_required
    def download_backup(filename):
        return _serve_backup_download(datastore.datastore_path, filename)

    @backups_blueprint.route("/", methods=['GET'])
    @backups_blueprint.route("/create", methods=['GET'])
    @login_optionally_required
    def create():
        backup_threads[:] = [t for t in backup_threads if t.is_alive()]
        return render_template(
            "backup_create.html",
            available_backups=find_backups(datastore.datastore_path),
            backup_running=any(thread.is_alive() for thread in backup_threads)
        )

    @backups_blueprint.route("/remove-backups", methods=['POST'])
    @login_optionally_required
    def remove_backups():
        pattern = os.path.join(datastore.datastore_path, BACKUP_FILENAME_FORMAT.format("*"))
        for backup in glob.glob(pattern):
            os.unlink(backup)

        flash(gettext("Backups were deleted."))

        return redirect(url_for('backups.create'))

    return backups_blueprint


def _cleanup_at_startup(datastore):
    try:
        removed = cleanup_stale_backup_candidates(datastore.datastore_path)
        if removed:
            logger.warning(f"Cleaned up {removed} incomplete backup candidate(s) from a previous run")
    except Exception as e:
        logger.error(f"Stale backup cleanup failed at startup: {e}")
