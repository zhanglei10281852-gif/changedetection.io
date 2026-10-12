import hashlib

from changedetectionio.browser_steps.browser_steps import browser_steps_get_valid_steps
from changedetectionio.content_fetchers.base import Fetcher
from changedetectionio.validate_url import validate_fetch_url_async
from copy import deepcopy
from abc import abstractmethod
import os
from urllib.parse import urlparse
from loguru import logger

SCREENSHOT_FORMAT_JPEG = 'JPEG'
SCREENSHOT_FORMAT_PNG = 'PNG'

class difference_detection_processor():
    browser_steps = None
    datastore = None
    fetcher = None
    screenshot = None
    watch = None
    xpath_data = None
    preferred_proxy = None
    screenshot_format = SCREENSHOT_FORMAT_JPEG
    last_raw_content_checksum = None
    worker_id = None

    def __init__(self, datastore, watch_uuid, worker_id=None):
        self.datastore = datastore
        self.watch_uuid = watch_uuid

        # Which async worker is driving this check, passed down to the fetcher in call_browser()
        # so it can keep per-worker browser state apart, None when we're not called from a worker
        self.worker_id = worker_id

        # Create a stable snapshot of the watch for processing
        # Why deepcopy?
        # 1. Prevents "dict changed during iteration" errors if watch is modified during processing
        # 2. Preserves Watch object with properties (.link, .is_pdf, etc.) - can't use dict()
        # 3. Safe now: Watch.__deepcopy__() shares datastore ref (no memory leak) but copies dict data
        self.watch = deepcopy(self.datastore.data['watching'].get(watch_uuid))

        # Generic fetcher that should be extended (requests, playwright etc)
        self.fetcher = Fetcher()

        # Load the last raw content checksum from file
        self.read_last_raw_content_checksum()

    def update_last_raw_content_checksum(self, checksum):
        """
        Save the raw content MD5 checksum to file.
        This is used for skip logic - avoid reprocessing if raw HTML unchanged.
        """
        if not checksum:
            return

        watch = self.datastore.data['watching'].get(self.watch_uuid)
        if not watch:
            return

        data_dir = watch.data_dir
        if not data_dir:
            return

        watch.ensure_data_dir_exists()
        checksum_file = os.path.join(data_dir, 'last-checksum.txt')

        try:
            with open(checksum_file, 'w', encoding='utf-8') as f:
                f.write(checksum)
            self.last_raw_content_checksum = checksum
        except IOError as e:
            logger.warning(f"Failed to write checksum file for {self.watch_uuid}: {e}")

    def read_last_raw_content_checksum(self):
        """
        Read the last raw content MD5 checksum from file.
        Returns None if file doesn't exist (first run) or can't be read.
        """
        watch = self.datastore.data['watching'].get(self.watch_uuid)
        if not watch:
            self.last_raw_content_checksum = None
            return

        data_dir = watch.data_dir
        if not data_dir:
            self.last_raw_content_checksum = None
            return

        checksum_file = os.path.join(data_dir, 'last-checksum.txt')

        if not os.path.isfile(checksum_file):
            self.last_raw_content_checksum = None
            return

        try:
            with open(checksum_file, 'r', encoding='utf-8') as f:
                self.last_raw_content_checksum = f.read().strip()
        except IOError as e:
            logger.warning(f"Failed to read checksum file for {self.watch_uuid}: {e}")
            self.last_raw_content_checksum = None

    async def validate_url_is_fetchable(self):
        """Pre-flight fetch gate for the regular check path (all fetchers, since they all come
        through call_browser()). The scheme/file:///private-IP rules live in
        validate_url.is_fetch_url_allowed() so that the fetch paths which do NOT come through
        here - the live Browser Steps UI, the Add Watch snapshot preview, and individual
        'Goto URL' browser steps - enforce exactly the same rules.
        """
        try:
            await validate_fetch_url_async(self.watch.link)
        except ValueError as e:
            # Re-raised as a plain Exception so it lands in the watch's last_error like every other
            # fetch failure, instead of looking like an internal type error.
            raise Exception(str(e)) from e

    def _consume_preloaded_fetch(self):
        """One-shot: if the Add Watch page parked a freshly-fetched snapshot for this
        watch (html + screenshot + xpath, see add_watch_ui/snapshot), populate self.fetcher
        from it instead of hitting the network. The marker file is deleted after use so
        every subsequent check fetches live.

        Returns True if a preload was consumed (caller should skip the network fetch).
        """
        import json, zlib

        data_dir = self.watch.data_dir
        if not data_dir:
            return False

        preload_path = os.path.join(data_dir, 'preload-fetch.json')
        if not os.path.isfile(preload_path):
            return False

        try:
            with open(preload_path, 'r', encoding='utf-8') as f:
                meta = json.load(f)
        except Exception as e:
            logger.warning(f"Could not read preloaded fetch for {self.watch.get('uuid')}: {e}")
            try:
                os.unlink(preload_path)
            except OSError:
                pass
            return False

        # Always delete first - this is one-shot regardless of what happens next.
        try:
            os.unlink(preload_path)
        except OSError:
            pass

        content = meta.get('content')
        self.fetcher.content = content
        self.fetcher.raw_content = content.encode('utf-8', errors='replace') if isinstance(content, str) else content
        self.fetcher.status_code = meta.get('status_code', 200)
        self.fetcher.headers = meta.get('headers') or {'content-type': 'text/html'}

        # Screenshot + xpath were migrated alongside in final on-disk format - reuse them.
        screenshot_path = os.path.join(data_dir, 'last-screenshot.png')
        if os.path.isfile(screenshot_path):
            with open(screenshot_path, 'rb') as f:
                self.fetcher.screenshot = f.read()

        elements_path = os.path.join(data_dir, 'elements.deflate')
        if os.path.isfile(elements_path):
            try:
                with open(elements_path, 'rb') as f:
                    self.fetcher.xpath_data = json.loads(zlib.decompress(f.read()))
            except Exception as e:
                logger.warning(f"Could not load preloaded xpath data for {self.watch.get('uuid')}: {e}")

        logger.info(f"Using preloaded Add-Watch snapshot for {self.watch.get('uuid')} - skipping network fetch")
        return True

    async def call_browser(self, preferred_proxy_id=None):

        from requests.structures import CaseInsensitiveDict

        url = self.watch.link

        # Scheme allowlist (file:// etc), parser-differential and private/reserved IP checks.
        await self.validate_url_is_fetchable()

        # Proxy ID "key"
        preferred_proxy_id = preferred_proxy_id if preferred_proxy_id else self.datastore.get_preferred_proxy_for_watch(
            uuid=self.watch.get('uuid'))

        # Resolve which content fetcher this watch should use. This is the single source of
        # truth (watch -> group -> system, extra_browser_/pdf/browser_steps overrides etc);
        # the resolved backend name is stamped onto the fetcher instance below.
        from changedetectionio.content_fetchers import resolve_content_fetcher
        fetcher_obj, prefer_fetch_backend, custom_browser_connection_url = resolve_content_fetcher(
            watch=self.watch, datastore=self.datastore)

        proxy_url = None
        if preferred_proxy_id:
            # Custom browser endpoints should NOT have a proxy added
            if not prefer_fetch_backend.startswith('extra_browser_'):
                proxy_url = self.datastore.proxy_list.get(preferred_proxy_id).get('url')
                logger.debug(f"Selected proxy key '{preferred_proxy_id}' as proxy URL '{proxy_url}' for {url}")
            else:
                logger.debug("Skipping adding proxy data when custom Browser endpoint is specified. ")

        logger.debug(f"Using proxy '{proxy_url}' for {self.watch['uuid']}")

        # Now call the fetcher (playwright/requests/etc) with arguments that only a fetcher would need.
        # When browser_connection_url is None, it method should default to working out whats the best defaults (os env vars etc)
        self.fetcher = fetcher_obj(proxy_override=proxy_url,
                                   custom_browser_connection_url=custom_browser_connection_url,
                                   screenshot_format=self.screenshot_format,
                                   worker_id=self.worker_id
                                   )

        # Stamp the resolved backend name so downstream consumers (processors, plugins)
        # can read it directly instead of re-deriving it from the fetcher class name.
        self.fetcher.backend_name = prefer_fetch_backend

        if self.watch.has_browser_steps:
            self.fetcher.browser_steps = browser_steps_get_valid_steps(self.watch.get('browser_steps', []))
            self.fetcher.browser_steps_screenshot_path = os.path.join(self.datastore.datastore_path, self.watch.get('uuid'))

        # Tweak the base config with the per-watch ones
        from changedetectionio.jinja2_custom import render as jinja_render
        request_headers = CaseInsensitiveDict()

        ua = self.datastore.data['settings']['requests'].get('default_ua')
        if ua and ua.get(prefer_fetch_backend):
            request_headers.update({'User-Agent': ua.get(prefer_fetch_backend)})

        request_headers.update(self.watch.get('headers', {}))
        request_headers.update(self.datastore.get_all_base_headers())
        request_headers.update(self.datastore.get_all_headers_in_textfile_for_watch(uuid=self.watch.get('uuid')))

        # https://github.com/psf/requests/issues/4525
        # Requests doesnt yet support brotli encoding, so don't put 'br' here, be totally sure that the user cannot
        # do this by accident.
        if 'Accept-Encoding' in request_headers and "br" in request_headers['Accept-Encoding']:
            request_headers['Accept-Encoding'] = request_headers['Accept-Encoding'].replace(', br', '')

        for header_name in request_headers:
            request_headers.update({header_name: jinja_render(template_str=request_headers.get(header_name))})

        timeout = self.datastore.data['settings']['requests'].get('timeout')

        request_body = self.watch.get('body')
        if request_body:
            request_body = jinja_render(template_str=self.watch.get('body'))

        request_method = self.watch.get('method')
        ignore_status_codes = self.watch.get('ignore_status_codes', False)

        # Configurable per-watch or global extra delay before extracting text (for webDriver types)
        system_webdriver_delay = self.datastore.data['settings']['application'].get('webdriver_delay', None)
        if self.watch.get('webdriver_delay'):
            self.fetcher.render_extract_delay = self.watch.get('webdriver_delay')
        elif system_webdriver_delay is not None:
            self.fetcher.render_extract_delay = system_webdriver_delay

        if self.watch.get('webdriver_js_execute_code') is not None and self.watch.get('webdriver_js_execute_code').strip():
            self.fetcher.webdriver_js_execute_code = self.watch.get('webdriver_js_execute_code')

        # Requests for PDF's, images etc should be passwd the is_binary flag
        is_binary = self.watch.is_pdf

        # And here we go! call the right browser with browser-specific settings
        empty_pages_are_a_change = self.datastore.data['settings']['application'].get('empty_pages_are_a_change', False)
        # All fetchers are now async
        await self.fetcher.run(
            current_include_filters=self.watch.get('include_filters'),
            empty_pages_are_a_change=empty_pages_are_a_change,
            fetch_favicon=self.watch.favicon_is_expired() and self.datastore.data['settings']['application'].get('ui', {}).get('favicons_enabled', True),
            ignore_status_codes=ignore_status_codes,
            is_binary=is_binary,
            request_body=request_body,
            request_headers=request_headers,
            request_method=request_method,
            screenshot_format=self.screenshot_format,
            timeout=timeout,
            url=url,
            watch_uuid=self.watch_uuid,
        )

        # @todo .quit here could go on close object, so we can run JS if change-detected
        await self.fetcher.quit(watch=self.watch)

        # Sanitize lone surrogates - these can appear when servers return malformed/mixed-encoding
        # content that gets decoded into surrogate characters (e.g. \udcad). Without this,
        # encode('utf-8') raises UnicodeEncodeError downstream in checksums, diffs, file writes, etc.
        # Covers all fetchers (requests, playwright, puppeteer, selenium) in one place.
        # Also note: By this point we SHOULD know the original encoding so it can safely convert to utf-8 for the rest of the app.
        # See: https://github.com/dgtlmoon/changedetection.io/issues/3952

        if self.fetcher.content and isinstance(self.fetcher.content, str):
            self.fetcher.content = self.fetcher.content.encode('utf-8', errors='replace').decode('utf-8')

        # After init, call run_changedetection() which will do the actual change-detection

    @staticmethod
    def _resolve_watch_config_path(data_dir, filename):
        """Resolve `filename` inside `data_dir`, refusing anything that escapes it.

        Security: callers derive `filename` from watch['processor'] (see
        processors/save_processor_config), and that value is not enum-validated on every
        write path - so it must be treated as untrusted here. os.path.join() will happily
        accept '../../../../tmp/pwned', which previously escaped the watch directory and
        allowed an arbitrary-path JSON file write (and read) as the app user.

        Returns the absolute path, or None if it is not safely contained.
        """
        import os

        if not filename or filename in ('.', '..'):
            logger.error(f"Refusing unsafe watch config filename {filename!r}")
            return None

        # Must be a bare filename - no directory component, no separator of either flavour
        if filename != os.path.basename(filename) or '/' in filename or '\\' in filename:
            logger.error(f"Refusing watch config filename with a path component: {filename!r}")
            return None

        # realpath both sides so a symlink planted inside data_dir cannot redirect the write
        base = os.path.realpath(data_dir)
        filepath = os.path.realpath(os.path.join(base, filename))
        if os.path.dirname(filepath) != base:
            logger.error(f"Refusing watch config path outside the watch directory: {filepath!r}")
            return None

        return filepath

    def get_extra_watch_config(self, filename):
        """
        Read processor-specific JSON config file from watch data directory.

        Goes through the joint-publication layer: while a save is mid-flight
        this returns the previous COMPLETE generation (from the transaction
        backup) rather than a possibly half-published file.

        Args:
            filename: Name of JSON file (e.g., "restock_diff.json")

        Returns:
            dict: Parsed JSON data, or empty dict if file doesn't exist
        """
        import os

        watch = self.datastore.data['watching'].get(self.watch_uuid)
        if not watch or not watch.data_dir:
            return {}

        filepath = self._resolve_watch_config_path(watch.data_dir, filename)
        if not filepath:
            return {}

        if not os.path.isfile(filepath):
            return {}

        # Transaction-aware: hides prepared (not yet committed) generations.
        datastore = getattr(self.datastore, 'read_watch_processor_config', None)
        if callable(datastore):
            return datastore(self.watch_uuid, filename)

        # Fallback for datastores that do not provide the joint-publish layer
        from changedetectionio.store.config_transaction import load_committed_json, strip_transaction_keys
        return strip_transaction_keys(load_committed_json(watch.data_dir, filename))

    def update_extra_watch_config(self, filename, data, merge=True):
        """
        Write processor-specific JSON config file to watch data directory.

        Atomic (temp file + os.replace): a crash can never leave a truncated
        JSON here that readers would silently fall back to defaults on.

        NOTE: Watch save entry points (edit page, REST API) must persist this
        TOGETHER with watch.json via datastore.publish_watch_update(); this
        standalone writer remains for processor-internal use.

        Args:
            filename: Name of JSON file (e.g., "visual_ssim_score.json")
            data: Dictionary to serialize as JSON
            merge: If True, merge with existing data; if False, overwrite completely
        """
        import os

        watch = self.datastore.data['watching'].get(self.watch_uuid)
        data_dir = watch.data_dir

        if not data_dir:
            logger.warning(f"Cannot save extra watch config {filename}: no data_dir")
            return

        # Ensure directory exists
        watch.ensure_data_dir_exists()

        filepath = self._resolve_watch_config_path(data_dir, filename)
        if not filepath:
            return

        # Read the last committed version through the transaction-aware reader,
        # so an in-flight joint publication cannot be merged into/overwritten.
        existing_data = {}
        if merge and os.path.isfile(filepath):
            existing_data = self.get_extra_watch_config(filename)

        if merge:
            existing_data.update(data)
            data_to_save = existing_data
        else:
            data_to_save = data

        # Atomic single-file publication
        from changedetectionio.store.file_saving_datastore import save_json_atomic
        save_json_atomic(filepath, data_to_save, label=f"processor config {filename}", max_size_mb=1)

    def get_raw_document_checksum(self):
        checksum = None

        if self.fetcher.content:
            checksum = hashlib.md5(self.fetcher.content.encode('utf-8')).hexdigest()

        return checksum

    @abstractmethod
    def run_changedetection(self, watch, force_reprocess=False):
        update_obj = {'last_notification_error': False, 'last_error': False}
        some_data = 'xxxxx'
        update_obj["previous_md5"] = hashlib.md5(some_data.encode('utf-8')).hexdigest()
        changed_detected = False
        return changed_detected, update_obj, ''.encode('utf-8')
