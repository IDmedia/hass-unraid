import os
import re
import httpx
import signal
import asyncio
import pkgutil
import importlib
import ssl as ssl_mod
from hashlib import sha256
from urllib.parse import urljoin
from typing import Any, Dict, List, Optional, Tuple

from .gql_http import GraphQLClient
from .legacy.base import LegacyChannel
from .legacy_ws import LegacyAuth, LegacyWSRunner
from .mqtt_pub import MQTTPublisher
from .smart_cache import SmartCache
from .utils import load_file, normalize_str, setup_logger

try:
    from gql import Client, gql  # type: ignore
    from gql.transport.websockets import WebsocketsTransport  # type: ignore
    _GQL_WS_AVAILABLE = True
except Exception:
    Client = None
    gql = None
    WebsocketsTransport = None
    _GQL_WS_AVAILABLE = False


DATA_DIR = '/data'


class LegacyHTTPContext:
    """
    Shared legacy HTTP session per node:
      - Caches Cookie and GUI CSRF token (scraped from /Dashboard)
      - Refreshes Cookie+CSRF on demand and retries once on 401/403 or login-page response
      - Provides http_get and http_post_form helpers that match Unraid UI
      - Supports a light CSRF refresh schedule without re-login
    """

    def __init__(self, http_base_url: str, verify_ssl: bool, auth: LegacyAuth, gql: GraphQLClient, smart_cache=None, logger=None):
        self.http_base_url = http_base_url.rstrip('/')
        self.verify_ssl = verify_ssl
        self.auth = auth
        self.gql = gql
        self.smart_cache = smart_cache
        self.logger = logger
        self._csrf: Optional[str] = None
        self._cookie_header: Optional[str] = None
        self._lock = asyncio.Lock()
        self._last_csrf_refresh_ts: float = 0.0
        self._csrf_refresh_interval: int = 600  # seconds (10 minutes), GET /Dashboard only

    async def _fetch_gui_csrf(self, cookie: str) -> str:
        """
        Fetch /Dashboard and scrape the GUI CSRF token from the page.
        Adds Referer/Origin and logs whether a token was found.
        """
        headers = {
            'Cookie': cookie,
            'Referer': urljoin(self.http_base_url + '/', 'Dashboard'),
            'Origin': self.http_base_url,
            'Accept': 'text/html,application/xhtml+xml',
            'User-Agent': 'unraid-integration/1.0 (+httpx)',
        }
        url = urljoin(self.http_base_url + '/', 'Dashboard')
        try:
            async with httpx.AsyncClient(
                verify=self.verify_ssl,
                timeout=15,
                follow_redirects=True,
                trust_env=False,
            ) as http:
                r = await http.get(url, headers=headers)
                text = r.text or ''
                # More permissive: capture anything until the next quote
                m = re.search(r'var\s+csrf_token\s*=\s*"([^"]+)"', text)
                if not m:
                    # Optional fallback: meta tag variant
                    m = re.search(
                        r'<meta[^>]+name=["\']csrf_token["\'][^>]+content=["\']([^"\']+)["\']',
                        text,
                        flags=re.IGNORECASE
                    )
                token = m.group(1) if m else ''
                if self.logger:
                    self.logger.debug(
                        'CSRF scrape: status=%s url=%s login=%s token_len=%d',
                        r.status_code, str(r.url), 'yes' if self._is_login_response(r) else 'no', len(token)
                    )
                return token
        except Exception as e:
            if self.logger:
                self.logger.debug('CSRF scrape error: %s', e)
            return ''

    async def refresh_session(self, force_login: bool = True) -> Tuple[str, str]:
        """
        Strong refresh (force_login=True) or light refresh (False).
        """
        async with self._lock:
            if force_login:
                try:
                    self.auth.invalidate()
                except Exception:
                    pass
            cookie = await self.auth.get_cookie(force=force_login)
            csrf = await self._fetch_gui_csrf(cookie) or ''
            self._cookie_header = cookie
            self._csrf = csrf
            self._last_csrf_refresh_ts = asyncio.get_event_loop().time()
            if self.logger:
                self.logger.debug('Legacy session refreshed (login=%s, csrf_len=%d)',
                                  'yes' if force_login else 'no', len(csrf))
            return cookie, csrf

    async def ensure_fresh_session(self, allow_relogin: bool = True) -> Tuple[str, str]:
        """
        Ensure cookie+CSRF are up-to-date:
          - If cookie changed since last use or CSRF stale, refresh CSRF from /Dashboard.
          - If CSRF still empty: re-login only if allow_relogin=True.
        """
        async with self._lock:
            current_cookie = await self.auth.get_cookie(force=False)
            now = asyncio.get_event_loop().time()
            cookie_changed = (self._cookie_header != current_cookie)
            csrf_stale = (now - self._last_csrf_refresh_ts) >= self._csrf_refresh_interval
            csrf_missing = not self._csrf

            if self.logger:
                def _hash(s: Optional[str]) -> str:
                    return sha256((s or '').encode()).hexdigest()[:8]
                self.logger.debug(
                    'ensure_fresh_session: cookie_changed=%s, csrf_stale=%s, csrf_missing=%s '
                    '(cookie_hash=%s, cached_cookie_hash=%s)',
                    cookie_changed, csrf_stale, csrf_missing, _hash(current_cookie), _hash(self._cookie_header)
                )

            if cookie_changed or csrf_stale or csrf_missing:
                csrf = await self._fetch_gui_csrf(current_cookie) or ''
                if not csrf and allow_relogin:
                    # As a last resort, force a re-login to scrape CSRF
                    current_cookie = await self.auth.get_cookie(force=True)
                    csrf = await self._fetch_gui_csrf(current_cookie) or ''
                    if self.logger:
                        self.logger.debug('ensure_fresh_session: forced re-login; csrf_len=%d', len(csrf))
                elif not csrf and self.logger and not allow_relogin:
                    self.logger.warning('ensure_fresh_session: CSRF scrape failed; preserving cookie (no re-login)')

                self._cookie_header = current_cookie
                self._csrf = csrf
                self._last_csrf_refresh_ts = now

            return self._cookie_header or current_cookie, self._csrf or ''

    async def get_session(self, force: bool = False) -> Tuple[str, str]:
        """
        Return (cookie, csrf_token).
        """
        if force:
            return await self.refresh_session(force_login=True)
        return await self.ensure_fresh_session(allow_relogin=True)

    @staticmethod
    def _is_login_response(r: httpx.Response) -> bool:
        """
        Detect if the response is the login page, even with 200 status.
        """
        try:
            if str(r.url).endswith('/login'):
                return True
            txt = r.text or ''
            if 'name="username"' in txt and 'name="password"' in txt:
                return True
        except Exception:
            pass
        return False

    async def http_get(self, path: str, *, params: Optional[dict] = None, timeout: int = 30, headers: Optional[dict] = None):
        """
        GET with Cookie (+ csrf_token param), and retry once on 401/403 or login-page response.
        """
        cookie, csrf = await self.get_session(force=False)
        hdrs = {'Cookie': cookie}
        if headers:
            hdrs.update(headers)
        p = dict(params or {})
        p.setdefault('csrf_token', csrf)
        url = urljoin(self.http_base_url + '/', path)
        async with httpx.AsyncClient(
            verify=self.verify_ssl,
            timeout=timeout,
            follow_redirects=True,
            trust_env=False,
        ) as http:
            r = await http.get(url, params=p, headers=hdrs)
            if r.status_code in (401, 403) or self._is_login_response(r):
                if self.logger:
                    self.logger.info(f'Legacy GET {path}: auth failed ({r.status_code}), retrying with strong refresh')
                cookie, csrf = await self.get_session(force=True)
                hdrs['Cookie'] = cookie
                p['csrf_token'] = csrf
                r = await http.get(url, params=p, headers=hdrs)
            return r

    async def http_post_form(self, path: str, *, form: Optional[dict] = None, timeout: int = 30, headers: Optional[dict] = None):
        """
        POST with URL-encoded form body. Retries on 401/403 or login-page response.
        """
        cookie, csrf = await self.get_session(force=False)
        hdrs = {
            'Cookie': cookie,
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'X-Requested-With': 'XMLHttpRequest',
        }
        if headers:
            hdrs.update(headers)
        from urllib.parse import urlencode
        form_data = dict(form or {})
        form_data.setdefault('csrf_token', csrf)
        body = urlencode(form_data, doseq=True)
        url = urljoin(self.http_base_url + '/', path)
        async with httpx.AsyncClient(
            verify=self.verify_ssl,
            timeout=timeout,
            follow_redirects=True,
            trust_env=False,
        ) as http:
            r = await http.post(url, content=body, headers=hdrs)
            if r.status_code in (401, 403) or self._is_login_response(r):
                if self.logger:
                    self.logger.info(f'Legacy POST {path}: auth failed ({r.status_code}), retrying with strong refresh')
                cookie, csrf = await self.get_session(force=True)
                hdrs['Cookie'] = cookie
                form_data['csrf_token'] = csrf
                body = urlencode(form_data, doseq=True)
                r = await http.post(url, content=body, headers=hdrs)
            return r


class UnraidGraphQLIntegration:
    """
    Per-node orchestrator with availability loop:
      - Probes GraphQL Vars
      - When available: connects MQTT, starts collectors and legacy WS channels
      - When unavailable: stops everything, logs one offline message, and retries with backoff
      - On online transition: refresh legacy Cookie+CSRF immediately and start a light CSRF refresher
    """

    def __init__(self, mqtt_config: Dict[str, Any], unraid_cfg: Dict[str, Any], loop: asyncio.AbstractEventLoop):
        self.loop = loop
        self.name = unraid_cfg.get('name', 'Unraid')
        self.logger = setup_logger(self.name)

        host = unraid_cfg.get('host')
        port = unraid_cfg.get('port', 80)
        use_ssl = unraid_cfg.get('ssl', False)
        verify_ssl = unraid_cfg.get('ssl_verify', True)
        api_key = unraid_cfg.get('api_key')
        if not api_key:
            raise ValueError(f'{self.name}: api_key is required in config')

        http_protocol = 'https' if use_ssl else 'http'
        ws_protocol = 'wss' if use_ssl else 'ws'

        self.http_base = f'{http_protocol}://{host}:{port}'
        self.ws_base = f'{ws_protocol}://{host}:{port}'
        endpoint_http = urljoin(self.http_base + '/', 'graphql')

        self.username = unraid_cfg.get('username')
        self.password = unraid_cfg.get('password')
        self.scan_interval = int(unraid_cfg.get('scan_interval', 30))
        self.api_key = api_key
        self.verify_ssl = verify_ssl

        self.logger.info(f'SSL mode: {"https/wss" if use_ssl else "http/ws"}, verify_ssl={self.verify_ssl}')

        self.gql = GraphQLClient(endpoint_http, api_key, verify_ssl=verify_ssl, timeout=max(15, self.scan_interval), logger=self.logger)

        unraid_id = normalize_str(self.name)
        self.smart_cache = SmartCache(unraid_id, DATA_DIR, self.logger)

        self.legacy_auth = None
        self.legacy_ctx: Optional[LegacyHTTPContext] = None
        if self.username and self.password:
            self.legacy_auth = LegacyAuth(self.http_base, self.username, self.password, verify_ssl, self.logger)
            self.legacy_ctx = LegacyHTTPContext(self.http_base, self.verify_ssl, self.legacy_auth, self.gql, smart_cache=self.smart_cache, logger=self.logger)

        self._mqtt_cfg = mqtt_config
        self.mqtt: Optional[MQTTPublisher] = None

        self.query_collectors: List[Any] = []
        self.subscription_collectors: List[Any] = []
        self.legacy_channels: List[LegacyChannel] = []

        self._load_query_collectors()
        self._load_subscription_collectors()
        self._load_legacy_channels()

        self._query_tasks: List[asyncio.Task] = []
        self._subscription_tasks: List[asyncio.Task] = []
        self._legacy_tasks: List[asyncio.Task] = []

        self._legacy_runner: Optional[LegacyWSRunner] = None
        self._stop_all = asyncio.Event()
        self._availability_task: Optional[asyncio.Task] = None

        self._started = False
        self._offline_logged = False
        self._session_refresh_task: Optional[asyncio.Task] = None

    def _load_query_collectors(self) -> None:
        loaded = 0
        try:
            import app.collectors as collectors_pkg
        except ImportError as e:
            self.logger.error(f'Collectors package not found: {e}')
            return

        for _, module_name, ispkg in pkgutil.iter_modules(collectors_pkg.__path__):
            if ispkg or module_name == 'base':
                continue

            full_module = f'{collectors_pkg.__name__}.{module_name}'
            try:
                mod = importlib.import_module(full_module)
                collector_cls = getattr(mod, 'COLLECTOR', None)
                if collector_cls is None:
                    continue

                requires_legacy = getattr(collector_cls, 'requires_legacy_auth', False)
                uses_smart = getattr(collector_cls, 'uses_smart_cache', False)
                kwargs = {}

                if requires_legacy and self.legacy_ctx:
                    kwargs['legacy_ctx'] = self.legacy_ctx
                if uses_smart:
                    kwargs['smart_cache'] = self.smart_cache

                instance = collector_cls(self.gql, self.logger, self.scan_interval, **kwargs)
                self.query_collectors.append(instance)
                loaded += 1
                self.logger.info(f'Loaded query collector: {instance.name}')
            except Exception as e:
                self.logger.error(f'Failed to load collector {full_module}: {e}')

        if loaded == 0:
            self.logger.warning('No query collectors loaded. Ensure app/collectors has modules and __init__.py.')

    def _load_subscription_collectors(self) -> None:
        loaded = 0
        try:
            import app.subscriptions as subs_pkg
        except ImportError:
            self.logger.info('No subscriptions package found (app/subscriptions). Skipping subscriptions.')
            return

        for _, module_name, ispkg in pkgutil.iter_modules(subs_pkg.__path__):
            if ispkg:
                continue

            full_module = f'{subs_pkg.__name__}.{module_name}'
            try:
                mod = importlib.import_module(full_module)
                subs_cls = getattr(mod, 'SUBSCRIPTION', None)
                if subs_cls is None:
                    continue

                instance = subs_cls(self.logger, self.scan_interval)
                self.subscription_collectors.append(instance)
                loaded += 1
                self.logger.info(f'Loaded subscription collector: {instance.name}')
            except Exception as e:
                self.logger.error(f'Failed to load subscription {full_module}: {e}')

        if loaded == 0:
            self.logger.info('No subscription collectors found.')

    def _load_legacy_channels(self) -> None:
        loaded = 0
        if not (self.username and self.password):
            self.logger.info('Legacy channels disabled (no username/password).')
            return

        try:
            import app.legacy as legacy_pkg
        except ImportError:
            self.logger.info('No legacy package found (app/legacy). Skipping legacy channels.')
            return

        for _, module_name, ispkg in pkgutil.iter_modules(legacy_pkg.__path__):
            if ispkg:
                continue

            full_module = f'{legacy_pkg.__name__}.{module_name}'
            try:
                mod = importlib.import_module(full_module)
                channel_cls = getattr(mod, 'CHANNEL', None)
                if channel_cls is None:
                    continue

                instance = channel_cls(self.logger, self.scan_interval)
                self.legacy_channels.append(instance)
                loaded += 1
                self.logger.info(f'Loaded legacy channel: {instance.name}')
            except Exception as e:
                self.logger.error(f'Failed to load legacy channel {full_module}: {e}')

        if loaded == 0:
            self.logger.info('No legacy channels found.')

    async def start(self) -> None:
        if self._availability_task is None:
            self._availability_task = asyncio.create_task(self._availability_loop())

    async def _availability_loop(self) -> None:
        backoff = 5

        while not self._stop_all.is_set():
            available = await self._probe_unraid()
            if available and not self._started:
                self._offline_logged = False
                self.logger.info('Unraid is available; starting runtime')
                try:
                    await self._start_runtime()
                    self._started = True
                    backoff = 5
                except Exception as e:
                    self.logger.error(f'Failed to start runtime: {e}')
                    self._started = False
            elif not available and self._started:
                self.logger.warning('Unraid unavailable; stopping collectors, legacy channels, and MQTT')
                try:
                    await self._stop_runtime()
                except Exception as e:
                    self.logger.error(f'Failed to stop runtime: {e}')
                self._started = False
                self._offline_logged = True
            elif not available and not self._started:
                if not self._offline_logged:
                    self.logger.warning('Unraid unavailable at start or currently offline; will retry with backoff')
                    self._offline_logged = True

            try:
                await asyncio.wait_for(self._stop_all.wait(), timeout=(self.scan_interval if self._started else backoff))
            except asyncio.TimeoutError:
                pass

            if not available and not self._started:
                backoff = min(backoff * 2, 60)

    async def _probe_unraid(self) -> bool:
        try:
            _ = await self.gql.get_version()
            return True
        except Exception:
            return False

    async def _fetch_version(self) -> None:
        try:
            version = await self.gql.get_version()
            if version and self.mqtt:
                self.logger.info(f'Unraid version detected: {version}')
                self.mqtt.set_device_overrides({'sw_version': version})
            elif not version:
                self.logger.warning('Unraid version not available from GraphQL Vars')
        except Exception as e:
            self.logger.error(f'Failed to fetch Unraid version: {e}')

    async def _session_refresher(self) -> None:
        """
        Light CSRF refresher while online. Never forces a re-login.
        """
        while self._started and not self._stop_all.is_set():
            try:
                if self.legacy_ctx:
                    await self.legacy_ctx.ensure_fresh_session(allow_relogin=False)
            except Exception as e:
                if self.logger:
                    self.logger.warning(f'Session refresher error: {e}')
            await asyncio.sleep(self.legacy_ctx._csrf_refresh_interval if self.legacy_ctx else 600)

    async def _start_runtime(self) -> None:
        if self.legacy_ctx:
            try:
                cookie, csrf = await self.legacy_ctx.refresh_session(force_login=True)
                if not csrf and self.logger:
                    self.logger.warning('Legacy session refreshed but CSRF token is empty; legacy HTTP may fail')
            except Exception as e:
                self.logger.error(f'Legacy session refresh failed: {e}')

        if not self.mqtt:
            self.mqtt = MQTTPublisher(self.name, self._mqtt_cfg, self.loop, self.logger, self.scan_interval)

        await self._fetch_version()

        self._query_tasks.clear()
        for collector in self.query_collectors:
            task = asyncio.create_task(self._run_query_collector(collector))
            self._query_tasks.append(task)

        self._subscription_tasks.clear()
        for collector in self.subscription_collectors:
            task = asyncio.create_task(self._run_subscription_collector(collector))
            self._subscription_tasks.append(task)

        if self.legacy_channels and self.legacy_auth:
            self._legacy_runner = LegacyWSRunner(
                base_ws_url=self.ws_base,
                http_base_url=self.http_base,
                verify_ssl=self.verify_ssl,
                auth=self.legacy_auth,
                logger=self.logger,
            )
            self._legacy_tasks.clear()
            for ch in self.legacy_channels:
                task = asyncio.create_task(self._run_legacy_channel(ch))
                self._legacy_tasks.append(task)

        if self._session_refresh_task:
            try:
                self._session_refresh_task.cancel()
            except Exception:
                pass
        self._session_refresh_task = asyncio.create_task(self._session_refresher())

    async def _stop_runtime(self) -> None:
        tasks = self._query_tasks + self._subscription_tasks + self._legacy_tasks
        for t in tasks:
            t.cancel()

        try:
            await asyncio.wait(tasks, timeout=2)
        except Exception:
            pass

        self._query_tasks.clear()
        self._subscription_tasks.clear()
        self._legacy_tasks.clear()
        self._legacy_runner = None

        if self._session_refresh_task:
            try:
                self._session_refresh_task.cancel()
            except Exception:
                pass
            self._session_refresh_task = None

        if self.mqtt:
            try:
                await self.mqtt.aclose()
            except Exception:
                pass
            self.mqtt = None

        if self.legacy_auth:
            try:
                self.legacy_auth.invalidate()
            except Exception:
                pass

    async def _run_legacy_channel(self, channel: LegacyChannel) -> None:
        publish_fn = self.mqtt.publish if self.mqtt else (lambda **_kwargs: None)
        inactivity_timeout = getattr(channel, 'inactivity_timeout', None)
        await self._legacy_runner.run_channel(
            channel_name=channel.channel,
            parse_fn=channel.parse,
            publish_fn=publish_fn,
            interval_seconds=max(1, int(getattr(channel, 'interval', self.scan_interval))),
            stop_event=self._stop_all,
            inactivity_timeout=inactivity_timeout,
        )

    async def _run_query_collector(self, collector) -> None:
        interval = max(1, int(getattr(collector, 'interval', self.scan_interval)))
        while not self._stop_all.is_set():
            try:
                data = await collector.fetch()
                updates: List[Any] = await collector.parse(data) or []
                if self.mqtt:
                    for upd in updates:
                        self.mqtt.publish(
                            payload=upd.payload,
                            sensor_type=upd.sensor_type,
                            state_value=upd.state,
                            json_attributes=upd.attributes,
                            retain=upd.retain,
                            device_overrides=upd.device_overrides,
                            unique_id_suffix=upd.unique_id_suffix,
                            expire_after=upd.expire_after,
                        )
            except asyncio.CancelledError:
                break
            except Exception as e:
                if self.logger:
                    self.logger.warning(f'Query collector "{getattr(collector, "name", "?")}" error: {e}')

            try:
                await asyncio.wait_for(self._stop_all.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass

    async def _run_subscription_collector(self, collector) -> None:
        base_interval = max(1, int(getattr(collector, 'interval', self.scan_interval)))
        backoff = 5

        while not self._stop_all.is_set():
            if not _GQL_WS_AVAILABLE:
                self.logger.error('gql websockets transport not available; cannot run subscription collector')
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)
                continue

            stop_local = asyncio.Event()
            event_queue: asyncio.Queue = asyncio.Queue(maxsize=1)

            async def publish_event(event: Dict[str, Any]):
                try:
                    updates = await collector.parse(event) or []
                    if self.mqtt:
                        for upd in updates:
                            self.mqtt.publish(
                                payload=upd.payload,
                                sensor_type=upd.sensor_type,
                                state_value=upd.state,
                                json_attributes=upd.attributes,
                                retain=upd.retain,
                                device_overrides=upd.device_overrides,
                                unique_id_suffix=upd.unique_id_suffix,
                                expire_after=upd.expire_after,
                            )
                except Exception as e:
                    if not self._stop_all.is_set():
                        self.logger.error(f'Subscription collector "{getattr(collector, "name", "?")}" parse/publish error: {e}')

            async def receiver_loop(session):
                try:
                    async for event in session.subscribe(gql(collector.subscription_query)):
                        try:
                            while True:
                                event_queue.get_nowait()
                                event_queue.task_done()
                        except asyncio.QueueEmpty:
                            pass
                        await event_queue.put(event)
                        if stop_local.is_set() or self._stop_all.is_set():
                            break
                except Exception as e:
                    if not (stop_local.is_set() or self._stop_all.is_set()):
                        self.logger.error(f'Subscription collector "{getattr(collector, "name", "?")}" receiver failed: {e}')

            async def publisher_loop():
                while event_queue.empty() and not (stop_local.is_set() or self._stop_all.is_set()):
                    await asyncio.sleep(0.05)
                if stop_local.is_set() or self._stop_all.is_set():
                    return

                first_event = await event_queue.get()
                await publish_event(first_event)
                event_queue.task_done()

                while not (stop_local.is_set() or self._stop_all.is_set()):
                    try:
                        await asyncio.wait_for(self._stop_all.wait(), timeout=base_interval)
                        if self._stop_all.is_set():
                            break
                    except asyncio.TimeoutError:
                        pass

                    latest = None
                    try:
                        while True:
                            latest = event_queue.get_nowait()
                            event_queue.task_done()
                    except asyncio.QueueEmpty:
                        pass

                    if latest is not None:
                        await publish_event(latest)

            try:
                csrf = await self.gql.get_csrf_token() or ''
                init_payload = {'x-csrf-token': csrf, 'x-api-key': self.api_key}

                ssl_ctx = None
                if self.ws_base.startswith('wss') and not self.verify_ssl:
                    ssl_ctx = ssl_mod._create_unverified_context()

                transport = WebsocketsTransport(url=urljoin(self.ws_base + '/', 'graphql'),
                                                init_payload=init_payload, ssl=ssl_ctx)
                client = Client(transport=transport, fetch_schema_from_transport=False)

                async with client as session:
                    recv_task = asyncio.create_task(receiver_loop(session))
                    pub_task = asyncio.create_task(publisher_loop())

                    await recv_task
                    stop_local.set()
                    try:
                        await asyncio.wait_for(pub_task, timeout=2)
                    except asyncio.TimeoutError:
                        pub_task.cancel()

                if not self._stop_all.is_set():
                    self.logger.error(f'Subscription collector "{getattr(collector, "name", "?")}" ended; retrying in {backoff}s')
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 60)
            except asyncio.CancelledError:
                break
            except Exception as e:
                if not self._stop_all.is_set():
                    self.logger.error(f'Subscription collector "{getattr(collector, "name", "?")}" failed: {e}; retrying in {backoff}s')
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 60)
            finally:
                stop_local.set()


async def main() -> None:
    config = load_file(os.path.join(DATA_DIR, 'config.yaml')) or {}
    mqtt_cfg = config.get('mqtt') or {}
    unraid_nodes = config.get('unraid') or []

    if not unraid_nodes:
        print('No unraid nodes defined in /data/config.yaml')
        return

    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    loop = asyncio.get_event_loop()
    integrators: List[UnraidGraphQLIntegration] = []

    for node_cfg in unraid_nodes:
        try:
            integrators.append(UnraidGraphQLIntegration(mqtt_cfg, node_cfg, loop))
        except Exception as e:
            print(f'Failed to initialize node: {e}')

    await asyncio.gather(*(i.start() for i in integrators), return_exceptions=True)

    stop_event = asyncio.Event()

    def trigger_stop(*_):
        stop_event.set()

    try:
        signal.signal(signal.SIGINT, trigger_stop)
    except Exception:
        pass
    try:
        signal.signal(signal.SIGTERM, trigger_stop)
    except Exception:
        pass

    await stop_event.wait()
    # Integrations stop via task cancellation on container stop.


if __name__ == '__main__':
    asyncio.run(main())
