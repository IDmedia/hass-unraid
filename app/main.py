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
from typing import Any, Callable, Dict, List, Optional, Tuple

from .entity_tracker import EntityTracker
from .gql_http import GraphQLClient
from .legacy.base import LegacyChannel
from .legacy_ws import LegacyAuth, LegacyWSRunner
from .mqtt_pub import MQTTPublisher
from .smart_cache import SmartCache
from .utils import load_file, normalize_str, publish_latest, put_latest, setup_logger

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

    def __init__(self, http_base_url: str, verify_ssl: bool, auth: LegacyAuth, logger=None):
        self.http_base_url = http_base_url.rstrip('/')
        self.verify_ssl = verify_ssl
        self.auth = auth
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
        self._disabled_collectors: set = set(unraid_cfg.get('disabled_collectors') or [])

        self.logger.info(f'SSL mode: {"https/wss" if use_ssl else "http/ws"}, verify_ssl={self.verify_ssl}')

        self.gql = GraphQLClient(endpoint_http, api_key, verify_ssl=verify_ssl, timeout=max(15, self.scan_interval), logger=self.logger)

        unraid_id = normalize_str(self.name)
        self.smart_cache = SmartCache(unraid_id, DATA_DIR, self.logger)
        self.entity_tracker = EntityTracker(unraid_id, DATA_DIR, self.logger)
        # Collectors whose entities we keep. Modules that fail to import count too, so a transient
        # import problem never wipes their entities; disabled collectors are left out on purpose.
        self._entity_owners: set = set()

        self.legacy_auth = None
        self.legacy_ctx: Optional[LegacyHTTPContext] = None
        if self.username and self.password:
            self.legacy_auth = LegacyAuth(self.http_base, self.username, self.password, verify_ssl, self.logger)
            self.legacy_ctx = LegacyHTTPContext(self.http_base, self.verify_ssl, self.legacy_auth, logger=self.logger)

        self._mqtt_cfg = mqtt_config
        self.mqtt: Optional[MQTTPublisher] = None

        self.query_collectors: List[Any] = self._load_plugins('collectors', 'COLLECTOR', 'query collector', self._build_query_collector)
        self.subscription_collectors: List[Any] = self._load_plugins(
            'subscriptions', 'SUBSCRIPTION', 'subscription collector', lambda cls: cls(self.logger, self.scan_interval))
        self.legacy_channels: List[LegacyChannel] = []
        if self.legacy_auth:
            self.legacy_channels = self._load_plugins(
                'legacy', 'CHANNEL', 'legacy channel', lambda cls: cls(self.logger, self.scan_interval))
        else:
            self.logger.info('Legacy channels disabled (no username/password).')

        for collector in self.query_collectors + self.subscription_collectors + self.legacy_channels:
            for name, suffix in getattr(collector, 'static_entities', []):
                self.entity_tracker.expect(f'{unraid_id}_{MQTTPublisher.topic_name(name, suffix)}', collector.name)

        self._query_tasks: List[asyncio.Task] = []
        self._subscription_tasks: List[asyncio.Task] = []
        self._legacy_tasks: List[asyncio.Task] = []

        self._legacy_runner: Optional[LegacyWSRunner] = None
        self._stop_all = asyncio.Event()
        self._availability_task: Optional[asyncio.Task] = None

        self._started = False
        self._offline_logged = False
        self._session_refresh_task: Optional[asyncio.Task] = None

    def _load_plugins(self, package: str, attr: str, kind: str, build: Callable[[Any], Any]) -> List[Any]:
        """
        Import every module in app.<package> exposing <attr> (a class), skip disabled ones and instantiate
        the rest with build(cls). Modules that fail to load still count as entity owners, so a transient
        import problem never removes their entities.
        """
        instances: List[Any] = []
        pkg = importlib.import_module(f'app.{package}')
        for _, module_name, ispkg in pkgutil.iter_modules(pkg.__path__):
            if ispkg or module_name == 'base':
                continue
            try:
                cls = getattr(importlib.import_module(f'app.{package}.{module_name}'), attr, None)
                if cls is None:
                    continue
                if cls.name in self._disabled_collectors:
                    self.logger.info(f'Skipping disabled collector: {cls.name}')
                    continue
                instance = build(cls)
                instances.append(instance)
                self._entity_owners.add(instance.name)
                self.logger.info(f'Loaded {kind}: {instance.name}')
            except Exception as e:
                self._entity_owners.add(module_name)
                self.logger.error(f'Failed to load {kind} app.{package}.{module_name}: {e}')
        return instances

    def _build_query_collector(self, cls) -> Any:
        kwargs = {}
        if getattr(cls, 'requires_legacy_auth', False) and self.legacy_ctx:
            kwargs['legacy_ctx'] = self.legacy_ctx
        if getattr(cls, 'uses_smart_cache', False):
            kwargs['smart_cache'] = self.smart_cache
        return cls(self.gql, self.logger, self.scan_interval, **kwargs)

    async def start(self) -> None:
        if self._availability_task is None:
            self._availability_task = asyncio.create_task(self._availability_loop())

    async def aclose(self) -> None:
        self._stop_all.set()
        if self._started:
            await self._stop_runtime()
        await self.gql.aclose()

    PROBE_FAILURES_BEFORE_STOP = 3

    async def _availability_loop(self) -> None:
        backoff = 5
        failures = 0

        while not self._stop_all.is_set():
            available = await self._probe_unraid()
            # A single failed probe (API restart, timeout) must not tear everything down
            failures = 0 if available else failures + 1
            if not available and self._started and failures < self.PROBE_FAILURES_BEFORE_STOP:
                self.logger.info(f'Unraid API probe failed ({failures}/{self.PROBE_FAILURES_BEFORE_STOP}); keeping runtime')
                available = True
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
        while not self._stop_all.is_set():
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
            self.mqtt = MQTTPublisher(self.name, self._mqtt_cfg, self.loop, self.logger, self.scan_interval,
                                      tracker=self.entity_tracker, active_owners=self._entity_owners,
                                      fallback_owners={ch.name for ch in self.legacy_channels})

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
            self._legacy_runner.start_keepalive(self.scan_interval)
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
        if self._legacy_runner:
            await self._legacy_runner.aclose()
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
        self.entity_tracker.save()

        if self.legacy_auth:
            try:
                self.legacy_auth.invalidate()
            except Exception:
                pass

    def _publish_updates(self, collector, updates: List[Any]) -> None:
        """
        Publish a collector's updates. Collectors with tracks_removals=True report their complete set
        every time, so entities they stop reporting are marked unavailable (and removed after a week).
        """
        if not self.mqtt:
            return
        owner = collector.name
        ids = [self.mqtt.publish_update(upd, owner) for upd in updates]
        if getattr(collector, 'tracks_removals', False):
            self.mqtt.reconcile(owner, ids)

    async def _run_legacy_channel(self, channel: LegacyChannel) -> None:
        inactivity_timeout = getattr(channel, 'inactivity_timeout', None)
        await self._legacy_runner.run_channel(
            channel_name=channel.channel,
            parse_fn=channel.parse,
            handle_updates=lambda updates: self._publish_updates(channel, updates),
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
                self._publish_updates(collector, updates)
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
        name = getattr(collector, 'name', '?')
        interval = max(1, int(getattr(collector, 'interval', self.scan_interval)))
        backoff = 5

        async def publish_event(event: Dict[str, Any]):
            try:
                self._publish_updates(collector, await collector.parse(event) or [])
            except Exception as e:
                if not self._stop_all.is_set():
                    self.logger.error(f'Subscription collector "{name}" parse/publish error: {e}')

        while not self._stop_all.is_set():
            if not _GQL_WS_AVAILABLE:
                self.logger.error('gql websockets transport not available; cannot run subscription collector')
                return

            event_queue: asyncio.Queue = asyncio.Queue(maxsize=1)
            unsupported = False
            try:
                csrf = await self.gql.get_csrf_token() or ''
                ssl_ctx = None
                if self.ws_base.startswith('wss') and not self.verify_ssl:
                    ssl_ctx = ssl_mod._create_unverified_context()
                transport = WebsocketsTransport(url=urljoin(self.ws_base + '/', 'graphql'),
                                                init_payload={'x-csrf-token': csrf, 'x-api-key': self.api_key}, ssl=ssl_ctx)

                async with Client(transport=transport, fetch_schema_from_transport=False) as session:
                    pub_task = asyncio.create_task(publish_latest(event_queue, publish_event, interval, self._stop_all))
                    try:
                        async for event in session.subscribe(gql(collector.subscription_query)):
                            put_latest(event_queue, event)
                            if self._stop_all.is_set():
                                break
                    except Exception as e:
                        if 'Cannot query field' not in str(e):
                            raise
                        unsupported = True
                    finally:
                        pub_task.cancel()
                        await asyncio.gather(pub_task, return_exceptions=True)

                if unsupported:
                    # Older Unraid API without this subscription; legacy channels (if any) keep providing the data
                    self.logger.warning(f'Subscription collector "{name}" is not supported by this Unraid API version; '
                                        f'disabled until reconnect')
                    return
                if not self._stop_all.is_set():
                    self.logger.error(f'Subscription collector "{name}" ended; retrying in {backoff}s')
            except asyncio.CancelledError:
                break
            except Exception as e:
                if self._stop_all.is_set():
                    break
                self.logger.error(f'Subscription collector "{name}" failed: {e}; retrying in {backoff}s')

            if not self._stop_all.is_set():
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)


async def main() -> None:
    config = load_file(os.path.join(DATA_DIR, 'config.yaml')) or {}
    mqtt_cfg = config.get('mqtt') or {}
    unraid_nodes = config.get('unraid') or []

    if not unraid_nodes:
        print('No unraid nodes defined in /data/config.yaml')
        return

    loop = asyncio.get_running_loop()
    integrators: List[UnraidGraphQLIntegration] = []

    for node_cfg in unraid_nodes:
        try:
            integrators.append(UnraidGraphQLIntegration(mqtt_cfg, node_cfg, loop))
        except Exception as e:
            print(f'Failed to initialize node: {e}')

    await asyncio.gather(*(i.start() for i in integrators), return_exceptions=True)

    stop_event = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except (NotImplementedError, RuntimeError):
            # Windows: no loop signal handlers
            signal.signal(sig, lambda *_: loop.call_soon_threadsafe(stop_event.set))

    await stop_event.wait()

    try:
        await asyncio.wait_for(asyncio.gather(*(i.aclose() for i in integrators), return_exceptions=True), timeout=35)
    except asyncio.TimeoutError:
        pass


if __name__ == '__main__':
    asyncio.run(main())
