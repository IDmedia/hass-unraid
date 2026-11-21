import httpx
import asyncio
import ssl as ssl_mod
from hashlib import sha256
from urllib.parse import urljoin
from http.cookies import SimpleCookie
from typing import Any, Awaitable, Callable, List, Optional, Tuple
from app.collectors.base import IncompleteData
from app.utils import publish_latest, put_latest


def _resolve_ws_connect():
    """
    Return a websockets connect callable, trying modern and legacy entry points.
    """
    try:
        from websockets.client import connect as _connect
        return _connect
    except Exception:
        try:
            from websockets.legacy.client import connect as _connect
            return _connect
        except Exception:
            try:
                from websockets import connect as _connect
                return _connect
            except Exception:
                return None


_WS_CONNECT = _resolve_ws_connect()


def _is_login_response(r: httpx.Response) -> bool:
    """
    Detect if the Unraid GUI returned the login page, even with 200 status.
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


class LegacyAuth:
    def __init__(self, base_http_url: str, username: str, password: str, verify_ssl: bool, logger):
        self.base_http_url = base_http_url.rstrip('/')
        self.username = username
        self.password = password
        self.verify_ssl = verify_ssl
        self.logger = logger
        self._cookie: Optional[str] = None
        self._lock = asyncio.Lock()

    def invalidate(self) -> None:
        """Invalidate the cached cookie so the next call re-logins."""
        self._cookie = None

    async def get_cookie(self, force: bool = False) -> str:
        async with self._lock:
            if self._cookie and not force:
                if self.logger:
                    self.logger.debug(f'LegacyAuth: reuse cached cookie (hash={sha256(self._cookie.encode()).hexdigest()[:8]})')
                return self._cookie
            async with httpx.AsyncClient(
                verify=self.verify_ssl,
                timeout=30,
                follow_redirects=True,
                trust_env=False,
            ) as http:
                try:
                    r = await http.post(
                        urljoin(self.base_http_url + '/', 'login'),
                        data={'username': self.username, 'password': self.password},
                    )
                    jar_items = list(http.cookies.items())
                    if jar_items:
                        cookie_header = '; '.join([f'{k}={v}' for k, v in jar_items])
                        self._cookie = cookie_header
                        if self.logger:
                            self.logger.info(f'Legacy WS: obtained Unraid cookie from session jar (hash={sha256(self._cookie.encode()).hexdigest()[:8]})')
                        return self._cookie
                    cookies_list: List[str] = []
                    if hasattr(r.headers, 'get_list'):
                        cookies_list = r.headers.get_list('set-cookie')
                    else:
                        sc = r.headers.get('set-cookie')
                        if sc:
                            cookies_list = [sc]
                    if cookies_list:
                        simple = SimpleCookie()
                        for sc in cookies_list:
                            simple.load(sc)
                        pairs = [f'{m.key}={m.value}' for m in simple.values()]
                        cookie_header = '; '.join(pairs)
                        self._cookie = cookie_header
                        if self.logger:
                            self.logger.info(f'Legacy WS: obtained Unraid cookie from Set-Cookie headers (hash={sha256(self._cookie.encode()).hexdigest()[:8]})')
                        return self._cookie
                    raise RuntimeError('Login succeeded but no cookies were set')
                except Exception as e:
                    if self.logger:
                        self.logger.error(f'Legacy WS: login failed: {e}')
                    raise


class LegacyWSRunner:
    def __init__(self, base_ws_url: str, http_base_url: str, verify_ssl: bool, auth: LegacyAuth, logger):
        self.base_ws_url = base_ws_url.rstrip('/')
        self.http_base_url = http_base_url.rstrip('/')
        self.verify_ssl = verify_ssl
        self.auth = auth
        self.logger = logger
        # Limit concurrent prime+connect steps to avoid a thundering herd at startup
        self._connect_sem = asyncio.Semaphore(2)
        self._keepalive_task: Optional[asyncio.Task] = None

    async def _connect_ws_compat(self, url: str, subprotocols: List[str], headers: dict, ssl_ctx):
        if _WS_CONNECT is None:
            self.logger.error('websockets library not available; cannot open legacy WS channels')
            raise RuntimeError('websockets library not available')
        hdrs_list: List[Tuple[str, str]] = [(k, v) for k, v in headers.items()]
        try:
            return _WS_CONNECT(url, subprotocols=subprotocols, extra_headers=hdrs_list, ssl=ssl_ctx)
        except TypeError:
            return _WS_CONNECT(url, subprotocols=subprotocols, headers=hdrs_list, ssl=ssl_ctx)

    def _http(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(verify=self.verify_ssl, timeout=10, follow_redirects=True, trust_env=False)

    async def _touch_gui(self, http: httpx.AsyncClient, context: str) -> None:
        """
        GET /Main and /Dashboard so Unraid keeps its Nchan producers running.
        Re-login only when the server serves a login page or 401/403.
        """
        for page in ('Main', 'Dashboard'):
            url = urljoin(self.http_base_url + '/', page)
            headers = {'Cookie': await self.auth.get_cookie(), 'Origin': self.http_base_url, 'Referer': url}
            r = await http.get(url, headers=headers)
            if r.status_code in (401, 403) or _is_login_response(r):
                self.logger.info(f'Legacy WS {context}: auth expired on /{page}, refreshing cookie')
                headers['Cookie'] = await self.auth.get_cookie(force=True)
                await http.get(url, headers=headers)

    async def prime(self, context: str) -> None:
        """Touch the GUI once; used at connect and as a re-kicker when a channel goes silent."""
        try:
            async with self._http() as http:
                await self._touch_gui(http, context)
            self.logger.debug(f'Legacy WS {context}: primed /Main and /Dashboard')
        except Exception as e:
            self.logger.warning(f'Legacy WS {context}: prime failed: {e}')

    def start_keepalive(self, interval_seconds: int) -> None:
        """A single GUI keepalive per server keeps the producers alive for every channel."""
        if self._keepalive_task is None:
            self._keepalive_task = asyncio.create_task(self._keepalive_loop(max(15, int(interval_seconds))))

    async def _keepalive_loop(self, period: int) -> None:
        async with self._http() as http:
            while True:
                await asyncio.sleep(period)
                try:
                    await self._touch_gui(http, 'keepalive')
                except Exception as e:
                    self.logger.warning(f'Legacy WS keepalive error: {e}')

    async def aclose(self) -> None:
        if self._keepalive_task:
            self._keepalive_task.cancel()
            await asyncio.gather(self._keepalive_task, return_exceptions=True)
            self._keepalive_task = None

    async def run_channel(
        self,
        channel_name: str,
        parse_fn: Callable[[str], Awaitable[Any]],
        handle_updates: Callable,
        interval_seconds: int,
        stop_event: asyncio.Event,
        inactivity_timeout: Optional[int] = None,
        first_message_grace: Optional[int] = None,
        inactivity_strikes: int = 2,
    ):
        """
        Run a single Nchan WS channel with:
          - Cookie refresh and re-prime on auth loss (login page or 401/403)
          - WS ping keepalive (built into the websockets client, every 20s)
          - Inactivity detection (optional): if no messages for N seconds, re-prime and eventually reconnect

        Set inactivity_timeout <= 0 to disable inactivity restarts for sparse channels.
        first_message_grace (seconds) gives producers time on a fresh connect before we judge inactivity.
        inactivity_strikes defines how many inactivity windows we tolerate (re-priming between) before reconnecting.
        """
        backoff = 5
        inactivity_strikes = max(1, int(inactivity_strikes))
        context = f'"{channel_name}"'
        effective_inactivity = (
            inactivity_timeout if inactivity_timeout is not None else int(max(240, 6 * interval_seconds))
        )
        inactivity_disabled = effective_inactivity <= 0
        grace = int(first_message_grace) if first_message_grace is not None else int(max(180, 2 * effective_inactivity))

        async def publish_updates(msg_data: str):
            try:
                updates = await parse_fn(msg_data) or []
                handle_updates(updates)
            except IncompleteData as e:
                self.logger.debug(f'Legacy WS {context} message skipped: {e}')
            except Exception as e:
                self.logger.error(f'Legacy WS {context} publish failed: {e}')

        while not stop_event.is_set():
            local_stop = asyncio.Event()
            event_queue: asyncio.Queue = asyncio.Queue(maxsize=1)
            last_msg_ts = asyncio.get_event_loop().time()
            received_first = False
            strikes_used = 0

            async def receiver_loop(ws):
                nonlocal last_msg_ts, received_first, strikes_used
                while not (local_stop.is_set() or stop_event.is_set()):
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=15)
                    except asyncio.TimeoutError:
                        if inactivity_disabled:
                            continue
                        now = asyncio.get_event_loop().time()
                        threshold = grace if not received_first else effective_inactivity
                        if (now - last_msg_ts) >= threshold:
                            strikes_used += 1
                            if strikes_used < inactivity_strikes:
                                self.logger.warning(
                                    f'Legacy WS {context} inactivity window {strikes_used}/{inactivity_strikes}: '
                                    f'no messages for {int(now - last_msg_ts)}s (>= {threshold}s); re-priming GUI'
                                )
                                await self.prime(context)
                                last_msg_ts = asyncio.get_event_loop().time()
                                continue
                            self.logger.warning(
                                f'Legacy WS {context} inactivity: no messages for {int(now - last_msg_ts)}s '
                                f'(>= {threshold}s) after {strikes_used} window(s); reconnecting'
                            )
                            break
                        continue
                    except Exception as e:
                        self.logger.error(f'Legacy WS {context} recv error: {e}')
                        break
                    try:
                        msg_data = raw.replace('\00', ' ').split('\n\n', 1)[1]
                    except Exception:
                        msg_data = raw

                    last_msg_ts = asyncio.get_event_loop().time()
                    received_first = True
                    strikes_used = 0
                    put_latest(event_queue, msg_data)

            async def post_connect_kicker():
                """While waiting for the first message, give producers a nudge once or twice."""
                for delay in (15, 30):
                    await asyncio.sleep(delay)
                    if received_first or local_stop.is_set() or stop_event.is_set():
                        return
                    self.logger.debug(f'Legacy WS {context} first-message kicker')
                    await self.prime(context)

            try:
                # Limit concurrent prime+connect across channels
                async with self._connect_sem:
                    await self.prime(context)

                    # Give producers a brief moment to spin up before opening WS
                    await asyncio.sleep(5.0)

                    headers = {
                        'Cookie': await self.auth.get_cookie(),
                        'Origin': self.http_base_url,
                        'Referer': urljoin(self.http_base_url + '/', 'Dashboard'),
                    }
                    ssl_ctx = None
                    if self.base_ws_url.startswith('wss') and not self.verify_ssl:
                        ssl_ctx = ssl_mod._create_unverified_context()

                    url = urljoin(self.base_ws_url + '/', f'sub/{channel_name}')
                    if inactivity_disabled:
                        self.logger.info(
                            f'Legacy WS opening channel: {channel_name} (inactivity detection disabled, grace={grace}s)'
                        )
                    else:
                        self.logger.info(
                            f'Legacy WS opening channel: {channel_name} (inactivity_timeout={effective_inactivity}s, '
                            f'grace={grace}s, strikes={inactivity_strikes})'
                        )
                    connect_cm = await self._connect_ws_compat(url, ['ws+meta.nchan'], headers, ssl_ctx)

                # Outside the semaphore to allow others to progress while we process
                async with connect_cm as ws:
                    helpers = [
                        asyncio.create_task(publish_latest(event_queue, publish_updates, interval_seconds, stop_event)),
                        asyncio.create_task(post_connect_kicker()),
                    ]
                    try:
                        await receiver_loop(ws)
                    finally:
                        local_stop.set()
                        for t in helpers:
                            t.cancel()
                        await asyncio.gather(*helpers, return_exceptions=True)

                if not stop_event.is_set():
                    # Do not invalidate the cookie here; only re-login when auth actually fails.
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 60)
                    self.logger.warning(f'Legacy WS channel ended: {channel_name}, reconnecting...')
            except Exception as e:
                if not stop_event.is_set():
                    # Do not blindly invalidate; LegacyAuth handles re-login on demand.
                    self.logger.error(f'Legacy WS channel {context} failed: {e}')
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 60)
            finally:
                local_stop.set()
