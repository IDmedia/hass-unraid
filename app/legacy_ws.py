import httpx
import asyncio
import ssl as ssl_mod
from hashlib import sha256
from urllib.parse import urljoin
from http.cookies import SimpleCookie
from typing import Callable, List, Optional, Tuple


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

    async def _connect_ws_compat(self, url: str, subprotocols: List[str], headers: dict, ssl_ctx):
        if _WS_CONNECT is None:
            self.logger.error('websockets library not available; cannot open legacy WS channels')
            raise RuntimeError('websockets library not available')
        hdrs_list: List[Tuple[str, str]] = [(k, v) for k, v in headers.items()]
        try:
            return _WS_CONNECT(url, subprotocols=subprotocols, extra_headers=hdrs_list, ssl=ssl_ctx)
        except TypeError:
            return _WS_CONNECT(url, subprotocols=subprotocols, headers=hdrs_list, ssl=ssl_ctx)

    async def run_channel(
        self,
        channel_name: str,
        parse_fn: Callable[[str], asyncio.Future],
        publish_fn: Callable,
        interval_seconds: int,
        stop_event: asyncio.Event,
        inactivity_timeout: Optional[int] = None,
        first_message_grace: Optional[int] = None,
        inactivity_strikes: int = 2,
    ):
        """
        Run a single Nchan WS channel with:
          - Cookie refresh and re-prime on auth loss (login page or 401/403)
          - WS ping keepalive
          - GUI keepalive to keep producers alive
          - Inactivity detection (optional): if no messages for N seconds, re-prime and eventually reconnect

        Set inactivity_timeout <= 0 to disable inactivity restarts for sparse channels.
        first_message_grace (seconds) gives producers time on a fresh connect before we judge inactivity.
        inactivity_strikes defines how many inactivity windows we tolerate (re-priming between) before reconnecting.
        """
        backoff = 5
        inactivity_strikes = max(1, int(inactivity_strikes))
        while not stop_event.is_set():
            local_stop = asyncio.Event()
            event_queue: asyncio.Queue = asyncio.Queue(maxsize=1)

            # Inactivity thresholds
            effective_inactivity = (
                inactivity_timeout if inactivity_timeout is not None else int(max(240, 6 * interval_seconds))
            )
            inactivity_disabled = (effective_inactivity is not None and effective_inactivity <= 0)
            grace = (
                int(first_message_grace)
                if first_message_grace is not None
                else int(max(180, 2 * effective_inactivity))
            )

            last_msg_ts = asyncio.get_event_loop().time()
            received_first = False
            strikes_used = 0

            async def publish_updates(msg_data: str):
                try:
                    updates = await parse_fn(msg_data) or []
                    for upd in updates:
                        publish_fn(
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
                    self.logger.error(f'Legacy WS "{channel_name}" publish failed: {e}')

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
                                # Try re-priming instead of immediate reconnect
                                self.logger.warning(
                                    f'Legacy WS "{channel_name}" inactivity window {strikes_used}/{inactivity_strikes}: '
                                    f'no messages for {int(now - last_msg_ts)}s (>= {threshold}s); re-priming GUI'
                                )
                                await prime_stream()
                                # Give producers a little time and reset the timer baseline
                                last_msg_ts = asyncio.get_event_loop().time()
                                continue
                            else:
                                self.logger.warning(
                                    f'Legacy WS "{channel_name}" inactivity: no messages for {int(now - last_msg_ts)}s '
                                    f'(>= {threshold}s) after {strikes_used} window(s); reconnecting'
                                )
                                break
                        continue
                    except Exception as e:
                        self.logger.error(f'Legacy WS "{channel_name}" recv error: {e}')
                        break
                    try:
                        msg_data = raw.replace('\00', ' ').split('\n\n', 1)[1]
                    except Exception:
                        msg_data = raw

                    # Update last message timestamp
                    last_msg_ts = asyncio.get_event_loop().time()
                    received_first = True
                    strikes_used = 0  # clear strikes once we get any message

                    # Drop any queued older message and keep the latest
                    try:
                        while True:
                            event_queue.get_nowait()
                            event_queue.task_done()
                    except asyncio.QueueEmpty:
                        pass
                    await event_queue.put(msg_data)

            async def publisher_loop():
                try:
                    while event_queue.empty() and not (local_stop.is_set() or stop_event.is_set()):
                        await asyncio.sleep(0.05)
                    if local_stop.is_set() or stop_event.is_set():
                        return
                    first = await event_queue.get()
                except asyncio.CancelledError:
                    return

                await publish_updates(first)
                event_queue.task_done()

                while not (local_stop.is_set() or stop_event.is_set()):
                    try:
                        await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
                        if stop_event.is_set():
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

                    if latest is None:
                        continue
                    await publish_updates(latest)

            async def keepalive_loop():
                """
                Periodically GET /Main and /Dashboard to keep Nchan producers active.
                Triggers re-login only if the server serves a login page or 401/403.
                Does NOT force reconnects on cookie change to avoid herd thrash.
                """
                period = max(15, int(interval_seconds))
                async with httpx.AsyncClient(
                    verify=self.verify_ssl,
                    timeout=10,
                    follow_redirects=True,
                    trust_env=False,
                ) as http:
                    while not (local_stop.is_set() or stop_event.is_set()):
                        try:
                            cookie = await self.auth.get_cookie()
                            headers = {
                                'Cookie': cookie,
                                'Origin': self.http_base_url,
                                'Referer': urljoin(self.http_base_url + '/', 'Main'),
                            }

                            r = await http.get(urljoin(self.http_base_url + '/', 'Main'), headers=headers)
                            if r.status_code in (401, 403) or _is_login_response(r):
                                self.logger.info(
                                    f'Legacy WS "{channel_name}" keepalive: auth expired on /Main, refreshing cookie'
                                )
                                headers['Cookie'] = await self.auth.get_cookie(force=True)
                                r = await http.get(urljoin(self.http_base_url + '/', 'Main'), headers=headers)

                            headers['Referer'] = urljoin(self.http_base_url + '/', 'Dashboard')
                            r = await http.get(urljoin(self.http_base_url + '/', 'Dashboard'), headers=headers)
                            if r.status_code in (401, 403) or _is_login_response(r):
                                self.logger.info(
                                    f'Legacy WS "{channel_name}" keepalive: auth expired on /Dashboard, refreshing cookie'
                                )
                                headers['Cookie'] = await self.auth.get_cookie(force=True)
                                await http.get(urljoin(self.http_base_url + '/', 'Dashboard'), headers=headers)

                        except Exception as e:
                            self.logger.warning(f'Legacy WS "{channel_name}" keepalive error: {e}')
                        await asyncio.sleep(period)

            async def ws_pinger_loop(ws):
                """
                Send WebSocket ping control frames to keep the WS itself from idling.
                """
                period = max(20, int(interval_seconds))
                while not (local_stop.is_set() or stop_event.is_set()):
                    try:
                        await ws.ping()
                    except Exception as e:
                        self.logger.debug(f'Legacy WS "{channel_name}" ping failed: {e}')
                        break
                    await asyncio.sleep(period)

            async def prime_stream():
                """
                Prime /Main and /Dashboard once. Used at connect and as a re-kicker if silent.
                """
                try:
                    async with httpx.AsyncClient(
                        verify=self.verify_ssl,
                        timeout=10,
                        follow_redirects=True,
                        trust_env=False,
                    ) as http:
                        headers = {
                            'Cookie': await self.auth.get_cookie(),
                            'Origin': self.http_base_url,
                            'Referer': urljoin(self.http_base_url + '/', 'Main'),
                        }
                        r = await http.get(urljoin(self.http_base_url + '/', 'Main'), headers=headers)
                        if r.status_code in (401, 403) or _is_login_response(r):
                            self.logger.info(
                                f'Legacy WS "{channel_name}" prime: auth expired on /Main, refreshing cookie'
                            )
                            headers['Cookie'] = await self.auth.get_cookie(force=True)
                            r = await http.get(urljoin(self.http_base_url + '/', 'Main'), headers=headers)

                        headers['Referer'] = urljoin(self.http_base_url + '/', 'Dashboard')
                        r = await http.get(urljoin(self.http_base_url + '/', 'Dashboard'), headers=headers)
                        if r.status_code in (401, 403) or _is_login_response(r):
                            self.logger.info(
                                f'Legacy WS "{channel_name}" prime: auth expired on /Dashboard, refreshing cookie'
                            )
                            headers['Cookie'] = await self.auth.get_cookie(force=True)
                            await http.get(urljoin(self.http_base_url + '/', 'Dashboard'), headers=headers)
                    self.logger.debug(f'Legacy WS "{channel_name}" primed: /Main and /Dashboard')
                except Exception as e:
                    self.logger.warning(f'Legacy WS "{channel_name}" prime failed: {e}')

            async def post_connect_kicker():
                """
                While waiting for the first message, give producers a nudge once or twice.
                """
                attempts = 0
                while not received_first and not (local_stop.is_set() or stop_event.is_set()):
                    attempts += 1
                    # Space the nudges; do fewer at longer gaps to reduce load
                    delay = 15 if attempts == 1 else 30
                    await asyncio.sleep(delay)
                    if received_first or local_stop.is_set() or stop_event.is_set():
                        break
                    self.logger.debug(f'Legacy WS "{channel_name}" first-message kicker attempt {attempts}')
                    await prime_stream()
                    if attempts >= 2:
                        break

            try:
                # Limit concurrent prime+connect across channels
                async with self._connect_sem:
                    await prime_stream()

                    # Give producers a brief moment to spin up before opening WS
                    await asyncio.sleep(5.0)

                    cookie = await self.auth.get_cookie()
                    headers = {
                        'Cookie': cookie,
                        'Origin': self.http_base_url,
                        'Referer': urljoin(self.http_base_url + '/', 'Dashboard'),
                    }
                    subprotocols = ['ws+meta.nchan']
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
                            f'Legacy WS opening channel: {channel_name} (inactivity_timeout={effective_inactivity}s, grace={grace}s, strikes={inactivity_strikes})'
                        )
                    connect_cm = await self._connect_ws_compat(url, subprotocols, headers, ssl_ctx)

                # Outside the semaphore to allow others to progress while we process
                async with connect_cm as ws:
                    recv_task = asyncio.create_task(receiver_loop(ws))
                    pub_task = asyncio.create_task(publisher_loop())
                    ka_task = asyncio.create_task(keepalive_loop())
                    ws_ping_task = asyncio.create_task(ws_pinger_loop(ws))
                    kicker_task = asyncio.create_task(post_connect_kicker())

                    await recv_task
                    local_stop.set()

                    for t in (pub_task, ka_task, ws_ping_task, kicker_task):
                        try:
                            await asyncio.wait_for(t, timeout=2)
                        except asyncio.TimeoutError:
                            t.cancel()

                if not stop_event.is_set():
                    # Do not invalidate the cookie here; only re-login when auth actually fails.
                    # Backoff to avoid tight reconnect loops.
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 60)
                    self.logger.warning(f'Legacy WS channel ended: {channel_name}, reconnecting...')
            except Exception as e:
                if not stop_event.is_set():
                    # Do not blindly invalidate; LegacyAuth handles re-login on demand.
                    self.logger.error(f'Legacy WS channel "{channel_name}" failed: {e}')
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 60)
            finally:
                local_stop.set()
