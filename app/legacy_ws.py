import asyncio
import httpx
import ssl as ssl_mod
from http.cookies import SimpleCookie
from typing import Callable, Optional, Tuple, List


class LegacyAuth:
    def __init__(self, base_http_url: str, username: str, password: str, verify_ssl: bool, logger):
        self.base_http_url = base_http_url.rstrip('/')
        self.username = username
        self.password = password
        self.verify_ssl = verify_ssl
        self.logger = logger
        self._cookie: Optional[str] = None
        self._lock = asyncio.Lock()

    async def get_cookie(self) -> str:
        async with self._lock:
            if self._cookie:
                return self._cookie

            # Follow redirects so 302 -> /Dashboard is handled, and cookies are collected in the jar
            async with httpx.AsyncClient(verify=self.verify_ssl, timeout=30, follow_redirects=True) as http:
                try:
                    r = await http.post(f'{self.base_http_url}/login', data={
                        'username': self.username,
                        'password': self.password
                    })
                    # No raise_for_status here; 302 is expected

                    # 1) Prefer cookie jar from the client (aggregates cookies across redirects)
                    jar_items = list(http.cookies.items())
                    if jar_items:
                        # Build a 'Cookie' header string: "name=value; name2=value2"
                        cookie_header = '; '.join([f'{k}={v}' for k, v in jar_items])
                        self._cookie = cookie_header
                        self.logger.info('Legacy WS: obtained Unraid cookie from session jar')
                        return self._cookie

                    # 2) Fallback: parse Set-Cookie headers from the response
                    cookies_list = []
                    # httpx's headers object supports get_list in recent versions
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
                        # Only send "name=value" pairs in the Cookie header
                        pairs = [f'{m.key}={m.value}' for m in simple.values()]
                        cookie_header = '; '.join(pairs)
                        self._cookie = cookie_header
                        self.logger.info('Legacy WS: obtained Unraid cookie from Set-Cookie headers')
                        return self._cookie

                    raise RuntimeError('Login succeeded but no cookies were set')
                except Exception as e:
                    self.logger.error(f'Legacy WS: login failed: {e}')
                    raise


class LegacyWSRunner:
    def __init__(self, base_ws_url: str, http_base_url: str, verify_ssl: bool, auth, logger):
        self.base_ws_url = base_ws_url.rstrip('/')
        self.http_base_url = http_base_url.rstrip('/')
        self.verify_ssl = verify_ssl
        self.auth = auth
        self.logger = logger

    async def _connect_ws_compat(self, url: str, subprotocols: List[str], headers: dict, ssl_ctx):
        hdrs_list: List[Tuple[str, str]] = [(k, v) for k, v in headers.items()]
        try:
            from websockets.client import connect as _connect
        except Exception:
            try:
                from websockets.legacy.client import connect as _connect
            except Exception:
                from websockets import connect as _connect
        try:
            return _connect(url, subprotocols=subprotocols, extra_headers=hdrs_list, ssl=ssl_ctx)
        except TypeError:
            return _connect(url, subprotocols=subprotocols, headers=hdrs_list, ssl=ssl_ctx)

    async def run_channel(
        self,
        channel_name: str,
        parse_fn: Callable[[str], asyncio.Future],
        publish_fn: Callable,
        interval_seconds: int,
        stop_event: asyncio.Event
    ):
        backoff = 5
        while not stop_event.is_set():
            local_stop = asyncio.Event()
            event_queue: asyncio.Queue = asyncio.Queue(maxsize=1)

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
                            expire_after=upd.expire_after
                        )
                except Exception as e:
                    self.logger.error(f'Legacy WS "{channel_name}" publish failed: {e}')

            async def receiver_loop(ws):
                while not (local_stop.is_set() or stop_event.is_set()):
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=120)
                    except asyncio.TimeoutError:
                        continue
                    except Exception as e:
                        self.logger.error(f'Legacy WS "{channel_name}" recv error: {e}')
                        break
                    try:
                        msg_data = raw.replace('\00', ' ').split('\n\n', 1)[1]
                    except Exception:
                        msg_data = raw
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

            # NEW: keepalive task for parity channel
            async def keepalive_loop():
                """
                Tickles the GUI backend by fetching /Main periodically so parity updates continue.
                """
                if channel_name != 'parity':
                    return
                async with httpx.AsyncClient(verify=self.verify_ssl, timeout=10, follow_redirects=True) as http:
                    while not (local_stop.is_set() or stop_event.is_set()):
                        try:
                            cookie = await self.auth.get_cookie()
                            headers = {'Cookie': cookie}
                            url = f'{self.http_base_url}/Main'
                            await http.get(url, headers=headers)
                        except Exception:
                            # Suppress errors; this is best-effort
                            pass
                        await asyncio.sleep(20)  # adjust period if needed

            try:
                cookie = await self.auth.get_cookie()
                headers = {'Cookie': cookie}
                subprotocols = ['ws+meta.nchan']

                ssl_ctx = None
                if self.base_ws_url.startswith('wss') and not self.verify_ssl:
                    ssl_ctx = ssl_mod._create_unverified_context()

                url = f'{self.base_ws_url}/sub/{channel_name}'
                self.logger.info(f'Legacy WS opening channel: {channel_name}')

                connect_cm = await self._connect_ws_compat(url, subprotocols, headers, ssl_ctx)
                async with connect_cm as ws:
                    recv_task = asyncio.create_task(receiver_loop(ws))
                    pub_task = asyncio.create_task(publisher_loop())
                    ka_task = asyncio.create_task(keepalive_loop())  # NEW

                    await recv_task
                    local_stop.set()
                    try:
                        await asyncio.wait_for(pub_task, timeout=2)
                    except asyncio.TimeoutError:
                        pub_task.cancel()
                    try:
                        await asyncio.wait_for(ka_task, timeout=2)
                    except asyncio.TimeoutError:
                        ka_task.cancel()

                if not stop_event.is_set():
                    self.logger.warning(f'Legacy WS channel ended: {channel_name}, reconnecting...')
            except Exception as e:
                if not stop_event.is_set():
                    self.logger.error(f'Legacy WS channel "{channel_name}" failed: {e}')
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 60)
            finally:
                local_stop.set()
