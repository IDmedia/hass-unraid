# app/main.py
import os
import signal
import pkgutil
import asyncio
import importlib
import ssl as ssl_mod
from typing import Any, Dict, List

from .gql_http import GraphQLClient
from .mqtt_pub import MQTTPublisher
from .smart_cache import SmartCache
from .legacy.base import LegacyChannel
from .utils import load_file, setup_logger, normalize_str
from .legacy_ws import LegacyAuth, LegacyWSRunner
from .collectors.base import QueryCollector, SubscriptionCollector, EntityUpdate

DATA_DIR = '/data'


class LegacyHTTPContext:
    def __init__(self, http_base_url: str, verify_ssl: bool, auth, smart_cache=None):
        self.http_base_url = http_base_url
        self.verify_ssl = verify_ssl
        self.auth = auth
        self.smart_cache = smart_cache


class UnraidGraphQLIntegration:
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
        endpoint_http = f'{http_protocol}://{host}:{port}/graphql'
        endpoint_ws = f'{ws_protocol}://{host}:{port}/graphql'

        # Legacy credentials (optional)
        self.username = unraid_cfg.get('username')
        self.password = unraid_cfg.get('password')

        self.legacy_auth = None
        self.legacy_ws_runner = None
        self.legacy_channels: List[LegacyChannel] = []
        self._legacy_tasks: List[asyncio.Task] = []

        # Build HTTP/WS base URLs for legacy (non-GraphQL)
        self.legacy_http_base = f'{http_protocol}://{host}:{port}'
        self.legacy_ws_base = f'{ws_protocol}://{host}:{port}'

        self.scan_interval = int(unraid_cfg.get('scan_interval', 30))
        self.api_key = api_key
        self.verify_ssl = verify_ssl
        self.endpoint_http = endpoint_http
        self.endpoint_ws = endpoint_ws
        self.logger.info(f'SSL mode: {"https/wss" if use_ssl else "http/ws"}, verify_ssl={self.verify_ssl}')

        # SMART cache per node
        unraid_id = normalize_str(self.name)
        self.smart_cache = SmartCache(unraid_id, DATA_DIR, self.logger)

        # Prepare LegacyAuth and legacy context once (no redundancy)
        if self.username and self.password:
            self.legacy_auth = LegacyAuth(self.legacy_http_base, self.username, self.password, self.verify_ssl, self.logger)
            self.legacy_ctx = LegacyHTTPContext(self.legacy_http_base, self.verify_ssl, self.legacy_auth, smart_cache=self.smart_cache)
        else:
            self.legacy_ctx = None

        # Global stop coordination for this integration
        self._stop = asyncio.Event()

        self.gql = GraphQLClient(endpoint_http, api_key, verify_ssl=verify_ssl, timeout=max(15, self.scan_interval))
        self.mqtt = MQTTPublisher(self.name, mqtt_config, loop, self.logger, self.scan_interval)

        self.query_collectors: List[QueryCollector] = []
        self.subscription_collectors: List[SubscriptionCollector] = []
        self._query_tasks: List[asyncio.Task] = []
        self._subscription_tasks: List[asyncio.Task] = []

        # Load collector classes (do not start yet)
        self._load_query_collectors()
        self._load_subscription_collectors()
        self._load_legacy_channels()

    async def start(self):
        """
        Fetch version (sw_version) first so device info includes it in initial discovery,
        then start collectors.
        """
        await self._fetch_version()
        self._start_query_collectors()
        self._start_subscription_collectors()
        self._start_legacy_channels()

    async def _fetch_version(self):
        try:
            version = await self.gql.get_version()
            if version:
                self.logger.info(f'Unraid version detected: {version}')
                self.mqtt.set_device_overrides({'sw_version': version})
            else:
                self.logger.warning('Unraid version not available from GraphQL Vars')
        except Exception as e:
            self.logger.error(f'Failed to fetch Unraid version: {e}')

    def _load_legacy_channels(self):
        loaded = 0
        # Only attempt if credentials are present
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

    def _load_query_collectors(self):
        """
        Auto-load collectors and pass optional dependencies:
          - legacy_ctx for collectors declaring requires_legacy_auth = True
          - smart_cache for collectors declaring uses_smart_cache = True
        """
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

                # Instantiate with flexible kwargs
                instance = collector_cls(self.gql, self.logger, self.scan_interval, **kwargs)
                self.query_collectors.append(instance)
                loaded += 1
                self.logger.info(f'Loaded query collector: {instance.name}')
            except Exception as e:
                self.logger.error(f'Failed to load collector {full_module}: {e}')

        if loaded == 0:
            self.logger.warning('No query collectors loaded. Ensure app/collectors has modules and __init__.py.')

    def _load_subscription_collectors(self):
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

    def _start_legacy_channels(self):
        if not self.legacy_channels or not self.legacy_auth:
            return
        # Prepare runner once; reuse existing LegacyAuth
        self.legacy_ws_runner = LegacyWSRunner(
            base_ws_url=self.legacy_ws_base,
            http_base_url=self.legacy_http_base,
            verify_ssl=self.verify_ssl,
            auth=self.legacy_auth,
            logger=self.logger
        )
        for ch in self.legacy_channels:
            task = asyncio.create_task(self._run_legacy_channel(ch))
            self._legacy_tasks.append(task)

    async def _run_legacy_channel(self, channel: LegacyChannel):
        """
        Run one Nchan channel with immediate-first publish then throttled cadence.
        """
        await self.legacy_ws_runner.run_channel(
            channel_name=channel.channel,
            parse_fn=channel.parse,
            publish_fn=self.mqtt.publish,
            interval_seconds=max(1, int(getattr(channel, 'interval', self.scan_interval))),
            stop_event=self._stop
        )

    def _start_query_collectors(self):
        for collector in self.query_collectors:
            task = asyncio.create_task(self._run_query_collector(collector))
            self._query_tasks.append(task)

    def _start_subscription_collectors(self):
        for collector in self.subscription_collectors:
            task = asyncio.create_task(self._run_subscription_collector(collector))
            self._subscription_tasks.append(task)

    async def _run_query_collector(self, collector: QueryCollector):
        interval = max(1, int(getattr(collector, 'interval', self.scan_interval)))
        while not self._stop.is_set():
            try:
                data = await collector.fetch()
                updates: List[EntityUpdate] = await collector.parse(data) or []
                for upd in updates:
                    self.mqtt.publish(
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
                self.logger.error(f'Collector "{collector.name}" run failed: {e}')
            # Sleep until next tick, but allow fast exit
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass

    async def _run_subscription_collector(self, collector: SubscriptionCollector):
        """
        Persistent WebSocket subscription with immediate-first publish and interval throttling.
        Graceful stop: exits when self._stop is set or when the subscription ends.
        """
        from gql import Client, gql
        from gql.transport.websockets import WebsocketsTransport
        import asyncio

        base_interval = max(1, int(getattr(collector, 'interval', self.scan_interval)))
        backoff = 5

        while not self._stop.is_set():
            stop_local = asyncio.Event()
            event_queue: asyncio.Queue = asyncio.Queue(maxsize=1)  # buffer only latest event

            async def publish_event(event: Dict[str, Any]):
                try:
                    updates = await collector.parse(event) or []
                    for upd in updates:
                        self.mqtt.publish(
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
                    self.logger.error(f'Subscription "{collector.name}" publish failed: {e}')

            async def receiver_loop(session):
                async for event in session.subscribe(gql(collector.subscription_query)):
                    # keep only the latest event
                    try:
                        while True:
                            event_queue.get_nowait()
                            event_queue.task_done()
                    except asyncio.QueueEmpty:
                        pass
                    await event_queue.put(event)

                    if stop_local.is_set() or self._stop.is_set():
                        break

            async def publisher_loop():
                # Wait for the first event to arrive
                try:
                    while event_queue.empty() and not (stop_local.is_set() or self._stop.is_set()):
                        await asyncio.sleep(0.05)
                    if stop_local.is_set() or self._stop.is_set():
                        return
                    first_event = await event_queue.get()
                except asyncio.CancelledError:
                    return
                # Immediate publish
                await publish_event(first_event)
                event_queue.task_done()

                # Throttled cadence thereafter
                while not (stop_local.is_set() or self._stop.is_set()):
                    try:
                        await asyncio.wait_for(self._stop.wait(), timeout=base_interval)
                        if self._stop.is_set():
                            break
                    except asyncio.TimeoutError:
                        pass
                    if stop_local.is_set():
                        break

                    latest = None
                    try:
                        while True:
                            latest = event_queue.get_nowait()
                            event_queue.task_done()
                    except asyncio.QueueEmpty:
                        pass
                    if latest is None:
                        continue

                    await publish_event(latest)

            try:
                csrf = await self.gql.get_csrf_token()
                if not csrf:
                    raise RuntimeError('Failed to obtain CSRF token')

                init_payload = {"x-csrf-token": csrf, "x-api-key": self.api_key}
                ssl_ctx = None
                if self.endpoint_ws.startswith('wss') and not self.verify_ssl:
                    ssl_ctx = ssl_mod._create_unverified_context()

                transport = WebsocketsTransport(url=self.endpoint_ws, init_payload=init_payload, ssl=ssl_ctx)
                client = Client(transport=transport, fetch_schema_from_transport=False)

                self.logger.info(f'Opening subscription for: {collector.name}')
                async with client as session:
                    recv_task = asyncio.create_task(receiver_loop(session))
                    pub_task = asyncio.create_task(publisher_loop())

                    await recv_task
                    stop_local.set()
                    try:
                        await asyncio.wait_for(pub_task, timeout=2)
                    except asyncio.TimeoutError:
                        pub_task.cancel()

                if not self._stop.is_set():
                    self.logger.warning(f'Subscription ended for {collector.name}, reconnecting...')
            except Exception as e:
                if not self._stop.is_set():
                    self.logger.error(f'Subscription "{collector.name}" failed: {e}')
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 60)
            finally:
                stop_local.set()

    async def aclose(self):
        self._stop.set()
        try:
            done, pending = await asyncio.wait(
                self._query_tasks + self._subscription_tasks + self._legacy_tasks,
                timeout=2,
                return_when=asyncio.ALL_COMPLETED
            )
            for t in pending:
                t.cancel()
        except Exception:
            for t in self._query_tasks + self._subscription_tasks + self._legacy_tasks:
                t.cancel()
        await self.gql.aclose()
        await self.mqtt.aclose()


async def main():
    # Load config
    config = load_file(os.path.join(DATA_DIR, 'config.yaml')) or {}
    mqtt_cfg = config.get('mqtt') or {}
    unraid_nodes = config.get('unraid') or []
    if not unraid_nodes:
        print('No unraid nodes defined in /data/config.yaml')
        return

    # Windows recommended policy
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    loop = asyncio.get_event_loop()
    integrators: List[UnraidGraphQLIntegration] = []
    for node_cfg in unraid_nodes:
        try:
            integrators.append(UnraidGraphQLIntegration(mqtt_cfg, node_cfg, loop))
        except Exception as e:
            print(f'Failed to initialize node: {e}')

    # Start each integrator (fetch version, then start tasks)
    await asyncio.gather(*(i.start() for i in integrators), return_exceptions=True)

    # Global stop event driven by signals
    stop_event = asyncio.Event()

    def trigger_stop(*_):
        stop_event.set()

    # Register signal handlers (SIGINT for Ctrl+C; SIGTERM for container stop)
    try:
        signal.signal(signal.SIGINT, trigger_stop)
    except Exception:
        pass
    try:
        signal.signal(signal.SIGTERM, trigger_stop)
    except Exception:
        pass

    # Wait until a stop signal arrives
    await stop_event.wait()

    # Gracefully close integrators
    await asyncio.gather(*(i.aclose() for i in integrators), return_exceptions=True)


if __name__ == '__main__':
    asyncio.run(main())
