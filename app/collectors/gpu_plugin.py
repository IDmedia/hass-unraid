import re
import json
from lxml import etree
from app.utils import normalize_keys_lower
from typing import Any, Dict, List, Optional
from .base import EntityUpdate, QueryCollector


class GpuPluginCollector(QueryCollector):
    """
    Polls the Unraid GPU Stat plugin via legacy webGui endpoints.

    Flow:
      - Discover GPU map once by parsing /Dashboard for gpustat_statusm({...}).
      - Fetch live stats via /plugins/gpustat/gpustatusmulti.php?gpus=<json>.
      - Publish per-GPU sensors and a summary entity.

    Requires legacy auth (Cookie). Uses LegacyHTTPContext implicitly through the provided legacy_ctx.
    """

    name = 'gpu_plugin'
    requires_legacy_auth = True
    query = None  # not GraphQL-based

    def __init__(self, gql_client, logger, interval: int, legacy_ctx: Optional[Any] = None):
        self.gql = gql_client
        self.logger = logger
        self.interval = int(interval)
        self.legacy_ctx = legacy_ctx
        self.gpus: Optional[Dict[str, Any]] = None

    async def fetch(self) -> Dict[str, Any]:
        """
        Returns plugin JSON data as a dict keyed by gpu_id.
        """
        if not self.legacy_ctx:
            self.logger.info('GPU plugin: legacy context missing; skipping')
            return {}

        if self.gpus is None:
            await self._discover_gpus()
            if not self.gpus:
                return {}

        try:
            r = await self.legacy_ctx.http_get(
                '/plugins/gpustat/gpustatusmulti.php',
                params={'gpus': json.dumps(self.gpus)},
                timeout=30,
            )
            if r.status_code != 200:
                self.logger.warning(f'GPU plugin fetch failed: HTTP {r.status_code}')
                return {}
            return r.json()
        except Exception as e:
            self.logger.error(f'GPU plugin fetch error: {e}')
            return {}

    async def parse(self, data: Dict[str, Any]) -> List[EntityUpdate]:
        updates: List[EntityUpdate] = []
        if not isinstance(data, dict) or not data:
            return updates

        def is_valid(value: Any) -> bool:
            invalid = {'N/A', 'N\\/A', 'Unknown', 'unknown', '', None}
            if str(value).strip() in invalid:
                return False
            try:
                cleaned = str(value)
                for suffix in ['%', '°C', 'W']:
                    cleaned = cleaned.replace(suffix, '')
                float(cleaned.strip())
                return True
            except Exception:
                return False

        for gpu_id, gpu_data in data.items():
            if not isinstance(gpu_data, dict):
                continue

            name = str(gpu_data.get('name') or f'GPU {gpu_id}')

            util = gpu_data.get('util')
            if is_valid(util):
                try:
                    load_pct = int(float(str(util).replace('%', '').strip()))
                except Exception:
                    load_pct = 0
                updates.append(
                    EntityUpdate(
                        sensor_type='sensor',
                        payload={
                            'name': f'{name} Load',
                            'unit_of_measurement': '%',
                            'icon': 'mdi:chart-line',
                            'state_class': 'measurement',
                        },
                        state=load_pct,
                        retain=False,
                        expire_after=max(self.interval * 2, 60),
                        unique_id_suffix=f'{gpu_id}_load',
                    )
                )

            if all(is_valid(gpu_data.get(k)) for k in ['memutil', 'memused', 'memtotal']):
                try:
                    mem_used = int(float(gpu_data['memused']))
                    mem_total = int(float(gpu_data['memtotal']))
                    mem_pct = int(float(str(gpu_data['memutil']).replace('%', '').strip()))
                    updates.append(
                        EntityUpdate(
                            sensor_type='sensor',
                            payload={
                                'name': f'{name} Memory Usage',
                                'unit_of_measurement': '%',
                                'icon': 'mdi:memory',
                                'state_class': 'measurement',
                            },
                            state=mem_pct,
                            attributes={'used': mem_used, 'total': mem_total},
                            retain=False,
                            expire_after=max(self.interval * 2, 60),
                            unique_id_suffix=f'{gpu_id}_mem',
                        )
                    )
                except Exception:
                    pass

            fan = gpu_data.get('fan')
            if is_valid(fan):
                try:
                    fan_pct = int(float(str(fan).replace('%', '').strip()))
                except Exception:
                    fan_pct = 0
                updates.append(
                    EntityUpdate(
                        sensor_type='sensor',
                        payload={
                            'name': f'{name} Fan Speed',
                            'unit_of_measurement': '%',
                            'icon': 'mdi:fan',
                            'state_class': 'measurement',
                        },
                        state=fan_pct,
                        retain=False,
                        expire_after=max(self.interval * 2, 60),
                        unique_id_suffix=f'{gpu_id}_fan',
                    )
                )

            power = gpu_data.get('power')
            if is_valid(power):
                try:
                    power_w = int(float(str(power).replace('W', '').strip()))
                except Exception:
                    power_w = 0
                updates.append(
                    EntityUpdate(
                        sensor_type='sensor',
                        payload={
                            'name': f'{name} Power Usage',
                            'unit_of_measurement': 'W',
                            'icon': 'mdi:flash',
                            'state_class': 'measurement',
                        },
                        state=power_w,
                        retain=False,
                        expire_after=max(self.interval * 2, 60),
                        unique_id_suffix=f'{gpu_id}_power',
                    )
                )

            temp = gpu_data.get('temp')
            if is_valid(temp):
                try:
                    temp_c = int(float(str(temp).replace('°C', '').strip()))
                except Exception:
                    temp_c = 0
                updates.append(
                    EntityUpdate(
                        sensor_type='sensor',
                        payload={
                            'name': f'{name} Temperature',
                            'unit_of_measurement': '°C',
                            'icon': 'mdi:thermometer',
                            'state_class': 'measurement',
                            'device_class': 'temperature',
                        },
                        state=temp_c,
                        retain=False,
                        expire_after=max(self.interval * 2, 60),
                        unique_id_suffix=f'{gpu_id}_temp',
                    )
                )

            if is_valid(util):
                try:
                    load_pct = int(float(str(util).replace('%', '').strip()))
                    valid_attrs = {k: v for k, v in gpu_data.items() if is_valid(v)}
                    valid_attrs = normalize_keys_lower(valid_attrs)
                    updates.append(
                        EntityUpdate(
                            sensor_type='sensor',
                            payload={
                                'name': name,
                                'icon': 'mdi:expansion-card',
                                'unit_of_measurement': '%',
                                'state_class': 'measurement',
                            },
                            state=load_pct,
                            attributes=valid_attrs,
                            retain=False,
                            expire_after=max(self.interval * 2, 60),
                            unique_id_suffix=f'{gpu_id}_summary',
                        )
                    )
                except Exception:
                    pass

        return updates

    async def _discover_gpus(self) -> None:
        """
        Fetch /Dashboard and parse gpustat_statusm(...) to discover GPUs.
        """
        if not self.legacy_ctx:
            return

        try:
            r = await self.legacy_ctx.http_get('/Dashboard', timeout=30)
            if r.status_code != 200:
                self.logger.warning(f'GPU discovery failed: HTTP {r.status_code}')
                return

            tree = etree.HTML(r.text)
            script_nodes = tree.xpath('.//script[contains(text(), "gpustat_statusm")]')
            if not script_nodes:
                return

            script_text = script_nodes[0].text or ''
            m = re.search(r'gpustat_statusm\((\{.+?\})\)', script_text, re.DOTALL)
            if not m:
                self.logger.info('GPU plugin discovery: unable to match gpustat_statusm JSON')
                return

            gpus_json = m.group(1)
            gpus_obj = json.loads(gpus_json)
            if isinstance(gpus_obj, dict) and gpus_obj:
                self.gpus = gpus_obj
                self.logger.info('GPU plugin: GPUs discovered')
        except Exception as e:
            self.logger.error(f'GPU discovery error: {e}')


COLLECTOR = GpuPluginCollector
