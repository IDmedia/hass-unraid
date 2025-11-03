import json
import re
from typing import Dict, List, Any, Optional

import httpx
from lxml import etree

from .base import QueryCollector, EntityUpdate
from app.utils import normalize_keys_lower


class GpuPluginCollector(QueryCollector):
    """
    Polls the Unraid GPU Stat plugin via HTTP (legacy webGui) and publishes GPU metrics.

    Behavior:
      - On first run, discovers the GPU list by fetching /Dashboard and parsing the gpustat_statusm(...) script.
      - Then polls /plugins/gpustat/gpustatusmulti.php with ?gpus=<json> to get live stats.
      - Publishes per-GPU sensors:
          * {GPU name} Load (%)
          * {GPU name} Memory Usage (%) with attributes {used,total}
          * {GPU name} Fan Speed (%)
          * {GPU name} Power Usage (W)
          * {GPU name} Temperature (°C)
          * {GPU name} (summary) with load % as state and all valid attributes

    Notes:
      - Requires legacy auth (username/password) for Cookie. Set `requires_legacy_auth = True`.
      - Uses immediate-first publish via the query collector loop (runs at start), then throttles to interval.
      - Live metrics: retain=False with expire_after=max(interval*2, 60).
    """
    name = 'gpu_plugin'
    requires_legacy_auth = True

    def __init__(self, gql_client, logger, interval: int, legacy_ctx: Optional[Any] = None):
        self.gql = gql_client
        self.logger = logger
        self.interval = int(interval)
        self.query = None  # not a GraphQL query; we override fetch() to use HTTP
        self.legacy_ctx = legacy_ctx
        self.gpus: Optional[Dict[str, Any]] = None  # discovered GPU map

    async def fetch(self) -> Dict[str, Any]:
        """
        Returns plugin JSON data as a dict.
        """
        if not self.legacy_ctx:
            self.logger.info('GPU plugin: legacy context missing; skipping')
            return {}

        # Ensure GPUs are discovered once
        if self.gpus is None:
            await self._discover_gpus()
            if not self.gpus:
                # self.logger.warning('GPU plugin: no GPUs discovered; skipping fetch')
                return {}

        cookie = await self.legacy_ctx.auth.get_cookie()
        headers = {'Cookie': cookie}

        url = f'{self.legacy_ctx.http_base_url}/plugins/gpustat/gpustatusmulti.php'
        params = {'gpus': json.dumps(self.gpus)}

        async with httpx.AsyncClient(verify=self.legacy_ctx.verify_ssl, timeout=30, follow_redirects=True) as http:
            try:
                r = await http.get(url, params=params, headers=headers)
                if r.status_code != httpx.codes.OK:
                    self.logger.warning(f'GPU plugin fetch failed: HTTP {r.status_code}')
                    return {}
                return r.json()
            except Exception as e:
                self.logger.error(f'GPU plugin fetch error: {e}')
                return {}

    async def parse(self, data: Dict[str, Any]) -> List[EntityUpdate]:
        updates: List[EntityUpdate] = []
        if not data:
            return updates

        # data is a dict keyed by gpu_id => gpu_data
        for gpu_id, gpu_data in data.items():
            # Defensive: ensure dict
            if not isinstance(gpu_data, dict):
                continue

            name = gpu_data.get('name', f'GPU {gpu_id}')

            def is_valid(value: Any) -> bool:
                invalid_values = {'N/A', 'N\\/A', 'Unknown', 'unknown', '', None}
                if str(value).strip() in invalid_values:
                    return False
                # Check numeric values with potential units
                try:
                    cleaned = str(value).replace('%', '').replace('°C', '').replace('W', '').strip()
                    float(cleaned)
                    return True
                except Exception:
                    return False

            # Load
            util = gpu_data.get('util')
            if is_valid(util):
                try:
                    load_pct = int(float(str(util).replace('%', '').strip()))
                    updates.append(EntityUpdate(
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
                        unique_id_suffix=f'{gpu_id}_load'
                    ))
                except Exception:
                    pass

            # Memory
            if all(is_valid(gpu_data.get(k)) for k in ['memutil', 'memused', 'memtotal']):
                try:
                    mem_used = int(float(gpu_data['memused']))
                    mem_total = int(float(gpu_data['memtotal']))
                    mem_pct = int(float(str(gpu_data['memutil']).replace('%', '').strip()))
                    updates.append(EntityUpdate(
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
                        unique_id_suffix=f'{gpu_id}_mem'
                    ))
                except Exception:
                    pass

            # Fan
            fan = gpu_data.get('fan')
            if is_valid(fan):
                try:
                    fan_pct = int(float(str(fan).replace('%', '').strip()))
                    updates.append(EntityUpdate(
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
                        unique_id_suffix=f'{gpu_id}_fan'
                    ))
                except Exception:
                    pass

            # Power
            power = gpu_data.get('power')
            if is_valid(power):
                try:
                    power_w = int(float(str(power).replace('W', '').strip()))
                    updates.append(EntityUpdate(
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
                        unique_id_suffix=f'{gpu_id}_power'
                    ))
                except Exception:
                    pass

            # Temperature
            temp = gpu_data.get('temp')
            if is_valid(temp):
                try:
                    temp_c = int(float(str(temp).replace('°C', '').strip()))
                    updates.append(EntityUpdate(
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
                        unique_id_suffix=f'{gpu_id}_temp'
                    ))
                except Exception:
                    pass

            # Summary sensor: load as state, attributes = all valid fields (lowercased keys)
            if is_valid(util):
                try:
                    load_pct = int(float(str(util).replace('%', '').strip()))
                    valid_attrs = {k: v for k, v in gpu_data.items() if is_valid(v)}
                    # Lowercase keys for consistency
                    valid_attrs = normalize_keys_lower(valid_attrs)
                    updates.append(EntityUpdate(
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
                        unique_id_suffix=f'{gpu_id}_summary'
                    ))
                except Exception:
                    pass

        return updates

    async def _discover_gpus(self):
        """
        Fetch /Dashboard and parse gpustat_statusm({ ... }) to discover GPU map.
        """
        if not self.legacy_ctx:
            return

        cookie = await self.legacy_ctx.auth.get_cookie()
        headers = {'Cookie': cookie}
        url = f'{self.legacy_ctx.http_base_url}/Dashboard'

        async with httpx.AsyncClient(verify=self.legacy_ctx.verify_ssl, timeout=30, follow_redirects=True) as http:
            try:
                r = await http.get(url, headers=headers)
                if r.status_code != httpx.codes.OK:
                    self.logger.warning(f'GPU discovery failed: HTTP {r.status_code}')
                    return
                tree = etree.HTML(r.text)
                script_nodes = tree.xpath('.//script[contains(text(), "gpustat_statusm")]')
                if not script_nodes:
                    # self.logger.info('GPU plugin discovery: gpustat_statusm script not found')
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
