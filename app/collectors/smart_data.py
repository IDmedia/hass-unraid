import time
import httpx
from app.utils import parse_smart_data
from typing import Dict, List, Any, Optional
from .base import QueryCollector, EntityUpdate


SMART_QUERY_ALL = """
query Array {
  array {
    parities {
      name
      device
      transport
      temp
    }
    disks {
      name
      device
      transport
      temp
    }
    caches {
      name
      device
      transport
      temp
    }
  }
}
"""


class SmartDataCollector(QueryCollector):
    """
    Legacy SMART attributes fetcher:
      - Queries GraphQL for parities, disks, caches (name, device, transport, temp).
      - For each device needing update, POST to SmartInfo.php with Cookie + CSRF.
      - Parses SMART table into snake_case attributes (parse_smart_data) and stores in SmartCache.

    Publishes no entities; the disks collector will merge 'smart_attributes' from the cache.
    """
    name = 'smart_data'
    requires_legacy_auth = True
    uses_smart_cache = True  # loader will pass smart_cache

    def __init__(self, gql_client, logger, interval: int, legacy_ctx: Optional[Any] = None, smart_cache=None):
        self.gql = gql_client
        self.logger = logger
        self.interval = int(interval)
        self.query = SMART_QUERY_ALL
        self.legacy_ctx = legacy_ctx
        self.smart_cache = smart_cache
        # Legacy behavior: fetch every 1800 seconds
        self.smart_interval = 1800

    async def fetch(self) -> Dict[str, Any]:
        # Obtain the full device list via GraphQL
        try:
            return await self.gql.query(self.query)
        except Exception as e:
            self.logger.error(f'SMART devices GraphQL query failed: {e}')
            return {}

    async def parse(self, data: Dict[str, Any]) -> List[EntityUpdate]:
        # Returns no EntityUpdate; only updates the cache
        arr = (data or {}).get('array') or {}

        parities = arr.get('parities') or []
        disks = arr.get('disks') or []
        caches = arr.get('caches') or []

        # Combine all devices into a single list
        devices: List[Dict[str, Any]] = []
        for group in (parities, disks, caches):
            if isinstance(group, list):
                devices.extend([d for d in group if isinstance(d, dict)])

        now = time.time()

        # Prune cache to only current devices
        valid_names = [d.get('name') for d in devices if d.get('name')]
        if self.smart_cache and valid_names:
            self.smart_cache.prune_to(valid_names)

        if not self.legacy_ctx or not self.smart_cache:
            return []

        # CSRF + Cookie
        try:
            cookie = await self.legacy_ctx.auth.get_cookie()
            csrf = await self.gql.get_csrf_token()
        except Exception as e:
            self.logger.error(f'SMART auth/csrf fetch failed: {e}')
            return []

        headers = {'Cookie': cookie}
        url = f'{self.legacy_ctx.http_base_url}/webGui/include/SmartInfo.php'

        dirty = False

        # honor verify_ssl and avoid env overrides
        async with httpx.AsyncClient(
            verify=self.legacy_ctx.verify_ssl,
            timeout=30,
            follow_redirects=True,
            trust_env=False
        ) as http:
            for dev in devices:
                name = dev.get('name') or ''
                device = dev.get('device')
                transport = (dev.get('transport') or '').lower()
                temp = dev.get('temp')

                try:
                    temp_val = int(float(temp)) if temp is not None else 0
                except Exception:
                    temp_val = 0

                # Determine if SMART should be fetched (legacy logic)
                last = self.smart_cache.last_update(name) if self.smart_cache else 0.0
                need_fetch = False
                if last == 0.0 or (now - last) >= self.smart_interval:
                    # skip if spun down and not usb
                    if temp_val == 0 and transport != 'usb':
                        need_fetch = False
                    else:
                        need_fetch = True

                if not need_fetch or not device:
                    continue

                data_form = {
                    'cmd': 'attributes',
                    'port': device,
                    'name': name,
                    'csrf_token': csrf,
                }

                try:
                    r = await http.post(url, data=data_form, headers=headers)
                    if r.status_code != httpx.codes.OK:
                        self.logger.warning(f'SMART fetch failed for "{name}" (HTTP {r.status_code})')
                        continue

                    # Parse SMART table into snake_case attributes
                    smart_attrs = parse_smart_data(r.text, self.logger)
                    if len(smart_attrs) > 1:
                        self.smart_cache.update(name, smart_attrs, ts=now)
                        dirty = True
                        self.logger.info(f'SMART data updated for device "{name}"')
                except Exception as e:
                    self.logger.error(f'SMART fetch error for "{name}": {e}')

        if dirty:
            self.smart_cache.save()

        # No MQTT entities from this collector
        return []


COLLECTOR = SmartDataCollector
