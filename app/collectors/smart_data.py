import time
import httpx
from app.utils import parse_smart_data
from typing import Dict, List, Any, Optional
from .base import QueryCollector, EntityUpdate


SMART_QUERY_DISKS = """
query Array {
  array {
    disks {
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
      - Queries GraphQL for disks (name, device, transport, temp).
      - For each disk needing update, POST to SmartInfo.php with Cookie + CSRF.
      - Parses SMART table into snake_case attributes (parse_smart_data) and stores in SmartCache.

    Publishes no entities; disks collector will merge 'smart_attributes' from the cache.

    Notes:
      - Set smart_interval to match your old 1800 seconds (configurable below).
      - Skip fetching if temp == 0 and transport != 'usb' (spun down), like legacy.
    """
    name = 'smart_data'
    requires_legacy_auth = True
    uses_smart_cache = True  # signal to loader to pass smart_cache

    def __init__(self, gql_client, logger, interval: int, legacy_ctx: Optional[Any] = None, smart_cache=None):
        self.gql = gql_client
        self.logger = logger
        self.interval = int(interval)
        self.query = SMART_QUERY_DISKS
        self.legacy_ctx = legacy_ctx
        self.smart_cache = smart_cache

        # Legacy behavior: fetch every 1800 seconds
        self.smart_interval = 1800

    async def fetch(self) -> Dict[str, Any]:
        # We actually use this fetch to obtain disk list via GraphQL
        try:
            return await self.gql.query(self.query)
        except Exception as e:
            self.logger.error(f'SMART disks GraphQL query failed: {e}')
            return {}

    async def parse(self, data: Dict[str, Any]) -> List[EntityUpdate]:
        # Returns no EntityUpdate; only updates the cache
        arr = (data or {}).get('array') or {}
        disks = arr.get('disks') or []
        now = time.time()

        # Prune cache to only current disks
        valid_names = [d.get('name') for d in disks if isinstance(d, dict) and d.get('name')]
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

        async with httpx.AsyncClient(verify=self.legacy_ctx.verify_ssl, timeout=30, follow_redirects=True) as http:
            for disk in disks:
                if not isinstance(disk, dict):
                    continue
                name = disk.get('name') or ''
                device = disk.get('device')
                transport = (disk.get('transport') or '').lower()
                temp = disk.get('temp')
                try:
                    temp_val = int(float(temp)) if temp is not None else 0
                except Exception:
                    temp_val = 0

                # Determine if SMART should be fetched (legacy logic)
                need_fetch = False
                last = self.smart_cache.last_update(name) if self.smart_cache else 0.0
                if last == 0.0 or (now - last) >= self.smart_interval:
                    # skip if spun down and not usb
                    if temp_val == 0 and transport != 'usb':
                        # self.logger.info(f'SMART pending skipped: "{name}" seems spun down')
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

                    # Parse SMART table
                    smart_attrs = parse_smart_data(r.text, self.logger)
                    if len(smart_attrs) > 1:
                        self.smart_cache.update(name, smart_attrs, ts=now)
                        dirty = True
                        self.logger.info(f'SMART data updated for disk "{name}"')
                except Exception as e:
                    self.logger.error(f'SMART fetch error for "{name}": {e}')

        if dirty:
            self.smart_cache.save()

        # No MQTT entities from this collector
        return []


COLLECTOR = SmartDataCollector
