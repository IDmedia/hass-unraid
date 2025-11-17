import time
from app.utils import parse_smart_data
from typing import Any, Dict, List, Optional
from .base import EntityUpdate, QueryCollector


class SmartDataCollector(QueryCollector):
    """
    Legacy SMART attributes fetcher:
      - Queries GraphQL for parities, disks, caches
      - For each device needing update, POST to SmartInfo.php with form-encoded body via LegacyHTTPContext
      - Parses SMART table into snake_case attributes and stores in SmartCache
    """

    name = 'smart_data'
    requires_legacy_auth = True
    uses_smart_cache = True
    query = """
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

    def __init__(self, gql_client, logger, interval: int, legacy_ctx: Optional[Any] = None, smart_cache=None):
        self.gql = gql_client
        self.logger = logger
        self.interval = int(interval)
        self.legacy_ctx = legacy_ctx
        self.smart_cache = smart_cache
        self.smart_interval = 1800

    async def fetch(self) -> Dict[str, Any]:
        try:
            return await self.gql.query(self.query)
        except Exception as e:
            self.logger.error(f'SMART devices GraphQL query failed: {e}')
            return {}

    async def parse(self, data: Dict[str, Any]) -> List[EntityUpdate]:
        arr = (data or {}).get('array') or {}
        parities = arr.get('parities') or []
        disks = arr.get('disks') or []
        caches = arr.get('caches') or []

        devices: List[Dict[str, Any]] = []
        for group in (parities, disks, caches):
            if isinstance(group, list):
                devices.extend([d for d in group if isinstance(d, dict)])

        now = time.time()
        valid_names = [d.get('name') for d in devices if d.get('name')]

        if self.smart_cache and valid_names:
            self.smart_cache.prune_to(valid_names)

        if not self.legacy_ctx or not self.smart_cache:
            return []

        dirty = False
        for dev in devices:
            name = dev.get('name') or ''
            device = dev.get('device')
            transport = (dev.get('transport') or '').lower()
            temp = dev.get('temp')

            try:
                temp_val = int(float(temp)) if temp is not None else 0
            except Exception:
                temp_val = 0

            last = self.smart_cache.last_update(name) if self.smart_cache else 0.0
            need_fetch = False
            if last == 0.0 or (now - last) >= self.smart_interval:
                if temp_val == 0 and transport != 'usb':
                    need_fetch = False
                else:
                    need_fetch = True

            if not need_fetch or not device:
                continue

            form = {
                'cmd': 'attributes',
                'port': device,
                'name': name,
            }

            r = await self.legacy_ctx.http_post_form('/webGui/include/SmartInfo.php', form=form, timeout=30)
            if r.status_code != 200:
                self.logger.warning(f'SMART fetch failed for "{name}" (HTTP {r.status_code})')
                continue

            ctype = r.headers.get('Content-Type', '')
            if 'html' not in ctype.lower():
                self.logger.warning(f'SMART fetch for "{name}" returned non-HTML content-type: {ctype}')
                continue

            try:
                smart_attrs = parse_smart_data(r.text, self.logger)
                if len(smart_attrs) > 1:
                    self.smart_cache.update(name, smart_attrs, ts=now)
                    dirty = True
                    self.logger.info(f'SMART data updated for device "{name}"')
            except Exception as e:
                self.logger.error(f'Failed to parse SMART data for "{name}": {e}')

        if dirty:
            self.smart_cache.save()

        return []


COLLECTOR = SmartDataCollector
