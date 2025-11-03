import httpx
import humanfriendly
from lxml import etree
from app.utils import normalize_keys_lower
from .base import QueryCollector, EntityUpdate
from typing import Dict, List, Any, Optional, Tuple


SHARES_QUERY = """
query Shares {
  shares {
    name
    free
    used
    size
    include
    exclude
    cache
    nameOrig
    comment
    allocator
    splitLevel
    floor
    cow
    color
    luksStatus
  }
}
"""


class SharesCollector(QueryCollector):
    name = 'shares'
    requires_legacy_auth = True

    def __init__(self, gql_client, logger, interval: int, legacy_ctx: Optional[Any] = None):
        self.gql = gql_client
        self.logger = logger
        self.interval = int(interval)
        self.query = SHARES_QUERY
        self.legacy_ctx = legacy_ctx

    async def fetch(self) -> Dict:
        return await self.gql.query(self.query)

    async def parse(self, data: Dict) -> List[EntityUpdate]:
        shares = (data or {}).get('shares') or []
        updates: List[EntityUpdate] = []

        for s in shares:
            name = s.get('name') or s.get('nameOrig') or 'unknown'

            # Base values from GraphQL
            used_g = int(s.get('used') or 0)
            size_g = int(s.get('size') or 0)
            free_g = int(s.get('free') or (size_g - used_g if size_g else 0))

            cache_mode = (s.get('cache') or '').lower()
            if self.legacy_ctx and cache_mode in ['no', 'yes', 'prefer'] and s.get('nameOrig'):
                try:
                    used_k, free_k = await self._fetch_share_usage_via_legacy(s['nameOrig'])
                    if used_k is not None and free_k is not None:
                        # Normalize to bytes
                        used_b = used_k * 1000
                        free_b = free_k * 1000
                    else:
                        used_b = used_g
                        free_b = free_g
                except Exception as e:
                    self.logger.error(f'Failed legacy ShareList enrichment for "{name}": {e}')
                    used_b = used_g
                    free_b = free_g
            else:
                used_b = used_g
                free_b = free_g

            # Compute share_disk_count from include (list or string)
            include_val = s.get('include')
            share_disk_count = self._count_includes(include_val)

            # Exclusive (if present in schema); assume not exclusive if missing
            exclusive_raw = (s.get('exclusive') or '').lower()
            if exclusive_raw == 'yes':
                share_disk_count = 1

            # Normalize floor to bytes
            floor_raw = s.get('floor') or 0
            floor_b = self._normalize_floor_to_bytes(floor_raw)

            # Apply floor per disk to free
            free_b_adjusted = max(free_b - (share_disk_count * floor_b), 0)

            total_b = used_b + free_b_adjusted
            used_pct = int(round((used_b / total_b) * 100)) if total_b > 0 else 0

            payload = {
                'name': f'Share {str(name).title()} Usage',
                'unit_of_measurement': '%',
                'icon': 'mdi:folder-network',
                'state_class': 'measurement',
            }

            # Lower-case all attribute keys
            attributes = normalize_keys_lower(s)
            attributes['computed_free_bytes'] = free_b_adjusted

            updates.append(EntityUpdate(
                sensor_type='sensor',
                payload=payload,
                state=used_pct,
                attributes=attributes,
                retain=True,
                unique_id_suffix=s.get('nameOrig') or s.get('name') or None
            ))

        return updates

    async def _fetch_share_usage_via_legacy(self, share_nameorig: str) -> Tuple[Optional[int], Optional[int]]:
        if not self.legacy_ctx:
            return (None, None)

        cookie = await self.legacy_ctx.auth.get_cookie()
        csrf = await self.gql.get_csrf_token()

        headers = {'Cookie': cookie}
        params = {'compute': share_nameorig, 'path': 'Shares', 'all': 1, 'csrf_token': csrf}
        url = f'{self.legacy_ctx.http_base_url}/webGui/include/ShareList.php'

        async with httpx.AsyncClient(verify=self.legacy_ctx.verify_ssl, timeout=600, follow_redirects=True) as http:
            r = await http.get(url, params=params, headers=headers)
            if r.status_code != httpx.codes.OK:
                self.logger.warning(f'ShareList.php for "{share_nameorig}" returned {r.status_code}')
                return (None, None)

            tree = etree.HTML(r.text)

            size_total_used_text = (next(iter(tree.xpath(f'//td/a[text()="{share_nameorig}"]/ancestor::tr[1]/td[6]/text()') or []), '0') or '0').strip()
            size_total_free_text = (next(iter(tree.xpath(f'//td/a[text()="{share_nameorig}"]/ancestor::tr[1]/td[7]/text()') or []), '0') or '0').strip()

            size_total_used = self._parse_size_safe(size_total_used_text)
            size_total_free = self._parse_size_safe(size_total_free_text)

            size_cache_used_text = (next(iter(tree.xpath(
                f'//td/a[text()="{share_nameorig}"]/following::tr[1]/td[1][not(contains(text(), "Disk "))]/../td[6]/text()'
            ) or []), '0') or '0').strip()
            size_cache_free_text = (next(iter(tree.xpath(
                f'//td/a[text()="{share_nameorig}"]/following::tr[1]/td[1][not(contains(text(), "Disk "))]/../td[7]/text()'
            ) or []), '0') or '0').strip()

            size_cache_used = self._parse_size_safe(size_cache_used_text)
            size_cache_free = self._parse_size_safe(size_cache_free_text)

            used_k = int(size_total_used / 1000)
            free_k = int((size_total_free - size_cache_free - size_cache_used) / 1000)

            return (used_k, free_k)

    @staticmethod
    def _parse_size_safe(text: str) -> int:
        try:
            return int(humanfriendly.parse_size(text))
        except Exception:
            return 0

    @staticmethod
    def _count_includes(include_val: Any) -> int:
        if include_val is None:
            return 0
        if isinstance(include_val, list):
            return len([x for x in include_val if str(x).strip()])
        if isinstance(include_val, str):
            items = [p.strip() for p in include_val.split(',')]
            return len([p for p in items if p and p != '*'])
        return 0

    @staticmethod
    def _normalize_floor_to_bytes(floor_raw: Any) -> int:
        try:
            floor_val = int(floor_raw)
        except Exception:
            return 0
        return floor_val if floor_val >= 10_000_000 else floor_val * 1000


COLLECTOR = SharesCollector
