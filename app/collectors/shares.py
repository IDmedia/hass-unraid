from typing import Dict, List, Any
from app.utils import normalize_keys_lower
from .base import QueryCollector, EntityUpdate


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
    requires_legacy_auth = False

    def __init__(self, gql_client, logger, interval: int, legacy_ctx: Any = None):
        self.gql = gql_client
        self.logger = logger
        self.interval = int(interval)
        self.query = SHARES_QUERY

    async def fetch(self) -> Dict:
        return await self.gql.query(self.query)

    async def parse(self, data: Dict) -> List[EntityUpdate]:
        shares = (data or {}).get('shares') or []
        updates: List[EntityUpdate] = []

        for s in shares:
            name = s.get('name') or 'unknown'

            # Values from GraphQL are in KB
            used_kb = self._to_int_safe(s.get('used'))
            free_kb = self._to_int_safe(s.get('free'))
            size_kb = self._to_int_safe(s.get('size'))

            # Fallback if free is missing but size is present (size is often 0 now)
            if free_kb == 0 and size_kb > 0:
                free_kb = max(size_kb - used_kb, 0)

            # Floor is always KB (reserved per included disk)
            floor_kb = self._to_int_safe(s.get('floor'))

            # Disk count from include
            disk_count = self._count_includes(s.get('include'))

            # Apply floor per disk to free
            adjusted_free_kb = max(free_kb - (disk_count * floor_kb), 0)

            total_kb = used_kb + adjusted_free_kb
            used_pct = int(round((used_kb / total_kb) * 100)) if total_kb > 0 else 0

            payload = {
                'name': f'Share {str(name).title()} Usage',
                'unit_of_measurement': '%',
                'icon': 'mdi:folder-network',
                'state_class': 'measurement',
            }

            attributes = normalize_keys_lower(s)

            updates.append(EntityUpdate(
                sensor_type='sensor',
                payload=payload,
                state=used_pct,
                attributes=attributes,
                retain=True,
                unique_id_suffix=s.get('nameOrig') or s.get('name') or None
            ))

        return updates

    @staticmethod
    def _to_int_safe(v: Any) -> int:
        try:
            return int(v or 0)
        except Exception:
            try:
                return int(float(v))
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


COLLECTOR = SharesCollector
