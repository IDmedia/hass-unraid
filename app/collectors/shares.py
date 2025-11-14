import time
import httpx
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

        # Throttle legacy fetches: only refresh every scan_interval * 10 (min 60s)
        self._legacy_refresh_period = max(self.interval * 10, 60)
        self._last_legacy_refresh: float = 0.0
        self._legacy_cache: Dict[str, Tuple[int, int]] = {}

    async def fetch(self) -> Dict:
        return await self.gql.query(self.query)

    async def parse(self, data: Dict) -> List[EntityUpdate]:
        shares = (data or {}).get('shares') or []
        updates: List[EntityUpdate] = []

        # Refresh legacy cache (once per period), only if credentials/context exists
        now = time.time()
        should_refresh_legacy = (
            self.legacy_ctx is not None and
            (now - self._last_legacy_refresh >= self._legacy_refresh_period)
        )

        if should_refresh_legacy and shares:
            try:
                await self._fetch_sharelist_bulk(shares)
                self._last_legacy_refresh = time.time()
            except Exception as e:
                self.logger.error(f'Bulk legacy ShareList refresh failed: {e}')

        for s in shares:
            name = s.get('name') or s.get('nameOrig') or 'unknown'
            share_nameorig = s.get('nameOrig') or s.get('name') or ''

            # Prefer cached legacy values when available; else fallback to GraphQL
            cached = self._legacy_cache.get(share_nameorig)
            if cached:
                used_kb, free_kb = cached
            else:
                used_kb = self._to_int_safe(s.get('used'))
                free_kb = self._to_int_safe(s.get('free'))
                size_kb = self._to_int_safe(s.get('size'))
                if free_kb == 0 and size_kb > 0:
                    free_kb = max(size_kb - used_kb, 0)

            total_kb = used_kb + free_kb
            used_pct = int(round((used_kb / total_kb) * 100)) if total_kb > 0 else 0

            payload = {
                'name': f'Share {str(name).title()} Usage',
                'unit_of_measurement': '%',
                'icon': 'mdi:folder-network',
                'state_class': 'measurement',
            }

            # Lower-case all attribute keys; add computed fields in KB
            attributes = normalize_keys_lower(s)
            attributes['used'] = used_kb
            attributes['free'] = free_kb
            attributes['size'] = total_kb

            updates.append(EntityUpdate(
                sensor_type='sensor',
                payload=payload,
                state=used_pct,
                attributes=attributes,
                retain=True,
                unique_id_suffix=share_nameorig or None
            ))

        return updates

    async def _fetch_sharelist_bulk(self, shares: List[Dict[str, Any]]) -> None:
        """
        Fetch ShareList.php once and populate self._legacy_cache for all shares in the dataset.
        Uses per-row aggregation:
          - Sum size (col 6) across all sub-rows (cache + disks) -> used_kb_total
          - Sum free (col 7) across all sub-rows, applying floor per disk row:
                adjusted_free_row_kb = max(free_row_kb - floor_kb, 0) if row_is_disk else free_row_kb
          - total_free_kb = sum(adjusted_free_row_kb)
        Stores values in KB (1000-based).
        """
        if not self.legacy_ctx:
            return

        # One cookie and csrf for the whole bulk operation
        cookie = await self.legacy_ctx.auth.get_cookie()
        csrf = await self.gql.get_csrf_token()
        headers = {'Cookie': cookie}
        params = {'path': 'Shares', 'all': 1, 'csrf_token': csrf}

        url = f'{self.legacy_ctx.http_base_url}/webGui/include/ShareList.php'
        async with httpx.AsyncClient(
            verify=self.legacy_ctx.verify_ssl,
            timeout=30,
            follow_redirects=True
        ) as http:
            r = await http.get(url, params=params, headers=headers)
            if r.status_code != httpx.codes.OK:
                self.logger.warning(f'ShareList.php bulk fetch returned HTTP {r.status_code}')
                return

            tree = etree.HTML(r.text)

            for s in shares:
                share_nameorig = s.get('nameOrig') or s.get('name') or ''
                if not share_nameorig:
                    continue

                # Floor is always KB (string/integer in GraphQL)
                try:
                    floor_kb = int(str(s.get('floor') or '0').strip())
                except Exception:
                    floor_kb = 0

                used_kb_total = 0
                free_kb_total = 0

                # Select sub-rows for this share by matching the "Recompute..." link's onclick
                # Example: onclick="computeShare('anime', $(this).parent())"
                sub_rows = tree.xpath(
                    f"//tr[td/a[@title='Recompute...' and contains(@onclick, \"computeShare('{share_nameorig}'\")]]"
                )

                for tr in sub_rows:
                    # Extract the row label from the first <td> (the text node after the refresh link)
                    label_texts = tr.xpath('td[1]/text()[last()]')
                    label = (label_texts[0] if label_texts else '').strip().lstrip('\u00A0').strip()
                    label_lower = label.lower()

                    # Parse per-row size/free from columns 6 and 7
                    size_text = (tr.xpath('td[6]/text()') or ['0'])[0].strip()
                    free_text = (tr.xpath('td[7]/text()') or ['0'])[0].strip()

                    size_bytes = self._parse_size_safe(size_text)
                    free_bytes = self._parse_size_safe(free_text)

                    size_kb = int(size_bytes / 1000)
                    free_kb = int(free_bytes / 1000)

                    # Sum size
                    used_kb_total += max(size_kb, 0)

                    # Apply per-disk floor only to array disks (labels like "Disk 1")
                    is_disk_row = label_lower.startswith('disk ')
                    if is_disk_row:
                        adjusted_free_kb = max(free_kb - floor_kb, 0)
                    else:
                        # Cache pools and other non-"Disk " rows: no floor subtraction
                        adjusted_free_kb = free_kb

                    free_kb_total += max(adjusted_free_kb, 0)

                # If for some reason there are no sub-rows (unexpected), fall back to main row totals
                if used_kb_total == 0 and free_kb_total == 0:
                    total_used_text = (next(iter(tree.xpath(
                        f'//td/a[text()="{share_nameorig}"]/ancestor::tr[1]/td[6]/text()'
                    ) or []), '0') or '0').strip()
                    total_free_text = (next(iter(tree.xpath(
                        f'//td/a[text()="{share_nameorig}"]/ancestor::tr[1]/td[7]/text()'
                    ) or []), '0') or '0').strip()

                    total_used_bytes = self._parse_size_safe(total_used_text)
                    total_free_bytes = self._parse_size_safe(total_free_text)

                    used_kb_total = int(total_used_bytes / 1000)
                    free_kb_total = int(total_free_bytes / 1000)

                # Store in cache
                self._legacy_cache[share_nameorig] = (used_kb_total, free_kb_total)

    @staticmethod
    def _parse_size_safe(text: str) -> int:
        """
        Parse a human-friendly size string into bytes; returns 0 on failure.
        """
        try:
            import humanfriendly
            return int(humanfriendly.parse_size(text))
        except Exception:
            return 0

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
