import time
from lxml import etree
from urllib.parse import urljoin
from app.utils import normalize_keys_lower
from .base import EntityUpdate, QueryCollector
from typing import Any, Dict, List, Optional, Tuple


class SharesCollector(QueryCollector):
    name = 'shares'
    requires_legacy_auth = True

    query = """
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

    def __init__(self, gql_client, logger, interval: int, legacy_ctx: Optional[Any] = None):
        self.gql = gql_client
        self.logger = logger
        self.interval = int(interval)
        self.legacy_ctx = legacy_ctx

        # How often to refresh legacy per-share data
        self._legacy_refresh_period = max(self.interval * 10, 60)
        self._per_share_timeout = 30

        # Cache keyed by share_nameorig -> (used_kb, free_kb)
        self._legacy_cache: Dict[str, Tuple[int, int]] = {}
        self._legacy_last_refresh: Dict[str, float] = {}

    async def fetch(self) -> Dict:
        return await self.gql.query(self.query)

    async def parse(self, data: Dict) -> List[EntityUpdate]:
        shares = (data or {}).get('shares') or []
        updates: List[EntityUpdate] = []

        for s in shares:
            name = s.get('name') or s.get('nameOrig') or 'unknown'
            share_nameorig = s.get('nameOrig') or s.get('name') or ''
            if not share_nameorig:
                continue

            now = time.time()
            if self.legacy_ctx and (now - self._legacy_last_refresh.get(share_nameorig, 0.0) >= self._legacy_refresh_period):
                try:
                    floor_kb = self._to_int_safe(s.get('floor'))
                    await self._refresh_sharelist_one(share_nameorig, floor_kb)
                    self._legacy_last_refresh[share_nameorig] = time.time()
                except Exception as e:
                    self.logger.error(
                        f'Per-share legacy refresh failed for "{share_nameorig}": '
                        f'{type(e).__name__}: {e}'
                    )

            # Use data solely from the legacy cache
            cached = self._legacy_cache.get(share_nameorig)
            if cached is not None and isinstance(cached, tuple) and len(cached) == 2:
                used_kb, free_kb = cached
            else:
                self.logger.warning(f"No legacy data available for share '{share_nameorig}', skipping...")
                continue

            size_kb = used_kb + free_kb
            used_pct = int(round((used_kb / size_kb) * 100)) if size_kb > 0 else 0

            payload = {
                'name': f'Share {str(name).title()} Usage',
                'unit_of_measurement': '%',
                'icon': 'mdi:folder-network',
                'state_class': 'measurement',
            }

            attributes = normalize_keys_lower(s)
            attributes['used'] = used_kb
            attributes['free'] = free_kb
            attributes['size'] = size_kb

            updates.append(
                EntityUpdate(
                    sensor_type='sensor',
                    payload=payload,
                    state=used_pct,
                    attributes=attributes,
                    retain=True,
                    unique_id_suffix=share_nameorig or None,
                )
            )

        return updates

    async def _refresh_sharelist_one(self, share_nameorig: str, floor_kb: int) -> None:
        if not self.legacy_ctx:
            return

        headers = {
            'Origin': self.legacy_ctx.http_base_url,
            'Referer': urljoin(self.legacy_ctx.http_base_url + '/', 'Shares'),
            'X-Requested-With': 'XMLHttpRequest',
        }
        form = {
            'compute': share_nameorig,
            'path': 'Shares',
            'all': 1,
        }

        r = await self.legacy_ctx.http_post_form(
            '/webGui/include/ShareList.php',
            form=form,
            timeout=self._per_share_timeout,
            headers=headers,
        )
        if r.status_code != 200:
            self.logger.warning(f'ShareList.php for "{share_nameorig}" returned HTTP {r.status_code}')
            return

        ctype = r.headers.get('Content-Type', '')
        if 'html' not in ctype.lower():
            self.logger.warning(f'ShareList.php for "{share_nameorig}" returned non-HTML content-type: {ctype}')
            return

        tree = etree.HTML(r.text or '')
        if tree is None:
            self.logger.warning(f'ShareList.php for "{share_nameorig}" returned empty/invalid HTML')
            return

        used_kb_total, free_kb_total = self._parse_share_rows(tree, share_nameorig, floor_kb)

        if used_kb_total == 0 and free_kb_total == 0:
            # Fallback to the main row totals (typical for exclusive cache shares with no sub-rows)
            total_used_text = (
                next(
                    iter(
                        tree.xpath(
                            f'//td/a[text()="{share_nameorig}"]/ancestor::tr[1]/td[6]/text()'
                        ) or []
                    ),
                    '0',
                )
                or '0'
            ).strip()
            total_free_text = (
                next(
                    iter(
                        tree.xpath(
                            f'//td/a[text()="{share_nameorig}"]/ancestor::tr[1]/td[7]/text()'
                        ) or []
                    ),
                    '0',
                )
                or '0'
            ).strip()

            total_used_bytes = self._parse_size_safe(total_used_text)
            total_free_bytes = self._parse_size_safe(total_free_text)

            used_kb_total = max(int(total_used_bytes / 1000), 0)
            # Cache-only fallback: subtract floor once
            free_kb_total = max(int(total_free_bytes / 1000) - floor_kb, 0)

        self._legacy_cache[share_nameorig] = (used_kb_total, free_kb_total)

    def _parse_share_rows(self, tree: etree._Element, share_nameorig: str, floor_kb: int) -> Tuple[int, int]:
        """
        Parse all sub-rows for a given share (those with the per-row 'Recompute...' link).
        Logic:
          - Always include all used across all rows.
          - If any disk rows exist: include free only from disk rows (minus floor per disk); ignore cache/pool free.
          - If no disk rows exist (cache-only): include free from all rows (minus floor per row).
          - If there are no sub-rows at all, return (0, 0) to trigger the caller's fallback to the main row totals.
        """
        used_kb_total = 0
        free_kb_total = 0

        # Match sub-rows by looking for the recompute link for this share
        sub_rows = tree.xpath(
            f'//tr[td/a[@title="Recompute..." and contains(@onclick, "computeShare") and contains(@onclick, "{share_nameorig}")]]'
        )

        rows: List[Dict[str, int]] = []
        for tr in sub_rows:
            # Column 1 trailing text holds the label, e.g., "Disk 3" or a pool/cache label
            label_texts = tr.xpath('td[1]/text()[last()]')
            label = (label_texts[0] if label_texts else '').strip().lstrip('\u00A0').strip()
            label_lower = label.lower()

            # Used and Free are columns 6 and 7
            size_text = (tr.xpath('td[6]/text()') or ['0'])[0].strip()
            free_text = (tr.xpath('td[7]/text()') or ['0'])[0].strip()

            size_bytes = self._parse_size_safe(size_text)
            free_bytes = self._parse_size_safe(free_text)

            size_kb = max(int(size_bytes / 1000), 0)
            free_kb = max(int(free_bytes / 1000), 0)

            rows.append({
                'is_disk': label_lower.startswith('disk '),
                'used_kb': size_kb,
                'free_kb': free_kb,
            })

        # No sub-rows
        if not rows:
            return 0, 0

        # Always include all used (disk + cache/pool)
        used_kb_total = sum(r['used_kb'] for r in rows)

        has_disk = any(r['is_disk'] for r in rows)
        if has_disk:
            # Mixed share
            free_kb_total = 0
            for r in rows:
                if r['is_disk']:
                    adjusted_free_kb = max(r['free_kb'] - floor_kb, 0)
                    free_kb_total += adjusted_free_kb
                else:
                    # Subtract used space on cache
                    free_kb_total -= r['used_kb']
        else:
            # Cache-only
            free_kb_total = 0
            for r in rows:
                adjusted_free_kb = max(r['free_kb'] - floor_kb, 0)
                free_kb_total += adjusted_free_kb

        return used_kb_total, free_kb_total

    @staticmethod
    def _parse_size_safe(text: str) -> int:
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


COLLECTOR = SharesCollector
