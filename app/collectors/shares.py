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
        self._legacy_refresh_period = max(self.interval * 10, 60)
        self._per_share_timeout = 30
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
                    # Continue with existing or GraphQL-provided values
                # No continue; we'll use cached or GraphQL data

            cached = self._legacy_cache.get(share_nameorig)
            if cached is not None and isinstance(cached, tuple) and len(cached) == 2:
                used_kb, free_kb = cached
            else:
                # Fallback to GraphQL fields (bytes -> kB) if legacy cache is not available
                used_kb = int((s.get('used') or 0) / 1000)
                free_kb = int((s.get('free') or 0) / 1000)

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
            used_kb_total = int(total_used_bytes / 1000)
            free_kb_total = int(total_free_bytes / 1000)

        self._legacy_cache[share_nameorig] = (used_kb_total, free_kb_total)

    def _parse_share_rows(self, tree: etree._Element, share_nameorig: str, floor_kb: int) -> Tuple[int, int]:
        used_kb_total = 0
        free_kb_total = 0

        sub_rows = tree.xpath(
            f'//tr[td/a[@title="Recompute..." and contains(@onclick, "computeShare(\'{share_nameorig}\'")]]'
        )

        for tr in sub_rows:
            label_texts = tr.xpath('td[1]/text()[last()]')
            label = (label_texts[0] if label_texts else '').strip().lstrip('\u00A0').strip()
            label_lower = label.lower()

            size_text = (tr.xpath('td[6]/text()') or ['0'])[0].strip()
            free_text = (tr.xpath('td[7]/text()') or ['0'])[0].strip()

            size_bytes = self._parse_size_safe(size_text)
            free_bytes = self._parse_size_safe(free_text)

            size_kb = int(size_bytes / 1000)
            free_kb = int(free_bytes / 1000)

            used_kb_total += max(size_kb, 0)

            is_disk_row = label_lower.startswith('disk ')
            if is_disk_row:
                adjusted_free_kb = max(free_kb - floor_kb, 0)
            else:
                adjusted_free_kb = free_kb

            free_kb_total += max(adjusted_free_kb, 0)

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
