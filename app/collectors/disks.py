import re
from app.utils import normalize_keys_lower
from .base import EntityUpdate, QueryCollector
from typing import Any, Dict, List, Optional, Tuple


class DisksCollector(QueryCollector):
    name = 'disks'
    uses_smart_cache = True  # loader will pass smart_cache
    query = """
    query Array {
      array {
        disks {
          idx
          name
          device
          size
          status
          rotational
          temp
          numReads
          numWrites
          numErrors
          fsSize
          fsFree
          fsUsed
          exportable
          type
          warning
          critical
          fsType
          comment
          format
          transport
          color
          isSpinning
        }
        caches {
          idx
          name
          device
          size
          status
          rotational
          temp
          numReads
          numWrites
          numErrors
          fsSize
          fsFree
          fsUsed
          exportable
          type
          warning
          critical
          fsType
          comment
          format
          transport
          color
          isSpinning
        }
        boot {
          idx
          name
          device
          size
          status
          rotational
          temp
          numReads
          numWrites
          numErrors
          fsSize
          fsFree
          fsUsed
          exportable
          type
          warning
          critical
          fsType
          comment
          format
          transport
          color
          isSpinning
        }
        parities {
          idx
          name
          device
          size
          status
          rotational
          temp
          numReads
          numWrites
          numErrors
          fsSize
          fsFree
          fsUsed
          exportable
          type
          warning
          critical
          fsType
          comment
          format
          transport
          color
          isSpinning
        }
      }
    }
    """

    def __init__(self, gql_client, logger, interval: int, smart_cache=None):
        self.gql = gql_client
        self.logger = logger
        self.interval = int(interval)
        self.smart_cache = smart_cache  # accept injected SmartCache

    async def fetch(self) -> Dict[str, Any]:
        return await self.gql.query(self.query)

    async def parse(self, data: Dict[str, Any]) -> List[EntityUpdate]:
        array = (data or {}).get('array') or {}
        devices: List[Dict[str, Any]] = []

        for key in ('disks', 'caches', 'parities'):
            items = array.get(key) or []
            if isinstance(items, list):
                devices.extend(items)

        boot = array.get('boot')
        if isinstance(boot, dict):
            devices.append(boot)

        updates: List[EntityUpdate] = []
        for d in devices:
            label = self._display_label(d)
            temp_value = self._safe_temp(d.get('temp'))

            attrs = normalize_keys_lower(d)

            if self.smart_cache and d.get('name'):
                entry = self.smart_cache.get(d['name'])
                if entry and entry.get('data'):
                    attrs['smart_attributes'] = entry['data']

            fs_size = d.get('fsSize')
            fs_used = d.get('fsUsed')
            if self._is_number(fs_size) and self._is_number(fs_used) and int(fs_size) > 0:
                try:
                    attrs['fs_used_pct'] = int(round((int(fs_used) / int(fs_size)) * 100))
                except Exception:
                    pass

            updates.append(
                EntityUpdate(
                    sensor_type='sensor',
                    payload={
                        'name': label,
                        'unit_of_measurement': '°C',
                        'device_class': 'temperature',
                        'icon': 'mdi:harddisk',
                        'state_class': 'measurement',
                    },
                    state=temp_value,
                    attributes=attrs,
                    retain=True,
                    unique_id_suffix=d.get('id') or d.get('name'),
                )
            )

        return updates

    @staticmethod
    def _is_number(v: Any) -> bool:
        try:
            int(v)
            return True
        except Exception:
            try:
                float(v)
                return True
            except Exception:
                return False

    @staticmethod
    def _safe_temp(temp: Any) -> int:
        try:
            if temp is None:
                return 0
            return int(round(float(temp)))
        except Exception:
            return 0

    @staticmethod
    def _split_name_number(raw: str) -> Tuple[str, Optional[str]]:
        if not raw:
            return '', None
        m = re.match(r'^([a-zA-Z_]+?)(\d+)$', raw)
        if m:
            return m.group(1), m.group(2)
        return raw, None

    @staticmethod
    def _pretty_words(s: str) -> str:
        return s.replace('_', ' ').strip().title()

    @staticmethod
    def _join_tokens(*parts: Any) -> str:
        out: List[str] = []
        for p in parts:
            if p is None:
                continue
            if isinstance(p, (int, float)):
                out.append(str(p))
                continue
            sp = str(p).strip()
            if sp:
                out.append(sp)
        return ' '.join(out)

    def _display_label(self, d: Dict[str, Any]) -> str:
        raw_name = (d.get('name') or '').strip()
        dtype = (d.get('type') or '').upper()
        base, num = self._split_name_number(raw_name.lower())
        if dtype == 'DATA':
            if base == 'disk' and num:
                return self._join_tokens('Disk', num)
            return self._join_tokens('Disk', self._pretty_words(raw_name))
        if dtype == 'CACHE':
            pretty = self._pretty_words(base if base else raw_name)
            return self._join_tokens('Disk', pretty, num)
        if dtype == 'PARITY':
            return self._join_tokens('Disk', 'Parity', num)
        if dtype == 'FLASH':
            return 'Disk Flash'
        return self._join_tokens('Disk', self._pretty_words(raw_name))


COLLECTOR = DisksCollector
