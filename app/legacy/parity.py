import re
import json
import humanfriendly
from typing import Any, Dict, List
from app.legacy.base import LegacyChannel
from app.collectors.base import EntityUpdate


class ParityChannel(LegacyChannel):
    name = 'parity'
    channel = 'parity'
    inactivity_timeout = 0

    def __init__(self, logger, interval: int):
        self.logger = logger
        self.interval = int(interval)

    async def parse(self, msg_data: str) -> List[EntityUpdate]:
        """
        Parse legacy parity payload, e.g.:
          ["8 TB","2 minutes","18.3 GB (0.2 %)","139.8 MB/sec","15 hours, 52 minutes","0"]

        Publishes a single 'Parity Check' sensor with % state and attributes:
          - total_size (bytes)
          - elapsed_time (string)
          - current_position (bytes)
          - estimated_speed (bytes; 0 if not parseable)
          - estimated_finish (string)
          - sync_errors_corrected (int/string)
        """
        updates: List[EntityUpdate] = []

        try:
            data = json.loads(msg_data)
        except Exception:
            return updates

        if not isinstance(data, list) or len(data) < 5:
            return updates

        total_text = str(data[0]) if len(data) > 0 else ''
        elapsed_text = str(data[1]) if len(data) > 1 else ''
        position_text = str(data[2]) if len(data) > 2 else ''
        speed_text = str(data[3]) if len(data) > 3 else ''
        finish_text = str(data[4]) if len(data) > 4 else ''
        errors_val = data[5] if len(data) > 5 else 0

        position_size_bytes = 0
        pct_value = 0.0
        m = re.search(r'(.+?)\s*\(([\d.]+)\s*%\)', position_text)
        if m:
            size_part = m.group(1).strip()
            pct_part = m.group(2).strip()
            pct_value = _to_float_safe(pct_part, default=0.0)
            position_size_bytes = _parse_size_safe(size_part)
        else:
            position_size_bytes = _parse_size_safe(position_text)

        total_size_bytes = _parse_size_safe(total_text)
        estimated_speed_bytes = _parse_size_safe(speed_text)

        attributes: Dict[str, Any] = {
            'total_size': total_size_bytes,
            'elapsed_time': elapsed_text,
            'current_position': position_size_bytes,
            'estimated_speed': estimated_speed_bytes,
            'estimated_finish': finish_text,
            'sync_errors_corrected': errors_val,
        }

        updates.append(
            EntityUpdate(
                sensor_type='sensor',
                payload={
                    'name': 'Parity Check',
                    'unit_of_measurement': '%',
                    'icon': 'mdi:database-eye',
                    'state_class': 'measurement',
                },
                state=pct_value,
                attributes=attributes,
                retain=False,
                expire_after=max(self.interval * 2, 60),
                unique_id_suffix='parity_check',
            )
        )

        return updates


def _parse_size_safe(text: str) -> int:
    """
    Parse a human-friendly size string into bytes; returns 0 on failure.
    Examples: '8 TB' -> bytes, '18.3 GB' -> bytes, '139.8 MB/sec' -> 0 (not a plain size).
    """
    try:
        return int(humanfriendly.parse_size(text))
    except Exception:
        return 0


def _to_float_safe(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


CHANNEL = ParityChannel
