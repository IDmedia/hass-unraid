import re
from typing import List
from app.legacy.base import LegacyChannel
from app.collectors.base import EntityUpdate


class Update1Channel(LegacyChannel):
    name = 'update1'
    channel = 'update1'

    def __init__(self, logger, interval: int):
        self.logger = logger
        self.interval = int(interval)

    async def parse(self, msg_data: str) -> List[EntityUpdate]:
        """
        Replicates legacy behavior:
        - Memory usage: publish the first four percentages as RAM/Flash/Log/Docker Usage.
          This matches the old regex-based approach.
        - Fan speeds: publish Fan {n} Speed sensors from RPM readings.
        All entities are non-retained and include expire_after to avoid lingering if stream stops.
        """
        updates: List[EntityUpdate] = []

        categories = ['RAM', 'Flash', 'Log', 'Docker']
        percents = re.findall(r'(\d+%)', msg_data)
        for cat, pct in zip(categories, percents[:len(categories)]):
            try:
                value = int(''.join(c for c in pct if c.isdigit()))
            except Exception:
                continue
            updates.append(
                EntityUpdate(
                    sensor_type='sensor',
                    payload={
                        'name': f'{cat} Usage',
                        'unit_of_measurement': '%',
                        'icon': 'mdi:memory',
                        'state_class': 'measurement',
                    },
                    state=value,
                    retain=False,
                    expire_after=max(self.interval * 2, 60),
                )
            )

        fan_rpms = re.findall(r'(\d+\s*RPM)', msg_data, flags=re.IGNORECASE)
        for idx, rpm_text in enumerate(fan_rpms, start=1):
            try:
                rpm_val = int(''.join(c for c in rpm_text if c.isdigit()))
            except Exception:
                continue
            updates.append(
                EntityUpdate(
                    sensor_type='sensor',
                    payload={
                        'name': f'Fan {idx} Speed',
                        'unit_of_measurement': 'RPM',
                        'icon': 'mdi:fan',
                        'state_class': 'measurement',
                    },
                    state=rpm_val,
                    retain=False,
                    expire_after=max(self.interval * 2, 60),
                )
            )

        return updates


CHANNEL = Update1Channel
