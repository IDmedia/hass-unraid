from lxml import etree
from typing import List
from app.legacy.base import LegacyChannel
from app.collectors.base import EntityUpdate, IncompleteData


class TemperatureChannel(LegacyChannel):
    name = 'temperature'
    channel = 'temperature'
    tracks_removals = True

    def __init__(self, logger, interval: int):
        self.logger = logger
        self.interval = int(interval)

    async def parse(self, msg_data: str) -> List[EntityUpdate]:
        tree = etree.HTML(msg_data) if msg_data and msg_data.strip() else None
        if tree is None:
            raise IncompleteData('empty temperature payload')
        sensors = tree.xpath('.//span[@title]')
        updates: List[EntityUpdate] = []

        for node in sensors:
            device_name = node.get('title') or ''
            device_value_raw = ''.join(node.itertext())
            device_value = ''.join(c for c in device_value_raw if c.isdigit() or c == '.')
            if not device_value:
                continue

            if 'rpm' in device_value_raw.lower():
                # Fans are published by update1 (all fans, numbered); publishing them here too
                # created a second entity for the same fan
                continue

            try:
                temp = float(device_value)
            except Exception:
                temp = 0.0
            updates.append(
                EntityUpdate(
                    sensor_type='sensor',
                    payload={
                        'name': f'{device_name} Temperature',
                        'unit_of_measurement': '°C',
                        'icon': 'mdi:thermometer',
                        'state_class': 'measurement',
                        'device_class': 'temperature',
                    },
                    state=temp,
                    retain=False,
                    expire_after=max(self.interval * 2, 60),
                )
            )

        return updates


CHANNEL = TemperatureChannel
