import re
from lxml import etree
from typing import List
from app.legacy.base import LegacyChannel
from app.collectors.base import EntityUpdate


class TemperatureChannel(LegacyChannel):
    name = 'temperature'
    channel = 'temperature'

    def __init__(self, logger, interval: int):
        self.logger = logger
        self.interval = int(interval)

    async def parse(self, msg_data: str) -> List[EntityUpdate]:
        tree = etree.HTML(msg_data)
        sensors = tree.xpath('.//span[@title]')
        updates: List[EntityUpdate] = []

        for node in sensors:
            device_name = node.get('title') or ''
            device_value_raw = ''.join(node.itertext())
            device_value = ''.join(c for c in device_value_raw if c.isdigit() or c == '.')
            if not device_value:
                continue

            if 'rpm' in device_value_raw.lower():
                pretty_name = re.sub('fan', '', device_name, flags=re.IGNORECASE).strip()
                try:
                    rpm = int(float(device_value))
                except Exception:
                    rpm = 0
                updates.append(
                    EntityUpdate(
                        sensor_type='sensor',
                        payload={
                            'name': f'Fan {pretty_name} Speed',
                            'unit_of_measurement': 'RPM',
                            'icon': 'mdi:fan',
                            'state_class': 'measurement',
                        },
                        state=rpm,
                        retain=False,
                        expire_after=max(self.interval * 2, 60),
                    )
                )
            else:
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
