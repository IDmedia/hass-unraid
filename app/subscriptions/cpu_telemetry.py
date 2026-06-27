from typing import Dict, List
from app.collectors.base import EntityUpdate, SubscriptionCollector


class CpuTelemetrySubscription(SubscriptionCollector):
    name = 'cpu_telemetry'
    subscription_query = """
    subscription SystemMetricsCpuTelemetry {
      systemMetricsCpuTelemetry {
        id
        totalPower
        temp
      }
    }
    """

    def __init__(self, logger, interval: int):
        self.logger = logger
        self.interval = int(interval)

    @staticmethod
    def _first_num(val):
        # temp/power arrive as a list (per package); fall back to scalar
        if isinstance(val, list):
            val = val[0] if val else None
        if val is None:
            return None
        try:
            return float(val)
        except (TypeError, ValueError):
            return None

    async def parse(self, event: Dict) -> List[EntityUpdate]:
        data = event.get('systemMetricsCpuTelemetry') or {}
        updates: List[EntityUpdate] = []
        expire = max(self.interval * 2, 60)

        temp = self._first_num(data.get('temp'))
        if temp is not None:
            updates.append(EntityUpdate(
                sensor_type='sensor',
                payload={
                    'name': 'CPU Temperature',
                    'unit_of_measurement': '°C',
                    'icon': 'mdi:thermometer',
                    'state_class': 'measurement',
                    'device_class': 'temperature',
                },
                state=round(temp, 1),
                retain=False,
                expire_after=expire,
            ))

        power = self._first_num(data.get('totalPower'))
        if power is not None:
            updates.append(EntityUpdate(
                sensor_type='sensor',
                payload={
                    'name': 'CPU Power',
                    'unit_of_measurement': 'W',
                    'icon': 'mdi:flash',
                    'state_class': 'measurement',
                    'device_class': 'power',
                },
                state=round(power, 1),
                retain=False,
                expire_after=expire,
            ))

        return updates


SUBSCRIPTION = CpuTelemetrySubscription
