from typing import Dict, List
from app.collectors.base import EntityUpdate, SubscriptionCollector


class CpuSubscription(SubscriptionCollector):
    name = 'cpu'
    subscription_query = """
    subscription SystemMetricsCpu {
      systemMetricsCpu {
        percentTotal
      }
    }
    """

    def __init__(self, logger, interval: int):
        self.logger = logger
        self.interval = int(interval)

    async def parse(self, event: Dict) -> List[EntityUpdate]:
        data = event.get('systemMetricsCpu') or {}
        total = data.get('percentTotal')
        updates: List[EntityUpdate] = []

        if total is not None:
            try:
                total_pct = int(round(float(total)))
            except Exception:
                total_pct = 0

            updates.append(
                EntityUpdate(
                    sensor_type='sensor',
                    payload={
                        'name': 'CPU Utilization',
                        'unit_of_measurement': '%',
                        'icon': 'mdi:chip',
                        'state_class': 'measurement',
                    },
                    state=total_pct,
                    retain=False,
                    expire_after=max(self.interval * 2, 60),
                )
            )

        return updates


SUBSCRIPTION = CpuSubscription
