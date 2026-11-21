from typing import Any, Dict, List, Optional
from app.collectors.base import EntityUpdate, SubscriptionCollector


class MemorySubscription(SubscriptionCollector):
    """
    RAM usage from the GraphQL metrics API. Publishes the same "RAM Usage" entity as the legacy
    update1 channel, which then only fills in when this subscription is unavailable.
    """

    name = 'memory'
    subscription_query = """
    subscription SystemMetricsMemory {
      systemMetricsMemory {
        total
        used
        free
        available
        buffcache
        percentTotal
        swapTotal
        swapUsed
        percentSwapTotal
      }
    }
    """

    def __init__(self, logger, interval: int):
        self.logger = logger
        self.interval = int(interval)

    @staticmethod
    def _num(value: Any) -> Optional[float]:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    async def parse(self, event: Dict) -> List[EntityUpdate]:
        data = event.get('systemMetricsMemory') or {}
        percent = self._num(data.get('percentTotal'))
        if percent is None:
            return []

        attributes = {}
        for key, attr in (('total', 'total'), ('used', 'used'), ('free', 'free'), ('available', 'available'),
                          ('buffcache', 'buffcache'), ('swapTotal', 'swap_total'), ('swapUsed', 'swap_used')):
            value = self._num(data.get(key))
            if value is not None:
                attributes[attr] = int(value)
        swap_pct = self._num(data.get('percentSwapTotal'))
        if swap_pct is not None:
            attributes['swap_used_pct'] = int(round(swap_pct))

        return [
            EntityUpdate(
                sensor_type='sensor',
                payload={
                    'name': 'RAM Usage',
                    'unit_of_measurement': '%',
                    'icon': 'mdi:memory',
                    'state_class': 'measurement',
                },
                state=int(round(percent)),
                attributes=attributes,
                retain=False,
                expire_after=max(self.interval * 2, 60),
            )
        ]


SUBSCRIPTION = MemorySubscription
