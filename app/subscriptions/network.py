import re
from typing import Any, Dict, List
from app.collectors.base import EntityUpdate, SubscriptionCollector

# Physical ports only; bridges/bonds/veths carry the same traffic again
_PORT_RE = re.compile(r'^eth\d+$')


class NetworkSubscription(SubscriptionCollector):
    """
    Per-port throughput from the GraphQL metrics API (Unraid API 4.35+). Publishes the same
    "Download/Upload Throughput (ethN)" entities as the legacy update3 channel, which then only
    fills in for ports this subscription does not cover or when it is unavailable.
    """

    name = 'network'
    subscription_query = """
    subscription SystemMetricsNetwork {
      systemMetricsNetwork {
        name
        operstate
        rxSec
        txSec
        receiveErrors
        transmitErrors
        receiveDropped
        transmitDropped
      }
    }
    """

    def __init__(self, logger, interval: int):
        self.logger = logger
        self.interval = int(interval)

    @staticmethod
    def _mbit(bytes_per_sec: Any) -> float:
        try:
            return round(float(bytes_per_sec) * 8 / 1_000_000, 1)
        except (TypeError, ValueError):
            return 0.0

    async def parse(self, event: Dict) -> List[EntityUpdate]:
        ports = event.get('systemMetricsNetwork') or []
        updates: List[EntityUpdate] = []
        expire = max(self.interval * 2, 60)

        for port in ports:
            if not isinstance(port, dict):
                continue
            name = str(port.get('name') or '')
            if not _PORT_RE.match(name):
                continue

            attributes = {
                'operstate': port.get('operstate'),
                'receive_errors': port.get('receiveErrors'),
                'transmit_errors': port.get('transmitErrors'),
                'receive_dropped': port.get('receiveDropped'),
                'transmit_dropped': port.get('transmitDropped'),
            }
            for direction, key, icon in (('Download', 'rxSec', 'mdi:download'), ('Upload', 'txSec', 'mdi:upload')):
                updates.append(
                    EntityUpdate(
                        sensor_type='sensor',
                        payload={
                            'name': f'{direction} Throughput ({name})',
                            'unit_of_measurement': 'Mbit/s',
                            'icon': icon,
                            'state_class': 'measurement',
                        },
                        state=self._mbit(port.get(key)),
                        attributes=attributes,
                        retain=False,
                        expire_after=expire,
                    )
                )

        return updates


SUBSCRIPTION = NetworkSubscription
