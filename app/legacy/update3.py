import json
from typing import List
from app.legacy.base import LegacyChannel
from app.collectors.base import EntityUpdate


class Update3Channel(LegacyChannel):
    name = 'update3'
    channel = 'update3'

    def __init__(self, logger, interval: int):
        self.logger = logger
        self.interval = int(interval)

    async def parse(self, msg_data: str) -> List[EntityUpdate]:
        """
        Replicates legacy behavior for network throughput:
        - Publish per-port Download/Upload Throughput (port_name) in Mbit/s.
        - Only for ports starting with 'eth'.
        Uses the numeric fields provided in 'port' entries when available.
        Non-retained with expire_after to go unavailable if stream stops.
        """
        updates: List[EntityUpdate] = []

        try:
            data = json.loads(msg_data)
        except Exception:
            return updates

        ports = data.get('port') or []
        for entry in ports:
            if not isinstance(entry, (list, tuple)) or len(entry) < 3:
                continue

            port_name = str(entry[0])
            rx_bps = float(entry[3]) if len(entry) > 3 and isinstance(entry[3], (int, float)) else 0.0
            tx_bps = float(entry[4]) if len(entry) > 4 and isinstance(entry[4], (int, float)) else 0.0

            download_mbps = round(rx_bps / 1_000_000.0, 1)
            upload_mbps = round(tx_bps / 1_000_000.0, 1)

            updates.append(
                EntityUpdate(
                    sensor_type='sensor',
                    payload={
                        'name': f'Download Throughput ({port_name})',
                        'unit_of_measurement': 'Mbit/s',
                        'icon': 'mdi:download',
                        'state_class': 'measurement',
                    },
                    state=download_mbps,
                    retain=False,
                    expire_after=max(self.interval * 2, 60),
                )
            )
            updates.append(
                EntityUpdate(
                    sensor_type='sensor',
                    payload={
                        'name': f'Upload Throughput ({port_name})',
                        'unit_of_measurement': 'Mbit/s',
                        'icon': 'mdi:upload',
                        'state_class': 'measurement',
                    },
                    state=upload_mbps,
                    retain=False,
                    expire_after=max(self.interval * 2, 60),
                )
            )

        return updates


CHANNEL = Update3Channel
