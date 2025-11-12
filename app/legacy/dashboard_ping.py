from typing import List
from app.legacy.base import LegacyChannel
from app.collectors.base import EntityUpdate


class DashboardPingChannel(LegacyChannel):
    name = 'dashboard_ping'
    channel = 'dashboardPing'

    def __init__(self, logger, interval: int):
        self.logger = logger
        self.interval = int(interval)

    async def parse(self, msg_data: str) -> List[EntityUpdate]:
        return []


CHANNEL = DashboardPingChannel
