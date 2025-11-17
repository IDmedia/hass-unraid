from typing import List, Protocol
from app.collectors.base import EntityUpdate


class LegacyChannel(Protocol):
    name: str            # module name/logging
    channel: str         # nchan sub name, e.g. 'temperature', 'update1'
    interval: int        # publish throttle (seconds)

    async def parse(self, msg_data: str) -> List[EntityUpdate]:
        """
        Parse the raw message payload (already extracted from the Nchan envelope)
        and return EntityUpdate entries to publish.
        """
        ...
