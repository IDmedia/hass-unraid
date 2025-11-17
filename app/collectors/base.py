from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Protocol


@dataclass
class EntityUpdate:
    sensor_type: str  # 'sensor', 'binary_sensor', 'button', etc.
    payload: Dict[str, Any]  # Includes 'name', optional 'icon', etc.
    state: Any
    attributes: Optional[Dict[str, Any]] = None
    retain: bool = False
    device_overrides: Optional[Dict[str, Any]] = None
    unique_id_suffix: Optional[str] = None
    expire_after: Optional[int] = None  # Per entity


class QueryCollector(Protocol):
    name: str
    interval: int
    query: Optional[str]

    async def fetch(self) -> Dict[str, Any]:
        ...

    async def parse(self, data: Dict[str, Any]) -> List[EntityUpdate]:
        ...


class SubscriptionCollector(Protocol):
    name: str
    interval: int  # Publish throttle interval (seconds)
    subscription_query: str

    async def parse(self, event: Dict[str, Any]) -> List[EntityUpdate]:
        ...
