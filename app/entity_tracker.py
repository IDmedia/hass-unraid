import os
import json
import time
from typing import Any, Dict, Iterable, List, Optional


class EntityTracker:
    """
    Remembers which Home Assistant discovery entities belong to this node and which collector owns them,
    so entities that disappear from Unraid can be removed without a restart.

    Persisted to /data/<unraid_id>_entities.json as {sensor_id: {'owner', 'type', 'topic'}}.
    Owner None means the entity was found on the broker but nothing has published it yet
    (e.g. left over from an older version).

    Removal rules:
      - Snapshot owners (collectors that report their full set every cycle): an entity missing from
        MISSES_BEFORE_REMOVAL consecutive snapshots AND for at least grace_seconds is removed.
      - Entities owned by a collector that is no longer loaded (disabled or deleted) are removed.
      - Unowned entities that nothing claims within unclaimed_seconds are removed.
    """

    MISSES_BEFORE_REMOVAL = 3

    def __init__(self, unraid_id: str, data_dir: str, logger, grace_seconds: int = 600, unclaimed_seconds: int = 3600):
        self.logger = logger
        self.path = os.path.join(data_dir, f'{unraid_id}_entities.json')
        self.grace_seconds = grace_seconds
        self.unclaimed_seconds = unclaimed_seconds
        self.entities: Dict[str, Dict[str, Any]] = {}
        self._last_seen: Dict[str, float] = {}
        self._misses: Dict[str, int] = {}
        self._dirty = False
        self.load()

    def load(self) -> None:
        if not os.path.exists(self.path):
            return
        try:
            with open(self.path, 'r', encoding='utf-8') as f:
                data = json.load(f) or {}
            self.entities = {k: v for k, v in data.items() if isinstance(v, dict)}
        except Exception as e:
            self.logger.error(f'Failed to load entity registry: {e}')
            self.entities = {}
        now = time.monotonic()
        self._last_seen = {sid: now for sid in self.entities}

    def save(self) -> None:
        if not self._dirty:
            return
        try:
            with open(self.path, 'w', encoding='utf-8') as f:
                json.dump(self.entities, f, indent=2, sort_keys=True)
            self._dirty = False
        except Exception as e:
            self.logger.error(f'Failed to save entity registry: {e}')

    def get(self, sid: str) -> Optional[Dict[str, Any]]:
        return self.entities.get(sid)

    def mark_published(self, sid: str, owner: str, sensor_type: str, topic: str) -> None:
        record = {'owner': owner, 'type': sensor_type, 'topic': topic}
        if self.entities.get(sid) != record:
            self.entities[sid] = record
            self._dirty = True
        self._last_seen[sid] = time.monotonic()
        self._misses.pop(sid, None)

    def note_on_broker(self, sid: str, sensor_type: str, topic: str) -> None:
        """Record a discovery config found on the broker. Known entities are left untouched."""
        if sid in self.entities:
            return
        self.entities[sid] = {'owner': None, 'type': sensor_type, 'topic': topic}
        self._last_seen[sid] = time.monotonic()
        self._dirty = True

    def forget(self, sid: str) -> None:
        if self.entities.pop(sid, None) is not None:
            self._dirty = True
        self._last_seen.pop(sid, None)
        self._misses.pop(sid, None)

    def stale_in_snapshot(self, owner: str, present: Iterable[str]) -> List[str]:
        present_set = set(present)
        now = time.monotonic()
        stale: List[str] = []
        for sid, record in self.entities.items():
            if record.get('owner') != owner or sid in present_set:
                continue
            misses = self._misses.get(sid, 0) + 1
            self._misses[sid] = misses
            if misses >= self.MISSES_BEFORE_REMOVAL and now - self._last_seen.get(sid, now) >= self.grace_seconds:
                stale.append(sid)
        return stale

    def orphans(self, active_owners: Iterable[str]) -> List[str]:
        active = set(active_owners)
        now = time.monotonic()
        out: List[str] = []
        for sid, record in self.entities.items():
            owner = record.get('owner')
            if owner is None:
                if now - self._last_seen.get(sid, now) >= self.unclaimed_seconds:
                    out.append(sid)
            elif owner not in active:
                out.append(sid)
        return out
