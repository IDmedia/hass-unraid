import os
import json
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple


class EntityTracker:
    """
    Remembers which Home Assistant discovery entities belong to this node and which collector owns them,
    so entities that disappear from Unraid can be cleaned up without a restart.

    Persisted to /data/<unraid_id>_entities.json as
    {sensor_id: {'owner', 'type', 'topic', 'missing_since'?}}.
    Owner None means the entity was found on the broker but nothing has published it yet
    (e.g. left over from an older version).

    Lifecycle of an entity owned by a snapshot collector (one that reports its complete set every cycle):
      - Missing from MISSES_BEFORE_UNAVAILABLE consecutive snapshots: marked unavailable in Home Assistant.
        Short outages (array stopped, driver reload, service restart) never go further than this.
      - Reported again: available again, nothing lost.
      - Still missing after removal_seconds (wall clock, survives restarts): removed from Home Assistant.

    Other cleanup:
      - Entities owned by a collector that is no longer loaded (disabled in config or deleted) are removed.
      - Unowned entities that nothing claims within unclaimed_seconds are marked unavailable, and removed
        after removal_seconds like any other missing entity.
    """

    MISSES_BEFORE_UNAVAILABLE = 3
    REMOVAL_SECONDS = 7 * 24 * 3600
    UNCLAIMED_SECONDS = 3600
    # When two collectors produce the same entity, the other one takes over after the owner is quiet this long
    HANDOVER_SECONDS = 600

    def __init__(self, unraid_id: str, data_dir: str, logger,
                 removal_seconds: int = REMOVAL_SECONDS, unclaimed_seconds: int = UNCLAIMED_SECONDS):
        self.logger = logger
        self.path = os.path.join(data_dir, f'{unraid_id}_entities.json')
        self.removal_seconds = removal_seconds
        self.unclaimed_seconds = unclaimed_seconds
        self.entities: Dict[str, Dict[str, Any]] = {}
        self._last_seen: Dict[str, float] = {}
        self._misses: Dict[str, int] = {}
        self._expected: Dict[str, str] = {}
        self._dirty = False
        self.load()

    def expect(self, sid: str, owner: str) -> None:
        """
        Claim an entity for a collector that only publishes occasionally (e.g. parity during a check),
        so it is never treated as an unowned leftover.
        """
        self._expected[sid] = owner
        record = self.entities.get(sid)
        if record is not None and record.get('owner') is None:
            record['owner'] = owner
            record.pop('missing_since', None)
            self._dirty = True

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
            tmp = f'{self.path}.tmp'
            with open(tmp, 'w', encoding='utf-8') as f:
                json.dump(self.entities, f, indent=2, sort_keys=True)
            os.replace(tmp, self.path)
            self._dirty = False
        except Exception as e:
            self.logger.error(f'Failed to save entity registry: {e}')

    def get(self, sid: str) -> Optional[Dict[str, Any]]:
        return self.entities.get(sid)

    def seen_within(self, sid: str, seconds: float) -> bool:
        last = self._last_seen.get(sid)
        return last is not None and time.monotonic() - last < seconds

    def mark_published(self, sid: str, owner: str, sensor_type: str, topic: str) -> bool:
        """Record a publish. Returns True when the entity was marked missing before (it is back)."""
        previous = self.entities.get(sid) or {}
        was_missing = bool(previous.get('missing_since'))
        record = {'owner': owner, 'type': sensor_type, 'topic': topic}
        if previous != record:
            self.entities[sid] = record
            self._dirty = True
        self._last_seen[sid] = time.monotonic()
        self._misses.pop(sid, None)
        return was_missing

    def note_on_broker(self, sid: str, sensor_type: str, topic: str) -> None:
        """Record a discovery config found on the broker. Known entities are left untouched."""
        if sid in self.entities:
            return
        self.entities[sid] = {'owner': self._expected.get(sid), 'type': sensor_type, 'topic': topic}
        self._last_seen[sid] = time.monotonic()
        self._dirty = True

    def forget(self, sid: str) -> None:
        if self.entities.pop(sid, None) is not None:
            self._dirty = True
        self._last_seen.pop(sid, None)
        self._misses.pop(sid, None)

    def missing_in_snapshot(self, owner: str, present: Iterable[str]) -> Tuple[List[str], List[str]]:
        """
        Compare a collector's complete snapshot with what it owns.
        Returns (newly_unavailable, expired): entities to mark unavailable now, and entities to remove.
        """
        present_set = set(present)
        now = time.time()
        newly_unavailable: List[str] = []
        expired: List[str] = []
        for sid, record in self.entities.items():
            if record.get('owner') != owner or sid in present_set:
                continue
            missing_since = record.get('missing_since')
            if missing_since:
                if now - float(missing_since) >= self.removal_seconds:
                    expired.append(sid)
                continue
            misses = self._misses.get(sid, 0) + 1
            self._misses[sid] = misses
            if misses >= self.MISSES_BEFORE_UNAVAILABLE:
                record['missing_since'] = now
                self._dirty = True
                newly_unavailable.append(sid)
        return newly_unavailable, expired

    def orphans(self, active_owners: Iterable[str]) -> Tuple[List[str], List[str]]:
        """Returns (newly_unavailable, to_remove) for entities no loaded collector owns."""
        active = set(active_owners)
        now = time.monotonic()
        wall = time.time()
        newly_unavailable: List[str] = []
        to_remove: List[str] = []
        for sid, record in self.entities.items():
            owner = record.get('owner')
            if owner is not None:
                if owner not in active:
                    to_remove.append(sid)
                continue
            missing_since = record.get('missing_since')
            if missing_since:
                if wall - float(missing_since) >= self.removal_seconds:
                    to_remove.append(sid)
            elif now - self._last_seen.get(sid, now) >= self.unclaimed_seconds:
                record['missing_since'] = wall
                self._dirty = True
                newly_unavailable.append(sid)
        return newly_unavailable, to_remove
