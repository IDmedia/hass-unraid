import os
import time
import json
from typing import Any, Dict, Iterable, Optional


class SmartCache:
    """
    Persistent SMART cache:
      store[disk_name] = {'data': <smart_attrs_dict>, 'last_update': <timestamp>}
    """

    def __init__(self, unraid_id: str, data_dir: str, logger):
        self.logger = logger
        self.path = os.path.join(data_dir, f'{unraid_id}_smart_cache.json')
        self.store: Dict[str, Dict[str, Any]] = {}
        self._dirty = False
        self.load()

    def load(self) -> None:
        if os.path.exists(self.path):
            try:
                with open(self.path, 'r', encoding='utf-8') as f:
                    self.store = json.load(f) or {}
                self.logger.info('SMART cache successfully loaded from disk')
            except Exception as e:
                self.logger.error(f'Failed to load SMART cache: {e}')
                self.store = {}
        else:
            self.store = {}

    def save(self) -> None:
        if not self._dirty:
            return
        try:
            with open(self.path, 'w', encoding='utf-8') as f:
                json.dump(self.store, f, indent=2)
            self._dirty = False
            self.logger.info('SMART cache saved to disk')
        except Exception as e:
            self.logger.error(f'Failed to save SMART cache: {e}')

    def update(self, disk_name: str, smart_attrs: Dict[str, Any], ts: Optional[float] = None) -> None:
        self.store[disk_name] = {
            'data': smart_attrs,
            'last_update': ts if ts is not None else time.time(),
        }
        self._dirty = True

    def get(self, disk_name: str) -> Optional[Dict[str, Any]]:
        return self.store.get(disk_name)

    def last_update(self, disk_name: str) -> float:
        entry = self.store.get(disk_name)
        if not entry:
            return 0.0
        return float(entry.get('last_update') or 0.0)

    def prune_to(self, valid_names: Iterable[str]) -> None:
        valid = set(valid_names)
        stale = [k for k in list(self.store.keys()) if k not in valid]
        if stale:
            for k in stale:
                del self.store[k]
            self._dirty = True
            self.logger.info(f'Removed stale SMART entries: {stale}')
