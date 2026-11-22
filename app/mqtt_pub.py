import json
import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional, Set
from gmqtt import Client as MQTTClient, Message
from .entity_tracker import EntityTracker
from .utils import calculate_hash, normalize_str

CONNECTIVITY_OWNER = 'connectivity'


class MQTTPublisher:
    def __init__(
        self,
        name: str,
        mqtt_config: Dict[str, Any],
        loop: asyncio.AbstractEventLoop,
        logger,
        scan_interval: int,
        tracker: Optional[EntityTracker] = None,
        active_owners: Optional[Set[str]] = None,
        fallback_owners: Optional[Set[str]] = None,
        inventory_owners: Optional[Dict[str, Any]] = None,
    ):
        self.name = name
        self.loop = loop
        self.logger = logger
        self.scan_interval = scan_interval
        self.mqtt_config = mqtt_config
        self.mqtt_connected = False
        self.parser_hashes: Dict[str, str] = {}
        self._stopping = False
        self.global_device_overrides: Dict[str, Any] = {}
        self._unraid_id = normalize_str(self.name)
        self._device_identifier = f'unraid_{self._unraid_id}'.lower()
        self._availability_topic = f'unraid/{self._unraid_id}/connectivity/state'
        self.tracker = tracker
        self.active_owners: Set[str] = set(active_owners or set()) | {CONNECTIVITY_OWNER}
        # Owners that only fill in when no other collector provides the same entity (legacy channels)
        self.fallback_owners: Set[str] = set(fallback_owners or set())
        self._conflicts_logged: Set[tuple] = set()
        # Owners whose entities are inventory (disks, shares): they keep their last state and attributes
        # while the server or this container is down, and go "present: false" instead of unavailable
        # when Unraid stops reporting them. Value: state to publish when missing (None keeps the last state).
        self.inventory_owners: Dict[str, Any] = dict(inventory_owners or {})
        # Retained attribute topics being read back to mark an inventory entity missing: topic -> sid
        self._pending_missing: Dict[str, str] = {}

        self._client = MQTTClient(
            self.name,
            will_message=Message(self._availability_topic, 'OFF', retain=True),
        )
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self._on_message

        username = mqtt_config.get('username')
        password = mqtt_config.get('password')
        if username or password:
            self._client.set_auth_credentials(username, password)

        self._connect_task = asyncio.create_task(self._connect_loop())
        self._connectivity_task = asyncio.create_task(self._periodic_connectivity_update())
        self._sweep_task = asyncio.create_task(self._orphan_sweep_loop()) if tracker else None

    def set_device_overrides(self, overrides: Dict[str, Any]) -> None:
        """
        Set or update global device overrides (e.g., {'sw_version': '7.2.0'}).
        These will be merged into the device block for all published entities.
        """
        self.global_device_overrides.update(overrides or {})

    async def _connect_loop(self) -> None:
        host = self.mqtt_config.get('host')
        port = self.mqtt_config.get('port', 1883)
        backoff = 5

        while True:
            try:
                self.logger.info(f'Connecting to MQTT server... ({host}:{port})')
                await self._client.connect(host, port)
                return
            except Exception as e:
                self.logger.error(f'Failed to connect to MQTT server: {e}')
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    def _on_connect(self, client, flags, rc, properties) -> None:
        self.logger.info('Successfully connected to MQTT server')
        self.mqtt_connected = True
        # Republish every config after a (re)connect in case the broker lost its retained messages
        self.parser_hashes.clear()
        self.mqtt_status(True)
        if self.tracker:
            # Retained discovery configs tell us which entities already exist in Home Assistant
            self._client.subscribe('homeassistant/+/+/config', qos=0)
            for topic in self._pending_missing:
                self._client.subscribe(topic, qos=0)

    def _on_disconnect(self, client, packet, exc=None) -> None:
        level = self.logger.info if self._stopping else self.logger.error
        level('Disconnected from MQTT server')
        self.mqtt_connected = False
        try:
            self.mqtt_status(False)
        except Exception as e:
            self.logger.exception(f'Failed to update connectivity status on disconnect: {e}')

    def _on_message(self, client, topic, payload, qos, properties) -> None:
        if not self.tracker:
            return
        if topic in self._pending_missing:
            self._publish_missing_attributes(topic, payload)
            return
        parts = topic.split('/')
        if len(parts) != 4 or parts[0] != 'homeassistant' or parts[3] != 'config':
            return
        sensor_type, sid = parts[1], parts[2]
        if not sid.startswith(f'{self._unraid_id}_'):
            return

        if not payload:
            self.tracker.forget(sid)
            return

        try:
            cfg = json.loads(payload)
            identifiers = (cfg.get('device') or {}).get('identifiers')
            if isinstance(identifiers, list):
                identifiers = identifiers[0] if identifiers else None
            if identifiers != self._device_identifier:
                return
            # state_topic is unraid/<unraid_id>/<topic_name>/state
            state_parts = str(cfg.get('state_topic') or '').split('/')
            if len(state_parts) != 4:
                return
            self.tracker.note_on_broker(sid, sensor_type, state_parts[2])
        except Exception:
            return

    def mqtt_status(self, connected: bool) -> None:
        payload = {'name': 'Connectivity', 'device_class': 'connectivity'}
        self.publish(payload, 'binary_sensor', 'ON' if connected else 'OFF', retain=True,
                     owner=CONNECTIVITY_OWNER, with_availability=False)

    async def _periodic_connectivity_update(self) -> None:
        last = None
        while True:
            try:
                current_state = 'ON' if self.mqtt_connected else 'OFF'
                if last != current_state:
                    self.mqtt_status(self.mqtt_connected)
                    last = current_state
            except Exception as e:
                self.logger.exception(f'Error during connectivity update: {e}')
            await asyncio.sleep(60)

    async def _orphan_sweep_loop(self) -> None:
        while True:
            await asyncio.sleep(60)
            try:
                if self.mqtt_connected:
                    newly_unavailable, to_remove = self.tracker.orphans(self.active_owners)
                    for sid in newly_unavailable:
                        self.logger.info(f'Entity "{sid}" is not provided by any collector; marking unavailable')
                        self._mark_missing(sid)
                    for sid in to_remove:
                        owner = self.tracker.get(sid).get('owner')
                        reason = f'collector "{owner}" is disabled' if owner else 'not provided by any collector for 7 days'
                        self._remove_entity(sid, reason)
                self.tracker.save()
            except Exception as e:
                self.logger.exception(f'Error during entity sweep: {e}')

    def _remove_entity(self, sid: str, reason: str) -> None:
        record = self.tracker.get(sid) if self.tracker else None
        if not record:
            return
        self.logger.info(f'Removing entity "{sid}" ({reason})')
        topic = record.get('topic')
        self._client.publish(f'homeassistant/{record.get("type")}/{sid}/config', '', retain=True)
        if topic:
            # Clear retained state/attributes so nothing lingers on the broker
            self._client.publish(f'unraid/{self._unraid_id}/{topic}/state', '', retain=True)
            self._client.publish(f'unraid/{self._unraid_id}/{topic}/attributes', '', retain=True)
            self._client.publish(f'unraid/{self._unraid_id}/{topic}/availability', '', retain=True)
        self.parser_hashes.pop(sid, None)
        self.tracker.forget(sid)

    def _set_entity_available(self, topic: str, available: bool) -> None:
        self._client.publish(f'unraid/{self._unraid_id}/{topic}/availability', 'ON' if available else 'OFF', retain=True)

    def _mark_missing(self, sid: str) -> None:
        """
        Unavailable hides all attributes in Home Assistant. Inventory entities stay readable instead: their
        retained attributes are read back from the broker and republished with present: false.
        """
        record = self.tracker.get(sid) or {}
        topic = record.get('topic')
        if not topic:
            return
        owner = record.get('owner')
        if owner not in self.inventory_owners:
            self._set_entity_available(topic, False)
            return
        missing_state = self.inventory_owners[owner]
        if missing_state is not None:
            self._client.publish(f'unraid/{self._unraid_id}/{topic}/state', str(missing_state), retain=True)
        attributes_topic = f'unraid/{self._unraid_id}/{topic}/attributes'
        self._pending_missing[attributes_topic] = sid
        self._client.subscribe(attributes_topic, qos=0)

    def _publish_missing_attributes(self, topic: str, payload: Any) -> None:
        sid = self._pending_missing.pop(topic)
        try:
            self._client.unsubscribe(topic)
        except Exception:
            pass
        record = self.tracker.get(sid) or {}
        if not record.get('missing_since'):
            return  # Reported again in the meantime
        try:
            attrs = json.loads(payload) if payload else {}
        except Exception:
            attrs = {}
        if not isinstance(attrs, dict):
            attrs = {}
        attrs['present'] = False
        attrs['missing_since'] = datetime.fromtimestamp(float(record['missing_since']), timezone.utc).isoformat(timespec='seconds')
        self._client.publish(topic, json.dumps(attrs), retain=True)

    def reconcile(self, owner: str, published_ids: Iterable[Optional[str]]) -> None:
        """
        Called after a collector published its complete set of entities. Entities of that collector that
        keep missing are marked unavailable (inventory: present: false), and only removed from Home Assistant
        once they have been gone for EntityTracker.REMOVAL_SECONDS, so short outages never delete anything.
        """
        ids = list(published_ids)
        if not self.tracker or not self.mqtt_connected or None in ids:
            return
        newly_unavailable, expired = self.tracker.missing_in_snapshot(owner, ids)
        for sid in newly_unavailable:
            kind = 'marking as not present' if owner in self.inventory_owners else 'marking unavailable'
            self.logger.info(f'Entity "{sid}" is no longer reported by "{owner}"; {kind}')
            self._mark_missing(sid)
        for sid in expired:
            self._remove_entity(sid, f'not reported by "{owner}" for {self.tracker.removal_seconds // 86400} days')

    def publish_update(self, upd, owner: Optional[str] = None) -> Optional[str]:
        if upd.presence_only:
            return self.entity_id(upd.payload['name'], upd.unique_id_suffix) if self.mqtt_connected else None
        return self.publish(
            payload=upd.payload,
            sensor_type=upd.sensor_type,
            state_value=upd.state,
            json_attributes=upd.attributes,
            retain=upd.retain,
            device_overrides=upd.device_overrides,
            unique_id_suffix=upd.unique_id_suffix,
            expire_after=upd.expire_after,
            owner=owner,
        )

    def publish(
        self,
        payload: Dict[str, Any],
        sensor_type: str,
        state_value: Any,
        json_attributes: Optional[Dict[str, Any]] = None,
        retain: bool = False,
        device_overrides: Optional[Dict[str, Any]] = None,
        unique_id_suffix: Optional[str] = None,
        expire_after: Optional[int] = None,
        owner: Optional[str] = None,
        with_availability: bool = True,
    ) -> Optional[str]:
        """
        Publish discovery config (when changed), state and attributes. Returns the entity id,
        or None when MQTT is not connected.
        """
        if not self._client or not self.mqtt_connected:
            self.logger.debug(f'MQTT not connected; skip publish: {payload.get("name")}')
            return None

        unraid_id = self._unraid_id
        topic_name = self.topic_name(payload['name'], unique_id_suffix)
        unraid_sensor_id = f'{unraid_id}_{topic_name}'

        if not self._may_publish(unraid_sensor_id, owner):
            return unraid_sensor_id

        device = {
            'name': self.name,
            'identifiers': self._device_identifier,
            'model': 'Unraid',
            'manufacturer': 'Lime Technology',
        }

        if self.global_device_overrides:
            device.update(self.global_device_overrides)
        if device_overrides:
            device.update(device_overrides)

        cfg = payload.copy()
        cfg['state_topic'] = f'unraid/{unraid_id}/{topic_name}/state'
        if json_attributes is not None:
            cfg['json_attributes_topic'] = f'unraid/{unraid_id}/{topic_name}/attributes'
        if sensor_type == 'button':
            cfg['command_topic'] = f'unraid/{unraid_id}/{topic_name}/commands'
        if expire_after is not None:
            cfg['expire_after'] = expire_after
        inventory = owner in self.inventory_owners
        with_availability = with_availability and not inventory
        if with_availability:
            # Unavailable when the integration is down (server-wide) or this entity is no longer reported
            cfg['availability'] = [
                {'topic': self._availability_topic, 'payload_available': 'ON', 'payload_not_available': 'OFF'},
                {'topic': f'unraid/{unraid_id}/{topic_name}/availability', 'payload_available': 'ON', 'payload_not_available': 'OFF'},
            ]
            cfg['availability_mode'] = 'all'
        cfg.update({
            'attribution': 'Data provided by UNRAID',
            'unique_id': unraid_sensor_id,
            'device': device,
        })

        is_new = self._has_config_changed(unraid_sensor_id, cfg)
        if is_new:
            self.logger.info(f'Publishing updated config for sensor "{payload["name"]}"')
            self._client.publish(
                f'homeassistant/{sensor_type}/{unraid_sensor_id}/config',
                json.dumps(cfg),
                retain=True,
            )

        came_back = bool(self.tracker and owner and self.tracker.mark_published(unraid_sensor_id, owner, sensor_type, topic_name))
        if came_back:
            self.logger.info(f'Entity "{unraid_sensor_id}" is reported again')
        if with_availability and (is_new or came_back):
            self._set_entity_available(topic_name, True)
        elif inventory and is_new:
            # Inventory entities have no availability topic; drop one left over from earlier versions
            self._client.publish(f'unraid/{unraid_id}/{topic_name}/availability', '', retain=True)

        state_retain = retain or is_new
        attrs_retain = retain or is_new

        if state_value is not None:
            self._client.publish(
                cfg['state_topic'],
                state_value if isinstance(state_value, str)
                else json.dumps(state_value) if not isinstance(state_value, (int, float))
                else str(state_value),
                retain=state_retain,
            )

        if json_attributes is not None:
            self._client.publish(
                cfg['json_attributes_topic'],
                json.dumps(json_attributes),
                retain=attrs_retain,
            )

        return unraid_sensor_id

    @staticmethod
    def topic_name(name: str, unique_id_suffix: Optional[str]) -> str:
        base_name = normalize_str(name)
        topic_suffix = normalize_str(unique_id_suffix) if unique_id_suffix else None
        return f'{base_name}_{topic_suffix}' if topic_suffix else base_name

    def entity_id(self, name: str, unique_id_suffix: Optional[str] = None) -> str:
        return f'{self._unraid_id}_{self.topic_name(name, unique_id_suffix)}'

    def _may_publish(self, sid: str, owner: Optional[str]) -> bool:
        """
        Two collectors producing the same entity would overwrite each other's values. The collector that
        currently owns it keeps it while it is still publishing; a legacy (fallback) owner always yields to a
        GraphQL one. If the current owner stops publishing for HANDOVER_SECONDS, the other one takes over.
        """
        if not self.tracker or not owner:
            return True
        record = self.tracker.get(sid)
        current = record.get('owner') if record else None
        if current in (None, owner) or current not in self.active_owners:
            return True
        if not self.tracker.seen_within(sid, self.tracker.HANDOVER_SECONDS):
            return True
        if current in self.fallback_owners and owner not in self.fallback_owners:
            return True
        if (sid, owner) not in self._conflicts_logged:
            self._conflicts_logged.add((sid, owner))
            self.logger.info(f'Entity "{sid}" is provided by both "{current}" and "{owner}"; using "{current}"')
        return False

    def _has_config_changed(self, sensor_key: str, cfg: Dict[str, Any]) -> bool:
        new_hash = calculate_hash(cfg)
        if self.parser_hashes.get(sensor_key) != new_hash:
            self.parser_hashes[sensor_key] = new_hash
            return True
        return False

    async def aclose(self) -> None:
        for task in (self._connect_task, self._connectivity_task, self._sweep_task):
            if task:
                task.cancel()
        if self.tracker:
            self.tracker.save()
        try:
            self._stopping = True
            self.mqtt_status(False)
            await asyncio.sleep(0.2)
            await self._client.disconnect()
        except Exception:
            pass
