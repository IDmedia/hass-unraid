import json
import asyncio
from typing import Any, Dict, Optional
from gmqtt import Client as MQTTClient, Message
from .utils import calculate_hash, normalize_str


class MQTTPublisher:
    def __init__(self, name: str, mqtt_config: Dict[str, Any], loop: asyncio.AbstractEventLoop, logger, scan_interval: int):
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

        self._client = MQTTClient(
            self.name,
            will_message=Message(f'unraid/{self._unraid_id}/connectivity/state', 'OFF', retain=True),
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
        self.mqtt_status(True)

    def _on_disconnect(self, client, packet, exc=None) -> None:
        level = self.logger.info if self._stopping else self.logger.error
        level('Disconnected from MQTT server')
        self.mqtt_connected = False
        try:
            self.mqtt_status(False)
        except Exception as e:
            self.logger.exception(f'Failed to update connectivity status on disconnect: {e}')

    def _on_message(self, client, topic, payload, qos, properties) -> None:
        self.logger.info(f'Message received: {topic}')

    def mqtt_status(self, connected: bool) -> None:
        payload = {'name': 'Connectivity', 'device_class': 'connectivity'}
        self.publish(payload, 'binary_sensor', 'ON' if connected else 'OFF', retain=True)

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

    def _calculate_structure_hash(self, payload: Dict[str, Any]) -> str:
        structure_keys = ['name', 'icon', 'unit_of_measurement', 'state_class', 'device_class', 'expire_after']
        filtered = {k: v for k, v in payload.items() if k in structure_keys}
        return calculate_hash(filtered)

    def _has_structure_changed(self, sensor_key: str, payload: Dict[str, Any]) -> bool:
        new_hash = self._calculate_structure_hash(payload)
        old_hash = self.parser_hashes.get(sensor_key)
        if old_hash != new_hash:
            self.parser_hashes[sensor_key] = new_hash
            return True
        return False

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
    ) -> None:
        if not self._client or not self.mqtt_connected:
            self.logger.debug(f'MQTT not connected; skip publish: {payload.get("name")}')
            return

        unraid_id = self._unraid_id
        base_name = normalize_str(payload['name'])
        topic_suffix = normalize_str(unique_id_suffix) if unique_id_suffix else None
        topic_name = f'{base_name}_{topic_suffix}' if topic_suffix else base_name

        unraid_sensor_id = f'{unraid_id}_{base_name}'
        if topic_suffix:
            unraid_sensor_id = f'{unraid_sensor_id}_{topic_suffix}'

        device = {
            'name': self.name,
            'identifiers': f'unraid_{unraid_id}'.lower(),
            'model': 'Unraid',
            'manufacturer': 'Lime Technology',
        }

        if self.global_device_overrides:
            device.update(self.global_device_overrides)
        if device_overrides:
            device.update(device_overrides)

        config_payload = payload.copy()
        config_payload['state_topic'] = f'unraid/{unraid_id}/{topic_name}/state'
        if json_attributes:
            config_payload['json_attributes_topic'] = f'unraid/{unraid_id}/{topic_name}/attributes'
        if sensor_type == 'button':
            config_payload['command_topic'] = f'unraid/{unraid_id}/{topic_name}/commands'
        if expire_after is not None:
            config_payload['expire_after'] = expire_after

        is_new = self._has_structure_changed(unraid_sensor_id, config_payload)
        if is_new:
            self.logger.info(f'Publishing updated config for sensor "{payload["name"]}"')
            cfg = dict(config_payload)
            cfg.update({
                'attribution': 'Data provided by UNRAID',
                'unique_id': unraid_sensor_id,
                'device': device,
            })
            self._client.publish(
                f'homeassistant/{sensor_type}/{unraid_sensor_id}/config',
                json.dumps(cfg),
                retain=True,
            )

        state_retain = retain or is_new
        attrs_retain = retain or is_new

        if state_value is not None:
            self._client.publish(
                f'unraid/{unraid_id}/{topic_name}/state',
                state_value if isinstance(state_value, str)
                else json.dumps(state_value) if not isinstance(state_value, (int, float))
                else str(state_value),
                retain=state_retain,
            )

        if json_attributes:
            self._client.publish(
                f'unraid/{unraid_id}/{topic_name}/attributes',
                json.dumps(json_attributes),
                retain=attrs_retain,
            )

    async def aclose(self) -> None:
        try:
            self._stopping = True
            self.mqtt_status(False)
            await asyncio.sleep(0.2)
            await self._client.disconnect()
        except Exception:
            pass
