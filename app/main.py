import os
import re
import sys
import ssl
import time
import json
import httpx
import signal
import asyncio
import logging
import parsers
import websockets
from lxml import etree
from gmqtt import Client as MQTTClient, Message
from utils import load_file, normalize_str, handle_sigterm, compare_versions


class UnRAIDServer:
    def __init__(self, mqtt_config, unraid_config, loop: asyncio.AbstractEventLoop):
        unraid_host = unraid_config.get('host')
        unraid_port = unraid_config.get('port')
        unraid_ssl = unraid_config.get('ssl', False)
        verify_ssl = unraid_config.get('ssl_verify', True)

        unraid_address = f'{unraid_host}:{unraid_port}'
        unraid_protocol = 'https://' if unraid_ssl else 'http://'

        self.unraid_version = ''
        self.unraid_version_min = '7.0.0'

        self.unraid_name = unraid_config.get('name')
        self.unraid_username = unraid_config.get('username')
        self.unraid_password = unraid_config.get('password')
        self.unraid_url = f'{unraid_protocol}{unraid_address}'
        self.unraid_ws = f'wss://{unraid_address}' if unraid_ssl else f'ws://{unraid_address}'

        self.scan_interval = unraid_config.get('scan_interval', 30)
        self.share_parser_lastrun = 0
        self.share_parser_interval = 3600

        self.csrf_token = ''
        self.unraid_cookie = ''
        self.verify_ssl = verify_ssl
        self.gpus = None
        self.gpu_task = None

        self.connectivity_task = loop.create_task(self.periodic_connectivity_update())

        self.mqtt_connected = False
        unraid_id = normalize_str(self.unraid_name)

        will_message = Message(
            f'unraid/{unraid_id}/connectivity/state', 'OFF', retain=True
        )
        self.mqtt_client = MQTTClient(self.unraid_name, will_message=will_message)
        asyncio.ensure_future(self.mqtt_connect(mqtt_config))

        self.logger = logging.getLogger(self.unraid_name)
        self.logger.setLevel(logging.INFO)

        unraid_logger = logging.StreamHandler(sys.stdout)
        unraid_logger_formatter = logging.Formatter(
            f'%(asctime)s [%(levelname)s] [{self.unraid_name}] %(message)s'
        )
        unraid_logger.setFormatter(unraid_logger_formatter)
        self.logger.addHandler(unraid_logger)

        self.loop = loop

    def on_connect(self, client, flags, rc, properties):
        self.logger.info('Successfully connected to MQTT server')
        self.mqtt_connected = True

        # Update the connectivity sensor and create its config if needed
        self.mqtt_status(connected=True, create_config=True)

        # Start the main UnRAID WebSocket connection task
        self.unraid_task = asyncio.ensure_future(self.ws_connect())

    def on_message(self, client, topic, payload, qos, properties):
        self.logger.info(f'Message received: {topic}')

    def on_disconnect(self, client, packet, exc=None):
        self.logger.error('Disconnected from MQTT server')
        self.mqtt_connected = False

        # Safely update the connectivity sensor
        try:
            self.mqtt_status(connected=False)
        except Exception as e:
            self.logger.exception(f'Failed to update connectivity status on disconnect: {e}')

    def mqtt_status(self, connected, create_config=False):
        """
        Safely publish the "Connectivity" sensor state to MQTT.
        """
        status_payload = {
            'name': 'Connectivity',
            'device_class': 'connectivity',
        }
        state_value = 'ON' if connected else 'OFF'

        # Avoid publishing the same state repeatedly
        if hasattr(self, '_last_connectivity_state') and self._last_connectivity_state == state_value:
            self.logger.debug(f'Skipping redundant mqtt_status update: {state_value}')
            return
        self._last_connectivity_state = state_value

        # Ensure MQTT client is ready before publishing
        if not self.mqtt_client or not self.mqtt_connected:
            self.logger.warning('MQTT client is not ready, skipping mqtt_status update')
            return

        # Publish the state
        self.logger.info(f'Publishing Connectivity state: {state_value}')
        try:
            self.mqtt_publish(
                status_payload,
                'binary_sensor',
                state_value,
                create_config=create_config,
                retain=True  # Ensure message is retained
            )
        except Exception as e:
            self.logger.exception(f'Failed to publish Connectivity state: {e}')

    async def periodic_connectivity_update(self):
        """
        Periodically refresh the "Connectivity" sensor state (even if unchanged)
        to prevent Home Assistant from marking it as unavailable.
        """
        self.logger.info('Starting periodic connectivity updates...')
        while True:
            try:
                # Publish the current connectivity state periodically
                if self.mqtt_connected:
                    self.mqtt_status(connected=True)
                else:
                    self.mqtt_status(connected=False)
            except Exception as e:
                self.logger.exception(f'Error in periodic connectivity update: {e}')

            # Wait before the next update
            await asyncio.sleep(60)  # Update every 60 seconds

    def mqtt_publish(
        self, payload, sensor_type, state_value, json_attributes=None, create_config=False, retain=False
    ):
        unraid_id = normalize_str(self.unraid_name)
        sensor_id = normalize_str(payload['name'])
        unraid_sensor_id = f'{unraid_id}_{sensor_id}'

        if create_config:
            device = {
                'name': self.unraid_name,
                'identifiers': f'unraid_{unraid_id}'.lower(),
                'model': 'Unraid',
                'manufacturer': 'Lime Technology',
            }

            if self.unraid_version:
                device['sw_version'] = self.unraid_version

            create_config = payload

            if state_value is not None:
                create_config['state_topic'] = f'unraid/{unraid_id}/{sensor_id}/state'

            if json_attributes:
                create_config['json_attributes_topic'] = f'unraid/{unraid_id}/{sensor_id}/attributes'

            if sensor_type == 'button':
                create_config['command_topic'] = f'unraid/{unraid_id}/{sensor_id}/commands'

            if not sensor_id.startswith(('connectivity', 'share_', 'disk_')):
                expire_in_seconds = max(self.scan_interval * 4, 120)
                create_config['expire_after'] = expire_in_seconds

            config_fields = {
                'name': f'{payload["name"]}',
                'attribution': 'Data provided by UNRAID',
                'unique_id': unraid_sensor_id,
                'device': device,
            }

            create_config.update(config_fields)

            self.mqtt_client.publish(
                f'homeassistant/{sensor_type}/{unraid_sensor_id}/config', json.dumps(create_config), retain=True
            )

        if state_value is not None:
            time.sleep(0.1)
            self.mqtt_client.publish(f'unraid/{unraid_id}/{sensor_id}/state', state_value, retain=retain)

        if json_attributes:
            self.mqtt_client.publish(
                f'unraid/{unraid_id}/{sensor_id}/attributes', json.dumps(json_attributes), retain=retain
            )

        if sensor_type == 'button':
            self.mqtt_client.subscribe(
                f'unraid/{unraid_id}/{sensor_id}/commands', qos=0, retain=retain
            )

    async def mqtt_connect(self, mqtt_config):
        mqtt_host = mqtt_config.get('host')
        mqtt_port = mqtt_config.get('port', 1883)
        mqtt_username = mqtt_config.get('username')
        mqtt_password = mqtt_config.get('password')

        self.mqtt_history = {}
        self.share_parser_lastrun = 0

        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message
        self.mqtt_client.on_disconnect = self.on_disconnect
        self.mqtt_client.set_auth_credentials(mqtt_username, mqtt_password)

        while True:
            try:
                self.logger.info('Connecting to mqtt server...')
                await self.mqtt_client.connect(mqtt_host, mqtt_port)
                break
            except ConnectionRefusedError:
                self.logger.error('Connection refused by mqtt server')
                await asyncio.sleep(30)
            except Exception:
                self.logger.exception('Failed to connect to mqtt server')
                await asyncio.sleep(30)

    async def fetch_gpu_status(self):
        """Fetch GPU status periodically."""
        while self.mqtt_connected and self.gpus:
            try:
                gpu_status_url = f'{self.unraid_url}/plugins/gpustat/gpustatusmulti.php?gpus={json.dumps(self.gpus)}'

                async with httpx.AsyncClient(verify=self.verify_ssl) as http:
                    response = await self.make_authenticated_request(
                        http, method='GET', url=gpu_status_url
                    )

                    if response and response.status_code == 200:
                        gpu_data = response.json()
                        self.logger.info('Parse GPU Statistics (plugin)')

                        await parsers.gpu_stat(self, json.dumps(gpu_data), create_config=True)
                    else:
                        self.logger.warning(f'Failed to fetch GPU Statistics, status code: {response.status_code}')

            except Exception as e:
                self.logger.exception(f'Error while fetching GPU status: {e}')

            await asyncio.sleep(5)

    async def make_authenticated_request(self, http_client, method, url, data=None):
        """
        Perform an HTTP request with the current cookie. If the cookie is expired or invalid,
        attempt to re-login and retry the request.
        """
        headers = {'Cookie': self.unraid_cookie}

        try:
            if method == 'GET':
                response = await http_client.get(url, headers=headers, timeout=120)
            elif method == 'POST':
                response = await http_client.post(url, headers=headers, data=data, timeout=120)
            else:
                raise ValueError('Unsupported HTTP method')

            # Check if authentication failure occurred (e.g., expired/invalid cookie)
            if response.status_code in (401, 403):
                self.logger.warning('Authentication failed, attempting to re-login...')
                await self.unraid_login()

                # Retry the request with the new cookie
                headers['Cookie'] = self.unraid_cookie
                if method == 'GET':
                    response = await http_client.get(url, headers=headers, timeout=120)
                elif method == 'POST':
                    response = await http_client.post(url, headers=headers, data=data, timeout=120)

            return response

        except Exception as e:
            self.logger.exception(f'Request to {url} failed: {e}')
            return None

    async def unraid_login(self):
        """Login and update the Unraid cookie."""
        async with httpx.AsyncClient(verify=self.verify_ssl) as http:
            payload = {'username': self.unraid_username, 'password': self.unraid_password}

            try:
                response = await http.post(f'{self.unraid_url}/login', data=payload, timeout=120)
                self.unraid_cookie = response.headers.get('set-cookie')

                if not self.unraid_cookie:
                    self.logger.error('Failed to obtain Unraid login cookie')
                    raise ValueError('Unraid login failed')

                self.logger.info('Successfully logged into Unraid and updated cookie')

            except Exception as e:
                self.logger.exception(f'Login attempt failed: {e}')
                raise

    async def ws_connect(self):
        while self.mqtt_connected:
            self.logger.info('Connecting to unraid...')
            last_msg = ''
            try:
                # Only login if cookie is missing or invalid
                if not self.unraid_cookie:
                    await self.unraid_login()

                # Check Unraid version and GPU data
                async with httpx.AsyncClient(verify=self.verify_ssl) as http:
                    r = await http.get(
                        f'{self.unraid_url}/Dashboard', headers={'Cookie': self.unraid_cookie}, timeout=120
                    )
                    tree = etree.HTML(r.text)

                    # Extract the Unraid version
                    user_profile_elem = tree.xpath('.//unraid-user-profile')
                    if user_profile_elem:
                        server_data = user_profile_elem[0].get('server')
                        if server_data:
                            server_data = json.loads(server_data.replace('&quot;', '"'))
                            self.unraid_version = server_data.get('osVersion', 'Unknown')
                        else:
                            self.unraid_version = 'Unknown'

                    # Extract GPU information from "gpustat_statusm"
                    gpu_script = tree.xpath('.//script[contains(text(), "gpustat_statusm")]')
                    if gpu_script:
                        gpu_content = gpu_script[0].text
                        match = re.search(r'gpustat_statusm\((\{.+?\})\)', gpu_content)
                        if match:
                            self.gpus = json.loads(match.group(1))
                            self.logger.info('GPU Statistics (plugin) detected')

                    # Start GPU status fetching as a background task
                    if self.gpus and not self.gpu_task:
                        self.gpu_task = asyncio.create_task(self.fetch_gpu_status())

                # Check if the Unraid version is unsupported
                comparison_result = compare_versions(self.unraid_version, self.unraid_version_min)

                # Handle invalid versions explicitly
                if comparison_result is None:
                    self.logger.error(f'Invalid Unraid version detected: "{self.unraid_version}"')
                    sys.exit(1)

                # Compare versions
                if comparison_result < 0:
                    self.logger.error(
                        f'Unsupported Unraid version: {self.unraid_version}. Requires version {self.unraid_version_min} or higher.'
                    )
                    sys.exit(1)

                headers = {'Cookie': self.unraid_cookie}
                subprotocols = ['ws+meta.nchan']
                sub_channels = {
                    'var': parsers.var,
                    'session': parsers.session,
                    'cpuload': parsers.cpuload,
                    'apcups': parsers.apcups,
                    'disks': parsers.disks,
                    'parity': parsers.parity,
                    'shares': parsers.shares,
                    'update1': parsers.update1,
                    'update3': parsers.update3,
                    'temperature': parsers.temperature,
                }
                websocket_url = f'{self.unraid_ws}/sub/{",".join(sub_channels)}'

                # Create a custom SSL context to ignore SSL certificate validation if needed
                ssl_context = None
                if not self.verify_ssl:
                    ssl_context = ssl.create_default_context()
                    ssl_context.check_hostname = False
                    ssl_context.verify_mode = ssl.CERT_NONE

                # Establish WebSocket connection using the custom SSL context
                async with websockets.connect(
                    websocket_url,
                    subprotocols=subprotocols,
                    extra_headers=headers,
                    ssl=ssl_context,
                ) as websocket:
                    self.logger.info(f'Successfully connected to Unraid {self.unraid_version}')

                    # Listen for messages
                    while self.mqtt_connected:
                        data = await asyncio.wait_for(websocket.recv(), timeout=120)
                        # Store last message
                        last_msg = data
                        # Parse message id and content
                        msg_data = data.replace('\00', ' ').split('\n\n', 1)[1]
                        msg_ids = re.findall(r'([-\[\d\],]+,[-\[\d\],]*)|$', data)[0].split(',')
                        sub_channel = next(
                            sub
                            for (sub, msg) in zip(sub_channels, msg_ids)
                            if msg.startswith('[')
                        )
                        msg_parser = sub_channels.get(sub_channel, parsers.default)

                        # Skip resource-intensive share parsing if within time limit
                        if sub_channel == 'shares':
                            current_time = time.time()
                            time_passed = current_time - self.share_parser_lastrun
                            if time_passed <= self.share_parser_interval:
                                continue
                            self.share_parser_lastrun = current_time

                        # Create config if it doesn't exist for the subchannel
                        if sub_channel not in self.mqtt_history:
                            self.logger.info(f'Create config for {sub_channel}')
                            self.mqtt_history[sub_channel] = (
                                time.time() - self.scan_interval
                            )
                            self.loop.create_task(msg_parser(self, msg_data, create_config=True))

                        # Parse content periodically for the subchannel
                        if self.scan_interval <= (
                            time.time() - self.mqtt_history.get(sub_channel, time.time())
                        ):
                            self.logger.info(f'Parse data for {sub_channel}')
                            self.mqtt_history[sub_channel] = time.time()
                            self.loop.create_task(msg_parser(self, msg_data, create_config=False))

            # Handle exceptions and connection issues
            except (httpx.ConnectTimeout, httpx.ConnectError):
                self.logger.error(
                    'Failed to connect to unraid due to a timeout or connection issue...'
                )
                self.mqtt_status(connected=False)
                await asyncio.sleep(30)
            except Exception:
                self.logger.exception('Failed to connect to unraid due to an exception...')
                self.logger.error('Last message received:')
                self.logger.error(last_msg)
                self.mqtt_status(connected=False)
                await asyncio.sleep(30)


if __name__ == '__main__':
    signal.signal(signal.SIGTERM, handle_sigterm)

    loggers = [
        logging.getLogger(name) for name in logging.root.manager.loggerDict if name.startswith('gmqtt')
    ]

    for log in loggers:
        logging.getLogger(log.name).disabled = True

    data_path = '../data'
    config = load_file(os.path.join(data_path, 'config.yaml'))

    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    loop = asyncio.get_event_loop()

    for unraid_config in config.get('unraid'):
        UnRAIDServer(config.get('mqtt'), unraid_config, loop)

    loop.run_forever()
