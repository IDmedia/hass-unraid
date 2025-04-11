import os
import re
import sys
import time
import json
import httpx
import signal
import asyncio
import logging
import parsers
import websockets
from lxml import etree
from utils import load_file, normalize_str, handle_sigterm
from gmqtt import Client as MQTTClient, Message


class UnRAIDServer(object):
    def __init__(self, mqtt_config, unraid_config, loop: asyncio.AbstractEventLoop):
        # Unraid config
        unraid_host = unraid_config.get('host')
        unraid_port = unraid_config.get('port')
        unraid_ssl = unraid_config.get('ssl', False)
        verify_ssl = unraid_config.get('ssl_verify', True)
        unraid_address = f'{unraid_host}:{unraid_port}'
        unraid_protocol = 'https://' if unraid_ssl else 'http://'

        self.unraid_version = ''
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

        # MQTT client
        self.mqtt_connected = False
        unraid_id = normalize_str(self.unraid_name)
        will_message = Message(f'unraid/{unraid_id}/connectivity/state', 'OFF', retain=True)
        self.mqtt_client = MQTTClient(self.unraid_name, will_message=will_message)

        asyncio.ensure_future(self.mqtt_connect(mqtt_config))

        # Logger
        self.logger = logging.getLogger(self.unraid_name)
        self.logger.setLevel(logging.INFO)
        unraid_logger = logging.StreamHandler(sys.stdout)
        unraid_logger_formatter = logging.Formatter(f'%(asctime)s [%(levelname)s] [{self.unraid_name}] %(message)s')
        unraid_logger.setFormatter(unraid_logger_formatter)
        self.logger.addHandler(unraid_logger)

        self.loop = loop

    def on_connect(self, client, flags, rc, properties):
        self.logger.info('Successfully connected to mqtt server')

        # Create and subscribe to Mover button
        # mover_payload = { 'name': 'Mover' }
        # self.mqtt_publish(mover_payload, 'button', state_value='OFF', create_config=True)

        self.mqtt_connected = True
        self.mqtt_status(connected=True, create_config=True)

        self.unraid_task = asyncio.ensure_future(self.ws_connect())

    def on_message(self, client, topic, payload, qos, properties):
        self.logger.info(f'Message received: {topic}')

    def on_disconnect(self, client, packet, exc=None):
        self.logger.error('Disconnected from mqtt server')
        self.mqtt_status(connected=False)
        self.mqtt_connected = False

    def mqtt_status(self, connected, create_config=False):
        # Update status
        status_payload = {
            'name': 'Connectivity',
            'device_class': 'connectivity'
        }
        state_value = 'ON' if connected else 'OFF'
        self.mqtt_publish(status_payload, 'binary_sensor', state_value, create_config=create_config)

    def mqtt_publish(self, payload, sensor_type, state_value, json_attributes=None, create_config=False, retain=False):
        # Make clean variables
        unraid_id = normalize_str(self.unraid_name)
        sensor_id = normalize_str(payload["name"])
        unraid_sensor_id = f'{unraid_id}_{sensor_id}'

        # Create config
        if create_config:
            device = {
                'name': self.unraid_name,
                'identifiers': f'unraid_{unraid_id}'.lower(),
                'model': 'Unraid',
                'manufacturer': 'Lime Technology'
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
                expire_in_seconds = self.scan_interval * 4
                create_config['expire_after'] = expire_in_seconds if expire_in_seconds > 120 else 120

            config_fields = {
                'name': f'{payload["name"]}',
                'attribution': 'Data provided by UNRAID',
                'unique_id': unraid_sensor_id,
                'device': device
            }
            create_config.update(config_fields)

            self.mqtt_client.publish(f'homeassistant/{sensor_type}/{unraid_sensor_id}/config', json.dumps(create_config), retain=True)

        if state_value is not None:
            # Add slight delay after creating config to prevent race conditions
            time.sleep(0.1)
            self.mqtt_client.publish(f'unraid/{unraid_id}/{sensor_id}/state', state_value, retain=retain)

        if json_attributes:
            self.mqtt_client.publish(f'unraid/{unraid_id}/{sensor_id}/attributes', json.dumps(json_attributes), retain=retain)

        if sensor_type == 'button':
            self.mqtt_client.subscribe(f'unraid/{unraid_id}/{sensor_id}/commands', qos=0, retain=retain)

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
                self.logger.error('Failed to connect to mqtt server because the connection was refused...')
                await asyncio.sleep(30)
            except Exception:
                self.logger.exception('Failed to connect to mqtt server due to an exception...')
                await asyncio.sleep(30)

    async def ws_connect(self):
        while self.mqtt_connected:
            self.logger.info('Connecting to unraid...')
            last_msg = ''
            try:
                payload = {
                    'username': self.unraid_username,
                    'password': self.unraid_password
                }
                async with httpx.AsyncClient(verify=self.verify_ssl) as http:
                    r = await http.post(f'{self.unraid_url}/login', data=payload, timeout=120)
                    self.unraid_cookie = r.headers.get('set-cookie')

                    r = await http.get(f'{self.unraid_url}/Dashboard', follow_redirects=True, timeout=120)
                    tree = etree.HTML(r.text)
                    version_elem = tree.xpath('.//div[@class="logo"]/text()[preceding-sibling::a]')
                    self.unraid_version = ''.join(c for c in ''.join(version_elem) if c.isdigit() or c == '.')

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
                    'temperature': parsers.temperature
                }
                websocket_url = f'{self.unraid_ws}/sub/{",".join(sub_channels)}'

                async with websockets.connect(websocket_url, subprotocols=subprotocols, extra_headers=headers) as websocket:
                    self.logger.info('Successfully connected to unraid')

                    while self.mqtt_connected:
                        data = await asyncio.wait_for(websocket.recv(), timeout=120)
                        last_msg = data
                        msg_data = data.replace('\00', ' ').split('\n\n', 1)[1]
                        msg_ids = re.findall(r'([-\[\d\],]+,[-\[\d\],]*)|$', data)[0].split(',')
                        sub_channel = next(sub for (sub, msg) in zip(sub_channels, msg_ids) if msg.startswith('['))
                        msg_parser = sub_channels.get(sub_channel, parsers.default)

                        if sub_channel == 'shares':
                            current_time = time.time()
                            time_passed = current_time - self.share_parser_lastrun
                            if time_passed <= self.share_parser_interval:
                                continue
                            self.share_parser_lastrun = current_time

                        if sub_channel not in self.mqtt_history:
                            self.logger.info(f'Create config for {sub_channel}')
                            self.mqtt_history[sub_channel] = (time.time() - self.scan_interval)
                            self.loop.create_task(msg_parser(self, msg_data, create_config=True))

                        if self.scan_interval <= (time.time() - self.mqtt_history.get(sub_channel, time.time())):
                            self.logger.info(f'Parse data for {sub_channel}')
                            self.mqtt_history[sub_channel] = time.time()
                            self.loop.create_task(msg_parser(self, msg_data, create_config=False))

            except (httpx.ConnectTimeout, httpx.ConnectError):
                self.logger.error('Failed to connect to unraid due to a timeout or connection issue...')
                self.mqtt_status(connected=False)
                await asyncio.sleep(30)
            except Exception:
                self.logger.exception('Failed to connect to unraid due to an exception...')
                self.logger.error('Last message received:')
                self.logger.error(last_msg)
                self.mqtt_status(connected=False)
                await asyncio.sleep(30)


if __name__ == '__main__':
    # Allow keyboard interrupts
    signal.signal(signal.SIGTERM, handle_sigterm)

    # Disable gmqtt log
    loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict if name.startswith(('gmqtt'))]
    for log in loggers:
        logging.getLogger(log.name).disabled = True

    # Read config file
    data_path = '../data'
    config = load_file(os.path.join(data_path, 'config.yaml'))

    # Required by the MQTT client on Windows
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    # Get event loop
    loop = asyncio.get_event_loop()

    # Create unraid instances
    for unraid_config in config.get('unraid'):
        UnRAIDServer(config.get('mqtt'), unraid_config, loop)

    # Loop forever
    loop.run_forever()
