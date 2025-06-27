import re
import json
import math
import html
import httpx
import humanfriendly
from lxml import etree
from utils import Preferences


# Default function for handling unknown types of data.
async def default(self, msg_data):
    pass


# Handles session messages to extract CSRF tokens.
async def session(self, msg_data):
    self.csrf_token = msg_data


# Processes GPU-related data and creates corresponding sensors dynamically.
async def gpu_stat(self, msg_data):
    parsed_data = json.loads(msg_data)

    def is_valid(value):
        """
        Checks if a value is valid (not None, not empty, and not placeholders like 'N/A' or 'Unknown').
        """
        invalid_values = {'N/A', 'N\\/A', 'Unknown', 'unknown', '', None}
        if str(value).strip() in invalid_values:
            return False

        # For numerical values with units (e.g., "5%", "10W", "28°C"), test for conversion
        try:
            cleaned_value = str(value).replace('%', '').replace('°C', '').replace('W', '').strip()
            float(cleaned_value)
            return True
        except ValueError:
            return False

    for gpu_id, gpu_data in parsed_data.items():
        name = gpu_data.get('name', f'GPU {gpu_id}')

        # Load sensor
        if is_valid(gpu_data.get('util')):
            load_pct = int(float(gpu_data['util'].replace('%', '').strip()))
            payload = {
                'name': f'{name} Load',
                'unit_of_measurement': '%',
                'icon': 'mdi:chart-line',
                'state_class': 'measurement',
            }
            self.mqtt_publish(payload, 'sensor', load_pct)

        # Memory sensor
        if all(is_valid(gpu_data.get(key)) for key in ['memutil', 'memused', 'memtotal']):
            mem_used = int(gpu_data['memused'])
            mem_total = int(gpu_data['memtotal'])
            mem_pct = int(float(gpu_data['memutil'].replace('%', '').strip()))
            payload = {
                'name': f'{name} Memory Usage',
                'unit_of_measurement': '%',
                'icon': 'mdi:memory',
                'state_class': 'measurement',
            }
            json_attributes = {'used': mem_used, 'total': mem_total}
            self.mqtt_publish(payload, 'sensor', mem_pct, json_attributes)

        # Fan sensor
        if is_valid(gpu_data.get('fan')):
            fan_pct = int(float(gpu_data['fan'].replace('%', '').strip()))
            payload = {
                'name': f'{name} Fan Speed',
                'unit_of_measurement': '%',
                'icon': 'mdi:fan',
                'state_class': 'measurement',
            }
            self.mqtt_publish(payload, 'sensor', fan_pct)

        # Power sensor
        if is_valid(gpu_data.get('power')):
            power_usage = int(float(gpu_data['power'].replace('W', '').strip()))
            payload = {
                'name': f'{name} Power Usage',
                'unit_of_measurement': 'W',
                'icon': 'mdi:flash',
                'state_class': 'measurement',
            }
            self.mqtt_publish(payload, 'sensor', power_usage)

        # Temperature sensor
        if is_valid(gpu_data.get('temp')):
            temp = int(float(gpu_data['temp'].replace('°C', '').strip()))
            payload = {
                'name': f'{name} Temperature',
                'unit_of_measurement': '°C',
                'icon': 'mdi:thermometer',
                'state_class': 'measurement',
                'device_class': 'temperature',
            }
            self.mqtt_publish(payload, 'sensor', temp)

        # GPU summary sensor with valid attributes
        payload = {
            'name': name,
            'icon': 'mdi:expansion-card',
            'unit_of_measurement': '%',
            'state_class': 'measurement',
        }
        load_pct = int(float(gpu_data['util'].replace('%', '').strip()))
        json_attributes = {k: v for k, v in gpu_data.items() if is_valid(v)}
        self.mqtt_publish(payload, 'sensor', load_pct, json_attributes)


# Processes CPU utilization data and creates an MQTT sensor.
async def cpuload(self, msg_data):
    prefs = Preferences(msg_data)
    state_value = int(prefs.as_dict()['cpu']['host'])
    payload = {
        'name': 'CPU Utilization',
        'unit_of_measurement': '%',
        'icon': 'mdi:chip',
        'state_class': 'measurement',
    }
    self.mqtt_publish(payload, 'sensor', state_value)


# Processes disk temperature data and creates separate temperature sensors for each disk.
async def disks(self, msg_data):
    prefs = Preferences(msg_data)
    disks = prefs.as_dict()

    for n in disks:
        disk = disks[n]
        disk_name = disk['name']
        disk_temp = int(disk['temp']) if str(disk['temp']).isnumeric() else 0

        # Format disk names for readability
        match = re.match(r'([a-z_]+)([0-9]+)', disk_name, re.I)
        if match:
            disk_num = match[2]
            disk_name = match[1] if match[1] != 'disk' else None
            disk_name = ' '.join(filter(None, [disk_name, disk_num]))

        disk_name = disk_name.title().replace('_', ' ')

        payload = {
            'name': f'Disk {disk_name}',
            'unit_of_measurement': '°C',
            'device_class': 'temperature',
            'icon': 'mdi:harddisk',
            'state_class': 'measurement',
        }
        json_attributes = disk

        self.mqtt_publish(payload, 'sensor', disk_temp, json_attributes, retain=True)


# Processes storage shares and calculates usage percentages, creating corresponding sensors.
async def shares(self, msg_data):
    prefs = Preferences(msg_data)
    shares = prefs.as_dict()

    for n in shares:
        share = shares[n]
        share_name = share['name']
        share_disk_count = len(share['include'].split(','))
        share_floor_size = share['floor']
        share_nameorig = share['nameorig']
        share_use_cache = share['usecache']

        # Handle cases where share caching is enabled
        if share_use_cache in ['no', 'yes', 'prefer']:
            async with httpx.AsyncClient(verify=self.verify_ssl) as http:
                headers = {'Cookie': self.unraid_cookie}
                data = {
                    'compute': share_nameorig,
                    'path': 'Shares',
                    'all': 1,
                    'csrf_token': self.csrf_token,
                }

                try:
                    r = await http.request(
                        'GET',
                        url=f'{self.unraid_url}/webGui/include/ShareList.php',
                        data=data,
                        headers=headers,
                        timeout=600,
                    )
                except httpx.RequestError as e:
                    self.logger.error(f'Failed to request share data for "{share_name}": {e}')
                    continue

                if r.status_code == httpx.codes.OK:
                    tree = etree.HTML(r.text)

                    # Calculate total and cache usage
                    size_total_used = tree.xpath(
                        f'//td/a[text()="{share_nameorig}"]/ancestor::tr[1]/td[6]/text()'
                    )
                    size_total_used = next(iter(size_total_used or []), '0').strip()
                    size_total_used = humanfriendly.parse_size(size_total_used)

                    size_total_free = tree.xpath(
                        f'//td/a[text()="{share_nameorig}"]/ancestor::tr[1]/td[7]/text()'
                    )
                    size_total_free = next(iter(size_total_free or []), '0').strip()
                    size_total_free = humanfriendly.parse_size(size_total_free)

                    size_cache_used = tree.xpath(
                        f'//td/a[text()="{share_nameorig}"]/following::tr[1]/td[1]'
                        f'[not(contains(text(), "Disk "))]/../td[6]/text()'
                    )
                    size_cache_used = next(iter(size_cache_used or []), '0').strip()
                    size_cache_used = humanfriendly.parse_size(size_cache_used)

                    size_cache_free = tree.xpath(
                        f'//td/a[text()="{share_nameorig}"]/following::tr[1]/td[1]'
                        f'[not(contains(text(), "Disk "))]/../td[7]/text()'
                    )
                    size_cache_free = next(iter(size_cache_free or []), '0').strip()
                    size_cache_free = humanfriendly.parse_size(size_cache_free)

                    # Update share usage values
                    share['used'] = int(size_total_used / 1000)
                    share['free'] = int((size_total_free - size_cache_free - size_cache_used) / 1000)

        if share['used'] == 0:
            continue

        if share.get('exclusive') in ['yes']:
            share_disk_count = 1

        share_size_floor = share_disk_count * share_floor_size
        share['free'] -= share_size_floor
        share_size_total = share['used'] + share['free']
        share_used_pct = math.ceil((share['used'] / share_size_total) * 100)

        payload = {
            'name': f'Share {share_name.title()} Usage',
            'unit_of_measurement': '%',
            'icon': 'mdi:folder-network',
            'state_class': 'measurement',
        }
        json_attributes = share

        self.mqtt_publish(payload, 'sensor', share_used_pct, json_attributes, retain=True)


# Processes device temperature and fan speed data.
async def temperature(self, msg_data):
    tree = etree.HTML(msg_data)
    sensors = tree.xpath('.//span[@title]')

    for node in sensors:
        device_name = node.get('title')
        device_value_raw = ''.join(node.itertext())
        device_value = ''.join(c for c in device_value_raw if c.isdigit() or c == '.')

        if device_value:
            if 'rpm' in device_value_raw:
                device_name = re.sub('fan', '', device_name, flags=re.IGNORECASE).strip()
                device_value = int(device_value)
                payload = {
                    'name': f'Fan {device_name} Speed',
                    'unit_of_measurement': 'RPM',
                    'icon': 'mdi:fan',
                    'state_class': 'measurement'
                }
            else:
                device_value = float(device_value)
                payload = {
                    'name': f'{device_name} Temperature',
                    'unit_of_measurement': '°C',
                    'icon': 'mdi:thermometer',
                    'state_class': 'measurement',
                    'device_class': 'temperature'
                }

            self.mqtt_publish(payload, 'sensor', device_value)


# Processes RAM and memory-related data.
async def update1(self, msg_data):
    memory_categories = ['RAM', 'Flash', 'Log', 'Docker']

    for (memory_name, memory_usage) in zip(memory_categories, re.findall(re.compile(r'(\d+%)'), msg_data)):
        memory_value = ''.join(c for c in memory_usage if c.isdigit())

        if memory_value:
            memory_value = int(memory_value)

            payload = {
                'name': f'{memory_name} Usage',
                'unit_of_measurement': '%',
                'icon': 'mdi:memory',
                'state_class': 'measurement'
            }

            self.mqtt_publish(payload, 'sensor', memory_value)

    for fan_id, fan_rpm in enumerate(re.findall(re.compile(r'(\d+ RPM)'), msg_data)):
        fan_id = fan_id + 1
        fan_name = f'Fan {fan_id}'

        fan_value = ''.join(c for c in fan_rpm if c.isdigit())

        if fan_value:
            fan_value = int(fan_value)

            payload = {
                'name': f'{fan_name} Speed',
                'unit_of_measurement': 'RPM',
                'icon': 'mdi:fan',
                'state_class': 'measurement'
            }

            self.mqtt_publish(payload, 'sensor', fan_value)


# Processes network throughput values.
async def update3(self, msg_data):
    data = msg_data.replace('\n', '\\n')
    parsed_data = json.loads(data)
    network_download = 0
    network_upload = 0

    for port_info in parsed_data.get('port', []):
        port_name = port_info[0]

        if not port_name.startswith('eth'):
            continue

        network_download_text = port_info[1]
        network_download += round(humanfriendly.parse_size(network_download_text) / 1000 / 1000, 1)

        network_upload_text = port_info[2]
        network_upload += round(humanfriendly.parse_size(network_upload_text) / 1000 / 1000, 1)

        payload_download = {
            'name': f'Download Throughput ({port_name})',
            'unit_of_measurement': 'Mbit/s',
            'icon': 'mdi:download',
            'state_class': 'measurement'
        }

        payload_upload = {
            'name': f'Upload Throughput ({port_name})',
            'unit_of_measurement': 'Mbit/s',
            'icon': 'mdi:upload',
            'state_class': 'measurement'
        }

        self.mqtt_publish(payload_download, 'sensor', network_download)
        self.mqtt_publish(payload_upload, 'sensor', network_upload)


# Processes and sends UPS data.
async def apcups(self, msg_data):
    msg_data = msg_data.replace(r"\/", "/")

    parsed_data = json.loads(msg_data)

    def clean_html(value):
        return re.sub(r'<[^>]+>', '', html.unescape(value)).strip()

    def parse_timespan(value):
        try:
            timespan_seconds = humanfriendly.parse_timespan(value)
            return int(timespan_seconds / 60)
        except humanfriendly.InvalidTimespan:
            return 0

    ups_model = clean_html(parsed_data[0])
    ups_model = "" if ups_model == "-" else ups_model

    ups_status = clean_html(parsed_data[1])
    ups_status = "" if ups_status == "-" else ups_status

    battery_charge = clean_html(parsed_data[2])
    runtime_left = clean_html(parsed_data[3])
    nominal_power = clean_html(parsed_data[4])
    ups_load = clean_html(parsed_data[5])
    output_voltage = clean_html(parsed_data[6])

    battery_charge = int(battery_charge.replace('%', '').strip()) if battery_charge != '-' else 0
    runtime_left = parse_timespan(runtime_left)
    nominal_power = int(nominal_power) if nominal_power != '-' else 0
    ups_load = int(ups_load.replace('%', '').strip()) if ups_load != '-' else 0
    output_voltage = int(output_voltage) if output_voltage != '-' else 0

    parsed_data = {
        'Model': ups_model,
        'Status': ups_status,
        'Battery Charge': battery_charge,
        'Runtime Left': runtime_left,
        'Nominal Power': nominal_power,
        'Load': ups_load,
        'Output Voltage': output_voltage,
    }

    if not parsed_data['Model']:
        return

    for key, value in parsed_data.items():
        if key in ['Model', 'Status'] and not value:
            continue

        payload = {
            'name': f'UPS {key}',
            'icon': 'mdi:power' if 'Status' in key or 'Power' in key else
                    'mdi:battery' if 'Charge' in key or 'Battery' in key else
                    'mdi:clock' if 'Runtime' in key else
                    'mdi:percent' if 'Load' in key else
                    'mdi:flash',
        }

        if isinstance(value, (int, float)):
            payload['state_class'] = 'measurement'
            unit_of_measurement = (
                '%' if 'Charge' in key or 'Load' in key else
                'minutes' if 'Runtime' in key else
                'V' if 'Voltage' in key else
                'W' if 'Nominal Power' in key else None
            )
            if unit_of_measurement:
                payload['unit_of_measurement'] = unit_of_measurement

        self.mqtt_publish(payload, 'sensor', value)


# Processes parity check data from unRAID.
async def parity(self, msg_data):
    data = json.loads(msg_data)
    if len(data) < 5:
        return

    current_position = re.search(r'(.+?) \(([\d.]+)\s?%\)', data[2])
    position_size = current_position.group(1).strip()
    position_pct = current_position.group(2).strip()
    state_value = float(position_pct)

    payload = {
        'name': 'Parity Check',
        'unit_of_measurement': '%',
        'icon': 'mdi:database-eye',
        'state_class': 'measurement'
    }

    json_attributes = {
        'total_size': humanfriendly.parse_size(data[0]),
        'elapsed_time': data[1],
        'current_position': humanfriendly.parse_size(position_size),
        'estimated_speed': humanfriendly.parse_size(data[3]),
        'estimated_finish': data[4],
        'sync_errors_corrected': data[5]
    }

    self.mqtt_publish(payload, 'sensor', state_value, json_attributes)


# Processes and manages the state of the array as a binary sensor.
async def var(self, msg_data):
    msg_data = f'[var]\n{msg_data}'

    prefs = Preferences(msg_data)
    var = prefs.as_dict()
    var_json = var['var']

    var_value = 'OFF'
    if 'started' in var_json['mdstate'].lower():
        var_value = 'ON'

    payload = {
        'name': 'Array',
        'device_class': 'running'
    }

    json_attributes = var_json
    self.mqtt_publish(payload, 'binary_sensor', var_value, json_attributes, retain=True)
