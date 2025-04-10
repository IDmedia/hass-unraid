import re
import json
import math
import httpx
from lxml import etree
from utils import Preferences
from humanfriendly import parse_size


async def default(self, msg_data, create_config):
    pass


async def session(self, msg_data, create_config):
    self.csrf_token = msg_data


async def cpuload(self, msg_data, create_config):
    prefs = Preferences(msg_data)
    state_value = int(prefs.as_dict()['cpu']['host'])

    payload = {
        'name': 'CPU Utilization',
        'unit_of_measurement': '%',
        'icon': 'mdi:chip',
        'state_class': 'measurement',
    }

    self.mqtt_publish(payload, 'sensor', state_value, create_config=create_config)


async def disks(self, msg_data, create_config):
    prefs = Preferences(msg_data)
    disks = prefs.as_dict()

    for n in disks:
        disk = disks[n]

        disk_name = disk['name']
        disk_temp = int(disk['temp']) if str(disk['temp']).isnumeric() else 0

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
            'state_class': 'measurement'
        }

        json_attributes = disk
        self.mqtt_publish(payload, 'sensor', disk_temp, json_attributes, create_config=create_config, retain=True)


async def shares(self, msg_data, create_config):
    prefs = Preferences(msg_data)
    shares = prefs.as_dict()

    for n in shares:
        share = shares[n]
        share_name = share['name']
        share_disk_count = len(share['include'].split(','))
        share_floor_size = share['floor']
        share_nameorig = share['nameorig']
        share_use_cache = share['usecache']
        share_cachepool = share['cachepool']

        if share_use_cache in ['no', 'yes', 'prefer']:

            async with httpx.AsyncClient() as http:

                # unRAID 6.11
                if self.unraid_version.startswith('6.11'):

                    # Auth header
                    headers = {'Cookie': self.unraid_cookie + ';ssz=ssz'}

                    # Calculate used space
                    params = {
                        'cmd': '/webGui/scripts/share_size',
                        'arg1': share_nameorig,
                        'arg2': 'ssz1',
                        'arg3': share_cachepool,
                        'csrf_token': self.csrf_token
                    }
                    await http.get(f'{self.unraid_url}/update.htm', params=params, headers=headers)

                    # Read result
                    params = {
                        'compute': 'no',
                        'path': 'Shares',
                        'scale': 1,
                        'fill': 'ssz',
                        'number': '.'
                    }

                    r = await http.get(f'{self.unraid_url}/webGui/include/ShareList.php', params=params, headers=headers, timeout=600)

                # unRAID 6.12+
                else:

                    # Auth header
                    headers = {'Cookie': self.unraid_cookie}

                    # Read result
                    data = {
                        'compute': share_nameorig,
                        'path': 'Shares',
                        'all': 1,
                        'csrf_token': self.csrf_token
                    }
                    r = await http.request("GET", url=f'{self.unraid_url}/webGui/include/ShareList.php', data=data, headers=headers, timeout=600)

                if r.status_code == httpx.codes.OK:
                    tree = etree.HTML(r.text)

                    size_total_used = tree.xpath(f'//td/a[text()="{share_nameorig}"]/ancestor::tr[1]/td[6]/text()')
                    size_total_used = next(iter(size_total_used or []), '0').strip()
                    size_total_used = parse_size(size_total_used)

                    size_total_free = tree.xpath(f'//td/a[text()="{share_nameorig}"]/ancestor::tr[1]/td[7]/text()')
                    size_total_free = next(iter(size_total_free or []), '0').strip()
                    size_total_free = parse_size(size_total_free)

                    size_cache_used = tree.xpath(f'//td/a[text()="{share_nameorig}"]/following::tr[1]/td[1][not(contains(text(), "Disk "))]/../td[6]/text()')
                    size_cache_used = next(iter(size_cache_used or []), '0').strip()
                    size_cache_used = parse_size(size_cache_used)

                    size_cache_free = tree.xpath(f'//td/a[text()="{share_nameorig}"]/following::tr[1]/td[1][not(contains(text(), "Disk "))]/../td[7]/text()')
                    size_cache_free = next(iter(size_cache_free or []), '0').strip()
                    size_cache_free = parse_size(size_cache_free)

                    # # Debug
                    # from humanfriendly import format_size
                    # print(f'Share: {share_nameorig}')
                    # print(f'Used (total): {format_size(size_total_used)} Free (total): {format_size(size_total_free)}')
                    # print(f'Used (cache): {format_size(size_cache_used)} Free (total): {format_size(size_cache_free)}')

                    # Recalculate used and free space, converted from bytes to kbytes
                    share['used'] = int(size_total_used / 1000)
                    share['free'] = int((size_total_free - size_cache_free - size_cache_used) / 1000)

        # Skip empty shares
        if share['used'] == 0:
            continue

        # If the drives is exclusive we change the share_disk_count to 1
        if share.get('exclusive') in ['yes']:
            share_disk_count = 1

        share_size_floor = share_disk_count * share_floor_size
        share['free'] -= share_size_floor

        share_size_total = share['used'] + share['free']
        share_used_pct = math.ceil((share['used'] / (share_size_total) * 100))

        payload = {
            'name': f'Share {share_name.title()} Usage',
            'unit_of_measurement': '%',
            'icon': 'mdi:folder-network',
            'state_class': 'measurement'
        }

        json_attributes = share
        self.mqtt_publish(payload, 'sensor', share_used_pct, json_attributes, create_config=create_config, retain=True)


async def temperature(self, msg_data, create_config):
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

            self.mqtt_publish(payload, 'sensor', device_value, create_config=create_config)


async def update1(self, msg_data, create_config):
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

            self.mqtt_publish(payload, 'sensor', memory_value, create_config=create_config)

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

            self.mqtt_publish(payload, 'sensor', fan_value, create_config=create_config)


async def update3(self, msg_data, create_config):
    network_download = 0
    network_upload = 0

    for line in msg_data.splitlines():
        network = [n.strip() for n in line.split(' ')]

        if not network[0].startswith('eth'):
            continue

        network_download_text = ' '.join(network[1:3])
        network_download += round(parse_size(network_download_text) / 1000 / 1000, 1)
        payload_download = {
            'name': 'Download Throughput',
            'unit_of_measurement': 'Mbit/s',
            'icon': 'mdi:download',
            'state_class': 'measurement'
        }

        network_upload_text = ' '.join(network[3:5])
        network_upload += round(parse_size(network_upload_text) / 1000 / 1000, 1)
        payload_upload = {
            'name': 'Upload Throughput',
            'unit_of_measurement': 'Mbit/s',
            'icon': 'mdi:download',
            'state_class': 'measurement'
        }

        self.mqtt_publish(payload_download, 'sensor', network_download, create_config=create_config)
        self.mqtt_publish(payload_upload, 'sensor', network_upload, create_config=create_config)


async def parity(self, msg_data, create_config):
    data = json.loads(msg_data)

    if len(data) < 5:
        return

    current_position = re.search(r"(.+?) \(([\d.]+)\s?%\)", data[2])
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
        'total_size': parse_size(data[0]),
        'elapsed_time': data[1],
        'current_position': parse_size(position_size),
        'estimated_speed': parse_size(data[3]),
        'estimated_finish': data[4],
        'sync_errors_corrected': data[5]
    }

    self.mqtt_publish(payload, 'sensor', state_value, json_attributes, create_config=create_config)


async def var(self, msg_data, create_config):
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
    self.mqtt_publish(payload, 'binary_sensor', var_value, json_attributes, create_config=create_config, retain=True)
