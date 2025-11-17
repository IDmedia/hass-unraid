import re
import html
import json
import humanfriendly
from typing import Any, Dict, List
from app.legacy.base import LegacyChannel
from app.collectors.base import EntityUpdate


class ApcUpsChannel(LegacyChannel):
    name = 'apcups'
    channel = 'apcups'
    inactivity_timeout = 0

    def __init__(self, logger, interval: int):
        self.logger = logger
        self.interval = int(interval)

    async def parse(self, msg_data: str) -> List[EntityUpdate]:
        """
        Legacy APC UPS payload (HTML-embedded strings in JSON array), e.g. when connected:
          ["<span>MODEL</span>", "<span>ONLINE</span>", "<span>19 %</span>",
           "<span>20 minutes</span>", "<span>650 W</span>", "<span>12 %</span>", "<span>230 V</span>"]

        When no UPS is connected:
          ["<span>-</span>", "<span>-</span>", "<span>-</span>", "<span>-</span>", "<span>-</span>", "<span>-</span>", "<span>-</span>"]

        Publishes one sensor per field:
          - UPS Model (string)
          - UPS Status (string)
          - UPS Battery Charge (%)
          - UPS Runtime Left (minutes)
          - UPS Nominal Power (W)
          - UPS Load (%)
          - UPS Output Voltage (V)

        Immediate-first publish is handled by LegacyWSRunner; here we add expire_after so HA marks
        unavailable if stream stops.
        """
        updates: List[EntityUpdate] = []

        try:
            msg_data = msg_data.replace(r'\/', '/')
            parsed = json.loads(msg_data)
        except Exception:
            return updates

        if not isinstance(parsed, list) or len(parsed) < 7:
            return updates

        def clean_html(value: str) -> str:
            return re.sub(r'<[^>]+>', '', html.unescape(str(value))).strip()

        def parse_timespan_minutes(value: str) -> int:
            try:
                seconds = humanfriendly.parse_timespan(value)
                return int(seconds / 60)
            except Exception:
                return 0

        def extract_percentage_or_number(value: str, default: int = 0) -> int:
            m_pct = re.search(r'(\d+)\s*%', value)
            if m_pct:
                try:
                    return int(m_pct.group(1))
                except Exception:
                    return default
            m_num = re.search(r'[\d.]+', value)
            if m_num:
                try:
                    return int(float(m_num.group()))
                except Exception:
                    return default
            return default

        ups_model_raw = clean_html(parsed[0])
        ups_status_raw = clean_html(parsed[1])
        battery_charge_raw = clean_html(parsed[2])
        runtime_left_raw = clean_html(parsed[3])
        nominal_power_raw = clean_html(parsed[4])
        ups_load_raw = clean_html(parsed[5])
        output_voltage_raw = clean_html(parsed[6])

        model = '' if ups_model_raw == '-' else ups_model_raw
        status = '' if ups_status_raw == '-' else ups_status_raw
        battery_charge = extract_percentage_or_number(battery_charge_raw) if battery_charge_raw != '-' else 0
        runtime_left = parse_timespan_minutes(runtime_left_raw)
        nominal_power = extract_percentage_or_number(nominal_power_raw, default=0)
        ups_load = extract_percentage_or_number(ups_load_raw) if ups_load_raw != '-' else 0
        output_voltage = extract_percentage_or_number(output_voltage_raw) if output_voltage_raw != '-' else 0

        if not model:
            return updates

        payloads: Dict[str, Any] = {
            'Model': model,
            'Status': status,
            'Battery Charge': battery_charge,
            'Runtime Left': runtime_left,
            'Nominal Power': nominal_power,
            'Load': ups_load,
            'Output Voltage': output_voltage,
        }

        for key, value in payloads.items():
            if key in ('Model', 'Status') and not value:
                continue

            payload = {
                'name': f'UPS {key}',
                'icon': (
                    'mdi:power' if ('Status' in key or 'Power' in key) else
                    'mdi:battery' if ('Charge' in key or 'Battery' in key) else
                    'mdi:clock' if 'Runtime' in key else
                    'mdi:percent' if 'Load' in key else
                    'mdi:flash'
                )
            }

            if isinstance(value, (int, float)):
                payload['state_class'] = 'measurement'
                unit = (
                    '%' if ('Charge' in key or 'Load' in key) else
                    'minutes' if 'Runtime' in key else
                    'V' if 'Voltage' in key else
                    'W' if 'Nominal Power' in key else None
                )
                if unit:
                    payload['unit_of_measurement'] = unit

            updates.append(
                EntityUpdate(
                    sensor_type='sensor',
                    payload=payload,
                    state=value,
                    retain=False,
                    expire_after=max(self.interval * 2, 60),
                    unique_id_suffix=key.lower().replace(' ', '_'),
                )
            )

        return updates


CHANNEL = ApcUpsChannel
