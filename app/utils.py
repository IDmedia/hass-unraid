import os
import re
import json
import yaml
import hashlib
import logging
import functools
from lxml import etree
from typing import Any, Callable, Dict


def load_file(path_to_file: str) -> Dict[str, Any]:
    if not os.path.isfile(path_to_file):
        return {}
    _, extension = os.path.splitext(path_to_file)
    with open(path_to_file, 'r', encoding='utf-8') as f:
        try:
            if extension.lower() == '.json':
                return json.load(f)
            return yaml.safe_load(f)
        except Exception:
            return {}


def parse_smart_data(html_data: str, logger) -> Dict[str, Any]:
    """
    Parses the SMART table HTML response and converts it into a snake_case key-value JSON format.

    The key is the 'Attribute Name' (converted to snake_case), and the value is extracted from
    the rightmost column with data for that row. Numeric values are converted to int or float.
    """

    def to_snake_case(name: str) -> str:
        return re.sub(r'\W+', '_', name).strip('_').lower()

    def parse_value(value: str) -> Any:
        try:
            return int(value)
        except ValueError:
            try:
                return float(value)
            except ValueError:
                return value

    try:
        tree = etree.HTML(html_data)
        rows = tree.xpath('//tr')
        smart_data: Dict[str, Any] = {}

        for row in rows:
            columns = row.xpath('td')
            if len(columns) < 2:
                continue

            attribute_name = columns[1].text.strip() if columns[1].text else ''
            if not attribute_name:
                continue

            snake_case_attr_name = to_snake_case(attribute_name)

            value = None
            for col in reversed(columns):
                cell_text = col.text.strip() if col.text else ''
                if cell_text:
                    value = parse_value(cell_text)
                    break

            if snake_case_attr_name and value is not None:
                smart_data[snake_case_attr_name] = value

        return smart_data
    except Exception as e:
        logger.error(f'Failed to parse SMART data: {e}')
        return {}


def normalize_str(string: str) -> str:
    s = string.lower().replace(' ', '_')
    s = ''.join([c for c in s if c.isalpha() or c.isdigit() or c == '_']).rstrip()
    return s


def normalize_keys_lower(obj: Any) -> Any:
    """
    Recursively lower-case dict keys. Lists are processed element-wise; non-dict types are returned as-is.
    """
    if isinstance(obj, dict):
        return {str(k).lower(): normalize_keys_lower(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [normalize_keys_lower(v) for v in obj]
    return obj


def to_snake_case(name: str) -> str:
    """
    Convert camelCase/PascalCase/mixed to snake_case lower.
    Examples: 'nameOrig' -> 'name_orig', 'splitLevel' -> 'split_level', 'URLValue' -> 'url_value'
    """
    s1 = re.sub(r'([A-Z]+)', r'_\1', name).strip('_')
    s2 = re.sub(r'__+', r'_', s1)
    return s2.lower()


def normalize_keys_snake(obj: Any) -> Any:
    """
    Recursively convert dict keys to snake_case lower.
    """
    if isinstance(obj, dict):
        return {to_snake_case(str(k)): normalize_keys_snake(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [normalize_keys_snake(v) for v in obj]
    return obj


def calculate_hash(data: Dict[str, Any]) -> str:
    return hashlib.md5(json.dumps(data, sort_keys=True).encode('utf-8')).hexdigest()


def setup_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s [%(name)s] [%(levelname)-8s] %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger


def log_errors(context: str) -> Callable:
    """
    Decorator for async functions to catch and log errors with context.
    """

    def decorator(func: Callable):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            logger = None
            if args:
                self_obj = args[0]
                logger = getattr(self_obj, 'logger', None)
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                if logger:
                    logger.error(f'Error in "{context}": {type(e).__name__} - {e}')
                    logger.exception(f'"{context}" failed with error')
                else:
                    print(f'Error in "{context}": {type(e).__name__} - {e}')
                return None

        return wrapper

    return decorator
