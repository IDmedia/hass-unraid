import os
import re
import json
import yaml
import hashlib
import configparser
from functools import wraps


class Preferences:
    def __init__(self, string_ini):
        self.config = configparser.ConfigParser()
        self.config.read_string(string_ini)
        self.d = self.to_dict(self.config._sections)

    def as_dict(self):
        return self.d

    def to_dict(self, config):
        """
        Nested OrderedDict to normal dict.
        Also, remove the annoying quotes (apostrophes) from around string values.
        """
        d = json.loads(json.dumps(config))
        d = remove_quotes(d)
        d = {k: v for k, v in d.items() if v}

        return d


def calculate_hash(data):
    return hashlib.md5(json.dumps(data, sort_keys=True).encode()).hexdigest()


def handle_sigterm(*args):
    raise KeyboardInterrupt()


def normalize_str(string):
    string = string.lower()
    string = string.replace(' ', '_')
    string = ''.join([c for c in string if c.isalpha() or c.isdigit() or c == '_']).rstrip()

    return string


def remove_quotes(config):
    for key, value in list(config.items()):

        # Remove quotes from section
        key_strip = key.strip('"')
        config[key_strip] = config.pop(key)

        if isinstance(value, str):
            s = config[key_strip]

            # Remove quotes from value
            s = s.strip('"')

            # Convert strings to numbers
            try:
                s = int(s)
            except ValueError:
                pass

            config[key_strip] = s
        if isinstance(value, dict):
            config[key_strip] = remove_quotes(value)

    return config


def compare_versions(version1, version2):
    def is_valid_version(version):
        # Check if the version contains at least one digit
        return bool(re.search(r'\d', version))

    def normalize_version(version):
        # Use a regex to extract all numeric parts separated by dots
        return [int(part) for part in re.findall(r'\d+', version)]

    # Handle invalid or non-numeric versions
    if not version1 or not is_valid_version(version1):
        return None
    if not version2 or not is_valid_version(version2):
        return None

    # Normalize the versions by extracting numeric parts and converting to a list of integers
    parts1 = normalize_version(version1)
    parts2 = normalize_version(version2)

    # Compare each part of the version (major, minor, patch, etc.)
    for v1, v2 in zip(parts1, parts2):
        if v1 < v2:
            return -1
        elif v1 > v2:
            return 1

    # Handle cases where the versions have different lengths
    if len(parts1) < len(parts2):
        return -1
    elif len(parts1) > len(parts2):
        return 1

    # Versions are equal
    return 0


def load_file(path_to_file):
    if not os.path.isfile(path_to_file):
        return {}

    filename, extension = os.path.splitext(path_to_file)
    with open(path_to_file) as f:
        try:
            if 'json' in extension:
                data = json.load(f)
            else:
                data = yaml.safe_load(f)
        except Exception:
            return {}

    return data


def log_errors(parser_name):
    """
    A decorator to catch and log errors along with the raw `msg_data`.

    :param parser_name: Name of the parser being wrapped (used for logging purposes).
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(self, msg_data, *args, **kwargs):
            try:
                # Call the original parser function
                return await func(self, msg_data, *args, **kwargs)
            except Exception as e:
                # Log the structured error message
                self.logger.error(
                    f'Error in parser "{parser_name}". Exception occurred: {type(e).__name__} - {e}'
                )
                self.logger.error(f'Raw msg_data causing the error:\n---\n{msg_data}\n---')

                # Log full tracebacks in their own entry (to avoid inline interleaving)
                self.logger.exception(f'Parser "{parser_name}" failed with error')

                # Prevent "Task exception was never retrieved" messages by fully handling the exception
                # Suppress further propagation to the asyncio event loop
                return None
        return wrapper
    return decorator
