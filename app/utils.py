import os
import json
import yaml
import configparser


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
        # Check if the version contains only digits and dots (e.g., "7.1.0")
        return all(part.isdigit() for part in version.split('.'))

    # Handle invalid or non-numeric versions
    if not version1 or not is_valid_version(version1):
        return None
    if not version2 or not is_valid_version(version2):
        return None

    # Split the versions into parts for comparison
    parts1 = list(map(int, version1.split('.')))
    parts2 = list(map(int, version2.split('.')))

    # Compare each part of the version (major, minor, patch)
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
