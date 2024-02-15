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
