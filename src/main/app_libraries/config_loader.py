# configs.py
import yaml
import os
import glob

# Global dictionary to store configurations from all YAML files
CONFIGS = {}

def read_yaml_file(file_path):
    """ Read a single YAML file and return its contents. """
    with open(file_path, 'r') as file:
        return yaml.safe_load(file)

def load_config(configs_path):
    """ Load configurations from all YAML files in the specified directory. """
    global CONFIGS
    yaml_files = glob.glob(os.path.join(configs_path, '*.yml')) + glob.glob(os.path.join(configs_path, '*.yaml'))
    for file_path in yaml_files:
        file_name = os.path.basename(file_path)
        CONFIGS[file_name] = read_yaml_file(file_path)

def get_configs(file_name, *args):
    """ Access specific values from the configurations using file name and keys. """
    if file_name not in CONFIGS:
        raise Exception(f"Configuration file {file_name} not loaded")

    value = CONFIGS[file_name]
    for key in args:
        if isinstance(value, dict) and key in value:
            value = value[key]
        else:
            return None
    return value

# Example usage of load_configs
# load_configs('path/to/configs')
