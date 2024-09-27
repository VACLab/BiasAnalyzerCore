import yaml

def load_config(config_file):
    with open(config_file) as f:
        config_dict = yaml.safe_load(f)

    return config_dict
