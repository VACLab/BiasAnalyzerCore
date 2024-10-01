import yaml
from healthdatabias.models import Configuration


def load_config(config_file):
    with open(config_file) as f:
        config = yaml.safe_load(f)
        print(config, flush=True)
        return Configuration(**config)
