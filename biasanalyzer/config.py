import yaml
from biasanalyzer.models import Configuration


def load_config(config_file):
    with open(config_file) as f:
        config = yaml.safe_load(f)
        Configuration(**config)
        return config
