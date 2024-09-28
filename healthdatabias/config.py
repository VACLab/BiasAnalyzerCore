import yaml
from models import RootOMOPCDM


def load_config(config_file):
    with open(config_file) as f:
        config = yaml.safe_load(f)
        return RootOMOPCDM(**config).model_dump()
