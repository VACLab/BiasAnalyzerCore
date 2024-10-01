import os
from healthdatabias.config import load_config


def test_load_config():
    try:
        config = load_config(os.path.join(os.path.dirname(__file__), 'assets', 'test_config.yaml'))
    except Exception as e:
        assert False, f"load_config() raised an exception: {e}"
