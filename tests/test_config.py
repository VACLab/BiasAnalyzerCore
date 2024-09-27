import os
from healthdatabias.config import load_config


def test_load_config():
    config = load_config(os.path.join(os.path.dirname(__file__), 'assets', 'test_config.yaml'))
    assert config.get('root_omop_cdm_database') == {
        'username': 'test_username',
        'password': 'test_password',
        'hostname': 'test_db_hostname',
        'database': 'test_db_name',
        'port': 5432
    }
