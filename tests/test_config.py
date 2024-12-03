import os
from biasanalyzer.config import load_config


def test_load_config():
    try:
        config = load_config(os.path.join(os.path.dirname(__file__), 'assets', 'test_config.yaml'))
    except Exception as e:
        assert False, f"load_config() raised an exception: {e}"

    assert config.get('root_omop_cdm_database') == {
        'database_type': 'duckdb',
        'username': 'test_username',
        'password': 'test_password',
        'hostname': 'test_db_hostname',
        'database': 'shared_test_db.duckdb',
        'port': 5432
    }
