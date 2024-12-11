import os
from biasanalyzer.config import load_config, load_cohort_creation_config


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

def test_load_cohort_creation_config():
    try:
        config = load_cohort_creation_config(os.path.join(os.path.dirname(__file__), 'assets',
                                                          'test_cohort_creation_config.yaml'))
    except Exception as e:
        assert False, f"test_load_cohort_creation_config() raised an exception: {e}"

    assert config.get('template_name') == 'cohort_creation_query'
    assert config.get('criteria') == {
        'condition_concept_id': 37311061,
        'gender_concept_id': 8532,
        'min_birth_year': 2000
    }
