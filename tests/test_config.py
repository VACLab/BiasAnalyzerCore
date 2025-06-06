import os

from biasanalyzer.config import load_config, load_cohort_creation_config


def test_load_config():
    try:
        config = load_config(os.path.join(os.path.dirname(__file__), 'assets', 'config', 'test_config.yaml'))
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
        config = load_cohort_creation_config(
            os.path.join(os.path.dirname(__file__), 'assets', 'cohort_creation',
                         'test_cohort_creation_condition_occurrence_config.yaml'))
    except Exception as e:
        assert False, f"test_load_cohort_creation_config() raised an exception: {e}"


    assert 'inclusion_criteria' in config
    # assert 'exclusion_criteria' in config
    assert 'temporal_events' in config.get('inclusion_criteria')
    # assert 'temporal_events' in config.get('exclusion_criteria')
    assert 'demographics' in config.get('inclusion_criteria')
    # assert 'demographics' in config.get('exclusion_criteria')
    demographics = config.get('inclusion_criteria').get('demographics')
    assert 'gender' in demographics
    assert 'min_birth_year' in demographics
    assert 'max_birth_year' in demographics
    assert demographics['max_birth_year'] >= demographics['min_birth_year']
    assert demographics['gender'] == 'female' or demographics['gender'] == 'male'

    in_events = config.get('inclusion_criteria')['temporal_events']
    assert 'operator' in in_events[0]
    assert 'events' in in_events[0]
