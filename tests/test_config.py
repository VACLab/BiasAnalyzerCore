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
        config = load_cohort_creation_config(
            os.path.join(os.path.dirname(__file__), 'assets',
                         'test_cohort_creation_condition_occurrence_config.yaml'))
    except Exception as e:
        assert False, f"test_load_cohort_creation_config() raised an exception: {e}"

    assert config.get('template_name') == 'cohort_creation_condition_occurrence_query'
    assert 'condition_occurrence' in config.get('inclusion_criteria')
    assert 'condition_occurrence' in config.get('exclusion_criteria')
    in_criteria = config.get('inclusion_criteria')['condition_occurrence']
    assert 'condition_concept_id' in in_criteria
    assert 'gender' in in_criteria
    assert 'min_birth_year' in in_criteria
    assert 'max_birth_year' in in_criteria
    assert in_criteria['max_birth_year'] >= in_criteria['min_birth_year']
    assert in_criteria['gender'] == 'female' or in_criteria['gender'] == 'male'
    ex_criteria = config.get('exclusion_criteria')['condition_occurrence']
    assert 'condition_concept_id' in ex_criteria
    assert 'min_birth_year' in ex_criteria
    assert 'gender' not in ex_criteria
    assert 'max_birth_year' not in ex_criteria
