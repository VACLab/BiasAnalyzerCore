import os

from numpy.ma.testutils import assert_equal

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


    assert 'template_name' in config
    assert config.get('template_name') == 'cohort_creation_condition_occurrence_query'
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

    # ex_events = config.get('exclusion_criteria')['temporal_events']
    # ex_demographics = config.get('exclusion_criteria').get('demographics')
    # assert 'operator' in ex_events[0]
    # assert 'events' in ex_events[0]
    # assert 'event_type' in ex_events[0]['events'][0]
    # assert_equal(ex_events[0]['events'][0]['event_type'], 'condition_occurrence',
    #              'exclusion event type is not condition_occurrence')
    # assert 'min_birth_year' in ex_demographics
    # assert 'gender' not in ex_demographics
    # assert 'max_birth_year' not in ex_demographics
