import os
import datetime
import logging
import pytest
from sqlalchemy.exc import SQLAlchemyError
from numpy.ma.testutils import assert_equal
from biasanalyzer.models import DemographicsCriteria, TemporalEvent, TemporalEventGroup


def test_cohort_yaml_validation(test_db):
    invalid_data = {
        "gender": "female",
        "min_birth_year": 2000,
        "max_birth_year": 1999  # Invalid: less than min_birth_year
    }
    with pytest.raises(ValueError):
        DemographicsCriteria(**invalid_data)

    invalid_data = {
        "event_type": "date",
        "event_concept_id": "dummy"
    }
    # validate date event_type must have a timestamp field
    with pytest.raises(ValueError):
        TemporalEvent(**invalid_data)

    invalid_data = {
        "operator": "BEFORE",
        "events": [
            {'event_type': 'condition_occurrence',
             'event_concept_id': 201826},
            {'event_type': 'drug_exposure',
             'event_concept_id': 4285892},
        ],
        "interval": [100, 50]
    }
    # validate interval start must be smaller than interval end
    with pytest.raises(ValueError):
        TemporalEventGroup(**invalid_data)

    # validate interval must be either a list of 2 integers or a None
    invalid_data["interval"] = [123]
    with pytest.raises(ValueError):
        TemporalEventGroup(**invalid_data)

    # validate NOT operator cannot have more than one event
    invalid_data["operator"] = "NOT"
    with pytest.raises(ValueError):
        TemporalEventGroup(**invalid_data)

    # validate BEFORE operator must have two events
    invalid_data["operator"] = "BEFORE"
    del invalid_data["events"][1]
    with pytest.raises(ValueError):
        TemporalEventGroup(**invalid_data)

def test_cohort_creation_baseline(caplog, test_db):
    bias = test_db
    cohort = bias.create_cohort(
        "COVID-19 patient",
        "Cohort of young female patients",
        os.path.join(os.path.dirname(__file__), '..', 'assets', 'cohort_creation',
                     'test_cohort_creation_condition_occurrence_config_baseline.yaml'),
        "test_user"
    )

    # Test cohort object and methods
    assert cohort is not None, "Cohort creation failed"
    cohort_id = cohort.cohort_id
    assert bias.bias_db.get_cohort_definition(cohort_id)['name'] == "COVID-19 patient"
    assert bias.bias_db.get_cohort_definition(cohort_id + 1) == {}
    assert cohort.metadata is not None, "Cohort creation wrongly returned None metadata"
    assert 'creation_info' in cohort.metadata, "Cohort creation does not contain 'creation_info' key"
    assert cohort.data is not None, "Cohort creation wrongly returned None data"
    caplog.clear()
    with caplog.at_level(logging.ERROR):
        cohort.get_distributions('ethnicity')
    assert "Distribution for variable 'ethnicity' is not available" in caplog.text

    assert len(cohort.get_distributions('age')) == 10, "Cohort get_distribution('age') does not return 10 age_bin items"
    assert len(cohort.get_distributions('gender')) == 3, ("Cohort get_distribution('gender') does not return "
                                                          "3 gender_bin items")

    patient_ids = set([item['subject_id'] for item in cohort.data])
    assert_equal(len(patient_ids), 5)
    assert_equal(patient_ids, {106, 108, 110, 111, 112})
    # select two patients to check for cohort_start_date and cohort_end_date automatically computed
    patient_106 = next(item for item in cohort.data if item['subject_id'] == 106)
    patient_108 = next(item for item in cohort.data if item['subject_id'] == 108)

    # Replace dates with actual values from your test data
    assert_equal(patient_106['cohort_start_date'], datetime.date(2023, 3, 1),
                 "Incorrect cohort_start_date for patient 106")
    assert_equal(patient_106['cohort_end_date'], datetime.date(2023, 3, 15),
                 "Incorrect cohort_end_date for patient 106")
    assert_equal(patient_108['cohort_start_date'], datetime.date(2020, 4, 10),
                 "Incorrect cohort_start_date for patient 108")
    assert_equal(patient_108['cohort_end_date'], datetime.date(2020, 4, 27),
                 "Incorrect cohort_end_date for patient 108")


def test_cohort_creation_study(test_db):
    bias = test_db
    cohort = bias.create_cohort(
        "COVID-19 patient",
        "Cohort of young female patients with COVID-19",
        os.path.join(os.path.dirname(__file__), '..', 'assets', 'cohort_creation',
                    'test_cohort_creation_condition_occurrence_config_study.yaml'),
        "test_user"
    )
    # Test cohort object and methods
    assert cohort is not None, "Cohort creation failed"
    assert cohort.metadata is not None, "Cohort creation wrongly returned None metadata"
    assert 'creation_info' in cohort.metadata, "Cohort creation does not contain 'creation_info' key"
    assert cohort.data is not None, "Cohort creation wrongly returned None data"
    patient_ids = set([item['subject_id'] for item in cohort.data])
    assert_equal(len(patient_ids), 4)
    assert_equal(patient_ids, {108, 110, 111, 112})

def test_cohort_creation_study2(caplog, test_db):
    bias = test_db
    caplog.clear()
    with caplog.at_level(logging.INFO):
        cohort = bias.create_cohort(
            "COVID-19 patient",
            "Cohort of young female patients with no COVID-19",
            os.path.join(os.path.dirname(__file__), '..', 'assets', 'cohort_creation',
                        'test_cohort_creation_condition_occurrence_config_study2.yaml'),
            "test_user",
            delay=1
        )
    assert 'Simulating long-running task' in caplog.text
    # Test cohort object and methods
    assert cohort is not None, "Cohort creation failed"
    assert cohort.metadata is not None, "Cohort creation wrongly returned None metadata"
    assert 'creation_info' in cohort.metadata, "Cohort creation does not contain 'creation_info' key"
    assert cohort.data is not None, "Cohort creation wrongly returned None data"
    patient_ids = set([item['subject_id'] for item in cohort.data])
    assert_equal(len(patient_ids), 1)
    assert_equal(patient_ids, {106})

def test_cohort_creation_all(caplog, test_db):
    bias = test_db
    cohort = bias.create_cohort(
        "COVID-19 patient",
        "Cohort of young female patients with COVID-19 who have the condition with difficulty breathing 2 to 5 days "
        "before a COVID diagnosis 3/15/20-12/11/20 AND have at least one emergency room visit or at least "
        "two inpatient visits",
        os.path.join(os.path.dirname(__file__), '..', 'assets', 'cohort_creation',
                     'test_cohort_creation_condition_occurrence_config.yaml'),
        "test_user"
    )
    # Test cohort object and methods
    assert cohort is not None, "Cohort creation failed"
    assert cohort.metadata is not None, "Cohort creation wrongly returned None metadata"
    assert 'creation_info' in cohort.metadata, "Cohort creation does not contain 'creation_info' key"
    stats = cohort.get_stats()
    assert stats is not None, "Created cohort's stats is None"
    gender_stats = cohort.get_stats(variable='gender')
    assert gender_stats is not None, "Created cohort's gender stats is None"
    caplog.clear()
    with caplog.at_level(logging.ERROR):
        cohort.get_stats(variable='address')
    assert 'is not available' in caplog.text
    assert gender_stats is not None, "Created cohort's gender stats is None"
    assert cohort.data is not None, "Cohort creation wrongly returned None data"
    patient_ids = set([item['subject_id'] for item in cohort.data])
    print(f'patient_ids: {patient_ids}', flush=True)
    assert_equal(len(patient_ids), 2)
    assert_equal(patient_ids, {108, 110})

def test_cohort_creation_multiple_temporary_groups_with_no_operator(test_db):
    bias = test_db
    cohort = bias.create_cohort(
        "Patients with COVID or other emergency conditions",
        "Cohort of young female patients who either have COVID-19 with difficulty breathing 2 to 5 days "
        "before a COVID diagnosis 3/15/20-12/11/20 OR have at least one emergency room visit or at least "
        "two inpatient visits",
        os.path.join(os.path.dirname(__file__), '..', 'assets', 'cohort_creation',
                     'test_cohort_creation_multiple_temporal_groups_without_operator.yaml'),
        "test_user"
    )
    # Test cohort object and methods
    patient_ids = set([item['subject_id'] for item in cohort.data])
    print(f'patient_ids: {patient_ids}', flush=True)
    assert_equal(len(patient_ids), 2)
    assert_equal(patient_ids, {108, 110})

def test_cohort_creation_mixed_domains(test_db):
    """
    Test cohort creation with mixed domains (condition, drug, visit, procedure).
    """
    bias = test_db
    cohort = bias.create_cohort(
        "Female diabetes patients born between 1970 and 2000",
        "Cohort of female patients with diabetes who had insulin prescribed 0-30 days after diagnosis "
        "and have at least one outpatient or emergency visit and underwent a blood test before 12/31/2020, "
        "with patients born after 1995 and with cardiac surgery excluded",
        os.path.join(os.path.dirname(__file__), '..', 'assets', 'cohort_creation',
                     'test_cohort_creation_config.yaml'),
        "test_user"
    )

    # Test cohort object and methods
    assert cohort is not None, "Cohort creation failed"
    print(f'metadata: {cohort.metadata}')
    assert cohort.metadata is not None, "Cohort creation wrongly returned None metadata"
    assert 'creation_info' in cohort.metadata, "Cohort creation does not contain 'creation_info' key"
    stats = cohort.get_stats()
    assert stats is not None, "Created cohort's stats is None"
    assert cohort.data is not None, "Cohort creation wrongly returned None data"
    patient_ids = set([item['subject_id'] for item in cohort.data])
    print(f'patient_ids: {patient_ids}', flush=True)
    assert_equal(len(patient_ids), 2)
    assert_equal(patient_ids, {1, 2})
    start_dates = [item['cohort_start_date'] for item in cohort.data]
    assert_equal(len(start_dates), 2)
    assert_equal(start_dates, [datetime.date(2020, 6, 1), datetime.date(2020, 6, 1)])
    end_dates = [item['cohort_end_date'] for item in cohort.data]
    assert_equal(len(end_dates), 2)
    assert_equal(end_dates, [datetime.date(2020, 6, 20), datetime.date(2020, 6, 20)])

def test_cohort_comparison(test_db):
    bias = test_db
    cohort_base = bias.create_cohort(
        "COVID-19 patient",
        "Cohort of young female patients",
        os.path.join(os.path.dirname(__file__), '..', 'assets', 'cohort_creation',
                     'test_cohort_creation_condition_occurrence_config_baseline.yaml'),
        "test_user"
    )
    cohort_study = bias.create_cohort(
        "Female diabetes patients born between 1970 and 2000",
        "Cohort of female patients with diabetes who had insulin prescribed 0-30 days after diagnosis "
        "and have at least one outpatient or emergency visit and underwent a blood test before 12/31/2020, "
        "with patients born after 1995 and with cardiac surgery excluded",
        os.path.join(os.path.dirname(__file__), '..', 'assets', 'cohort_creation',
                     'test_cohort_creation_config.yaml'),
        "test_user"
    )
    results = bias.compare_cohorts(cohort_base.cohort_id, cohort_study.cohort_id)
    assert {'gender_hellinger_distance': 0.0} in results
    assert any('age_hellinger_distance' in r for r in results)

def test_cohort_invalid(caplog, test_db):
    caplog.clear()
    with caplog.at_level(logging.INFO):
        invalid_cohort = test_db.create_cohort('invalid_cohort', 'invalid_cohort',
                                               'invalid_yaml_file.yml',
                                               'invalid_created_by')
    assert 'cohort creation configuration file does not exist' in caplog.text
    assert invalid_cohort is None

    caplog.clear()
    with caplog.at_level(logging.INFO):
        invalid_cohort = test_db.create_cohort('invalid_cohort', 'invalid_cohort',
                                               os.path.join(os.path.dirname(__file__), '..', 'assets', 'config',
                                                            'test_config.yaml'), 'invalid_created_by')
    assert 'configuration yaml file is not valid' in caplog.text
    assert invalid_cohort is None

    with caplog.at_level(logging.INFO):
        invalid_cohort = test_db.create_cohort('invalid_cohort', 'invalid_cohort',
                                               'INVALID SQL QUERY STRING',
                                               'invalid_created_by')
    assert 'Error executing query:' in caplog.text
    assert invalid_cohort is None

def test_create_cohort_sqlalchemy_error(monkeypatch, fresh_bias_obj):
    # Mock omop_db methods
    class MockOmopDB:
        def get_session(self):
            return self  # not used after error
        def execute_query(self, query):
            raise SQLAlchemyError("Mocked SQLAlchemy error")
        def close(self):
            pass

    class MockBiasDB:
        def create_cohort_definition(self, *args, **kwargs):
            pass
        def create_cohort_in_bulk(self, *args, **kwargs):
            pass
        def close(self):
            pass

    fresh_bias_obj.omop_cdm_db = MockOmopDB()
    fresh_bias_obj.bias_db = MockBiasDB()

    result = fresh_bias_obj.create_cohort("test", "desc", "SELECT * FROM person", "test_user")

    assert result is None
