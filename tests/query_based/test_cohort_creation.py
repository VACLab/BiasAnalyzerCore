import pytest
import os

from numpy.ma.testutils import assert_equal


@pytest.mark.usefixtures
def test_cohort_creation_baseline(test_db):
    bias = test_db
    cohort = bias.create_cohort(
        "COVID-19 patient",
        "Cohort of young female patients",
        os.path.join(os.path.dirname(__file__), '..', 'assets',
                     'test_cohort_creation_condition_occurrence_config_baseline.yaml'),
        "test_user"
    )
    # Test cohort object and methods
    assert cohort is not None, "Cohort creation failed"
    print(f'metadata: {cohort.metadata}')
    assert cohort.metadata is not None, "Cohort creation wrongly returned None metadata"
    assert 'creation_info' in cohort.metadata, "Cohort creation does not contain 'creation_info' key"
    assert cohort.data is not None, "Cohort creation wrongly returned None data"
    patient_ids = set([item['subject_id'] for item in cohort.data])
    assert_equal(len(patient_ids), 5)
    assert_equal(patient_ids, {106, 108, 110, 111, 112})

@pytest.mark.usefixtures
def test_cohort_creation_study(test_db):
    bias = test_db
    cohort = bias.create_cohort(
        "COVID-19 patient",
        "Cohort of young female patients with COVID-19",
        os.path.join(os.path.dirname(__file__), '..', 'assets',
                    'test_cohort_creation_condition_occurrence_config_study.yaml'),
        "test_user"
    )
    # Test cohort object and methods
    assert cohort is not None, "Cohort creation failed"
    print(f'metadata: {cohort.metadata}')
    print(f'data: {cohort.data}')
    assert cohort.metadata is not None, "Cohort creation wrongly returned None metadata"
    assert 'creation_info' in cohort.metadata, "Cohort creation does not contain 'creation_info' key"
    assert cohort.data is not None, "Cohort creation wrongly returned None data"
    patient_ids = set([item['subject_id'] for item in cohort.data])
    assert_equal(len(patient_ids), 4)
    assert_equal(patient_ids, {108, 110, 111, 112})

@pytest.mark.usefixtures
def test_cohort_creation_all(test_db):
    bias = test_db
    cohort = bias.create_cohort(
        "COVID-19 patient",
        "Cohort of young female patients with COVID-19 who have the condition with difficulty breathing 2 to 5 days "
        "before a COVID diagnosis 3/15/20-12/11/20 AND have at least one emergency room visit or at least "
        "two inpatient visits",
        os.path.join(os.path.dirname(__file__), '..', 'assets',
                     'test_cohort_creation_condition_occurrence_config.yaml'),
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
    assert_equal(patient_ids, {108, 110})
