import pytest
import os
import datetime
from numpy.ma.testutils import assert_equal


@pytest.mark.usefixtures
def test_cohort_creation_baseline(test_db):
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
    print(f'metadata: {cohort.metadata}')
    assert cohort.metadata is not None, "Cohort creation wrongly returned None metadata"
    assert 'creation_info' in cohort.metadata, "Cohort creation does not contain 'creation_info' key"
    assert cohort.data is not None, "Cohort creation wrongly returned None data"
    print(f'baseline cohort data: {cohort.data}', flush=True)
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


@pytest.mark.usefixtures
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
    print(f'metadata: {cohort.metadata}')
    print(f'data: {cohort.data}')
    assert cohort.metadata is not None, "Cohort creation wrongly returned None metadata"
    assert 'creation_info' in cohort.metadata, "Cohort creation does not contain 'creation_info' key"
    assert cohort.data is not None, "Cohort creation wrongly returned None data"
    patient_ids = set([item['subject_id'] for item in cohort.data])
    assert_equal(len(patient_ids), 4)
    assert_equal(patient_ids, {108, 110, 111, 112})

@pytest.mark.usefixtures
def test_cohort_creation_study2(test_db):
    bias = test_db
    cohort = bias.create_cohort(
        "COVID-19 patient",
        "Cohort of young female patients with no COVID-19",
        os.path.join(os.path.dirname(__file__), '..', 'assets', 'cohort_creation',
                    'test_cohort_creation_condition_occurrence_config_study2.yaml'),
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
    assert_equal(len(patient_ids), 1)
    assert_equal(patient_ids, {106})

@pytest.mark.usefixtures
def test_cohort_creation_all(test_db):
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

@pytest.mark.usefixtures
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
