import pytest
import os

from numpy.ma.testutils import assert_equal


@pytest.mark.usefixtures
def test_cohort_creation_all(test_db):
    bias = test_db
    cohort = bias.create_cohort(
        "COVID-19 patient",
        "Cohort of young female patients with COVID-19",
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
    print(f'stats: {stats}')
    assert_equal(stats[0]['total_count'], 2)
