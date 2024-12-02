import pytest

@pytest.mark.usefixtures
def test_cohort_concept_hierarchical_prevalence(test_db):
    bias = test_db
    cohort_query = """
        SELECT person_id, condition_concept_id, 
        condition_start_date as cohort_start_date, 
        condition_end_date as cohort_end_date
        FROM condition_occurrence;
    """
    cohort = bias.create_cohort(
        cohort_name="Diabetes Cohort",
        cohort_desc="Cohort of patients with diabetes-related conditions",
        query=cohort_query,
        created_by="test_user"
    )
    # Test cohort object and methods
    assert cohort is not None, "Cohort creation failed"
    concept_stats = cohort.get_concept_stats()
    assert concept_stats is not None, "Failed to fetch concept stats"
    assert len(concept_stats) > 0, "No concept stats returned"
    print(concept_stats)
    # Check concept prevalence for overlaps
    diabetes_prevalence = next((c for c in concept_stats['condition_occurrence'] if c['ancestor_concept_id'] == 1), None)
    print(diabetes_prevalence)
    assert diabetes_prevalence is not None, "Parent concept prevalence missing"
    assert diabetes_prevalence['prevalence'] < sum(
        c['prevalence'] for c in concept_stats['condition_occurrence'] if c['ancestor_concept_id'] in [2, 3]
    ), "Parent concept prevalence does not reflect overlap"

