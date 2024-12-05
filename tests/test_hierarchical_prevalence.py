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
    # Check concept prevalence for overlaps
    diabetes_prevalence = next((c for c in concept_stats['condition_occurrence']
                                if c['ancestor_concept_id'] == 1 and c['descendant_concept_id'] == 1), None)
    assert diabetes_prevalence is not None, "Parent diabetes concept prevalence missing"
    type1_prevalence = next((c for c in concept_stats['condition_occurrence']
                                if c['ancestor_concept_id'] == 2 and c['descendant_concept_id'] == 2), None)
    assert type1_prevalence is not None, "Child type 1 diabetes concept prevalence missing"
    type2_prevalence = next((c for c in concept_stats['condition_occurrence']
                             if c['ancestor_concept_id'] == 3 and c['descendant_concept_id'] == 3), None)
    assert type2_prevalence is not None, "Child type 2 diabetes concept prevalence missing"
    print(f"type1_prevalence: {type1_prevalence['prevalence']}, type2_prevalence: {type2_prevalence['prevalence']}, "
          f"diabetes_prevalence: {diabetes_prevalence['prevalence']}")
    assert diabetes_prevalence['prevalence'] < type1_prevalence['prevalence'] + type2_prevalence['prevalence'], \
        ("Parent diabetes concept prevalence does not reflect overlap between type 1 and type 2 diabetes "
         "children concept prevalence")
