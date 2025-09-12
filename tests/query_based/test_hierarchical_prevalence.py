import logging

def test_cohort_concept_hierarchical_prevalence(test_db, caplog):
    bias = test_db
    cohort_query = """
        SELECT person_id, condition_concept_id, 
        condition_start_date as cohort_start_date, 
        condition_end_date as cohort_end_date
        FROM condition_occurrence;
    """

    cohort = bias.create_cohort(
        "Diabetes Cohort",
        "Cohort of patients with diabetes-related conditions",
        cohort_query,
        "test_user"
    )
    # Test cohort object and methods
    assert cohort is not None, "Cohort creation failed"
    # test concept_type must be one of the supported OMOP domain name
    caplog.clear()
    with caplog.at_level(logging.ERROR):
        concept_stats = cohort.get_concept_stats(concept_type='dummy_invalid')
    assert 'Invalid concept_type' in caplog.text
    assert concept_stats == {}

    # test vocab must be None to use the default vocab or one of the supported OMOP vocabulary id
    caplog.clear()
    with caplog.at_level(logging.ERROR):
        concept_stats = cohort.get_concept_stats(vocab='dummy_invalid_vocab')
    assert 'is not a valid vocabulary' in caplog.text
    assert concept_stats == {}

    # test the cohort does not have procedure_occurrence related concepts
    concept_stats = cohort.get_concept_stats(concept_type='procedure_occurrence')
    assert concept_stats == {'procedure_occurrence': []}

    concept_stats = cohort.get_concept_stats(vocab='ICD10CM')
    assert concept_stats is not None, "Failed to fetch concept stats"
    assert len(concept_stats) > 0, "No concept stats returned"
    # check returned data
    assert not all(s['ancestor_concept_id'] == s['descendant_concept_id']
                   for s in concept_stats['condition_occurrence']), \
        "Some ancestor_concept_id and descendant_concept_id should differ"
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
