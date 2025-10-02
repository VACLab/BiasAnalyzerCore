import pytest
from functools import reduce
from biasanalyzer.concept import ConceptHierarchy, ConceptNode


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
    with pytest.raises(ValueError):
        cohort.get_concept_stats(concept_type='dummy_invalid')

    # test vocab must be None to use the default vocab or one of the supported OMOP vocabulary id
    with pytest.raises(ValueError):
        cohort.get_concept_stats(vocab='dummy_invalid_vocab')

    # test the cohort does not have procedure_occurrence related concepts
    with pytest.raises(ValueError):
        cohort.get_concept_stats(concept_type='procedure_occurrence')

    concept_stats, _ = cohort.get_concept_stats(vocab='ICD10CM', print_concept_hierarchy=True)
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

def test_identifier_normalization_and_cache():
    ConceptHierarchy.clear_cache()
    # identifiers are normalized
    assert ConceptHierarchy._normalize_identifier("2+1") == "1+2"
    assert ConceptHierarchy._normalize_identifier("1+2+2") == "1+2"

    # fake minimal results to build hierarchy
    results = [
        {"ancestor_concept_id": 1, "descendant_concept_id": 1,
         "concept_name": "Diabetes", "concept_code": "DIA",
         "count_in_cohort": 5, "prevalence": 0.5}
    ]
    h1 = ConceptHierarchy.build_concept_hierarchy_from_results(1, results)
    h2 = ConceptHierarchy.build_concept_hierarchy_from_results(1, results)
    assert h1 is h2  # cache reuse
    assert h1.identifier == "1"

def test_union_and_cache_behavior():
    ConceptHierarchy.clear_cache()
    results1 = [
        {"ancestor_concept_id": 1, "descendant_concept_id": 1,
         "concept_name": "Diabetes", "concept_code": "DIA",
         "count_in_cohort": 5, "prevalence": 0.5}
    ]
    results2 = [
        {"ancestor_concept_id": 2, "descendant_concept_id": 2,
         "concept_name": "Hypertension", "concept_code": "HYP",
         "count_in_cohort": 3, "prevalence": 0.3}
    ]

    h1 = ConceptHierarchy.build_concept_hierarchy_from_results(1, results1)
    h2 = ConceptHierarchy.build_concept_hierarchy_from_results(2, results2)
    assert "1" in ConceptHierarchy._graph_cache
    assert "2" in ConceptHierarchy._graph_cache
    h12 = h1.union(h2)
    h21 = h2.union(h1)
    assert h12.identifier == "1+2"
    assert h21.identifier == "1+2"
    assert h12 is h21

def test_traversal_and_serialization():
    ConceptHierarchy.clear_cache()
    results = [
        {"ancestor_concept_id": 1, "descendant_concept_id": 1,
         "concept_name": "Root", "concept_code": "R",
         "count_in_cohort": 5, "prevalence": 0.5},
        {"ancestor_concept_id": 1, "descendant_concept_id": 2,
         "concept_name": "Child", "concept_code": "C",
         "count_in_cohort": 2, "prevalence": 0.2}
    ]
    h = ConceptHierarchy.build_concept_hierarchy_from_results(1, results)

    # roots
    roots = h.get_root_nodes()
    assert len(roots) == 1
    assert roots[0].name == "Root"
    assert roots[0].get_metrics(1) == {"count": 5, "prevalence": 0.5}
    children = roots[0].children
    ch_names = [ch.name for ch in children]
    assert ch_names == ["Child"]
    # leaves
    leaf_nodes = h.get_leaf_nodes(serialization=True)
    assert leaf_nodes == [
        {
            'concept_id': 2,
            'concept_name': 'Child',
            'concept_code': 'C',
            'metrics': {
                '1': {
                    'count': 2, 'prevalence': 0.2
                }
            },
            'source_cohorts': [1],
            'parent_ids': [1]
        }
    ]

    leaves = h.get_leaf_nodes()
    assert len(leaves) == 1
    assert leaves[0].name == "Child"
    parents = leaves[0].parents
    par_names = [par.name for par in parents]
    assert par_names == ["Root"]

    assert h.get_node(1, serialization=True) == {
        "concept_id": 1,
        "concept_name": "Root",
        "concept_code": "R",
        "metrics": {
            "1": {
                "count": 5,
                "prevalence": 0.5
            }
        },
        'source_cohorts': [1],
        "parent_ids": []
    }

    # graph traversal
    with pytest.raises(ValueError):
        # make sure to use list() to force generator execution
        # test invalid root_id raises ValueError
        list(h.iter_nodes(111, order="bfs"))

    with pytest.raises(ValueError):
        # make sure to use list() to force generator execution
        # test invalid order raises ValueError
        list(h.iter_nodes(1, order="dummy"))

    bfs_nodes = [n.id for n in h.iter_nodes(1, order="bfs")]
    assert bfs_nodes == [1, 2]

    # DFS traversal
    dfs_nodes = [n.id for n in h.iter_nodes(1, order="dfs")]
    assert set(dfs_nodes) == {1, 2}

    dfs_nodes = [n['concept_id'] for n in h.iter_nodes(1, order="dfs", serialization=True)]
    assert set(dfs_nodes) == {1, 2}

    # serialization
    serialized_root = h.get_root_nodes(serialization=True)[0]
    assert serialized_root["concept_name"] == "Root"
    assert "metrics" in serialized_root

    serialized_iter = list(h.iter_nodes(1, serialization=True))
    assert all(isinstance(n, dict) for n in serialized_iter)
    assert serialized_iter[0]["concept_id"] == 1

    with pytest.raises(ValueError):
        h.to_dict(111)

    h_dict = h.to_dict(1, include_union_metrics=True)
    assert h_dict == {'hierarchy': [{
        'concept_id': 1, 'concept_name': 'Root', 'concept_code': 'R',
        'metrics': {'union': {'count': 5, 'prevalence': 0.5},
                    '1': {'count': 5, 'prevalence': 0.5}},
        'source_cohorts': [1],
        'parent_ids': [],
        'children': [{'concept_id': 2, 'concept_name': 'Child', 'concept_code': 'C',
                      'metrics': {'union': {'count': 2, 'prevalence': 0.2},
                                  '1': {'count': 2, 'prevalence': 0.2}},
                      'source_cohorts': [1],
                      'parent_ids': [1], 'children': []}]}
    ]}

    h_dict = h.to_dict()
    assert h_dict == {'hierarchy': [{
        'concept_id': 1, 'concept_name': 'Root', 'concept_code': 'R',
        'metrics': {'1': {'count': 5, 'prevalence': 0.5}},
        'source_cohorts': [1],
        'parent_ids': [],
        'children': [{'concept_id': 2, 'concept_name': 'Child', 'concept_code': 'C',
                      'metrics': {'1': {'count': 2, 'prevalence': 0.2}},
                      'source_cohorts': [1],
                      'parent_ids': [1], 'children': []}]}
    ]}
