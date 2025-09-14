import os
import datetime
import logging
import pytest
from ipytree import Node
from biasanalyzer.concept import ConceptHierarchy
from biasanalyzer import __version__


def test_version():
    assert __version__ == '0.1.0'

def test_set_config(caplog, fresh_bias_obj):
    caplog.clear()
    with caplog.at_level(logging.INFO):
        fresh_bias_obj.set_config('')
    assert 'no configuration file specified' in caplog.text

    caplog.clear()
    with caplog.at_level(logging.ERROR):
        fresh_bias_obj.set_config('non_existent_config_file.yaml')
    assert 'does not exist' in caplog.text

    caplog.clear()
    with caplog.at_level(logging.ERROR):
        invalid_config_file = os.path.join(os.path.dirname(__file__), 'assets', 'config',
                                           'test_invalid_config.yaml')
        fresh_bias_obj.set_config(invalid_config_file)
    assert 'is not valid' in caplog.text

def test_set_root_omop(monkeypatch, caplog, fresh_bias_obj):
    caplog.clear()
    with caplog.at_level(logging.INFO):
        fresh_bias_obj.set_root_omop()
    assert 'no valid configuration' in caplog.text

    caplog.clear()
    with caplog.at_level(logging.INFO):
        config_file_with_unsupported_db_type = os.path.join(os.path.dirname(__file__), 'assets', 'config',
                                                            'test_config_unsupported_db_type.yaml')
        fresh_bias_obj.set_config(config_file_with_unsupported_db_type)
        fresh_bias_obj.set_root_omop()
    assert 'Unsupported database type' in caplog.text

    # Create a fake postgresql config
    config = {
        "root_omop_cdm_database": {
            "database_type": "postgresql",
            "username": "testuser",
            "password": "testpass",
            "hostname": "localhost",
            "port": 5432,
            "database": "testdb"
        }
    }

    # Patch the config parser to return this directly instead of reading a file
    monkeypatch.setattr(fresh_bias_obj, "config", config)

    # Patch OMOPCDMDatabase to avoid real DB connection
    class MockOMOPCDMDatabase:
        def __init__(self, db_url):
            self.db_url = db_url
        def close(self):
            pass

    monkeypatch.setattr("biasanalyzer.api.OMOPCDMDatabase", MockOMOPCDMDatabase)

    # --- Step 3: Mock BiasDatabase and its methods ---
    class MockBiasDatabase:
        def __init__(self, path):
            self.omop_cdm_db_url = None

        def load_postgres_extension(self):
            pass

        def close(self):
            pass

    monkeypatch.setattr("biasanalyzer.api.BiasDatabase", MockBiasDatabase)

    # Run
    fresh_bias_obj.set_config("dummy.yaml")  # This will now inject the mocked config
    fresh_bias_obj.set_root_omop()

    # Check values
    assert fresh_bias_obj.omop_cdm_db.db_url == "postgresql://testuser:testpass@localhost:5432/testdb"
    assert fresh_bias_obj.bias_db is not None
    assert fresh_bias_obj.bias_db.omop_cdm_db_url == "postgresql://testuser:testpass@localhost:5432/testdb"

def test_set_cohort_action(caplog, fresh_bias_obj):
    caplog.clear()
    with caplog.at_level(logging.INFO):
        fresh_bias_obj._set_cohort_action()
    assert 'valid OMOP CDM must be set' in caplog.text

def test_create_cohort_with_no_action(caplog, fresh_bias_obj):
    caplog.clear()
    with caplog.at_level(logging.INFO):
        fresh_bias_obj.create_cohort('test', 'test', 'test.yaml', 'test')
    assert 'failed to create a valid cohort action object' in caplog.text

def test_compare_cohort_with_no_action(caplog, fresh_bias_obj):
    caplog.clear()
    with caplog.at_level(logging.INFO):
        fresh_bias_obj.compare_cohorts(1, 2)
    assert 'failed to create a valid cohort action object' in caplog.text

def test_cohorts_concept_stats_empty_input_cohorts(caplog, fresh_bias_obj):
    caplog.clear()
    with caplog.at_level(logging.INFO):
        fresh_bias_obj.get_cohorts_concept_stats([])
    assert 'The input cohorts list is empty. At least one cohort id must be provided.' in caplog.text

def test_cohorts_concept_stats_no_cohort_action(caplog, fresh_bias_obj):
    caplog.clear()
    with caplog.at_level(logging.INFO):
        fresh_bias_obj.get_cohorts_concept_stats([1])
    assert 'failed to get concept prevalence stats for the union of cohorts' in caplog.text

def test_cohorts_union_concept_stats(test_db):
    ConceptHierarchy.clear_cache()
    # Show what cohorts exist in the test DB and print cohorts and stats so we know what raw data looks like
    cohorts_df = test_db.bias_db.conn.execute("""
                                             SELECT cohort_definition_id, COUNT(*) as n_subjects
                                             FROM cohort
                                             WHERE cohort_definition_id = 1 or cohort_definition_id = 2     
                                             GROUP BY cohort_definition_id
                                             ORDER BY cohort_definition_id
                                             """).fetchdf()
    print("Cohorts in DB:\n", cohorts_df.to_string(index=False), flush=True)

    # Show concept stats per cohort (aggregated for clarity)
    stats_df = test_db.bias_db.conn.execute("""
                                           SELECT c.cohort_definition_id,
                                                  co.condition_concept_id,
                                                  COUNT(*) as n
                                           FROM cohort c
                                                    JOIN condition_occurrence co
                                                         ON c.subject_id = co.person_id
                                           WHERE c.cohort_definition_id = 1 or c.cohort_definition_id = 2    
                                           GROUP BY c.cohort_definition_id, co.condition_concept_id
                                           ORDER BY c.cohort_definition_id, co.condition_concept_id
                                           """).fetchdf()
    print("Concept stats per cohort:\n", stats_df.to_string(index=False), flush=True)

    union_result = test_db.get_cohorts_concept_stats([1, 2])
    print(f'union_result: {union_result}', flush=True)
    union_result['hierarchy'] = sorted(union_result['hierarchy'], key=lambda x: x['concept_id'])
    # NOTE: The union_result takes cohort_start_date and cohort_end_date into account
    # when joining cohort with condition_occurrence for inclusion/exclusion criteria.
    # That means counts may differ from the raw numbers above. For example:
    #   - Concept 4041664 appears 5 times in cohort 1 raw, but only 4 fall within
    #     the cohort window → {'1': {'count': 4}}
    #   - Concept 4041664 appears 5 times in cohort 2 raw, but only 1 falls within
    #     the window → {'2': {'count': 1}}
    #   - Concept 5 disappears entirely, because its single occurrence is outside
    #     the cohort date window.
    # This explains why union_result values differ from the raw stats above.
    assert union_result == {'hierarchy': [
        {'concept_id': 316139, 'concept_name': 'Heart failure', 'concept_code': '84114007',
         'metrics': {'union': {'count': 4, 'prevalence': 0.45},
                     'cohorts': {'1': {'count': 2, 'prevalence': 0.4},
                                 '2': {'count': 2, 'prevalence': 0.5}}},
         'parent_ids': [], 'children': []},
        {'concept_id': 4041664, 'concept_name': 'Difficulty breathing', 'concept_code': '230145002',
        'metrics': {
            'union': {'count': 5, 'prevalence': 0.525},
            'cohorts': {'1': {'count': 4, 'prevalence': 0.8},
                        '2': {'count': 1, 'prevalence': 0.25}
                        }
        },
        'parent_ids': [], 'children': []},
        {'concept_id': 37311061, 'concept_name': 'COVID-19', 'concept_code': '840539006',
         'metrics': {'union': {'count': 8, 'prevalence': 0.9},
                     'cohorts': {'1': {'count': 4, 'prevalence': 0.8},
                                 '2': {'count': 4, 'prevalence': 1.0}}},
         'parent_ids': [], 'children': []},
    ]}

def test_get_domains_and_vocabularies_invalid(caplog, fresh_bias_obj):
    caplog.clear()
    with caplog.at_level(logging.INFO):
        fresh_bias_obj.get_domains_and_vocabularies()
    assert 'valid OMOP CDM must be set' in caplog.text

def test_get_domains_and_vocabularies(test_db):
    domains_and_vocabularies = test_db.get_domains_and_vocabularies()
    expected = [{'domain_id': 'Condition', 'vocabulary_id': 'ICD10CM'},
                {'domain_id': 'Condition', 'vocabulary_id': 'SNOMED'}]
    assert domains_and_vocabularies == expected

def test_get_concepts_no_omop_cdm(caplog, fresh_bias_obj):
    caplog.clear()
    with caplog.at_level(logging.INFO):
        fresh_bias_obj.get_concepts('dummy')
    assert 'valid OMOP CDM must be set' in caplog.text

def test_get_concepts_no_domain_and_vocab(caplog, test_db):
    caplog.clear()
    with caplog.at_level(logging.INFO):
        test_db.get_concepts('dummy')
    assert 'either domain or vocabulary must be set' in caplog.text

def test_get_concepts(test_db):
    concepts = test_db.get_concepts('Heart failure', domain='Condition', vocabulary='SNOMED')
    expected = [{'concept_id': 316139, 'concept_name': 'Heart failure',
                 'valid_start_date': datetime.date(2012, 4, 1),
                 'valid_end_date': datetime.date(2020, 4, 1),
                 'domain_id': 'Condition', 'vocabulary_id': 'SNOMED'}]
    assert concepts == expected
    concepts = test_db.get_concepts('Heart failure', vocabulary='SNOMED')
    assert concepts == expected
    concepts = test_db.get_concepts('Heart failure', domain='Condition')
    print(f'concepts: {concepts}', flush=True)
    assert concepts == expected

def test_get_concept_hierarchy_no_omop_cdm(caplog, fresh_bias_obj):
    caplog.clear()
    with caplog.at_level(logging.INFO):
        fresh_bias_obj.get_concept_hierarchy('dummy')
    assert 'valid OMOP CDM must be set' in caplog.text

def test_get_concept_hierarchy(test_db):
    with pytest.raises(ValueError):
        test_db.get_concept_hierarchy('not_int_str')

    hierarchy = test_db.get_concept_hierarchy(2)
    print(f'hierarchy: {hierarchy}', flush=True)
    expected = ({'details': {'concept_id': 2, 'concept_name': 'Type 1 Diabetes Mellitus', 'vocabulary_id': 'ICD10CM',
                             'concept_code': 'E10'}, 'parents': [{'details': {'concept_id': 1, 'concept_name':
        'Diabetes Mellitus', 'vocabulary_id': 'ICD10CM', 'concept_code': 'E10-E14'}, 'parents': []}]},
                {'details': {'concept_id': 2, 'concept_name': 'Type 1 Diabetes Mellitus', 'vocabulary_id': 'ICD10CM',
                             'concept_code': 'E10'}, 'children': [{'details': {'concept_id': 4, 'concept_name':
                    'Diabetic Retinopathy', 'vocabulary_id': 'ICD10CM', 'concept_code': 'E10.3/E11.3'},
                                                                   'children': []}]})
    assert hierarchy == expected

def test_display_concept_tree_text_format(capsys, test_db):
    sample_tree = {
        "details": {
            "concept_id": 123,
            "concept_name": "Hypertension",
            "concept_code": "I10"
        }
    }
    test_db.display_concept_tree(sample_tree)
    captured = capsys.readouterr()
    assert "concept tree must contain parents or children key" in captured.out

    sample_tree['children'] = [{
        "details": {
            "concept_id": 456,
            "concept_name": "Essential Hypertension",
            "concept_code": "I10.0"
            },
        "children": []
        }]
    test_db.display_concept_tree(sample_tree, show_in_text_format=True)
    captured = capsys.readouterr()
    assert "Hypertension (ID: 123" in captured.out
    assert "Essential Hypertension (ID: 456" in captured.out

def test_display_concept_tree_widget(test_db):
    sample_tree = {
        "details": {
            "concept_id": 456,
            "concept_name": "Essential Hypertension",
            "concept_code": "I10.0"
        },
        "parents": [{
            "details": {
                "concept_id": 123,
                "concept_name": "Hypertension",
                "concept_code": "I10"
                },
            "parents": []
        }]
    }

    tree_output = test_db.display_concept_tree(sample_tree, show_in_text_format=False)
    assert tree_output is not None
    print(tree_output)
    assert isinstance(tree_output, Node)
    assert "Essential Hypertension" in tree_output.name
    assert len(tree_output.nodes) == 1
    parent_node = tree_output.nodes[0]
    assert "Hypertension" in parent_node.name
