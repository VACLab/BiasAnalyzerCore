import os
import pytest
import logging
from biasanalyzer import __version__


def test_version():
    assert __version__ == '0.1.0'

def test_set_config(caplog, fresh_bias_obj):
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

def test_get_domains_and_vocabularies_invalid(caplog, fresh_bias_obj):
    caplog.clear()
    with caplog.at_level(logging.INFO):
        fresh_bias_obj.get_domains_and_vocabularies()
    assert 'valid OMOP CDM must be set' in caplog.text

def test_get_domains_and_vocabularies(test_db):
    domains_and_vocabularies = test_db.get_domains_and_vocabularies()
    print(f'domains_and_vocabs: {domains_and_vocabularies}', flush=True)
    expected = [{'domain_id': 'Condition', 'vocabulary_id': 'ICD10CM'},
                {'domain_id': 'Condition', 'vocabulary_id': 'SNOMED'}]
    assert domains_and_vocabularies == expected

def test_get_concepts(caplog, fresh_bias_obj):
    caplog.clear()
    with caplog.at_level(logging.INFO):
        fresh_bias_obj.get_concepts('dummy')
    assert 'valid OMOP CDM must be set' in caplog.text

def test_get_concept_hierarchy(caplog, fresh_bias_obj):
    caplog.clear()
    with caplog.at_level(logging.INFO):
        fresh_bias_obj.get_concept_hierarchy('dummy')
    assert 'valid OMOP CDM must be set' in caplog.text
