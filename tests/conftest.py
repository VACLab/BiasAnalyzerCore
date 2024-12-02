import pytest
import duckdb
from biasanalyzer.api import BIAS
from biasanalyzer.config import load_config
import os


@pytest.fixture(scope="session")
def test_db():
    config_file = os.path.join(os.path.dirname(__file__), 'assets', 'test_config.yaml')
    config = load_config(config_file)
    db_path = config['root_omop_cdm_database']['database']
    conn = duckdb.connect(db_path)
    if not os.path.exists(db_path):
        conn.execute("""
                    CREATE TABLE IF NOT EXISTS person (
                        person_id INTEGER PRIMARY KEY,
                        gender_concept_id INTEGER,
                        race_concept_id INTEGER,
                        ethnicity_concept_id INTEGER,
                        year_of_birth INTEGER,
                    );
                """)
        conn.execute("""
                CREATE TABLE IF NOT EXISTS concept (
                    concept_id INTEGER PRIMARY KEY,
                    concept_name TEXT,
                    concept_code TEXT,
                    vocabulary_id TEXT
                );
            """)
        conn.execute("""
                CREATE TABLE IF NOT EXISTS concept_ancestor (
                    ancestor_concept_id INTEGER,
                    descendant_concept_id INTEGER,
                    min_levels_of_separation INTEGER
                );
            """)
        conn.execute("""
                CREATE TABLE IF NOT EXISTS condition_occurrence (
                    person_id INTEGER,
                    condition_concept_id INTEGER,
                    condition_start_date DATE,
                    condition_end_date DATE
                );
            """)
        conn.execute("""
                    CREATE TABLE IF NOT EXISTS drug_exposure (
                        person_id INTEGER,
                        drug_concept_id INTEGER,
                        drug_exposure_start_date DATE,
                        drug_exposure_end_date DATE
                    );
                """)

        # Insert mock data as needed
        result = conn.execute("SELECT COUNT(*) FROM person").fetchone()
        if result[0] == 0:
            conn.execute("""
                    INSERT INTO person (person_id, gender_concept_id, race_concept_id, ethnicity_concept_id, year_of_birth)
                    VALUES 
                        (101, 8507, 8516, 38003563, 1980), -- Male, Asian, Hispanic
                        (102, 8532, 8527, 38003564, 1990), -- Female, White, Non-Hispanic
                        (103, 8507, 8515, 38003563, 1975), -- Male, Black, Hispanic
                        (104, 8532, 8516, 38003563, 1985), -- Female, Asian, Hispanic
                        (105, 8532, 8527, 38003564, 2000); -- Female, White, Non-Hispanic
                """)

        # Insert mock concepts as needed
        result = conn.execute("SELECT COUNT(*) FROM concept").fetchone()
        if result[0] == 0:
            conn.execute("""
                    INSERT INTO concept (concept_id, concept_name, concept_code, vocabulary_id)
                    VALUES 
                        (1, 'Diabetes Mellitus', 'E10-E14', 'ICD10CM'), 
                        (2, 'Type 1 Diabetes Mellitus', 'E10', 'ICD10CM'),
                        (3, 'Type 2 Diabetes Mellitus', 'E11', 'ICD10CM'), 
                        (4, 'Diabetic Retinopathy', 'E10.3/E11.3', 'ICD10CM');
                """)

        # Insert hierarchical relationships as needed
        result = conn.execute("SELECT COUNT(*) FROM concept_ancestor").fetchone()
        if result[0] == 0:
            conn.execute("""
                    INSERT INTO concept_ancestor (ancestor_concept_id, descendant_concept_id, min_levels_of_separation)
                    VALUES 
                        (1, 1, 0),
                        (2, 2, 0),
                        (3, 3, 0),
                        (4, 4, 0),
                        (1, 2, 1), -- Diabetes -> Type 1
                        (1, 3, 1), -- Diabetes -> Type 2
                        (1, 4, 2), -- Diabetes -> Retinopathy
                        (2, 4, 1), -- Type 1 -> Diabetes Retinopathy
                        (3, 4, 1); -- Type 2 -> Diabetes Retinopathy
                """)

        # Insert mock condition occurrences as needed
        result = conn.execute("SELECT COUNT(*) FROM condition_occurrence").fetchone()
        if result[0] == 0:
            conn.execute("""
                    INSERT INTO condition_occurrence (person_id, condition_concept_id, condition_start_date, condition_end_date)
                    VALUES
                        (101, 2, '2023-01-01', '2023-01-31'), -- Patient 101 has Type 1 Diabetes
                        (102, 3, '2023-02-01', NULL),        -- Patient 102 has Type 2 Diabetes, ongoing
                        (103, 4, '2023-03-01', '2023-03-15'), -- Patient 103 has Diabetic Retinopathy
                        (104, 2, '2023-01-01', '2023-01-31'), -- Patient 104 has Type 1 Diabetes
                        (104, 4, '2023-03-01', '2023-03-15'), -- Patient 104 has Diabetic Retinopathy
                        (105, 3, '2023-02-01', NULL),         -- Patient 105 has Type 2 Diabetes, ongoing
                        (105, 4, '2023-03-01', '2023-03-15'); -- Patient 105 has Diabetic Retinopathy
                """)

    # mock configuration file
    bias = BIAS()
    bias.set_config(config_file)
    bias.set_root_omop()

    yield bias  # Provide the connection to the test

    # Teardown: Close the connection
    conn.close()
    bias.cleanup()
