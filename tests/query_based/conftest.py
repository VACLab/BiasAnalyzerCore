import pytest
import duckdb
from biasanalyzer.api import BIAS
from biasanalyzer.config import load_config
import os


@pytest.fixture(scope="session")
def test_db():
    config_file = os.path.join(os.path.dirname(__file__), '..', 'assets', 'test_config.yaml')
    config = load_config(config_file)
    db_path = config['root_omop_cdm_database']['database']
    conn = duckdb.connect(db_path)
    conn.execute("""
                CREATE TABLE IF NOT EXISTS person (
                    person_id INTEGER PRIMARY KEY,
                    gender_concept_id INTEGER,
                    race_concept_id INTEGER,
                    ethnicity_concept_id INTEGER,
                    year_of_birth INTEGER
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

    conn.execute("""
                CREATE TABLE IF NOT EXISTS visit_occurrence (
                    person_id INTEGER,
                    visit_occurrence_id INTEGER PRIMARY KEY,
                    visit_concept_id INTEGER,
                    visit_start_date DATE,
                    visit_end_date DATE
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
                    (105, 8507, 8516, 38003564, 1983), -- Male, Asian, Non-Hispanic
                    (106, 8532, 8527, 38003564, 2000), -- Female, White, Non-Hispanic
                    (107, 8507, 8516, 38003563, 2001), -- Male, Asian, Hispanic
                    (108, 8532, 8527, 38003564, 2002), -- Female, White, Non-Hispanic
                    (109, 8507, 8515, 38003563, 2003), -- Male, Black, Hispanic
                    (110, 8532, 8516, 38003563, 2004), -- Female, Asian, Hispanic
                    (111, 8532, 8516, 38003564, 2011), -- Female, Asian, Non-Hispanic
                    (112, 8532, 8527, 38003564, 2012); -- Female, White, Non-Hispanic
            """)

    # Insert mock concepts as needed
    result = conn.execute("SELECT COUNT(*) FROM concept").fetchone()
    if result[0] == 0:
        conn.execute("""
                INSERT INTO concept (concept_id, concept_name, concept_code, vocabulary_id)
                VALUES
                    (4274025, 'Disease', '64572001', 'SNOMED'), 
                    (1, 'Diabetes Mellitus', 'E10-E14', 'ICD10CM'), 
                    (2, 'Type 1 Diabetes Mellitus', 'E10', 'ICD10CM'),
                    (3, 'Type 2 Diabetes Mellitus', 'E11', 'ICD10CM'), 
                    (4, 'Diabetic Retinopathy', 'E10.3/E11.3', 'ICD10CM'), 
                    (5, 'Fever', 'R50.9', 'ICD10CM'),
                    (37311061, 'COVID-19', '840539006', 'SNOMED'),
                    (4041664, 'Difficulty breathing', '230145002', 'SNOMED'),
                    (316139, 'Heart failure', '84114007', 'SNOMED');
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
                    (5, 5, 0),
                    (37311061, 37311061, 0),
                    (4041664, 4041664, 0),
                    (316139, 316139, 0),
                    (4274025, 4274025, 0),
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
                    (101, 3, '2023-01-01', '2023-02-27'), -- Patient 101 has Type 2 Diabetes
                    (102, 3, '2023-02-01', NULL),        -- Patient 102 has Type 2 Diabetes, ongoing
                    (103, 4, '2023-03-01', '2023-03-15'), -- Patient 103 has Diabetic Retinopathy
                    (104, 2, '2023-01-01', '2023-01-31'), -- Patient 104 has Type 1 Diabetes
                    (104, 4, '2023-03-01', '2023-03-15'), -- Patient 104 has Diabetic Retinopathy
                    (105, 3, '2023-02-01', NULL),         -- Patient 105 has Type 2 Diabetes, ongoing
                    (105, 4, '2023-03-01', '2023-03-15'), -- Patient 105 has Diabetic Retinopathy
                    (106, 5, '2023-03-01', '2023-03-15'), -- Patient 106 has fever
                    (107, 5, '2020-03-01', '2020-03-15'), -- Patient 107 has fever
                    (108, 4041664, '2020-04-10', '2020-04-17'), -- Patient 108 has difficulty breathing
                    (108, 37311061, '2020-04-13', '2020-04-27'), -- Patient 108 has COVID-19
                    (110, 4041664, '2020-05-10', '2020-05-17'), -- Patient 110 has difficulty breathing
                    (110, 37311061, '2020-05-13', '2020-05-27'), -- Patient 110 has COVID-19
                    (111, 4041664, '2020-06-10', '2020-06-17'), -- Patient 111 has difficulty breathing
                    (111, 37311061, '2020-06-13', '2020-06-27'), -- Patient 111 has COVID-19
                    (112, 4041664, '2020-07-10', '2020-07-17'), -- Patient 112 has difficulty breathing
                    (112, 37311061, '2020-07-13', '2020-07-27'), -- Patient 112 has COVID-19
                    (111, 4041664, '2020-04-10', '2020-04-17'), -- Patient 108 has difficulty breathing
                    (111, 37311061, '2020-05-13', '2020-05-27'), -- Patient 108 has COVID-19
                    (111, 316139, '2020-06-14', '2020-06-27'), -- Patient 111 has heart failure
                    (112, 316139, '2020-07-14', '2020-07-27'); -- Patient 112 has heart failure
            """)

        # Insert mock visit data
    result = conn.execute("SELECT COUNT(*) FROM visit_occurrence").fetchone()
    if result[0] == 0:
        conn.execute("""
                    INSERT INTO visit_occurrence (person_id, visit_occurrence_id, visit_concept_id, visit_start_date, visit_end_date)
                    VALUES 
                        (108, 1, 9201, '2020-04-13', '2020-04-14'), -- Inpatient Visit
                        (108, 2, 9201, '2020-04-16', '2020-04-27'), -- Second inpatient visit (meets criteria)
                        (110, 3, 9202, '2020-05-13', '2020-05-13'), -- Outpatient visit (should be excluded)
                        (110, 4, 9201, '2020-05-16', '2020-05-27'), -- Inpatient visit but only one occurrence
                        (110, 5, 9203, '2020-05-16', '2020-05-16'), -- Single emergency room visit (meets criteria)
                        (111, 6, 9203, '2020-06-13', '2020-06-13'), -- Single emergency room visit (meets criteria)
                        (112, 7, 9203, '2020-07-13', '2020-07-13'); -- Single emergency room visit (meets criteria)
                """)

    # mock configuration file
    bias = BIAS()
    bias.set_config(config_file)
    bias.set_root_omop()

    yield bias  # Provide the connection to the test

    # Teardown: Close the connection
    conn.close()
    bias.cleanup()
    os.remove(db_path)
