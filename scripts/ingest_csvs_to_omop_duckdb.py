# ruff: noqa: S608

"""
This script ingests both clinical and vocabulary OMOP CSV exports into a single DckDB database for
downstream use of the core BiasAnalyzer python library.
Example for running this script:
    python scripts/ingest_csvs_to_omop_duckdb.py \
        --clinical data/clinical \
        --vocab data/omop_vocabs \
        --output data/omop.duckdb
"""

import argparse
import sys
import csv
import time
from pathlib import Path

import duckdb


FILENAME_STEM_TO_TABLE_NAME_MAPPING = {
    # 'demographics': 'person'
    # 'conditions': 'condition_occurrence'
    # 'drugs': 'drug_exposure'
    # 'procedures': 'procedure_occurrence'
    # 'visits': 'visit_occurrence'
    'observations': 'observation'
}

COLUMN_MAPPINGS = {
    "person": {
        "deid_pat_id": "person_id"
    },
    "condition_occurrence": {
        "deid_pat_id": "person_id"
    },
    "drug_exposure": {
        "deid_pat_id": "person_id"
    },
    "procedure_occurrence": {
        "deid_pat_id": "person_id"
    },
    "visit_occurrence": {
        "deid_pat_id": "person_id"
    },
    "observation": {
        "deid_pat_id": "person_id"
    },
    "measurement": {
        "deid_pat_id": "person_id"
    },
}

OMOP_TABLE_SCHEMAS = {
    "person": [
        "person_id",
        "gender_concept_id",
        "year_of_birth",
        "month_of_birth",
        "day_of_birth",
        "birth_datetime",
        "race_concept_id",
        "ethnicity_concept_id",
        "location_id",
        "provider_id",
        "care_site_id",
        "person_source_value",
        "gender_source_value",
        "gender_source_concept_id",
        "race_source_value",
        "race_source_concept_id",
        "ethnicity_source_value",
        "ethnicity_source_concept_id"
    ],
    "condition_occurrence": [
        "condition_occurrence_id",
        "person_id",
        "condition_concept_id",
        "condition_start_date",
        "condition_start_datetime",
        "condition_end_date",
        "condition_end_datetime",
        "condition_type_concept_id",
        "condition_status_concept_id",
        "stop_reason",
        "provider_id",
        "visit_occurrence_id",
        "visit_detail_id",
        "condition_source_value",
        "condition_source_concept_id",
        "condition_status_source_value"
    ],
    'drug_exposure': [
        "drug_exposure_id",
        "person_id",
        "drug_concept_id",
        "drug_exposure_start_date",
        "drug_exposure_start_datetime",
        "drug_exposure_end_date",
        "drug_exposure_end_datetime",
        "verbatim_end_date",
        "drug_type_concept_id",
        "stop_reason",
        "refills",
        "quantity",
        "days_supply",
        "sig",
        "route_concept_id",
        "lot_number",
        "provider_id",
        "visit_occurrence_id",
        "visit_detail_id",
        "drug_source_value",
        "drug_source_concept_id",
        "route_source_value",
        "dose_unit_source_value"
    ],
    'procedure_occurrence': [
        "procedure_occurrence_id",
        "person_id",
        "procedure_concept_id",
        "procedure_date",
        "procedure_datetime",
        "procedure_end_date",
        "procedure_end_datetime",
        "procedure_type_concept_id",
        "modifier_concept_id",
        "quantity",
        "provider_id",
        "visit_occurrence_id",
        "visit_detail_id",
        "procedure_source_value",
        "procedure_source_concept_id",
        "modifier_source_value"
    ],
    'visit_occurrence': [
        "visit_occurrence_id",
        "person_id",
        "visit_concept_id",
        "visit_start_date",
        "visit_start_datetime",
        "visit_end_date",
        "visit_end_datetime",
        "visit_type_concept_id",
        "provider_id",
        "care_site_id",
        "visit_source_value",
        "visit_source_concept_id",
        "admitted_from_concept_id",
        "admitted_from_source_value",
        "discharged_to_concept_id",
        "discharged_to_source_value",
        "preceding_visit_occurrence_id"
    ],
    'observation': [
        "observation_id",
        "person_id",
        "observation_concept_id",
        "observation_date",
        "observation_datetime",
        "observation_type_concept_id",
        "value_as_number",
        "value_as_string",
        "value_as_concept_id",
        "qualifier_concept_id",
        "unit_concept_id",
        "provider_id",
        "visit_occurrence_id",
        "visit_detail_id",
        "observation_source_value",
        "observation_source_concept_id",
        "unit_source_value",
        "qualifier_source_value",
        "value_source_value",
        "observation_event_id",
        "obs_event_field_concept_id"
    ]
}

def load_csv_to_duckdb(con, csv_path: Path, table_name: str):
    """Load a single CSV file into DuckDB."""
    t0 = time.time()
    print(f"loading {table_name} from {csv_path}")

    # read and normalize header
    with open(csv_path, "r", newline="") as f:
        reader = csv.reader(f)
        raw_header = next(reader)

    # normalize: lower case + strip quotes/spaces
    raw_header = [h.strip().replace('"', '') for h in raw_header]
    header = [h.lower() for h in raw_header]
    print(f'normalized header: {header}')

    mapping = COLUMN_MAPPINGS.get(table_name, {})
    final_cols = [mapping.get(col, col) for col in header]
    print(f'mapped header: {final_cols}')

    expected = OMOP_TABLE_SCHEMAS.get(table_name, [])
    final_set = set(final_cols)

    missing = set(expected) - final_set
    if missing:
        print(f"Missing expected OMOP columns in {table_name}: {sorted(missing)} - cannot ingest")
        elapsed = time.time() - t0
        return None, elapsed

    extra = final_set - set(expected)
    if extra:
        print(f"WARNING: Extra columns in CSV for {table_name}: {sorted(extra)}")
        print(f"Extra columns will NOT be ingested.")

    select_clauses = []
    for orig, new in zip(raw_header, final_cols):
        if new not in expected:
            # skip extra columns entirely
            continue
        if orig != new:
            select_clauses.append(f'{orig} AS {new}')
        else:
            select_clauses.append(orig)

    select_clause = ", ".join(select_clauses)
    print(f"Final SELECT clause: {select_clause}")

    con.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT {select_clause}
            FROM read_csv_auto('{csv_path}', header=True, parallel=True)
        """)

    row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    elapsed = time.time() - t0
    print(f"Loaded {table_name} ({row_count} rows) in {elapsed:5.2f}s")
    return row_count, elapsed


def ingest_directory(con, csv_dir: Path):
    """Ingest all CSVs in a directory."""
    if not csv_dir.exists():
        print(f"directory not found: {csv_dir}")
        return []

    results = []
    for csv_path in sorted(csv_dir.glob("*.csv")):
        table_name = csv_path.stem.lower()
        if table_name not in FILENAME_STEM_TO_TABLE_NAME_MAPPING:
            continue
        mapped_table_name = FILENAME_STEM_TO_TABLE_NAME_MAPPING[table_name]
        rc, t = load_csv_to_duckdb(con, csv_path, mapped_table_name)
        results.append((table_name, rc, t))
    return results


def main():
    parser = argparse.ArgumentParser(description="Ingest OMOP CSVs into DuckDB")
    parser.add_argument(
        "--clinical",
        type=Path,
        default=Path("Y:/"),
        required=False,
        help="Directory containing OMOP clinical CSVs (person, condition_occurrence, etc.)",
    )
    parser.add_argument(
        "--vocab",
        type=Path,
        required=False,
        help="Directory containing OMOP vocabulary CSVs (concept, concept_relationship, etc.)",
    )
    parser.add_argument("--output", type=Path,
                        default=Path("Y:/OMOP_duckdb/omop.duckdb"),
                        required=False, help="Output DuckDB file path")

    args = parser.parse_args()

    input_clinical = args.clinical
    input_vocab = args.vocab
    db_path = args.output

    if input_clinical is None and input_vocab is None:
        print("Error: You must provide at least one of --clinical or --vocab for data ingestion.")
        sys.exit(1)

    print(f"Creating DuckDB at: {db_path}")
    # db_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path))
    all_results = []

    if input_clinical:
        if not input_clinical.exists():
            print(f"Clinical directory does not exist: {input_clinical}")
            sys.exit(1)
        all_results += ingest_directory(con, input_clinical)

    if input_vocab:
        if not input_vocab.exists():
            print(f"Vocabulary directory does not exist: {input_vocab}")
            sys.exit(1)
        all_results += ingest_directory(con, input_vocab)

    con.close()

    print(f"Ingestion complete with {len(all_results)} tables loaded. Details shown below:")
    print(f"\n{all_results}")


if __name__ == "__main__":
    main()
