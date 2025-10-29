"""
This script ingests both clinical and vocabulary OMOP CSV exports into a single DckDB database for
downstream use of the core BiasAnalyzer python library.
Example for running this script:
    python scripts/ingest_csvs_to_omop_duckdb.py \
        --clinical data/clinical \
        --vocab data/omop_vocabs \
        --output data/omop.duckdb
"""

import duckdb
import time
import argparse
import sys
from pathlib import Path


def load_csv_to_duckdb(con, csv_path: Path, table_name: str):
    """Load a single CSV file into DuckDB."""
    t0 = time.time()
    print(f'loading {table_name} from {csv_path}')
    con.execute(f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT * FROM read_csv_auto('{csv_path}', header=True, quote='', parallel=True)
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
        rc, t = load_csv_to_duckdb(con, csv_path, table_name)
        results.append((table_name, rc, t))
    return results


def main():
    parser = argparse.ArgumentParser(description="Ingest OMOP CSVs into DuckDB")
    parser.add_argument("--clinical", type=Path, required=False,
                        help="Directory containing OMOP clinical CSVs (person, condition_occurrence, etc.)")
    parser.add_argument("--vocab", type=Path, required=False,
                        help="Directory containing OMOP vocabulary CSVs (concept, concept_relationship, etc.)")
    parser.add_argument("--output", type=Path, required=True,
                        help="Output DuckDB file path")

    args = parser.parse_args()

    input_clinical = args.clinical
    input_vocab = args.vocab
    db_path = args.output

    if input_clinical is None and input_vocab is None:
        print("Error: You must provide at least one of --clinical or --vocab for data ingestion.")
        sys.exit(1)

    print(f"Creating DuckDB at: {db_path}")
    db_path.parent.mkdir(parents=True, exist_ok=True)

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
