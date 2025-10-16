import duckdb
import pytest
import logging
from biasanalyzer.cohort_query_builder import CohortQueryBuilder
from biasanalyzer.database import BiasDatabase


def test_load_postgres_extension_executes_twice(monkeypatch):
    # Reset singleton to get a clean instance
    BiasDatabase._instance = None
    db = BiasDatabase(":memory:")

    calls = []

    class MockConn:
        def execute(self, query):
            calls.append(query)
            return None

    db.conn = MockConn()

    # Run the method under test
    db.load_postgres_extension()

    # Assert that execute() was called twice
    assert len(calls) == 2
    assert "INSTALL postgres" in calls[0]
    assert "LOAD postgres" in calls[1]

def test_create_cohort_definition_table_error_on_sequence():
    BiasDatabase._instance = None
    db = BiasDatabase(":memory:")
    class MockConn:
        def __init__(self):
            self.calls = []

        def execute(self, sql):
            self.calls.append(sql)
            if "CREATE SEQUENCE" in sql:
                raise duckdb.Error("random error")  # simulate failure
            return None

        def close(self):
            pass

    db.conn = MockConn()

    with pytest.raises(duckdb.Error, match="random error"):
        db._create_cohort_definition_table()

def test_create_cohort_definition_table_sequence_exists():
    BiasDatabase._instance = None
    db = BiasDatabase(":memory:")
    class MockConn:
        def __init__(self):
            self.call_count = 0
            self.executed_sql = []

        def execute(self, sql):
            self.call_count += 1
            self.executed_sql.append(sql)
            if "CREATE SEQUENCE" in sql:
                raise duckdb.Error("Sequence already exists")

            return None

        def close(self):
            pass

    db.conn = MockConn()

    # Should handle "Index already exists" without raising
    db._create_cohort_definition_table()

    assert db.conn.call_count >= 2
    assert any("CREATE SEQUENCE" in sql for sql in db.conn.executed_sql)

def test_create_cohort_index_error():
    BiasDatabase._instance = None
    db = BiasDatabase(":memory:")
    class MockConn:
        def __init__(self):
            self.calls = []

        def execute(self, sql):
            self.calls.append(sql)
            if "CREATE INDEX" in sql:
                raise duckdb.Error("random error")  # simulate failure
            return None

        def close(self):
            pass

    db.conn = MockConn()

    with pytest.raises(duckdb.Error, match="random error"):
        db._create_cohort_table()

def test_create_cohort_index_exists():
    BiasDatabase._instance = None
    db = BiasDatabase(":memory:")
    class MockConn:
        def __init__(self):
            self.call_count = 0
            self.executed_sql = []

        def execute(self, sql):
            self.call_count += 1
            self.executed_sql.append(sql)
            if "CREATE INDEX" in sql:
                raise duckdb.Error("Index already exists")

            return None

        def close(self):
            pass

    db.conn = MockConn()

    # Should handle "Index already exists" without raising
    db._create_cohort_table()

    assert db.conn.call_count >= 2
    assert any("CREATE INDEX" in sql for sql in db.conn.executed_sql)

def test_get_cohort_concept_stats_handles_exception(caplog):
    BiasDatabase._instance = None
    db = BiasDatabase(":memory:")
    db.omop_cdm_db_url = 'duckdb'
    qry_builder = CohortQueryBuilder(cohort_creation=False)
    with pytest.raises(ValueError):
        db.get_cohort_concept_stats(123, qry_builder)

def test_get_cohort_attributes_handles_exception():
    BiasDatabase._instance = None
    db = BiasDatabase(":memory:")
    qry_builder = CohortQueryBuilder(cohort_creation=False)
    db.omop_cdm_db_url = None
    result_stats = db.get_cohort_basic_stats(123, variable='age')
    assert result_stats is None
    result = db.get_cohort_distributions(123, 'age')
    assert result is None
    with pytest.raises(ValueError):
        db.get_cohort_concept_stats(123, qry_builder)
