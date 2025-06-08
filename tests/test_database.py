import duckdb
import pytest
from biasanalyzer.database import BiasDatabase


def test_create_cohort_definition_table_error_on_sequence():
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

    # Optional assertions
    assert db.conn.call_count >= 2
    assert any("CREATE SEQUENCE" in sql for sql in db.conn.executed_sql)

def test_create_cohort_index_error():
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

    # Optional assertions
    assert db.conn.call_count >= 2
    assert any("CREATE INDEX" in sql for sql in db.conn.executed_sql)

def test_create_omop_table_postgres(monkeypatch):
    # Set up tracking dict
    called = {"executed": False, "query": None}

    # Patch before BiasDatabase instance is created
    def mock_execute(self, query):
        called["executed"] = True
        called["query"] = query
        return None

    # Monkeypatch at class level first
    monkeypatch.setattr(duckdb.DuckDBPyConnection, "execute", mock_execute)

    # Now create the instance (so it uses the patched class method)
    BiasDatabase._instance = None
    db = BiasDatabase(":memory:")
    db.omop_cdm_db_url = "postgresql://user:pass@localhost:5432/mydb"

    result = db._create_omop_table("person")

    assert result is True
    assert called["executed"] is True
    assert "postgres_scan" in called["query"]
