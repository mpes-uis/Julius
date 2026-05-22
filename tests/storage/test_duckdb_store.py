"""Tests for DuckDB storage layer."""

import pytest

from julius.storage.duckdb_store import DuckDBStore


@pytest.fixture
def store() -> DuckDBStore:
    return DuckDBStore(":memory:")


class TestWriteRecords:
    def test_creates_table_and_inserts(self, store: DuckDBStore) -> None:
        records = [{"nome": "João", "valor": "100"}]
        meta = {
            "municipio": "vitoria",
            "prefeitura": "PM Vitória",
            "provider": "portaltp",
            "ano": 2024,
            "mes": 1,
        }
        store.write_records("servidores", records, meta)

        rows = store.conn.execute("SELECT * FROM servidores").fetchall()
        assert len(rows) == 1

    def test_adds_metadata_columns(self, store: DuckDBStore) -> None:
        records = [{"nome": "Maria"}]
        meta = {
            "municipio": "vitoria",
            "prefeitura": "PM Vitória",
            "provider": "portaltp",
            "ano": 2024,
            "mes": 1,
        }
        store.write_records("servidores", records, meta)

        cols = store.conn.execute("PRAGMA table_info(servidores)").fetchall()
        col_names = [c[1] for c in cols]
        assert "municipio" in col_names
        assert "provider" in col_names
        assert "extracted_at" in col_names

    def test_idempotent_schema_evolution(self, store: DuckDBStore) -> None:
        store.write_records(
            "t",
            [{"a": "1"}],
            {"municipio": "x", "prefeitura": "y", "provider": "p", "ano": 2024, "mes": 1},
        )
        # Second batch with new column — should not raise
        store.write_records(
            "t",
            [{"a": "2", "b": "novo"}],
            {"municipio": "x", "prefeitura": "y", "provider": "p", "ano": 2024, "mes": 2},
        )
        rows = store.conn.execute("SELECT count(*) FROM t").fetchone()
        assert rows is not None and rows[0] == 2

    def test_serializes_nested_values(self, store: DuckDBStore) -> None:
        records = [{"tags": ["a", "b"], "info": {"x": 1}}]
        meta = {"municipio": "x", "prefeitura": "y", "provider": "p", "ano": 2024, "mes": 1}
        store.write_records("t", records, meta)
        row = store.conn.execute("SELECT tags, info FROM t").fetchone()
        assert row is not None
        assert isinstance(row[0], str)  # serialized as JSON string

    def test_empty_records_noop(self, store: DuckDBStore) -> None:
        store.write_records("t", [], {})
        tables = store.conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        assert all(r[0] != "t" for r in tables)
