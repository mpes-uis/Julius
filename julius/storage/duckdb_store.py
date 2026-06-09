"""DuckDB persistence layer."""

import json
import re
import unicodedata
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

_SAFE_NAME = re.compile(r"^[a-z_][a-z0-9_]*$")


def _safe(name: str) -> str:
    """Normalize and validate a table/column name for safe SQL interpolation.

    Strips diacritics (ç→c, ã→a, etc.) and replaces non-alphanumeric chars with _.
    """
    ascii_name = unicodedata.normalize("NFD", name).encode("ascii", "ignore").decode()
    cleaned = re.sub(r"[^a-z0-9_]", "_", ascii_name.lower())
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    if not cleaned or not _SAFE_NAME.match(cleaned):
        raise ValueError(f"Could not produce a safe identifier from: {name!r}")
    return cleaned


def _serialize(v: Any) -> Any:
    """Ensure non-primitive values are JSON strings."""
    if isinstance(v, list | dict):
        return json.dumps(v, ensure_ascii=False)
    return v


class DuckDBStore:
    def __init__(self, path: Path | str = ":memory:") -> None:
        self._conn = duckdb.connect(str(path))
        self._known_tables: set[str] = set()

    @property
    def conn(self) -> duckdb.DuckDBPyConnection:
        return self._conn

    def close(self) -> None:
        self._conn.close()

    def write_records(
        self,
        table: str,
        records: list[dict[str, Any]],
        meta: dict[str, Any],
    ) -> None:
        """Insert records into the table, adding metadata columns. Creates table if needed.

        API field names are normalized to safe identifiers first. Metadata fields are only
        added when they don't conflict with an existing normalized API field name.
        """
        if not records:
            return

        table = _safe(table)
        now = datetime.now(UTC).isoformat()

        # Pre-normalize metadata keys once
        safe_meta = {_safe(str(k)): _serialize(v) for k, v in meta.items()}
        safe_meta["extracted_at"] = now

        # Normalize each record's keys and merge metadata without overwriting API fields
        enriched: list[dict[str, Any]] = []
        for rec in records:
            row: dict[str, Any] = {_safe(str(k)): _serialize(v) for k, v in rec.items()}
            for mk, mv in safe_meta.items():
                if mk not in row:
                    row[mk] = mv
            enriched.append(row)

        # Collect all column names (already safe) in stable order
        all_cols: dict[str, None] = {}
        for row in enriched:
            all_cols.update(dict.fromkeys(row))
        columns = list(all_cols)

        # Create table if not exists (all TEXT — type normalization is a pipeline concern)
        if table not in self._known_tables:
            col_defs = ", ".join(f'"{c}" TEXT' for c in columns)
            self._conn.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({col_defs})')
            self._known_tables.add(table)

        # Add any columns that appeared in a later batch (schema evolution)
        existing = {
            row[1].lower()
            for row in self._conn.execute(f'PRAGMA table_info("{table}")').fetchall()
        }
        for col in columns:
            if col not in existing:
                self._conn.execute(f'ALTER TABLE "{table}" ADD COLUMN "{col}" TEXT')

        # Insert rows
        placeholders = ", ".join("?" * len(columns))
        col_clause = ", ".join(f'"{c}"' for c in columns)
        rows = [
            [str(row.get(c, "")) if row.get(c) is not None else None for c in columns]
            for row in enriched
        ]
        self._conn.executemany(
            f'INSERT INTO "{table}" ({col_clause}) VALUES ({placeholders})',
            rows,
        )
