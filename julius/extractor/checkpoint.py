"""Idempotency checkpoint — tracks which jobs have already been extracted."""

import duckdb

from julius.models.job import ExtractionJob

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS _checkpoints (
    job_key      TEXT PRIMARY KEY,
    provider     TEXT NOT NULL,
    municipio    TEXT NOT NULL,
    endpoint     TEXT NOT NULL,
    year         INTEGER NOT NULL,
    month        INTEGER,
    status       TEXT NOT NULL,
    records      INTEGER DEFAULT 0,
    error        TEXT,
    extracted_at TIMESTAMP DEFAULT current_timestamp
)
"""


class Checkpoint:
    def __init__(self, conn: duckdb.DuckDBPyConnection) -> None:
        self._conn = conn
        conn.execute(_CREATE_TABLE)

    def already_done(self, job: ExtractionJob) -> bool:
        result = self._conn.execute(
            "SELECT 1 FROM _checkpoints WHERE job_key = ? AND status = 'success'",
            [job.key],
        ).fetchone()
        return result is not None

    def mark_done(self, job: ExtractionJob, *, records: int) -> None:
        cols = "job_key, provider, municipio, endpoint, year, month, status, records"
        self._conn.execute(
            f"""
            INSERT INTO _checkpoints ({cols})
            VALUES (?, ?, ?, ?, ?, ?, 'success', ?)
            ON CONFLICT (job_key) DO UPDATE SET
                status = 'success', records = excluded.records, extracted_at = current_timestamp
            """,
            [
                job.key,
                str(job.prefeitura.empresa),
                job.prefeitura.municipio,
                job.endpoint.name,
                job.year,
                job.month,
                records,
            ],
        )

    def mark_failed(self, job: ExtractionJob, *, error: str) -> None:
        cols = "job_key, provider, municipio, endpoint, year, month, status, error"
        self._conn.execute(
            f"""
            INSERT INTO _checkpoints ({cols})
            VALUES (?, ?, ?, ?, ?, ?, 'failed', ?)
            ON CONFLICT (job_key) DO UPDATE SET
                status = 'failed', error = excluded.error, extracted_at = current_timestamp
            """,
            [
                job.key,
                str(job.prefeitura.empresa),
                job.prefeitura.municipio,
                job.endpoint.name,
                job.year,
                job.month,
                error,
            ],
        )

    def failed_jobs(self, provider: str | None = None) -> list[dict[str, object]]:
        """Return all failed checkpoints, optionally filtered by provider."""
        query = "SELECT * FROM _checkpoints WHERE status = 'failed'"
        params: list[str] = []
        if provider:
            query += " AND provider = ?"
            params.append(provider)
        rows = self._conn.execute(query, params).fetchall()
        cols = [d[0] for d in self._conn.description or []]
        return [dict(zip(cols, row, strict=True)) for row in rows]

    def summary(self) -> dict[str, int]:
        rows = self._conn.execute(
            "SELECT status, count(*) FROM _checkpoints GROUP BY status"
        ).fetchall()
        return {row[0]: row[1] for row in rows}
