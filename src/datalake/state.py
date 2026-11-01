from __future__ import annotations

import hashlib
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path

from datalake.config import IST


class StateStore:
    """SQLite watermark, checkpoint, and SHA-256 provenance store."""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        return conn

    @contextmanager
    def _session(self) -> Iterator[sqlite3.Connection]:
        """Transactional connection that is always closed (avoids Windows file locks)."""
        conn = self._connect()
        try:
            with conn:
                yield conn
        finally:
            conn.close()

    def _init_db(self) -> None:
        with self._session() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ingestion_log (
                    dataset TEXT NOT NULL,
                    partition_key TEXT NOT NULL,
                    status TEXT NOT NULL,
                    row_count INTEGER NOT NULL DEFAULT 0,
                    sha256 TEXT,
                    source_url TEXT,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (dataset, partition_key)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_dataset_status ON ingestion_log(dataset, status);"
            )

    def is_completed(self, dataset: str, partition_key: str) -> bool:
        with self._session() as conn:
            row = conn.execute(
                "SELECT status FROM ingestion_log WHERE dataset = ? AND partition_key = ?",
                (dataset, partition_key),
            ).fetchone()
        return bool(row and row[0] in ("OK", "HOLIDAY"))

    def completed_keys(self, dataset: str) -> set[str]:
        with self._session() as conn:
            rows = conn.execute(
                "SELECT partition_key FROM ingestion_log WHERE dataset = ? AND status IN ('OK', 'HOLIDAY')",
                (dataset,),
            ).fetchall()
        return {r[0] for r in rows}

    def max_ok_date(self, dataset: str) -> date | None:
        with self._session() as conn:
            row = conn.execute(
                "SELECT MAX(partition_key) FROM ingestion_log WHERE dataset = ? AND status = 'OK'",
                (dataset,),
            ).fetchone()
        if row and row[0]:
            try:
                return date.fromisoformat(row[0])
            except ValueError:
                return None
        return None

    def record(
        self,
        dataset: str,
        partition_key: str,
        status: str,
        row_count: int = 0,
        raw_bytes: bytes | None = None,
        source_url: str | None = None,
    ) -> None:
        digest = hashlib.sha256(raw_bytes).hexdigest() if raw_bytes else None
        now_str = datetime.now(IST).isoformat()
        with self._session() as conn:
            conn.execute(
                """
                INSERT INTO ingestion_log (dataset, partition_key, status, row_count, sha256, source_url, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(dataset, partition_key) DO UPDATE SET
                    status = excluded.status,
                    row_count = excluded.row_count,
                    sha256 = COALESCE(excluded.sha256, ingestion_log.sha256),
                    source_url = COALESCE(excluded.source_url, ingestion_log.source_url),
                    updated_at = excluded.updated_at
                """,
                (dataset, partition_key, status, row_count, digest, source_url, now_str),
            )

    def summary(self) -> list[dict]:
        with self._session() as conn:
            rows = conn.execute(
                """
                SELECT
                    dataset,
                    COUNT(*) FILTER (WHERE status = 'OK') AS ok_partitions,
                    COUNT(*) FILTER (WHERE status = 'HOLIDAY') AS holidays,
                    SUM(row_count) AS total_rows,
                    MIN(partition_key) FILTER (WHERE status = 'OK') AS min_key,
                    MAX(partition_key) FILTER (WHERE status = 'OK') AS max_key,
                    MAX(updated_at) AS last_updated
                FROM ingestion_log
                GROUP BY dataset
                ORDER BY dataset
                """
            ).fetchall()
        return [
            {
                "dataset": r[0],
                "ok_partitions": r[1],
                "holidays": r[2],
                "total_rows": r[3] or 0,
                "min_key": r[4] or "-",
                "max_key": r[5] or "-",
                "last_updated": r[6] or "-",
            }
            for r in rows
        ]
