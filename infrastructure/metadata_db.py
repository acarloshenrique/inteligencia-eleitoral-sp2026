from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
import json
from pathlib import Path
import sqlite3
from typing import Any


class MetadataDb:
    def __init__(self, db_path: Path):
        self._db_path = db_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(self._db_path)
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA foreign_keys=ON;")
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_schema(self) -> None:
        with self._conn() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    job_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    result_json TEXT,
                    error_text TEXT,
                    created_at_utc TEXT NOT NULL,
                    updated_at_utc TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS audit_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    actor TEXT NOT NULL,
                    role TEXT NOT NULL,
                    action TEXT NOT NULL,
                    resource TEXT NOT NULL,
                    metadata_json TEXT,
                    created_at_utc TEXT NOT NULL
                )
                """
            )

    def create_job(self, job_id: str, job_type: str, payload: dict[str, Any]) -> None:
        now = datetime.now(UTC).isoformat()
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO jobs (id, job_type, status, payload_json, result_json, error_text, created_at_utc, updated_at_utc)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (job_id, job_type, "queued", json.dumps(payload, ensure_ascii=False), None, None, now, now),
            )

    def set_status(self, job_id: str, status: str) -> None:
        now = datetime.now(UTC).isoformat()
        with self._conn() as conn:
            conn.execute("UPDATE jobs SET status = ?, updated_at_utc = ? WHERE id = ?", (status, now, job_id))

    def set_result(self, job_id: str, result: dict[str, Any]) -> None:
        now = datetime.now(UTC).isoformat()
        with self._conn() as conn:
            conn.execute(
                "UPDATE jobs SET status = ?, result_json = ?, updated_at_utc = ? WHERE id = ?",
                ("finished", json.dumps(result, ensure_ascii=False), now, job_id),
            )

    def set_error(self, job_id: str, error_text: str) -> None:
        now = datetime.now(UTC).isoformat()
        with self._conn() as conn:
            conn.execute(
                "UPDATE jobs SET status = ?, error_text = ?, updated_at_utc = ? WHERE id = ?",
                ("failed", error_text, now, job_id),
            )

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT id, job_type, status, payload_json, result_json, error_text, created_at_utc, updated_at_utc
                FROM jobs WHERE id = ?
                """,
                (job_id,),
            ).fetchone()
        if row is None:
            return None
        payload = json.loads(row[3]) if row[3] else {}
        result = json.loads(row[4]) if row[4] else None
        return {
            "id": row[0],
            "job_type": row[1],
            "status": row[2],
            "payload": payload,
            "result": result,
            "error": row[5],
            "created_at_utc": row[6],
            "updated_at_utc": row[7],
        }

    def log_audit(self, *, actor: str, role: str, action: str, resource: str, metadata: dict[str, Any] | None = None) -> None:
        now = datetime.now(UTC).isoformat()
        payload = json.dumps(metadata or {}, ensure_ascii=False)
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO audit_events (actor, role, action, resource, metadata_json, created_at_utc)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (actor, role, action, resource, payload, now),
            )

    def list_audit(self, limit: int = 100) -> list[dict[str, Any]]:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT id, actor, role, action, resource, metadata_json, created_at_utc
                FROM audit_events
                ORDER BY id DESC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            out.append(
                {
                    "id": row[0],
                    "actor": row[1],
                    "role": row[2],
                    "action": row[3],
                    "resource": row[4],
                    "metadata": json.loads(row[5]) if row[5] else {},
                    "created_at_utc": row[6],
                }
            )
        return out

    def purge_older_than_days(self, days: int) -> dict[str, int]:
        cutoff_dt = datetime.now(UTC).timestamp() - max(1, days) * 86400
        removed = {"jobs": 0, "audit_events": 0}
        with self._conn() as conn:
            # timestamps são ISO UTC; comparação textual funciona para esse formato.
            cutoff_iso = datetime.fromtimestamp(cutoff_dt, UTC).isoformat()
            cur = conn.execute("DELETE FROM jobs WHERE created_at_utc < ?", (cutoff_iso,))
            removed["jobs"] = cur.rowcount if cur.rowcount is not None else 0
            cur2 = conn.execute("DELETE FROM audit_events WHERE created_at_utc < ?", (cutoff_iso,))
            removed["audit_events"] = cur2.rowcount if cur2.rowcount is not None else 0
        return removed
