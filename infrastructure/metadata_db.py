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
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS operational_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tenant_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    resource TEXT NOT NULL,
                    status TEXT NOT NULL,
                    latency_ms REAL NOT NULL DEFAULT 0,
                    cost_usd REAL NOT NULL DEFAULT 0,
                    usage_count INTEGER NOT NULL DEFAULT 1,
                    error_text TEXT,
                    metadata_json TEXT,
                    created_at_utc TEXT NOT NULL
                )
                """
            )
            self._ensure_column(conn, "jobs", "tenant_id", "TEXT NOT NULL DEFAULT 'default'")
            self._ensure_column(conn, "jobs", "latency_ms", "REAL")
            self._ensure_column(conn, "jobs", "cost_usd", "REAL")
            self._ensure_column(conn, "audit_events", "tenant_id", "TEXT NOT NULL DEFAULT 'default'")

    def _ensure_column(self, conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
        columns = {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
        if column not in columns:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")

    def create_job(self, job_id: str, job_type: str, payload: dict[str, Any], *, tenant_id: str = "default") -> None:
        now = datetime.now(UTC).isoformat()
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO jobs (id, job_type, status, payload_json, result_json, error_text, created_at_utc, updated_at_utc, tenant_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (job_id, job_type, "queued", json.dumps(payload, ensure_ascii=False), None, None, now, now, tenant_id),
            )

    def set_status(self, job_id: str, status: str) -> None:
        now = datetime.now(UTC).isoformat()
        with self._conn() as conn:
            conn.execute("UPDATE jobs SET status = ?, updated_at_utc = ? WHERE id = ?", (status, now, job_id))

    def set_result(self, job_id: str, result: dict[str, Any], *, latency_ms: float | None = None, cost_usd: float | None = None) -> None:
        now = datetime.now(UTC).isoformat()
        with self._conn() as conn:
            conn.execute(
                "UPDATE jobs SET status = ?, result_json = ?, updated_at_utc = ?, latency_ms = COALESCE(?, latency_ms), cost_usd = COALESCE(?, cost_usd) WHERE id = ?",
                ("finished", json.dumps(result, ensure_ascii=False), now, latency_ms, cost_usd, job_id),
            )

    def set_error(self, job_id: str, error_text: str, *, latency_ms: float | None = None, cost_usd: float | None = None) -> None:
        now = datetime.now(UTC).isoformat()
        with self._conn() as conn:
            conn.execute(
                "UPDATE jobs SET status = ?, error_text = ?, updated_at_utc = ?, latency_ms = COALESCE(?, latency_ms), cost_usd = COALESCE(?, cost_usd) WHERE id = ?",
                ("failed", error_text, now, latency_ms, cost_usd, job_id),
            )

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT id, job_type, status, payload_json, result_json, error_text, created_at_utc, updated_at_utc, tenant_id, latency_ms, cost_usd
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
            "tenant_id": row[8],
            "latency_ms": row[9],
            "cost_usd": row[10],
        }

    def log_audit(self, *, actor: str, role: str, action: str, resource: str, metadata: dict[str, Any] | None = None, tenant_id: str = "default") -> None:
        now = datetime.now(UTC).isoformat()
        payload = json.dumps(metadata or {}, ensure_ascii=False)
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO audit_events (actor, role, action, resource, metadata_json, created_at_utc, tenant_id)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (actor, role, action, resource, payload, now, tenant_id),
            )

    def list_audit(self, limit: int = 100) -> list[dict[str, Any]]:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT id, actor, role, action, resource, metadata_json, created_at_utc, tenant_id
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
                    "tenant_id": row[7],
                }
            )
        return out

    def record_operational_event(
        self,
        *,
        tenant_id: str,
        event_type: str,
        resource: str,
        status: str,
        latency_ms: float = 0.0,
        cost_usd: float = 0.0,
        usage_count: int = 1,
        error_text: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        now = datetime.now(UTC).isoformat()
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO operational_events
                (tenant_id, event_type, resource, status, latency_ms, cost_usd, usage_count, error_text, metadata_json, created_at_utc)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    tenant_id,
                    event_type,
                    resource,
                    status,
                    float(latency_ms),
                    float(cost_usd),
                    int(usage_count),
                    error_text,
                    json.dumps(metadata or {}, ensure_ascii=False),
                    now,
                ),
            )

    def summarize_operations(self, *, tenant_id: str | None = None, limit: int = 500) -> dict[str, Any]:
        where = "WHERE tenant_id = ?" if tenant_id else ""
        params: tuple[Any, ...] = (tenant_id,) if tenant_id else ()
        with self._conn() as conn:
            rows = conn.execute(
                f"""
                SELECT tenant_id, event_type, status, latency_ms, cost_usd, usage_count, error_text, created_at_utc
                FROM operational_events
                {where}
                ORDER BY id DESC
                LIMIT ?
                """,
                (*params, int(limit)),
            ).fetchall()
        latencies = [float(row[3] or 0.0) for row in rows]
        latencies_sorted = sorted(latencies)
        p95_idx = int(round(0.95 * (len(latencies_sorted) - 1))) if latencies_sorted else 0
        failed = sum(1 for row in rows if row[2] == "failed")
        return {
            "tenant_id": tenant_id or "all",
            "events_total": len(rows),
            "jobs_total": sum(1 for row in rows if row[1].startswith("job.")),
            "errors_total": failed,
            "error_rate": float(failed / len(rows)) if rows else 0.0,
            "latency_p95_ms": latencies_sorted[p95_idx] if latencies_sorted else 0.0,
            "cost_total_usd": float(sum(float(row[4] or 0.0) for row in rows)),
            "usage_total": int(sum(int(row[5] or 0) for row in rows)),
            "recent_errors": [row[6] for row in rows if row[6]][:10],
        }

    def purge_older_than_days(self, days: int) -> dict[str, int]:
        cutoff_dt = datetime.now(UTC).timestamp() - max(1, days) * 86400
        removed = {"jobs": 0, "audit_events": 0, "operational_events": 0}
        with self._conn() as conn:
            # timestamps são ISO UTC; comparação textual funciona para esse formato.
            cutoff_iso = datetime.fromtimestamp(cutoff_dt, UTC).isoformat()
            cur = conn.execute("DELETE FROM jobs WHERE created_at_utc < ?", (cutoff_iso,))
            removed["jobs"] = cur.rowcount if cur.rowcount is not None else 0
            cur2 = conn.execute("DELETE FROM audit_events WHERE created_at_utc < ?", (cutoff_iso,))
            removed["audit_events"] = cur2.rowcount if cur2.rowcount is not None else 0
            cur3 = conn.execute("DELETE FROM operational_events WHERE created_at_utc < ?", (cutoff_iso,))
            removed["operational_events"] = cur3.rowcount if cur3.rowcount is not None else 0
        return removed
