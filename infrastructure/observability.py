from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import time
from typing import Any, Iterator

from infrastructure.metadata_db import MetadataDb


@dataclass(frozen=True)
class AlertThresholds:
    error_rate: float = 0.10
    latency_p95_ms: float = 30000.0
    daily_cost_usd: float = 50.0


class OperationObserver:
    def __init__(self, db: MetadataDb, *, tenant_id: str):
        self._db = db
        self._tenant_id = tenant_id

    @contextmanager
    def track(self, *, event_type: str, resource: str, metadata: dict[str, Any] | None = None) -> Iterator[dict[str, Any]]:
        started = time.perf_counter()
        ctx: dict[str, Any] = {"cost_usd": 0.0, "usage_count": 1, "metadata": metadata or {}}
        try:
            yield ctx
            latency_ms = (time.perf_counter() - started) * 1000.0
            self._db.record_operational_event(
                tenant_id=self._tenant_id,
                event_type=event_type,
                resource=resource,
                status="success",
                latency_ms=latency_ms,
                cost_usd=float(ctx.get("cost_usd", 0.0) or 0.0),
                usage_count=int(ctx.get("usage_count", 1) or 1),
                metadata=dict(ctx.get("metadata", {}) or {}),
            )
        except Exception as exc:
            latency_ms = (time.perf_counter() - started) * 1000.0
            self._db.record_operational_event(
                tenant_id=self._tenant_id,
                event_type=event_type,
                resource=resource,
                status="failed",
                latency_ms=latency_ms,
                cost_usd=float(ctx.get("cost_usd", 0.0) or 0.0),
                usage_count=int(ctx.get("usage_count", 1) or 1),
                error_text=str(exc),
                metadata=dict(ctx.get("metadata", {}) or {}),
            )
            raise


def build_alerts(summary: dict[str, Any], thresholds: AlertThresholds) -> list[dict[str, Any]]:
    alerts: list[dict[str, Any]] = []
    if float(summary.get("error_rate", 0.0)) > thresholds.error_rate:
        alerts.append(
            {
                "severity": "high",
                "metric": "error_rate",
                "value": float(summary.get("error_rate", 0.0)),
                "threshold": thresholds.error_rate,
                "message": "Taxa de erro operacional acima do limite",
            }
        )
    if float(summary.get("latency_p95_ms", 0.0)) > thresholds.latency_p95_ms:
        alerts.append(
            {
                "severity": "medium",
                "metric": "latency_p95_ms",
                "value": float(summary.get("latency_p95_ms", 0.0)),
                "threshold": thresholds.latency_p95_ms,
                "message": "Latencia p95 acima do limite operacional",
            }
        )
    if float(summary.get("cost_total_usd", 0.0)) > thresholds.daily_cost_usd:
        alerts.append(
            {
                "severity": "medium",
                "metric": "cost_total_usd",
                "value": float(summary.get("cost_total_usd", 0.0)),
                "threshold": thresholds.daily_cost_usd,
                "message": "Custo acumulado acima do limite configurado",
            }
        )
    return alerts


def build_observability_snapshot(
    db: MetadataDb,
    *,
    tenant_id: str,
    thresholds: AlertThresholds,
    limit: int = 500,
) -> dict[str, Any]:
    summary = db.summarize_operations(tenant_id=tenant_id, limit=limit)
    return {
        "tenant_id": tenant_id,
        "summary": summary,
        "alerts": build_alerts(summary, thresholds),
    }
