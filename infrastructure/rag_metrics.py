from __future__ import annotations

from datetime import UTC, datetime
import json
from pathlib import Path
from statistics import mean
from typing import Any

from config.settings import AppPaths


def _p95(values: list[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = max(0, min(len(ordered) - 1, int(round(0.95 * (len(ordered) - 1)))))
    return float(ordered[idx])


def _extract_ground_truth_municipios(question: str, retrieved: list[str]) -> list[str]:
    q = question.lower()
    matches = [m for m in retrieved if m.lower() in q]
    return matches


def _recall_at_k(question: str, retrieved: list[str]) -> float | None:
    truth = _extract_ground_truth_municipios(question, retrieved)
    if not truth:
        return None
    hits = sum(1 for t in truth if t in retrieved)
    return float(hits / len(truth))


class RagMetricsTracker:
    def __init__(self, paths: AppPaths, model_name: str = "llama-3.3-70b-versatile"):
        self._model_name = model_name
        self._dir = paths.data_root / "outputs" / "metrics"
        self._dir.mkdir(parents=True, exist_ok=True)
        self._events_path = self._dir / "rag_metrics_events.jsonl"
        self._snapshot_path = self._dir / "rag_metrics_snapshot.json"

    def _read_events(self) -> list[dict[str, Any]]:
        if not self._events_path.exists():
            return []
        out: list[dict[str, Any]] = []
        for line in self._events_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        return out

    def _write_snapshot(self, events: list[dict[str, Any]]) -> dict[str, Any]:
        recalls = [float(e["recall_at_k"]) for e in events if e.get("recall_at_k") is not None]
        total_lat = [float(e.get("latency_total_ms", 0.0)) for e in events]
        vector_lat = [float(e.get("latency_vector_ms", 0.0)) for e in events]
        llm_lat = [float(e.get("latency_llm_ms", 0.0)) for e in events]
        costs = [float(e.get("cost_estimated_usd", 0.0)) for e in events]
        n = len(events)
        fallback_any = sum(1 for e in events if e.get("fallback_any"))
        fallback_vector = sum(1 for e in events if e.get("fallback_vector"))
        fallback_llm = sum(1 for e in events if e.get("fallback_llm"))
        snap = {
            "model": self._model_name,
            "updated_at_utc": datetime.now(UTC).isoformat(),
            "queries_total": n,
            "recall_at_k_avg": float(mean(recalls)) if recalls else None,
            "latency_total_p95_ms": _p95(total_lat),
            "latency_vector_p95_ms": _p95(vector_lat),
            "latency_llm_p95_ms": _p95(llm_lat),
            "fallback_rate_any": float(fallback_any / n) if n else 0.0,
            "fallback_rate_vector": float(fallback_vector / n) if n else 0.0,
            "fallback_rate_llm": float(fallback_llm / n) if n else 0.0,
            "cost_per_query_avg_usd": float(mean(costs)) if costs else 0.0,
        }
        self._snapshot_path.write_text(json.dumps(snap, ensure_ascii=False, indent=2), encoding="utf-8")
        return snap

    def record_query(
        self,
        *,
        question: str,
        retrieved_municipios: list[str],
        latency_total_ms: float,
        latency_vector_ms: float,
        latency_llm_ms: float,
        fallback_vector: bool,
        fallback_llm: bool,
        tokens_total: int,
        cost_estimated_usd: float,
        cached_vector: bool,
        cached_llm: bool,
    ) -> dict[str, Any]:
        event = {
            "ts_utc": datetime.now(UTC).isoformat(),
            "question": question,
            "retrieved_count": len(retrieved_municipios),
            "recall_at_k": _recall_at_k(question, retrieved_municipios),
            "latency_total_ms": float(latency_total_ms),
            "latency_vector_ms": float(latency_vector_ms),
            "latency_llm_ms": float(latency_llm_ms),
            "fallback_vector": bool(fallback_vector),
            "fallback_llm": bool(fallback_llm),
            "fallback_any": bool(fallback_vector or fallback_llm),
            "tokens_total": int(tokens_total),
            "cost_estimated_usd": float(cost_estimated_usd),
            "cached_vector": bool(cached_vector),
            "cached_llm": bool(cached_llm),
        }
        with self._events_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")
        events = self._read_events()
        return self._write_snapshot(events)

    def get_snapshot(self) -> dict[str, Any]:
        if self._snapshot_path.exists():
            return json.loads(self._snapshot_path.read_text(encoding="utf-8"))
        return self._write_snapshot(self._read_events())
