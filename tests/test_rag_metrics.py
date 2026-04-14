import json
from pathlib import Path
import tempfile

from config.settings import AppPaths
from infrastructure.rag_metrics import RagMetricsTracker


def _build_paths(tmp: str) -> AppPaths:
    root = Path(tmp)
    ingestion_root = root / "ingestion"
    lake_root = root / "lake"
    bronze_root = lake_root / "bronze"
    silver_root = lake_root / "silver"
    gold_root = lake_root / "gold"
    gold_reports_root = gold_root / "reports"
    gold_serving_root = gold_root / "serving"
    catalog_root = gold_root / "_catalog"
    chroma = root / "chromadb"
    runtime_reports_root = root / "runtime_rel"
    for p in [
        ingestion_root,
        bronze_root,
        silver_root,
        gold_root,
        gold_reports_root,
        gold_serving_root,
        catalog_root,
        chroma,
        runtime_reports_root,
    ]:
        p.mkdir(parents=True, exist_ok=True)
    return AppPaths(
        data_root=root,
        ingestion_root=ingestion_root,
        lake_root=lake_root,
        bronze_root=bronze_root,
        silver_root=silver_root,
        gold_root=gold_root,
        gold_reports_root=gold_reports_root,
        gold_serving_root=gold_serving_root,
        catalog_root=catalog_root,
        chromadb_path=chroma,
        runtime_reports_root=runtime_reports_root,
        ts="20260407_000000",
        metadata_db_path=root / "metadata" / "jobs.sqlite3",
        artifact_root=root / "artifacts",
    )


def test_rag_metrics_tracker_records_snapshot_with_p95_and_fallback_rate():
    with tempfile.TemporaryDirectory() as tmp:
        tracker = RagMetricsTracker(paths=_build_paths(tmp))
        tracker.record_query(
            question="Perfil de Cidade A",
            retrieved_municipios=["Cidade A", "Cidade B"],
            latency_total_ms=100.0,
            latency_vector_ms=30.0,
            latency_llm_ms=70.0,
            fallback_vector=False,
            fallback_llm=False,
            tokens_total=200,
            cost_estimated_usd=0.1,
            cached_vector=False,
            cached_llm=False,
        )
        snap = tracker.record_query(
            question="Pergunta sem municipio explicito",
            retrieved_municipios=["Cidade C"],
            latency_total_ms=300.0,
            latency_vector_ms=90.0,
            latency_llm_ms=210.0,
            fallback_vector=True,
            fallback_llm=False,
            tokens_total=0,
            cost_estimated_usd=0.0,
            cached_vector=True,
            cached_llm=False,
        )

        assert snap["queries_total"] == 2
        assert snap["latency_total_p95_ms"] >= 100.0
        assert snap["fallback_rate_vector"] == 0.5
        assert snap["cost_per_query_avg_usd"] == 0.05

        metrics_path = Path(tmp) / "outputs" / "metrics" / "rag_metrics_snapshot.json"
        disk_snap = json.loads(metrics_path.read_text(encoding="utf-8"))
        assert disk_snap["queries_total"] == 2
