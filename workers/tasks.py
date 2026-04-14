from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from config.settings import get_settings
from domain.lgpd import anonymize_columns, minimize_dataframe
from infrastructure.artifact_store import LocalArtifactStore, S3ArtifactStore
from infrastructure.automated_ingestion import run_automated_ingestion
from infrastructure.data_pipeline import run_versioned_data_pipeline
from infrastructure.dataset_catalog import build_dataset_metadata, register_dataset_version
from infrastructure.env import is_within_gold_layer
from infrastructure.metadata_db import MetadataDb
from infrastructure.observability import AlertThresholds, OperationObserver, evaluate_and_dispatch_alerts
from infrastructure.secret_factory import build_secret_provider
from infrastructure.vector_index_job import run_vector_reindex_job


def _metadata_db() -> MetadataDb:
    settings = get_settings()
    paths = settings.build_paths()
    return MetadataDb(paths.metadata_db_path)


def _alert_thresholds(settings) -> AlertThresholds:
    return AlertThresholds(
        error_rate=float(getattr(settings, "ops_alert_error_rate_threshold", 0.10)),
        latency_p95_ms=float(getattr(settings, "ops_alert_latency_p95_ms", 30000.0)),
        daily_cost_usd=float(getattr(settings, "ops_alert_daily_cost_usd", 50.0)),
    )


def _evaluate_job_alerts(db: MetadataDb, settings, tenant_id: str) -> None:
    evaluate_and_dispatch_alerts(db, tenant_id=tenant_id, thresholds=_alert_thresholds(settings), settings=settings)


def _artifact_store():
    settings = get_settings()
    paths = settings.build_paths()
    if settings.artifact_backend.lower() == "s3":
        return S3ArtifactStore(
            bucket=settings.s3_bucket,
            endpoint_url=settings.s3_endpoint_url,
            access_key=settings.s3_access_key,
            secret_key=settings.s3_secret_key,
            region=settings.s3_region,
        )
    return LocalArtifactStore(paths.artifact_root)


def run_reindex_task(job_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    db = _metadata_db()
    settings = get_settings()
    paths = settings.build_paths()
    tenant_id = str(payload.get("tenant_id") or paths.tenant_id)
    observer = OperationObserver(db, tenant_id=tenant_id)
    db.set_status(job_id, "running")
    try:
        with observer.track(event_type="job.reindex", resource=job_id, metadata={"payload": payload}) as span:
            source_path = Path(payload["input_path"])
            if not source_path.exists() or not is_within_gold_layer(paths, source_path):
                raise ValueError("input_path precisa existir na camada gold")
            result = run_vector_reindex_job(
                chromadb_path=paths.chromadb_path,
                input_parquet=source_path,
                collection_name=payload.get("collection_name", "municipios_v2"),
                force=bool(payload.get("force", False)),
            )
            span["usage_count"] = int(result.get("rows_indexed", 1) or 1) if isinstance(result, dict) else 1
        db.set_result(job_id, result)
        return result
    except Exception as e:
        db.set_error(job_id, str(e))
        _evaluate_job_alerts(db, settings, tenant_id)
        raise


def run_export_task(job_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    db = _metadata_db()
    settings = get_settings()
    paths = settings.build_paths()
    tenant_id = str(payload.get("tenant_id") or paths.tenant_id)
    observer = OperationObserver(db, tenant_id=tenant_id)
    store = _artifact_store()
    db.set_status(job_id, "running")
    try:
        with observer.track(event_type="job.export", resource=job_id, metadata={"payload": payload}) as span:
            source_path = Path(payload["input_path"])
            if not source_path.exists() or not is_within_gold_layer(paths, source_path):
                raise ValueError("input_path precisa existir na camada gold")
            work_source = source_path
            if bool(payload.get("minimize", True)) or bool(payload.get("anonymize", False)):
                df = pd.read_parquet(source_path)
                span["usage_count"] = len(df)
                if bool(payload.get("minimize", True)):
                    df = minimize_dataframe(df)
                if bool(payload.get("anonymize", False)):
                    salt = (
                        build_secret_provider(settings).get_secret("LGPD_ANONYMIZATION_SALT")
                        or settings.lgpd_anonymization_salt
                    )
                    df = anonymize_columns(df, ["municipio"], salt=salt)
                tmp_path = paths.runtime_reports_root / f"export_input_{job_id}.parquet"
                tmp_path.parent.mkdir(parents=True, exist_ok=True)
                df.to_parquet(tmp_path, index=False)
                work_source = tmp_path
            pipeline_result = run_versioned_data_pipeline(paths=paths, input_path=work_source, pipeline_version="v2")
            published = Path(pipeline_result["publish"]["published_path"])
            artifact_key = f"tenants/{tenant_id}/datasets/{pipeline_result['run_id']}/{published.name}"
            artifact_uri = store.put_file(published, artifact_key)
            metadata = build_dataset_metadata(
                dataset_name="df_municipios_exported",
                dataset_version=pipeline_result["run_id"],
                dataset_path=published,
                pipeline_version="v2",
                run_id=pipeline_result["run_id"],
            )
            register_dataset_version(paths, metadata)
            result = {
                "run_id": pipeline_result["run_id"],
                "tenant_id": tenant_id,
                "artifact_uri": artifact_uri,
                "manifest_path": pipeline_result["manifest_path"],
            }
        db.set_result(job_id, result)
        return result
    except Exception as e:
        db.set_error(job_id, str(e))
        _evaluate_job_alerts(db, settings, tenant_id)
        raise


def run_ingestion_task(job_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    db = _metadata_db()
    settings = get_settings()
    paths = settings.build_paths()
    tenant_id = str(payload.get("tenant_id") or paths.tenant_id)
    observer = OperationObserver(db, tenant_id=tenant_id)
    db.set_status(job_id, "running")
    try:
        with observer.track(event_type="job.ingest", resource=job_id, metadata={"payload": payload}) as span:
            catalog_raw = str(
                payload.get("source_catalog_path") or settings.ingestion_source_catalog_path or ""
            ).strip()
            if not catalog_raw:
                raise ValueError("source_catalog_path nao informado")
            result = run_automated_ingestion(
                paths=paths,
                catalog_path=Path(catalog_raw).resolve(),
                pipeline=str(payload.get("pipeline", "")).strip() or None,
                pipeline_version=str(payload.get("pipeline_version", "")).strip() or None,
            )
            if isinstance(result, dict):
                span["usage_count"] = int(result.get("sources_total", 1) or 1)
        db.set_result(job_id, result)
        return result
    except Exception as e:
        db.set_error(job_id, str(e))
        _evaluate_job_alerts(db, settings, tenant_id)
        raise
