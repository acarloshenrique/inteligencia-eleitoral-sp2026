from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
import uuid

from fastapi import Depends, FastAPI, HTTPException, Request

from api.contracts import (
    AlertEvaluationResponse,
    AuditListResponse,
    ExportJobPayload,
    ExportRequest,
    HealthResponse,
    IngestionJobPayload,
    IngestionRequest,
    JobQueuedResponse,
    JobRecordResponse,
    ObservabilityResponse,
    OpsScheduleRequest,
    OpsScheduleResponse,
    ReindexJobPayload,
    ReindexRequest,
)
from api.security import AuthContext, audit_metadata_from_request, require_roles, validate_auth_configuration
from config.settings import get_settings
from infrastructure.env import is_within_gold_layer, validate_prod_runtime_hardening
from infrastructure.metadata_db import MetadataDb
from infrastructure.observability import AlertThresholds, build_observability_snapshot, evaluate_and_dispatch_alerts
from infrastructure.operation_scheduler import build_default_schedule, write_schedule_manifest
from infrastructure.queue_rq import get_queue


def _metadata_db() -> MetadataDb:
    settings = get_settings()
    return MetadataDb(settings.build_paths().metadata_db_path)


def _tenant_id() -> str:
    settings = get_settings()
    paths = settings.build_paths()
    return getattr(paths, "tenant_id", getattr(settings, "tenant_id", "default"))


def _validate_gold_input_path(input_path: str) -> None:
    paths = get_settings().build_paths()
    candidate = Path(input_path)
    if not candidate.exists():
        raise HTTPException(status_code=400, detail="input_path nao encontrado")
    if not is_within_gold_layer(paths, candidate):
        raise HTTPException(status_code=400, detail="input_path precisa estar na camada gold")


@asynccontextmanager
async def lifespan(app_: FastAPI):
    settings = get_settings()
    paths = settings.build_paths()
    hardening_errors = validate_prod_runtime_hardening(settings, paths)
    if hardening_errors:
        raise RuntimeError("; ".join(hardening_errors))
    validate_auth_configuration()
    yield


app = FastAPI(title="Inteligencia Eleitoral API", version="1.0.0", lifespan=lifespan)


@app.get("/health", response_model=HealthResponse)
def health():
    return {"status": "ok", "ts_utc": datetime.now(UTC).isoformat()}


@app.post("/v1/jobs/reindex", response_model=JobQueuedResponse)
def enqueue_reindex(
    req: ReindexRequest,
    request: Request,
    auth: AuthContext = Depends(require_roles("admin", "operator")),
):
    settings = get_settings()
    payload_model = ReindexJobPayload(**req.model_dump(), tenant_id=_tenant_id())
    payload = payload_model.model_dump()
    _validate_gold_input_path(payload_model.input_path)

    job_id = str(uuid.uuid4())
    db = _metadata_db()
    db.create_job(job_id, "reindex", payload, tenant_id=payload_model.tenant_id)
    db.log_audit(
        actor=auth.actor,
        role=auth.role,
        action="enqueue_reindex",
        resource=job_id,
        metadata={**audit_metadata_from_request(request), "token_fp": auth.token_fingerprint},
        tenant_id=payload_model.tenant_id,
    )
    queue = get_queue(settings.redis_url, settings.rq_queue_name)
    queue.enqueue("workers.tasks.run_reindex_task", job_id, payload, job_id=job_id)
    return JobQueuedResponse(job_id=job_id, job_type="reindex", tenant_id=payload_model.tenant_id)


@app.post("/v1/jobs/export", response_model=JobQueuedResponse)
def enqueue_export(
    req: ExportRequest,
    request: Request,
    auth: AuthContext = Depends(require_roles("admin", "operator")),
):
    settings = get_settings()
    payload_model = ExportJobPayload(**req.model_dump(), tenant_id=_tenant_id())
    payload = payload_model.model_dump()
    _validate_gold_input_path(payload_model.input_path)

    job_id = str(uuid.uuid4())
    db = _metadata_db()
    db.create_job(job_id, "export", payload, tenant_id=payload_model.tenant_id)
    db.log_audit(
        actor=auth.actor,
        role=auth.role,
        action="enqueue_export",
        resource=job_id,
        metadata={**audit_metadata_from_request(request), "token_fp": auth.token_fingerprint},
        tenant_id=payload_model.tenant_id,
    )
    queue = get_queue(settings.redis_url, settings.rq_queue_name)
    queue.enqueue("workers.tasks.run_export_task", job_id, payload, job_id=job_id)
    return JobQueuedResponse(job_id=job_id, job_type="export", tenant_id=payload_model.tenant_id)


@app.post("/v1/jobs/ingest", response_model=JobQueuedResponse)
def enqueue_ingest(
    req: IngestionRequest,
    request: Request,
    auth: AuthContext = Depends(require_roles("admin", "operator")),
):
    settings = get_settings()
    payload_model = IngestionJobPayload(**req.model_dump(), tenant_id=_tenant_id())
    payload = payload_model.model_dump()
    if not payload_model.source_catalog_path and not settings.ingestion_source_catalog_path:
        raise HTTPException(status_code=400, detail="source_catalog_path nao informado")

    job_id = str(uuid.uuid4())
    db = _metadata_db()
    db.create_job(job_id, "ingest", payload, tenant_id=payload_model.tenant_id)
    db.log_audit(
        actor=auth.actor,
        role=auth.role,
        action="enqueue_ingest",
        resource=job_id,
        metadata={**audit_metadata_from_request(request), "token_fp": auth.token_fingerprint},
        tenant_id=payload_model.tenant_id,
    )
    queue = get_queue(settings.redis_url, settings.rq_queue_name)
    queue.enqueue("workers.tasks.run_ingestion_task", job_id, payload, job_id=job_id)
    return JobQueuedResponse(job_id=job_id, job_type="ingest", tenant_id=payload_model.tenant_id)


@app.get("/v1/jobs/{job_id}", response_model=JobRecordResponse)
def get_job(
    job_id: str,
    request: Request,
    auth: AuthContext = Depends(require_roles("admin", "operator", "viewer")),
):
    db = _metadata_db()
    data = db.get_job(job_id)
    if data is None:
        raise HTTPException(status_code=404, detail="job nao encontrado")
    db.log_audit(
        actor=auth.actor,
        role=auth.role,
        action="get_job",
        resource=job_id,
        metadata={**audit_metadata_from_request(request), "token_fp": auth.token_fingerprint},
        tenant_id=_tenant_id(),
    )
    return data


@app.get("/v1/audit", response_model=AuditListResponse)
def list_audit(
    request: Request,
    limit: int = 100,
    auth: AuthContext = Depends(require_roles("admin")),
):
    db = _metadata_db()
    db.log_audit(
        actor=auth.actor,
        role=auth.role,
        action="list_audit",
        resource="audit_events",
        metadata={**audit_metadata_from_request(request), "limit": int(limit), "token_fp": auth.token_fingerprint},
        tenant_id=_tenant_id(),
    )
    return {"items": db.list_audit(limit=limit)}


@app.get("/v1/ops/observability", response_model=ObservabilityResponse)
def get_observability(
    request: Request,
    limit: int = 500,
    auth: AuthContext = Depends(require_roles("admin", "operator", "viewer")),
):
    settings = get_settings()
    tenant_id = _tenant_id()
    db = _metadata_db()
    db.log_audit(
        actor=auth.actor,
        role=auth.role,
        action="get_observability",
        resource="operational_events",
        metadata={**audit_metadata_from_request(request), "limit": int(limit), "token_fp": auth.token_fingerprint},
        tenant_id=tenant_id,
    )
    thresholds = AlertThresholds(
        error_rate=float(getattr(settings, "ops_alert_error_rate_threshold", 0.10)),
        latency_p95_ms=float(getattr(settings, "ops_alert_latency_p95_ms", 30000.0)),
        daily_cost_usd=float(getattr(settings, "ops_alert_daily_cost_usd", 50.0)),
    )
    return build_observability_snapshot(db, tenant_id=tenant_id, thresholds=thresholds, limit=limit)


@app.post("/v1/ops/alerts/evaluate", response_model=AlertEvaluationResponse)
def evaluate_ops_alerts(
    request: Request,
    limit: int = 500,
    auth: AuthContext = Depends(require_roles("admin", "operator")),
):
    settings = get_settings()
    tenant_id = _tenant_id()
    db = _metadata_db()
    db.log_audit(
        actor=auth.actor,
        role=auth.role,
        action="evaluate_ops_alerts",
        resource="alerts",
        metadata={**audit_metadata_from_request(request), "limit": int(limit), "token_fp": auth.token_fingerprint},
        tenant_id=tenant_id,
    )
    thresholds = AlertThresholds(
        error_rate=float(getattr(settings, "ops_alert_error_rate_threshold", 0.10)),
        latency_p95_ms=float(getattr(settings, "ops_alert_latency_p95_ms", 30000.0)),
        daily_cost_usd=float(getattr(settings, "ops_alert_daily_cost_usd", 50.0)),
    )
    alerts = evaluate_and_dispatch_alerts(
        db, tenant_id=tenant_id, thresholds=thresholds, settings=settings, limit=limit
    )
    return {"tenant_id": tenant_id, "alerts": alerts}


@app.post("/v1/ops/schedule", response_model=OpsScheduleResponse)
def create_ops_schedule(
    req: OpsScheduleRequest,
    request: Request,
    auth: AuthContext = Depends(require_roles("admin", "operator")),
):
    settings = get_settings()
    paths = settings.build_paths()
    schedules = build_default_schedule(
        tenant_id=paths.tenant_id,
        daily_hour=req.daily_ingestion_hour
        if req.daily_ingestion_hour is not None
        else settings.ops_daily_ingestion_hour,
        weekly_day=req.weekly_update_day if req.weekly_update_day is not None else settings.ops_weekly_update_day,
        weekly_hour=req.weekly_update_hour if req.weekly_update_hour is not None else settings.ops_weekly_update_hour,
    )
    manifest = write_schedule_manifest(paths, schedules)
    db = _metadata_db()
    db.log_audit(
        actor=auth.actor,
        role=auth.role,
        action="create_ops_schedule",
        resource=str(manifest),
        metadata={**audit_metadata_from_request(request), "token_fp": auth.token_fingerprint},
        tenant_id=paths.tenant_id,
    )
    return {
        "tenant_id": paths.tenant_id,
        "manifest_path": str(manifest),
        "pipelines": [item.name for item in schedules],
    }
