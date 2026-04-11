from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import uuid

from fastapi import Depends, FastAPI, HTTPException, Request
from pydantic import BaseModel, Field

from api.security import AuthContext, audit_metadata_from_request, require_roles
from config.settings import get_settings
from infrastructure.env import is_within_gold_layer
from infrastructure.metadata_db import MetadataDb
from infrastructure.queue_rq import get_queue


class ReindexRequest(BaseModel):
    input_path: str
    collection_name: str = Field(default="municipios_v2")
    force: bool = False


class ExportRequest(BaseModel):
    input_path: str
    minimize: bool = True
    anonymize: bool = False


class IngestionRequest(BaseModel):
    source_catalog_path: str | None = None
    pipeline: str | None = None
    pipeline_version: str | None = None


def _metadata_db() -> MetadataDb:
    settings = get_settings()
    return MetadataDb(settings.build_paths().metadata_db_path)


def _validate_gold_input_path(input_path: str) -> None:
    paths = get_settings().build_paths()
    candidate = Path(input_path)
    if not candidate.exists():
        raise HTTPException(status_code=400, detail="input_path nao encontrado")
    if not is_within_gold_layer(paths, candidate):
        raise HTTPException(status_code=400, detail="input_path precisa estar na camada gold")


app = FastAPI(title="Inteligencia Eleitoral API", version="1.0.0")


@app.get("/health")
def health():
    return {"status": "ok", "ts_utc": datetime.now(UTC).isoformat()}


@app.post("/v1/jobs/reindex")
def enqueue_reindex(
    req: ReindexRequest,
    request: Request,
    auth: AuthContext = Depends(require_roles("admin", "operator")),
):
    settings = get_settings()
    payload = req.model_dump()
    _validate_gold_input_path(payload["input_path"])

    job_id = str(uuid.uuid4())
    db = _metadata_db()
    db.create_job(job_id, "reindex", payload)
    db.log_audit(
        actor=auth.actor,
        role=auth.role,
        action="enqueue_reindex",
        resource=job_id,
        metadata={**audit_metadata_from_request(request), "token_fp": auth.token_fingerprint},
    )
    queue = get_queue(settings.redis_url, settings.rq_queue_name)
    queue.enqueue("workers.tasks.run_reindex_task", job_id, payload, job_id=job_id)
    return {"job_id": job_id, "status": "queued"}


@app.post("/v1/jobs/export")
def enqueue_export(
    req: ExportRequest,
    request: Request,
    auth: AuthContext = Depends(require_roles("admin", "operator")),
):
    settings = get_settings()
    payload = req.model_dump()
    _validate_gold_input_path(payload["input_path"])

    job_id = str(uuid.uuid4())
    db = _metadata_db()
    db.create_job(job_id, "export", payload)
    db.log_audit(
        actor=auth.actor,
        role=auth.role,
        action="enqueue_export",
        resource=job_id,
        metadata={**audit_metadata_from_request(request), "token_fp": auth.token_fingerprint},
    )
    queue = get_queue(settings.redis_url, settings.rq_queue_name)
    queue.enqueue("workers.tasks.run_export_task", job_id, payload, job_id=job_id)
    return {"job_id": job_id, "status": "queued"}


@app.post("/v1/jobs/ingest")
def enqueue_ingest(
    req: IngestionRequest,
    request: Request,
    auth: AuthContext = Depends(require_roles("admin", "operator")),
):
    settings = get_settings()
    payload = req.model_dump()
    if not payload.get("source_catalog_path") and not settings.ingestion_source_catalog_path:
        raise HTTPException(status_code=400, detail="source_catalog_path nao informado")

    job_id = str(uuid.uuid4())
    db = _metadata_db()
    db.create_job(job_id, "ingest", payload)
    db.log_audit(
        actor=auth.actor,
        role=auth.role,
        action="enqueue_ingest",
        resource=job_id,
        metadata={**audit_metadata_from_request(request), "token_fp": auth.token_fingerprint},
    )
    queue = get_queue(settings.redis_url, settings.rq_queue_name)
    queue.enqueue("workers.tasks.run_ingestion_task", job_id, payload, job_id=job_id)
    return {"job_id": job_id, "status": "queued"}


@app.get("/v1/jobs/{job_id}")
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
    )
    return data


@app.get("/v1/audit")
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
    )
    return {"items": db.list_audit(limit=limit)}
