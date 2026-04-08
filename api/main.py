from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import uuid

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from config.settings import get_settings
from infrastructure.metadata_db import MetadataDb
from infrastructure.queue_rq import get_queue


class ReindexRequest(BaseModel):
    input_path: str
    collection_name: str = Field(default="municipios_v2")
    force: bool = False


class ExportRequest(BaseModel):
    input_path: str


def _metadata_db() -> MetadataDb:
    settings = get_settings()
    return MetadataDb(settings.build_paths().metadata_db_path)


app = FastAPI(title="Inteligencia Eleitoral API", version="1.0.0")


@app.get("/health")
def health():
    return {"status": "ok", "ts_utc": datetime.now(UTC).isoformat()}


@app.post("/v1/jobs/reindex")
def enqueue_reindex(req: ReindexRequest):
    settings = get_settings()
    payload = req.model_dump()
    if not Path(payload["input_path"]).exists():
        raise HTTPException(status_code=400, detail="input_path nao encontrado")

    job_id = str(uuid.uuid4())
    db = _metadata_db()
    db.create_job(job_id, "reindex", payload)
    queue = get_queue(settings.redis_url, settings.rq_queue_name)
    queue.enqueue("workers.tasks.run_reindex_task", job_id, payload, job_id=job_id)
    return {"job_id": job_id, "status": "queued"}


@app.post("/v1/jobs/export")
def enqueue_export(req: ExportRequest):
    settings = get_settings()
    payload = req.model_dump()
    if not Path(payload["input_path"]).exists():
        raise HTTPException(status_code=400, detail="input_path nao encontrado")

    job_id = str(uuid.uuid4())
    db = _metadata_db()
    db.create_job(job_id, "export", payload)
    queue = get_queue(settings.redis_url, settings.rq_queue_name)
    queue.enqueue("workers.tasks.run_export_task", job_id, payload, job_id=job_id)
    return {"job_id": job_id, "status": "queued"}


@app.get("/v1/jobs/{job_id}")
def get_job(job_id: str):
    db = _metadata_db()
    data = db.get_job(job_id)
    if data is None:
        raise HTTPException(status_code=404, detail="job nao encontrado")
    return data
