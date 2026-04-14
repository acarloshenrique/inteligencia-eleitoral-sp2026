from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

JobStatus = Literal["queued", "running", "finished", "failed"]
JobType = Literal["reindex", "export", "ingest"]


class TenantPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tenant_id: str = "default"


class ReindexJobPayload(TenantPayload):
    input_path: str
    collection_name: str = Field(default="municipios_v2")
    force: bool = False


class ExportJobPayload(TenantPayload):
    input_path: str
    minimize: bool = True
    anonymize: bool = False


class IngestionJobPayload(TenantPayload):
    source_catalog_path: str | None = None
    pipeline: str | None = None
    pipeline_version: str | None = None


class ReindexJobResult(BaseModel):
    model_config = ConfigDict(extra="allow")

    status: str
    collection_name: str
    state_path: str | None = None
    source_path: str | None = None
    source_hash: str | None = None
    source_rows: int | None = None
    indexed_count: int | None = None
    reason: str | None = None


class ExportJobResult(BaseModel):
    model_config = ConfigDict(extra="allow")

    run_id: str
    tenant_id: str
    artifact_uri: str
    manifest_path: str


class IngestionJobResult(BaseModel):
    model_config = ConfigDict(extra="allow")

    run_id: str
    pipeline: str
    pipeline_version: str
    downloads: list[dict[str, Any]] = Field(default_factory=list)
    manifest_path: str
    promotion_result: dict[str, Any] = Field(default_factory=dict)
