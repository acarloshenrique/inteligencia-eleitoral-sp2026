from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from domain.job_contracts import ExportJobPayload, IngestionJobPayload, JobStatus, JobType, ReindexJobPayload


class ReindexRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    input_path: str
    collection_name: str = Field(default="municipios_v2")
    force: bool = False


class ExportRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    input_path: str
    minimize: bool = True
    anonymize: bool = False


class IngestionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_catalog_path: str | None = None
    pipeline: str | None = None
    pipeline_version: str | None = None


class OpsScheduleRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    daily_ingestion_hour: int | None = None
    weekly_update_day: str | None = None
    weekly_update_hour: int | None = None


class JobQueuedResponse(BaseModel):
    job_id: str
    status: Literal["queued"] = "queued"
    job_type: JobType
    tenant_id: str


class HealthResponse(BaseModel):
    status: Literal["ok"]
    ts_utc: str


class JobRecordResponse(BaseModel):
    id: str
    job_type: str
    status: str
    payload: dict[str, Any] = Field(default_factory=dict)
    result: dict[str, Any] | None = None
    error: str | None = None
    created_at_utc: str | None = None
    updated_at_utc: str | None = None
    tenant_id: str = "default"
    latency_ms: float | None = None
    cost_usd: float | None = None


class AuditItem(BaseModel):
    id: int
    actor: str
    role: str
    action: str
    resource: str
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at_utc: str
    tenant_id: str = "default"


class AuditListResponse(BaseModel):
    items: list[AuditItem]


class OperationSummary(BaseModel):
    tenant_id: str
    events_total: int = 0
    jobs_total: int = 0
    errors_total: int = 0
    error_rate: float = 0.0
    latency_p95_ms: float = 0.0
    cost_total_usd: float = 0.0
    usage_total: int = 0
    recent_errors: list[str] = Field(default_factory=list)


class AlertItem(BaseModel):
    id: int | None = None
    tenant_id: str | None = None
    severity: str
    metric: str
    value: float
    threshold: float
    message: str
    status: str | None = None
    channels: list[str] = Field(default_factory=list)
    error: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at_utc: str | None = None
    updated_at_utc: str | None = None


class ObservabilityResponse(BaseModel):
    tenant_id: str
    summary: OperationSummary
    alerts: list[AlertItem] = Field(default_factory=list)
    persisted_alerts: list[AlertItem] = Field(default_factory=list)


class AlertEvaluationResponse(BaseModel):
    tenant_id: str
    alerts: list[AlertItem]


class OpsScheduleResponse(BaseModel):
    tenant_id: str
    manifest_path: str
    pipelines: list[str]


__all__ = [
    "AlertEvaluationResponse",
    "AuditListResponse",
    "ExportJobPayload",
    "ExportRequest",
    "HealthResponse",
    "IngestionJobPayload",
    "IngestionRequest",
    "JobQueuedResponse",
    "JobRecordResponse",
    "JobStatus",
    "JobType",
    "ObservabilityResponse",
    "OpsScheduleRequest",
    "OpsScheduleResponse",
    "ReindexJobPayload",
    "ReindexRequest",
]
