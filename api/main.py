from __future__ import annotations

import logging
import time
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path

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
from api.decision_contracts import (
    AllocationScenarioRequest,
    AllocationScenarioResponse,
    CandidateListResponse,
    CandidateProfileSchema,
    CandidateUpsertResponse,
    PrioritizedTerritoriesResponse,
    RecommendationExplanationResponse,
    ServingManifestResponse,
    ServingOutputResponse,
    TerritoryScoreResponse,
)
from api.rate_limit import build_rate_limiter, install_rate_limit_middleware
from api.security import AuthContext, audit_metadata_from_request, require_roles, validate_auth_configuration
from application.candidate_registry_service import CandidateRegistryService
from application.decision_platform_service import DecisionPlatformService
from application.serving_service import ServingDataNotFoundError, ServingOutputService
from config.settings import get_settings
from data_catalog.sources import build_default_catalog
from infrastructure.env import is_within_gold_layer, validate_prod_runtime_hardening
from infrastructure.metadata_db import MetadataDb
from infrastructure.observability import AlertThresholds, build_observability_snapshot, evaluate_and_dispatch_alerts
from infrastructure.operation_scheduler import build_default_schedule, write_schedule_manifest
from infrastructure.queue_rq import get_queue

logger = logging.getLogger(__name__)


def _record_api_operation(
    db: MetadataDb,
    *,
    tenant_id: str,
    action: str,
    resource: str,
    started_at: float,
    status: str,
    metadata: dict[str, object] | None = None,
    error_text: str | None = None,
) -> None:
    latency_ms = round((time.perf_counter() - started_at) * 1000, 3)
    event_metadata = metadata or {}
    db.record_operational_event(
        tenant_id=tenant_id,
        event_type=f"api.{action}",
        resource=resource,
        status=status,
        latency_ms=latency_ms,
        usage_count=1,
        error_text=error_text,
        metadata=event_metadata,
    )
    logger.info(
        "api_operation_recorded",
        extra={
            "tenant_id": tenant_id,
            "action": action,
            "resource": resource,
            "status": status,
            "latency_ms": latency_ms,
            "metadata": event_metadata,
        },
    )


def _metadata_db() -> MetadataDb:
    settings = get_settings()
    return MetadataDb(settings.build_paths().metadata_db_path)


def _tenant_id() -> str:
    settings = get_settings()
    paths = settings.build_paths()
    return getattr(paths, "tenant_id", getattr(settings, "tenant_id", "default"))


def _candidate_to_schema(candidate) -> CandidateProfileSchema:
    return CandidateProfileSchema(
        candidate_id=candidate.candidate_id,
        nome_politico=candidate.nome_politico,
        cargo=candidate.cargo,
        partido=candidate.partido,
        idade=candidate.idade,
        faixa_etaria=candidate.faixa_etaria,
        origem_territorial=candidate.origem_territorial,
        incumbente=candidate.incumbente,
        biografia_resumida=candidate.biografia_resumida,
        temas_prioritarios=list(candidate.temas_prioritarios),
        temas_secundarios=list(candidate.temas_secundarios),
        historico_eleitoral=list(candidate.historico_eleitoral),
        municipios_base=list(candidate.municipios_base),
        zonas_base=list(candidate.zonas_base),
        observacoes_estrategicas=candidate.observacoes_estrategicas,
    )


def _decision_service() -> DecisionPlatformService:
    return DecisionPlatformService(get_settings().build_paths())


def _candidate_registry() -> CandidateRegistryService:
    return CandidateRegistryService(get_settings().build_paths())


def _serving_service() -> ServingOutputService:
    return ServingOutputService(get_settings().build_paths())


def _serving_response(result) -> ServingOutputResponse:
    return ServingOutputResponse(
        tenant_id=result.tenant_id,
        campaign_id=result.campaign_id,
        snapshot_id=result.snapshot_id,
        output_id=result.output_id,
        row_count=result.row_count,
        records=result.records,
        warnings=result.warnings,
        source_path=str(result.path),
    )


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
    if settings.api_rate_limit_enabled:
        app_.state.rate_limiter = build_rate_limiter(settings)
    yield


app = FastAPI(title="Inteligencia Eleitoral API", version="1.0.0", lifespan=lifespan)
install_rate_limit_middleware(app)


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


@app.get("/v1/data-catalog")
def get_data_catalog(
    request: Request,
    auth: AuthContext = Depends(require_roles("admin", "operator", "viewer")),
):
    db = _metadata_db()
    db.log_audit(
        actor=auth.actor,
        role=auth.role,
        action="get_data_catalog",
        resource="data_catalog",
        metadata={**audit_metadata_from_request(request), "token_fp": auth.token_fingerprint},
        tenant_id=_tenant_id(),
    )
    return build_default_catalog().model_dump(mode="json")


@app.get("/v1/serving/manifest", response_model=ServingManifestResponse)
def get_serving_manifest(
    request: Request,
    campaign_id: str | None = None,
    snapshot_id: str | None = None,
    auth: AuthContext = Depends(require_roles("admin", "operator", "viewer")),
):
    tenant_id = _tenant_id()
    try:
        manifest = _serving_service().manifest(tenant_id=tenant_id, campaign_id=campaign_id, snapshot_id=snapshot_id)
    except ServingDataNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    _metadata_db().log_audit(
        actor=auth.actor,
        role=auth.role,
        action="get_serving_manifest",
        resource="serving_manifest",
        metadata={**audit_metadata_from_request(request), "token_fp": auth.token_fingerprint},
        tenant_id=tenant_id,
    )
    return ServingManifestResponse(tenant_id=tenant_id, manifest=manifest)


@app.get("/v1/serving/territory-ranking", response_model=ServingOutputResponse)
def get_serving_territory_ranking(
    request: Request,
    campaign_id: str | None = None,
    snapshot_id: str | None = None,
    candidate_id: str | None = None,
    limit: int = 50,
    auth: AuthContext = Depends(require_roles("admin", "operator", "viewer")),
):
    tenant_id = _tenant_id()
    try:
        result = _serving_service().territory_ranking(
            tenant_id=tenant_id,
            campaign_id=campaign_id,
            snapshot_id=snapshot_id,
            candidate_id=candidate_id,
            limit=limit,
        )
    except ServingDataNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    _metadata_db().log_audit(
        actor=auth.actor,
        role=auth.role,
        action="get_serving_territory_ranking",
        resource="serving_territory_ranking",
        metadata={**audit_metadata_from_request(request), "token_fp": auth.token_fingerprint},
        tenant_id=tenant_id,
    )
    return _serving_response(result)


@app.get("/v1/serving/allocation-recommendations", response_model=ServingOutputResponse)
def get_serving_allocation_recommendations(
    request: Request,
    campaign_id: str | None = None,
    snapshot_id: str | None = None,
    candidate_id: str | None = None,
    scenario_id: str | None = None,
    limit: int = 50,
    auth: AuthContext = Depends(require_roles("admin", "operator", "viewer")),
):
    tenant_id = _tenant_id()
    try:
        result = _serving_service().allocation_recommendations(
            tenant_id=tenant_id,
            campaign_id=campaign_id,
            snapshot_id=snapshot_id,
            candidate_id=candidate_id,
            scenario_id=scenario_id,
            limit=limit,
        )
    except ServingDataNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    _metadata_db().log_audit(
        actor=auth.actor,
        role=auth.role,
        action="get_serving_allocation_recommendations",
        resource="serving_allocation_recommendations",
        metadata={**audit_metadata_from_request(request), "token_fp": auth.token_fingerprint},
        tenant_id=tenant_id,
    )
    return _serving_response(result)


@app.get("/v1/serving/data-readiness", response_model=ServingOutputResponse)
def get_serving_data_readiness(
    request: Request,
    campaign_id: str | None = None,
    snapshot_id: str | None = None,
    auth: AuthContext = Depends(require_roles("admin", "operator", "viewer")),
):
    tenant_id = _tenant_id()
    try:
        result = _serving_service().data_readiness(
            tenant_id=tenant_id,
            campaign_id=campaign_id,
            snapshot_id=snapshot_id,
        )
    except ServingDataNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    _metadata_db().log_audit(
        actor=auth.actor,
        role=auth.role,
        action="get_serving_data_readiness",
        resource="serving_data_readiness",
        metadata={**audit_metadata_from_request(request), "token_fp": auth.token_fingerprint},
        tenant_id=tenant_id,
    )
    return _serving_response(result)


@app.post("/v1/decision/allocation-scenario", response_model=AllocationScenarioResponse)
def create_allocation_scenario(
    req: AllocationScenarioRequest,
    request: Request,
    auth: AuthContext = Depends(require_roles("admin", "operator")),
):
    started_at = time.perf_counter()
    settings = get_settings()
    tenant_id = _tenant_id()
    db = _metadata_db()
    db.log_audit(
        actor=auth.actor,
        role=auth.role,
        action="create_allocation_scenario",
        resource=req.candidate.candidate_id,
        metadata={**audit_metadata_from_request(request), "token_fp": auth.token_fingerprint, "scenario": req.scenario},
        tenant_id=tenant_id,
    )
    service = DecisionPlatformService(settings.build_paths())
    try:
        response = service.generate_allocation_scenario(req)
    except Exception as exc:
        _record_api_operation(
            db,
            tenant_id=tenant_id,
            action="create_allocation_scenario",
            resource=req.candidate.candidate_id,
            started_at=started_at,
            status="failed",
            metadata={"scenario": req.scenario, "top_n": req.top_n},
            error_text=str(exc),
        )
        raise
    _record_api_operation(
        db,
        tenant_id=tenant_id,
        action="create_allocation_scenario",
        resource=req.candidate.candidate_id,
        started_at=started_at,
        status="ok",
        metadata={
            "scenario": req.scenario,
            "top_n": req.top_n,
            "recommendations": len(response.recommendations),
            "evidence_count": response.evidence_count,
        },
    )
    return response


@app.put("/v1/candidates/{candidate_id}", response_model=CandidateUpsertResponse)
def upsert_candidate(
    candidate_id: str,
    req: CandidateProfileSchema,
    request: Request,
    auth: AuthContext = Depends(require_roles("admin", "operator")),
):
    if req.candidate_id != candidate_id:
        raise HTTPException(status_code=400, detail="candidate_id do path difere do payload")
    tenant_id = _tenant_id()
    candidate = _candidate_registry().upsert(req)
    _metadata_db().log_audit(
        actor=auth.actor,
        role=auth.role,
        action="upsert_candidate",
        resource=candidate_id,
        metadata={**audit_metadata_from_request(request), "token_fp": auth.token_fingerprint},
        tenant_id=tenant_id,
    )
    return CandidateUpsertResponse(candidate=_candidate_to_schema(candidate), tenant_id=tenant_id)


@app.get("/v1/candidates", response_model=CandidateListResponse)
def list_candidates(
    request: Request,
    auth: AuthContext = Depends(require_roles("admin", "operator", "viewer")),
):
    tenant_id = _tenant_id()
    _metadata_db().log_audit(
        actor=auth.actor,
        role=auth.role,
        action="list_candidates",
        resource="candidate_registry",
        metadata={**audit_metadata_from_request(request), "token_fp": auth.token_fingerprint},
        tenant_id=tenant_id,
    )
    return CandidateListResponse(
        items=[_candidate_to_schema(candidate) for candidate in _candidate_registry().list()], tenant_id=tenant_id
    )


@app.get("/v1/decision/territories/prioritized", response_model=PrioritizedTerritoriesResponse)
def list_prioritized_territories(
    candidate_id: str,
    request: Request,
    budget_total: float = 200000.0,
    capacidade_operacional: float = 0.7,
    janela_temporal_dias: int = 45,
    top_n: int = 20,
    scenario: str = "hibrido",
    auth: AuthContext = Depends(require_roles("admin", "operator", "viewer")),
):
    started_at = time.perf_counter()
    tenant_id = _tenant_id()
    db = _metadata_db()
    try:
        response = _decision_service().list_prioritized_territories(
            candidate_id=candidate_id,
            tenant_id=tenant_id,
            budget_total=budget_total,
            capacidade_operacional=capacidade_operacional,
            janela_temporal_dias=janela_temporal_dias,
            top_n=top_n,
            scenario=scenario,
        )
    except ValueError as exc:
        _record_api_operation(
            db,
            tenant_id=tenant_id,
            action="list_prioritized_territories",
            resource=candidate_id,
            started_at=started_at,
            status="failed",
            metadata={"scenario": scenario, "top_n": top_n},
            error_text=str(exc),
        )
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    db.log_audit(
        actor=auth.actor,
        role=auth.role,
        action="list_prioritized_territories",
        resource=candidate_id,
        metadata={**audit_metadata_from_request(request), "token_fp": auth.token_fingerprint, "scenario": scenario},
        tenant_id=tenant_id,
    )
    _record_api_operation(
        db,
        tenant_id=tenant_id,
        action="list_prioritized_territories",
        resource=candidate_id,
        started_at=started_at,
        status="ok",
        metadata={"scenario": scenario, "top_n": top_n, "items": len(response.items)},
    )
    return response


@app.get("/v1/decision/territories/{territorio_id}/score", response_model=TerritoryScoreResponse)
def get_territory_score(
    territorio_id: str,
    candidate_id: str,
    request: Request,
    scenario: str = "hibrido",
    auth: AuthContext = Depends(require_roles("admin", "operator", "viewer")),
):
    tenant_id = _tenant_id()
    try:
        response = _decision_service().get_territory_score(
            candidate_id=candidate_id,
            territorio_id=territorio_id,
            tenant_id=tenant_id,
            scenario=scenario,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    _metadata_db().log_audit(
        actor=auth.actor,
        role=auth.role,
        action="get_territory_score",
        resource=territorio_id,
        metadata={
            **audit_metadata_from_request(request),
            "token_fp": auth.token_fingerprint,
            "candidate_id": candidate_id,
        },
        tenant_id=tenant_id,
    )
    return response


@app.get("/v1/decision/recommendations/{territorio_id}/explanation", response_model=RecommendationExplanationResponse)
def get_recommendation_explanation(
    territorio_id: str,
    candidate_id: str,
    request: Request,
    scenario: str = "hibrido",
    auth: AuthContext = Depends(require_roles("admin", "operator", "viewer")),
):
    tenant_id = _tenant_id()
    try:
        response = _decision_service().get_recommendation_explanation(
            candidate_id=candidate_id,
            territorio_id=territorio_id,
            tenant_id=tenant_id,
            scenario=scenario,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    _metadata_db().log_audit(
        actor=auth.actor,
        role=auth.role,
        action="get_recommendation_explanation",
        resource=territorio_id,
        metadata={
            **audit_metadata_from_request(request),
            "token_fp": auth.token_fingerprint,
            "candidate_id": candidate_id,
        },
        tenant_id=tenant_id,
    )
    return response


@app.post("/v1/pipelines/ingestions", response_model=JobQueuedResponse)
def enqueue_pipeline_ingestion(
    req: IngestionRequest,
    request: Request,
    auth: AuthContext = Depends(require_roles("admin", "operator")),
):
    return enqueue_ingest(req=req, request=request, auth=auth)


@app.get("/v1/pipelines/status/{job_id}", response_model=JobRecordResponse)
def get_pipeline_status(
    job_id: str,
    request: Request,
    auth: AuthContext = Depends(require_roles("admin", "operator", "viewer")),
):
    return get_job(job_id=job_id, request=request, auth=auth)
