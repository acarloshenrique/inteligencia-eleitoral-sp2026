from pathlib import Path

import pytest

from config.settings import Settings
from infrastructure.metadata_db import MetadataDb
from infrastructure.notifiers import NotificationResult
from infrastructure.observability import (
    AlertThresholds,
    OperationObserver,
    build_observability_snapshot,
    evaluate_and_dispatch_alerts,
)
from infrastructure.operation_scheduler import build_default_schedule, load_schedule_manifest, write_schedule_manifest
from infrastructure.tenancy import ensure_tenant_path, normalize_tenant_id


def test_tenant_default_preserves_existing_layout(tmp_path):
    paths = Settings(DATA_ROOT=str(tmp_path / "data"), TENANT_ID="default").build_paths()
    assert paths.tenant_id == "default"
    assert paths.tenant_root == (tmp_path / "data").resolve()
    assert paths.lake_root == (tmp_path / "data" / "data_lake").resolve()


def test_tenant_custom_isolates_lake_metadata_and_artifacts(tmp_path):
    paths = Settings(DATA_ROOT=str(tmp_path / "data"), TENANT_ID="cliente-sp").build_paths()
    tenant_root = (tmp_path / "data" / "tenants" / "cliente-sp").resolve()
    assert paths.tenant_id == "cliente-sp"
    assert paths.tenant_root == tenant_root
    assert paths.lake_root == tenant_root / "data_lake"
    assert paths.metadata_db_path == tenant_root / "metadata" / "jobs.sqlite3"
    assert paths.artifact_root == tenant_root / "artifacts"


def test_tenant_validation_blocks_path_escape(tmp_path):
    tenant_root = tmp_path / "data" / "tenants" / "cliente-a"
    tenant_root.mkdir(parents=True)
    assert normalize_tenant_id("Cliente A") == "cliente-a"
    assert ensure_tenant_path(tenant_root, tenant_root / "data_lake") == (tenant_root / "data_lake").resolve()
    with pytest.raises(ValueError):
        ensure_tenant_path(tenant_root, tmp_path / "outro")


def test_metadata_db_records_observability_and_alerts(tmp_path):
    db = MetadataDb(tmp_path / "metadata" / "jobs.sqlite3")
    db.create_job("job-1", "ingest", {"x": 1}, tenant_id="cliente-a")
    db.record_operational_event(
        tenant_id="cliente-a",
        event_type="job.ingest",
        resource="job-1",
        status="failed",
        latency_ms=40000,
        cost_usd=60,
        error_text="falha externa",
    )
    snapshot = build_observability_snapshot(
        db,
        tenant_id="cliente-a",
        thresholds=AlertThresholds(error_rate=0.01, latency_p95_ms=1000, daily_cost_usd=1),
    )
    assert snapshot["summary"]["errors_total"] == 1
    assert {alert["metric"] for alert in snapshot["alerts"]} == {"error_rate", "latency_p95_ms", "cost_total_usd"}


def test_operation_observer_records_success_and_failure(tmp_path):
    db = MetadataDb(tmp_path / "metadata" / "jobs.sqlite3")
    observer = OperationObserver(db, tenant_id="cliente-a")
    with observer.track(event_type="job.test", resource="ok") as span:
        span["usage_count"] = 3
        span["cost_usd"] = 0.5
    with pytest.raises(RuntimeError):
        with observer.track(event_type="job.test", resource="bad"):
            raise RuntimeError("boom")
    summary = db.summarize_operations(tenant_id="cliente-a")
    assert summary["events_total"] == 2
    assert summary["errors_total"] == 1
    assert summary["usage_total"] == 4


def test_schedule_manifest_contains_daily_weekly_and_alerts(tmp_path):
    paths = Settings(DATA_ROOT=str(tmp_path / "data"), TENANT_ID="cliente-a").build_paths()
    schedules = build_default_schedule(tenant_id=paths.tenant_id, daily_hour=5, weekly_day="MON", weekly_hour=6)
    manifest = write_schedule_manifest(paths, schedules)
    payload = load_schedule_manifest(manifest)
    names = {item["name"] for item in payload["pipelines"]}
    assert payload["tenant_id"] == "cliente-a"
    assert names == {"ingestao_diaria", "atualizacao_semanal_gold", "alertas_operacionais"}


def test_alert_evaluation_persists_and_sends(tmp_path):
    db = MetadataDb(tmp_path / "metadata" / "jobs.sqlite3")
    db.record_operational_event(
        tenant_id="cliente-a",
        event_type="job.ingest",
        resource="ingest-1",
        status="failed",
        latency_ms=5000,
        cost_usd=12,
        error_text="fonte indisponivel",
    )
    sent: list[dict] = []

    def sender(alert):
        sent.append(alert)
        return [NotificationResult(channel="webhook", ok=True, detail="sent")]

    alerts = evaluate_and_dispatch_alerts(
        db,
        tenant_id="cliente-a",
        thresholds=AlertThresholds(error_rate=0.01, latency_p95_ms=1000, daily_cost_usd=1),
        settings=object(),
        sender=sender,
    )

    persisted = db.list_alerts(tenant_id="cliente-a")
    assert len(sent) == 3
    assert len(alerts) == 3
    assert {item["status"] for item in persisted} == {"sent"}
    assert all(item["channels"] == ["webhook"] for item in persisted)


def test_metadata_db_lists_jobs_events_and_alerts(tmp_path):
    db = MetadataDb(tmp_path / "metadata" / "jobs.sqlite3")
    db.create_job("job-1", "ingest", {"x": 1}, tenant_id="cliente-a")
    db.set_error("job-1", "boom")
    db.record_operational_event(
        tenant_id="cliente-a", event_type="job.ingest", resource="job-1", status="failed", error_text="boom"
    )
    alert_id = db.record_alert(
        tenant_id="cliente-a", severity="high", metric="error_rate", value=1, threshold=0.1, message="erro"
    )
    db.set_alert_status(alert_id, status="sent", channels=["webhook"])

    assert db.list_jobs(tenant_id="cliente-a", limit=1)[0]["status"] == "failed"
    assert db.list_operational_events(tenant_id="cliente-a", limit=1)[0]["event_type"] == "job.ingest"
    assert db.list_alerts(tenant_id="cliente-a", limit=1)[0]["channels"] == ["webhook"]
