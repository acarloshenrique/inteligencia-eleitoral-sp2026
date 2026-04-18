import json
from pathlib import Path

from config.settings import AppPaths
from infrastructure.metadata_db import MetadataDb
from infrastructure.privacy import redact_for_log, redact_text, sql_references_sensitive_columns
from infrastructure.rag_metrics import RagMetricsTracker
from infrastructure.sql_safety import CHAT_QUERY_TEMPLATES, validate_chat_query_templates_lgpd

CPF = "123.456.789-00"
TITULO = "123456789012"
EMAIL = "eleitor@example.com"
PHONE = "(11) 99999-8888"


def _paths(tmp_path: Path) -> AppPaths:
    lake_root = tmp_path / "lake"
    gold_root = lake_root / "gold"
    return AppPaths(
        data_root=tmp_path,
        ingestion_root=tmp_path / "ingestion",
        lake_root=lake_root,
        bronze_root=lake_root / "bronze",
        silver_root=lake_root / "silver",
        gold_root=gold_root,
        gold_reports_root=gold_root / "reports",
        gold_serving_root=gold_root / "serving",
        catalog_root=lake_root / "catalog",
        chromadb_path=tmp_path / "chromadb",
        runtime_reports_root=tmp_path / "runtime_reports",
        ts="20260414_000000",
        metadata_db_path=tmp_path / "metadata" / "jobs.sqlite3",
        artifact_root=tmp_path / "artifacts",
    )


def test_redact_for_log_masks_personal_identifiers_and_sensitive_fields():
    payload = {
        "cpf": CPF,
        "titulo_eleitor": TITULO,
        "nested": {"message": f"contato {EMAIL} telefone {PHONE}"},
    }

    redacted = redact_for_log(payload)
    rendered = json.dumps(redacted, ensure_ascii=False)

    assert CPF not in rendered
    assert TITULO not in rendered
    assert EMAIL not in rendered
    assert PHONE not in rendered
    assert "[REDACTED]" in rendered
    assert "[REDACTED_EMAIL]" in rendered


def test_rag_metrics_never_persist_raw_personal_data(tmp_path):
    tracker = RagMetricsTracker(paths=_paths(tmp_path))

    tracker.record_query(
        question=f"Perfil do eleitor CPF {CPF}, titulo {TITULO}, email {EMAIL}",
        retrieved_municipios=["Cidade A"],
        latency_total_ms=10.0,
        latency_vector_ms=2.0,
        latency_llm_ms=8.0,
        fallback_vector=False,
        fallback_llm=False,
        tokens_total=1,
        cost_estimated_usd=0.0,
        cached_vector=False,
        cached_llm=False,
    )

    events_path = tmp_path / "outputs" / "metrics" / "rag_metrics_events.jsonl"
    raw = events_path.read_text(encoding="utf-8")
    event = json.loads(raw.splitlines()[0])

    assert CPF not in raw
    assert TITULO not in raw
    assert EMAIL not in raw
    assert event["question_redacted"] is True


def test_metadata_db_redacts_operational_logs_and_errors(tmp_path):
    db = MetadataDb(tmp_path / "metadata" / "jobs.sqlite3")
    db.create_job("job-1", "reindex", {"cpf": CPF, "safe": "ok"})
    db.set_error("job-1", f"falha para cpf {CPF} e titulo {TITULO}")
    db.record_operational_event(
        tenant_id="cliente-a",
        event_type="job.test",
        resource="job-1",
        status="failed",
        error_text=f"email {EMAIL}",
        metadata={"telefone": PHONE, "note": f"titulo {TITULO}"},
    )

    job = db.get_job("job-1")
    ops = db.list_operational_events(tenant_id="cliente-a")
    rendered = json.dumps({"job": job, "ops": ops}, ensure_ascii=False)

    assert CPF not in rendered
    assert TITULO not in rendered
    assert EMAIL not in rendered
    assert PHONE not in rendered
    assert "[REDACTED" in rendered


def test_metadata_db_redacts_alert_messages_and_channel_errors(tmp_path):
    db = MetadataDb(tmp_path / "metadata" / "jobs.sqlite3")
    alert_id = db.record_alert(
        tenant_id="cliente-a",
        severity="high",
        metric="error_rate",
        value=1.0,
        threshold=0.1,
        message=f"falha do eleitor {CPF}",
        channels=["webhook"],
        error_text=f"telefone {PHONE}",
        metadata={"email": EMAIL},
    )
    db.set_alert_status(alert_id, status="failed", channels=["slack"], error_text=f"titulo {TITULO}")

    rendered = json.dumps(db.list_alerts(tenant_id="cliente-a"), ensure_ascii=False)

    assert CPF not in rendered
    assert TITULO not in rendered
    assert EMAIL not in rendered
    assert PHONE not in rendered
    assert "slack" in rendered


def test_chat_sql_templates_do_not_reference_personal_voter_columns():
    result = validate_chat_query_templates_lgpd()

    assert result
    assert all(result.values())
    for sql in CHAT_QUERY_TEMPLATES.values():
        assert not sql_references_sensitive_columns(sql)
        assert "cpf" not in sql.lower()
        assert "titulo" not in sql.lower()


def test_redact_text_is_safe_for_exception_messages():
    message = redact_text(f"erro consultando eleitor {CPF} telefone {PHONE}")

    assert CPF not in message
    assert PHONE not in message
    assert "[REDACTED_CPF]" in message
