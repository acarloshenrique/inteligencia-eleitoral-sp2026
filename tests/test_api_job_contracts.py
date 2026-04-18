import tempfile
from pathlib import Path

import pytest

from api.contracts import ExportRequest, IngestionRequest, ReindexRequest
from domain.job_contracts import ExportJobPayload, IngestionJobPayload, ReindexJobPayload, ReindexJobResult


def test_job_payload_contracts_validate_required_fields():
    reindex = ReindexJobPayload(input_path="/tmp/gold.parquet", tenant_id="cliente-a")
    export = ExportJobPayload(input_path="/tmp/gold.parquet", tenant_id="cliente-a", anonymize=True)
    ingest = IngestionJobPayload(source_catalog_path="/tmp/catalog.json", pipeline="medallion", tenant_id="cliente-a")

    assert reindex.collection_name == "municipios_v2"
    assert export.minimize is True
    assert ingest.pipeline == "medallion"


def test_job_payload_contracts_reject_ambiguous_extra_fields():
    with pytest.raises(ValueError):
        ReindexJobPayload(input_path="/tmp/gold.parquet", tenant_id="cliente-a", sql="drop table municipios")


@pytest.mark.integration
def test_api_reindex_response_and_enqueued_payload_are_typed(monkeypatch):
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient

    from api.main import app

    with tempfile.TemporaryDirectory() as tmp:
        input_path = Path(tmp) / "lake" / "gold" / "df.parquet"
        input_path.parent.mkdir(parents=True, exist_ok=True)
        input_path.write_bytes(b"x")
        queued: dict[str, object] = {}

        class _Queue:
            def enqueue(self, func, task_job_id, payload, **kwargs):
                queued["func"] = func
                queued["job_id"] = task_job_id
                queued["payload"] = payload
                queued["kwargs"] = kwargs

        class _Db:
            def create_job(self, *args, **kwargs):
                queued["db_create"] = {"args": args, "kwargs": kwargs}

            def log_audit(self, *args, **kwargs):
                return None

        class _Settings:
            redis_url = "redis://test"
            rq_queue_name = "jobs"
            tenant_id = "cliente-a"

            def build_paths(self):
                root = Path(tmp) / "lake" / "gold"
                return type(
                    "_Paths",
                    (),
                    {
                        "tenant_id": "cliente-a",
                        "gold_root": root,
                        "gold_reports_root": root / "reports",
                        "gold_serving_root": root / "serving",
                        "catalog_root": root / "_catalog",
                        "metadata_db_path": Path(tmp) / "metadata" / "jobs.sqlite3",
                    },
                )()

        monkeypatch.setattr("api.main._metadata_db", lambda: _Db())
        monkeypatch.setattr("api.main.get_queue", lambda *_: _Queue())
        monkeypatch.setattr("api.main.get_settings", lambda: _Settings())

        client = TestClient(app)
        res = client.post(
            "/v1/jobs/reindex",
            json={"input_path": str(input_path), "collection_name": "municipios_v2", "force": True},
            headers={"Authorization": "Bearer dev-admin-token"},
        )

    body = res.json()
    assert res.status_code == 200
    assert body["status"] == "queued"
    assert body["job_type"] == "reindex"
    assert body["tenant_id"] == "cliente-a"
    payload = ReindexJobPayload.model_validate(queued["payload"])
    assert payload.force is True
    assert payload.tenant_id == "cliente-a"


def test_api_request_contracts_reject_extra_fields():
    for model in (ReindexRequest, ExportRequest, IngestionRequest):
        with pytest.raises(ValueError):
            model.model_validate({"input_path": "/tmp/a.parquet", "unexpected": "x"})


def test_reindex_result_contract_keeps_provider_metadata():
    result = ReindexJobResult.model_validate(
        {
            "status": "indexed",
            "collection_name": "municipios_v2",
            "indexed_count": 10,
            "state_path": "/tmp/state.json",
            "provider_latency_ms": 12.5,
        }
    )

    assert result.model_dump()["provider_latency_ms"] == 12.5
