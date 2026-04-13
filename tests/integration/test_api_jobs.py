from pathlib import Path
import tempfile

import pytest


@pytest.mark.integration
def test_api_enqueue_reindex_job(monkeypatch):
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient

    from api.main import app

    with tempfile.TemporaryDirectory() as tmp:
        monkeypatch.setenv("DATA_ROOT", tmp)
        p = Path(tmp) / "lake" / "gold" / "df.parquet"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b"x")

        class _Queue:
            def enqueue(self, *args, **kwargs):
                return None

        class _Db:
            def create_job(self, *args, **kwargs):
                return None

            def get_job(self, *args, **kwargs):
                return {"id": "1", "status": "queued"}

            def log_audit(self, *args, **kwargs):
                return None

        monkeypatch.setattr("api.main._metadata_db", lambda: _Db())
        monkeypatch.setattr("api.main.get_queue", lambda *_: _Queue())
        monkeypatch.setattr(
            "api.main.get_settings",
            lambda: type(
                "_Settings",
                (),
                {
                    "redis_url": "redis://test",
                    "rq_queue_name": "jobs",
                    "build_paths": lambda self: type(
                        "_Paths",
                        (),
                        {
                            "gold_root": Path(tmp) / "lake" / "gold",
                            "gold_reports_root": Path(tmp) / "lake" / "gold" / "reports",
                            "gold_serving_root": Path(tmp) / "lake" / "gold" / "serving",
                            "catalog_root": Path(tmp) / "lake" / "gold" / "_catalog",
                            "metadata_db_path": Path(tmp) / "metadata" / "jobs.sqlite3",
                        },
                    )(),
                },
            )(),
        )

        client = TestClient(app)
        res = client.post(
            "/v1/jobs/reindex",
            json={"input_path": str(p), "collection_name": "municipios_v2", "force": False},
            headers={"Authorization": "Bearer dev-admin-token"},
        )
        assert res.status_code == 200
        assert res.json()["status"] == "queued"


@pytest.mark.integration
def test_api_enqueue_ingestion_job(monkeypatch):
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient

    from api.main import app

    with tempfile.TemporaryDirectory() as tmp:
        catalog = Path(tmp) / "catalog.json"
        catalog.write_text("{}", encoding="utf-8")

        class _Queue:
            def enqueue(self, *args, **kwargs):
                return None

        class _Db:
            def create_job(self, *args, **kwargs):
                return None

            def get_job(self, *args, **kwargs):
                return {"id": "1", "status": "queued"}

            def log_audit(self, *args, **kwargs):
                return None

        monkeypatch.setattr("api.main._metadata_db", lambda: _Db())
        monkeypatch.setattr("api.main.get_queue", lambda *_: _Queue())
        monkeypatch.setattr(
            "api.main.get_settings",
            lambda: type(
                "_Settings",
                (),
                {
                    "redis_url": "redis://test",
                    "rq_queue_name": "jobs",
                    "ingestion_source_catalog_path": None,
                    "build_paths": lambda self: type(
                        "_Paths",
                        (),
                        {
                            "gold_root": Path(tmp) / "lake" / "gold",
                            "gold_reports_root": Path(tmp) / "lake" / "gold" / "reports",
                            "gold_serving_root": Path(tmp) / "lake" / "gold" / "serving",
                            "catalog_root": Path(tmp) / "lake" / "gold" / "_catalog",
                            "metadata_db_path": Path(tmp) / "metadata" / "jobs.sqlite3",
                        },
                    )(),
                },
            )(),
        )

        client = TestClient(app)
        res = client.post(
            "/v1/jobs/ingest",
            json={"source_catalog_path": str(catalog), "pipeline": "open_data", "pipeline_version": "auto_v1"},
            headers={"Authorization": "Bearer dev-admin-token"},
        )
        assert res.status_code == 200
        assert res.json()["status"] == "queued"


@pytest.mark.integration
def test_api_ops_observability_and_schedule(monkeypatch):
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient

    from api.main import app

    with tempfile.TemporaryDirectory() as tmp:
        class _Db:
            def log_audit(self, *args, **kwargs):
                return None

            def summarize_operations(self, *, tenant_id=None, limit=500):
                return {
                    "tenant_id": tenant_id,
                    "events_total": 1,
                    "jobs_total": 1,
                    "errors_total": 0,
                    "error_rate": 0.0,
                    "latency_p95_ms": 10.0,
                    "cost_total_usd": 0.5,
                    "usage_total": 2,
                    "recent_errors": [],
                }

        class _Settings:
            redis_url = "redis://test"
            rq_queue_name = "jobs"
            tenant_id = "cliente-a"
            ops_daily_ingestion_hour = 5
            ops_weekly_update_day = "MON"
            ops_weekly_update_hour = 6
            ops_alert_error_rate_threshold = 0.1
            ops_alert_latency_p95_ms = 1000.0
            ops_alert_daily_cost_usd = 10.0

            def build_paths(self):
                root = Path(tmp) / "data" / "tenants" / "cliente-a"
                catalog = root / "data_lake" / "catalog"
                catalog.mkdir(parents=True, exist_ok=True)
                return type(
                    "_Paths",
                    (),
                    {
                        "tenant_id": "cliente-a",
                        "tenant_root": root,
                        "catalog_root": catalog,
                        "gold_root": root / "data_lake" / "gold",
                        "gold_reports_root": root / "data_lake" / "gold" / "reports",
                        "gold_serving_root": root / "data_lake" / "gold" / "serving",
                        "metadata_db_path": root / "metadata" / "jobs.sqlite3",
                    },
                )()

        monkeypatch.setattr("api.main._metadata_db", lambda: _Db())
        monkeypatch.setattr("api.main.get_settings", lambda: _Settings())

        client = TestClient(app)
        obs = client.get("/v1/ops/observability", headers={"Authorization": "Bearer dev-admin-token"})
        assert obs.status_code == 200
        assert obs.json()["tenant_id"] == "cliente-a"
        assert obs.json()["summary"]["events_total"] == 1

        sched = client.post("/v1/ops/schedule", json={}, headers={"Authorization": "Bearer dev-admin-token"})
        assert sched.status_code == 200
        body = sched.json()
        assert body["tenant_id"] == "cliente-a"
        assert set(body["pipelines"]) == {"ingestao_diaria", "atualizacao_semanal_gold", "alertas_operacionais"}
