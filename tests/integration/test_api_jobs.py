from pathlib import Path
import tempfile

import pytest


@pytest.mark.integration
def test_api_enqueue_reindex_job(monkeypatch):
    fastapi = pytest.importorskip("fastapi")
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
    fastapi = pytest.importorskip("fastapi")
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
