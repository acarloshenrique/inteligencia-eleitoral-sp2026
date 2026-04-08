from pathlib import Path
import tempfile

import pytest


@pytest.mark.integration
def test_api_enqueue_reindex_job(monkeypatch):
    fastapi = pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient

    from api.main import app

    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp) / "df.parquet"
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

        client = TestClient(app)
        res = client.post(
            "/v1/jobs/reindex",
            json={"input_path": str(p), "collection_name": "municipios_v2", "force": False},
            headers={"Authorization": "Bearer dev-admin-token"},
        )
        assert res.status_code == 200
        assert res.json()["status"] == "queued"
