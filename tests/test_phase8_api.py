from __future__ import annotations

from pathlib import Path

import pandas as pd
from fastapi.testclient import TestClient

from config.settings import AppPaths


class _Settings:
    app_env = "dev"
    redis_url = "redis://test"
    rq_queue_name = "jobs"
    tenant_id = "cliente-a"
    ingestion_source_catalog_path = "catalog.json"
    api_rate_limit_enabled = False

    def __init__(self, root: Path):
        self.root = root

    def build_paths(self):
        lake = self.root / "data_lake"
        for folder in [
            self.root / "ingestion",
            lake / "bronze",
            lake / "silver",
            lake / "gold",
            lake / "gold" / "reports",
            lake / "gold" / "serving",
            lake / "catalog",
            self.root / "chromadb",
            self.root / "metadata",
            self.root / "artifacts",
        ]:
            folder.mkdir(parents=True, exist_ok=True)
        return AppPaths(
            data_root=self.root,
            ingestion_root=self.root / "ingestion",
            lake_root=lake,
            bronze_root=lake / "bronze",
            silver_root=lake / "silver",
            gold_root=lake / "gold",
            gold_reports_root=lake / "gold" / "reports",
            gold_serving_root=lake / "gold" / "serving",
            catalog_root=lake / "catalog",
            chromadb_path=self.root / "chromadb",
            runtime_reports_root=self.root / "runtime_reports",
            ts="20260417_000000",
            metadata_db_path=self.root / "metadata" / "jobs.sqlite3",
            artifact_root=self.root / "artifacts",
            tenant_id="cliente-a",
            tenant_root=self.root,
        )


def _client(monkeypatch, tmp_path: Path):
    from api import main

    settings = _Settings(tmp_path)
    monkeypatch.setattr(main, "get_settings", lambda: settings)
    return TestClient(main.app), settings


def _seed_gold(settings: _Settings):
    paths = settings.build_paths()
    pd.DataFrame(
        [
            {
                "territorio_id": "2024:SP:71072:ZE1",
                "municipio": "SAO PAULO",
                "cod_tse_municipio": "71072",
                "zona_eleitoral": 1,
                "eleitores_aptos": 10000,
                "votos_validos": 7000,
                "abstencao_pct": 0.25,
                "competitividade": 0.8,
                "data_quality_score": 0.94,
                "join_confidence": 0.96,
                "source_name": "fact_zona_eleitoral",
                "ingestion_run_id": "run_api",
                "lake_layer": "gold",
            },
            {
                "territorio_id": "2024:SP:71072:ZE2",
                "municipio": "SAO PAULO",
                "cod_tse_municipio": "71072",
                "zona_eleitoral": 2,
                "eleitores_aptos": 20000,
                "votos_validos": 9000,
                "abstencao_pct": 0.35,
                "competitividade": 0.6,
                "data_quality_score": 0.94,
                "join_confidence": 0.96,
                "source_name": "fact_zona_eleitoral",
                "ingestion_run_id": "run_api",
                "lake_layer": "gold",
            },
        ]
    ).to_parquet(paths.gold_root / "fact_zona_eleitoral_20260417_000000.parquet", index=False)


def _candidate_payload():
    return {
        "candidate_id": "cand-api",
        "nome_politico": "Candidato API",
        "cargo": "Prefeito",
        "partido": "P",
        "temas_prioritarios": ["saude"],
        "municipios_base": ["SAO PAULO"],
    }


def test_phase8_candidate_prioritized_score_and_explanation_endpoints(monkeypatch, tmp_path):
    client, settings = _client(monkeypatch, tmp_path)
    _seed_gold(settings)
    headers = {"Authorization": "Bearer dev-admin-token"}

    upsert = client.put("/v1/candidates/cand-api", json=_candidate_payload(), headers=headers)
    assert upsert.status_code == 200
    assert upsert.json()["status"] == "saved"

    listed = client.get("/v1/candidates", headers=headers)
    assert listed.status_code == 200
    assert listed.json()["items"][0]["candidate_id"] == "cand-api"

    prioritized = client.get(
        "/v1/decision/territories/prioritized",
        params={"candidate_id": "cand-api", "top_n": 2, "budget_total": 100000},
        headers=headers,
    )
    assert prioritized.status_code == 200
    body = prioritized.json()
    assert body["tenant_id"] == "cliente-a"
    assert len(body["items"]) == 2
    territorio_id = body["items"][0]["territorio_id"]

    score = client.get(
        f"/v1/decision/territories/{territorio_id}/score",
        params={"candidate_id": "cand-api"},
        headers=headers,
    )
    assert score.status_code == 200
    assert score.json()["score"]["score_prioridade_final"] >= 0

    explanation = client.get(
        f"/v1/decision/recommendations/{territorio_id}/explanation",
        params={"candidate_id": "cand-api"},
        headers=headers,
    )
    assert explanation.status_code == 200
    exp = explanation.json()
    assert exp["supporting_bases"]
    assert exp["confidence_score"] > 0
    assert "Por que priorizar" in exp["detailed_justification"]
    assert exp["provenance"]["ingestion_run_id"] == "run_api"


def test_phase8_allocation_and_catalog_endpoints_are_typed(monkeypatch, tmp_path):
    client, settings = _client(monkeypatch, tmp_path)
    _seed_gold(settings)
    headers = {"Authorization": "Bearer dev-admin-token"}

    allocation = client.post(
        "/v1/decision/allocation-scenario",
        json={"candidate": _candidate_payload(), "budget_total": 100000, "top_n": 2, "scenario": "hibrido"},
        headers=headers,
    )
    assert allocation.status_code == 200
    assert allocation.json()["recommendations"]
    assert allocation.json()["evidence_count"] >= 2

    catalog = client.get("/v1/data-catalog", headers=headers)
    assert catalog.status_code == 200
    assert catalog.json()["version"] == "decision_catalog_v1"
    assert catalog.json()["sources"]


def test_phase8_pipeline_ingestion_alias_and_status(monkeypatch, tmp_path):
    client, _settings = _client(monkeypatch, tmp_path)
    headers = {"Authorization": "Bearer dev-admin-token"}
    queued: dict[str, object] = {}

    class _Queue:
        def enqueue(self, func, task_job_id, payload, **kwargs):
            queued["func"] = func
            queued["job_id"] = task_job_id
            queued["payload"] = payload
            queued["kwargs"] = kwargs

    from api import main

    monkeypatch.setattr(main, "get_queue", lambda *_: _Queue())

    res = client.post(
        "/v1/pipelines/ingestions",
        json={"source_catalog_path": "catalog.json", "pipeline": "layered"},
        headers=headers,
    )
    assert res.status_code == 200
    job_id = res.json()["job_id"]
    assert res.json()["job_type"] == "ingest"
    assert queued["payload"]["pipeline"] == "layered"

    status = client.get(f"/v1/pipelines/status/{job_id}", headers=headers)
    assert status.status_code == 200
    assert status.json()["id"] == job_id
    assert status.json()["status"] == "queued"


def test_phase8_rejects_candidate_path_payload_mismatch(monkeypatch, tmp_path):
    client, _settings = _client(monkeypatch, tmp_path)
    res = client.put(
        "/v1/candidates/a",
        json={**_candidate_payload(), "candidate_id": "b"},
        headers={"Authorization": "Bearer dev-admin-token"},
    )
    assert res.status_code == 400
    assert "candidate_id" in res.json()["detail"]


def test_phase8_requires_authentication(monkeypatch, tmp_path):
    client, _settings = _client(monkeypatch, tmp_path)
    res = client.get("/v1/candidates")
    assert res.status_code == 401
