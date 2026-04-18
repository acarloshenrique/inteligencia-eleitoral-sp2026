from __future__ import annotations

import json
import logging
from pathlib import Path

import pandas as pd
import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from allocation.recommendation_engine import RecommendationEngine
from api.decision_contracts import AllocationRecommendationSchema, CandidateProfileSchema
from application.candidate_context_service import CandidateContextService
from application.decision_mappers import allocation_recommendation_to_schema
from application.evidence_service import EvidenceService
from application.territorial_master_service import MASTER_COLUMNS, TerritorialMasterIndexBuilder
from config.settings import AppPaths
from data_catalog.sources import source_by_name
from domain.decision_models import CandidateProfile
from ingestion import LayeredIngestionPipeline
from scoring.priority_score import ScoringEngine


class _Settings:
    app_env = "dev"
    redis_url = "redis://test"
    rq_queue_name = "jobs"
    tenant_id = "cliente-quality"
    ingestion_source_catalog_path = "catalog.json"
    api_rate_limit_enabled = False

    def __init__(self, root: Path):
        self.root = root

    def build_paths(self) -> AppPaths:
        return _paths(self.root, tenant_id="cliente-quality")


def _paths(root: Path, *, tenant_id: str = "default") -> AppPaths:
    lake = root / "data_lake"
    for folder in [
        root / "ingestion",
        lake / "bronze",
        lake / "silver",
        lake / "gold",
        lake / "catalog",
        lake / "gold" / "reports",
        lake / "gold" / "serving",
        root / "chromadb",
        root / "metadata",
        root / "artifacts",
    ]:
        folder.mkdir(parents=True, exist_ok=True)
    return AppPaths(
        data_root=root,
        ingestion_root=root / "ingestion",
        lake_root=lake,
        bronze_root=lake / "bronze",
        silver_root=lake / "silver",
        gold_root=lake / "gold",
        gold_reports_root=lake / "gold" / "reports",
        gold_serving_root=lake / "gold" / "serving",
        catalog_root=lake / "catalog",
        chromadb_path=root / "chromadb",
        runtime_reports_root=root / "runtime_reports",
        ts="20260417_000000",
        metadata_db_path=root / "metadata" / "jobs.sqlite3",
        artifact_root=root / "artifacts",
        tenant_id=tenant_id,
        tenant_root=root,
    )


def _zone_fact() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ano_eleicao": 2024,
                "uf": "SP",
                "cod_tse_municipio": "71072",
                "municipio_id_ibge7": "3550308",
                "municipio": "Sao Paulo",
                "zona_eleitoral": 1,
                "territorio_id": "2024:SP:71072:ZE1",
                "eleitores_aptos": 10000,
                "votos_validos": 7000,
                "abstencao_pct": 0.22,
                "competitividade": 0.72,
                "data_quality_score": 0.95,
                "join_confidence": 0.98,
                "source_name": "fact_zona_eleitoral",
                "ingestion_run_id": "run_quality",
                "lake_layer": "gold",
            },
            {
                "ano_eleicao": 2024,
                "uf": "SP",
                "cod_tse_municipio": "62910",
                "municipio_id_ibge7": "3509502",
                "municipio": "Campinas",
                "zona_eleitoral": 2,
                "territorio_id": "2024:SP:62910:ZE2",
                "eleitores_aptos": 7000,
                "votos_validos": 4200,
                "abstencao_pct": 0.31,
                "competitividade": 0.44,
                "data_quality_score": 0.90,
                "join_confidence": 0.91,
                "source_name": "fact_zona_eleitoral",
                "ingestion_run_id": "run_quality",
                "lake_layer": "gold",
            },
        ]
    )


def _section_fact() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ano_eleicao": 2024,
                "uf": "SP",
                "cod_tse_municipio": "71072",
                "municipio_id_ibge7": "3550308",
                "municipio": "Sao Paulo",
                "zona_eleitoral": 1,
                "secao_eleitoral": 10,
                "local_votacao": "Escola A",
                "setor_censitario": "355030800001",
                "join_confidence": 0.94,
            }
        ]
    )


def _ibge_municipios() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"SIGLA_UF": "SP", "COD_MUN_IBGE": "3550308", "MUNICIPIO": "Sao Paulo"},
            {"SIGLA_UF": "SP", "COD_MUN_IBGE": "3509502", "MUNICIPIO": "Campinas"},
        ]
    )


def _candidate_payload() -> dict[str, object]:
    return {
        "candidate_id": "cand-quality",
        "nome_politico": "Candidato Quality",
        "cargo": "Prefeito",
        "partido": "P",
        "temas_prioritarios": ["saude", "educacao"],
        "municipios_base": ["Sao Paulo"],
    }


def test_phase10_master_index_join_consistency_and_publish_contract(tmp_path: Path) -> None:
    builder = TerritorialMasterIndexBuilder()
    master = builder.build_master_index(
        zone_fact=_zone_fact(),
        section_fact=_section_fact(),
        ibge_municipios=_ibge_municipios(),
        candidate_id="cand-quality",
    )

    assert list(master.columns) == MASTER_COLUMNS
    assert master["territorio_id"].is_unique
    assert set(master["COD_MUN_IBGE"]) == {"3550308", "3509502"}
    assert master["join_confidence"].between(0, 1).all()
    assert not master["join_ambiguity_flag"].any()
    assert {"2024:SP:71072:ZE1", "2024:SP:71072:ZE1:S10"}.issubset(set(master["territorio_id"]))

    result = builder.publish_master_index(master, tmp_path / "gold", dataset_version="quality")
    assert result.parquet_path is not None and result.parquet_path.exists()
    assert result.manifest_path is not None and result.manifest_path.exists()
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["quality"]["coverage_ibge"] == 1.0
    assert manifest["quality"]["unique_territories"] == len(master)
    assert "join_policy" in manifest


def test_phase10_score_recommendation_explanation_and_schema_contracts() -> None:
    candidate = CandidateProfile(
        candidate_id="cand-quality",
        nome_politico="Candidato Quality",
        cargo="Prefeito",
        partido="P",
        temas_prioritarios=("saude", "educacao"),
        municipios_base=("Sao Paulo",),
    )
    engine = RecommendationEngine(
        context_service=CandidateContextService(),
        scoring_engine=ScoringEngine(),
        evidence_service=EvidenceService(),
    )

    result = engine.recommend_scenario(
        candidate=candidate,
        territories=_zone_fact(),
        budget_total=100000,
        top_n=2,
        capacidade_operacional=0.8,
        scenario_name="hibrido",
    )

    assert len(result.recommendations) == 2
    assert result.scored["score_prioridade_final"].between(0, 1).all()
    assert result.allocated["recurso_sugerido"].sum() == pytest.approx(100000)
    first = result.recommendations[0]
    assert first.evidencias
    assert first.confidence_score > 0.7
    assert first.provenance["ingestion_run_id"] == "run_quality"
    assert "Por que priorizar" in first.justificativa

    schema = allocation_recommendation_to_schema(first)
    assert isinstance(schema, AllocationRecommendationSchema)
    assert schema.evidencias[0].dataset
    assert schema.confidence_score == pytest.approx(first.confidence_score)


def test_phase10_pydantic_schemas_reject_ambiguous_payloads() -> None:
    with pytest.raises(ValidationError):
        CandidateProfileSchema(
            candidate_id="cand-invalid",
            nome_politico="Nome",
            cargo="Prefeito",
            partido="P",
            idade=12,
        )
    with pytest.raises(ValidationError):
        CandidateProfileSchema(
            candidate_id="cand-invalid",
            nome_politico="Nome",
            cargo="Prefeito",
            partido="P",
            campo_extra="nao permitido",
        )


def test_phase10_pipeline_integration_emits_structured_logs_and_quality_manifest(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    paths = _paths(tmp_path)
    source = source_by_name("tse_resultados_secao_boletim_urna")
    assert source is not None
    input_path = tmp_path / "votacao.csv"
    input_path.write_text(
        "ANO_ELEICAO,SG_UF,CD_MUNICIPIO,NM_MUNICIPIO,NR_ZONA,NR_SECAO,QT_VOTOS\n2024,SP,71072,SAO PAULO,1,10,100\n",
        encoding="utf-8",
    )

    caplog.set_level(logging.INFO, logger="ingestion.pipeline")
    result = LayeredIngestionPipeline(paths).ingest_source(source=source, input_path=input_path, run_id="quality_run")

    assert result.status == "ok"
    assert any(
        record.getMessage() == "ingestion_started" and record.source_name == source.name for record in caplog.records
    )
    assert any(record.getMessage() == "ingestion_finished" and record.rows == 1 for record in caplog.records)
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["quality"]["rows"] == 1
    assert manifest["quality"]["missing_keys"] == []
    assert manifest["schema"]["COD_MUN_TSE"] == "object"


def test_phase10_api_records_decision_metrics_in_observability(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from api import main

    settings = _Settings(tmp_path)
    monkeypatch.setattr(main, "get_settings", lambda: settings)
    paths = settings.build_paths()
    _zone_fact().to_parquet(paths.gold_root / "fact_zona_eleitoral_20260417_000000.parquet", index=False)

    client = TestClient(main.app)
    headers = {"Authorization": "Bearer dev-admin-token"}
    upsert = client.put("/v1/candidates/cand-quality", json=_candidate_payload(), headers=headers)
    assert upsert.status_code == 200

    allocation = client.post(
        "/v1/decision/allocation-scenario",
        json={"candidate": _candidate_payload(), "budget_total": 100000, "top_n": 2, "scenario": "hibrido"},
        headers=headers,
    )
    assert allocation.status_code == 200

    observability = client.get("/v1/ops/observability", headers=headers)
    assert observability.status_code == 200
    summary = observability.json()["summary"]
    assert summary["usage_total"] >= 1
    assert summary["latency_p95_ms"] >= 0
    events = main._metadata_db().list_operational_events(tenant_id="cliente-quality", limit=10)
    assert any(event["event_type"] == "api.create_allocation_scenario" for event in events)
