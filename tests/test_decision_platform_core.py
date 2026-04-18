from __future__ import annotations

from pathlib import Path

import pandas as pd

from api.decision_contracts import (
    AllocationScenarioRequest,
    CampaignFinanceSummarySchema,
    CandidateProfileSchema,
    CandidateThemeSchema,
    ElectoralBaseStrengthSchema,
    ElectoralResultSchema,
    EvidenceRecordSchema,
    TerritoryProfileSchema,
)
from application.decision_mappers import (
    campaign_finance_summary_to_domain,
    candidate_profile_to_domain,
    candidate_theme_to_domain,
    electoral_base_strength_to_domain,
    electoral_result_to_domain,
    territory_profile_to_domain,
)
from application.decision_platform_service import DecisionPlatformService
from application.territorial_master_service import TerritorialMasterIndexBuilder, normalize_name
from config.settings import AppPaths
from data_catalog.sources import build_default_catalog
from domain.decision_models import (
    CampaignFinanceSummary,
    CandidateProfile,
    CandidateTheme,
    ElectoralBaseStrength,
    ElectoralResult,
    TerritoryProfile,
)
from scoring.priority_score import ScoringEngine


def _paths(tmp_path: Path) -> AppPaths:
    lake = tmp_path / "data_lake"
    return AppPaths(
        data_root=tmp_path,
        ingestion_root=tmp_path / "ingestion",
        lake_root=lake,
        bronze_root=lake / "bronze",
        silver_root=lake / "silver",
        gold_root=lake / "gold",
        gold_reports_root=lake / "gold" / "reports",
        gold_serving_root=lake / "gold" / "serving",
        catalog_root=lake / "catalog",
        chromadb_path=tmp_path / "chromadb",
        runtime_reports_root=tmp_path / "runtime_reports",
        ts="20260416_000000",
        metadata_db_path=tmp_path / "metadata" / "jobs.sqlite3",
        artifact_root=tmp_path / "artifacts",
        tenant_id="default",
        tenant_root=tmp_path,
    )


def test_candidate_schema_normalizes_lists():
    candidate = CandidateProfileSchema(
        candidate_id="1",
        nome_politico="Nome",
        cargo="Prefeito",
        partido="P",
        temas_prioritarios="saude, educacao",
        municipios_base="SAO PAULO, OSASCO",
    )

    assert candidate.temas_prioritarios == ["saude", "educacao"]
    assert candidate.municipios_base == ["SAO PAULO", "OSASCO"]


def test_phase1_decision_contracts_cover_required_entities():
    candidate_schema = CandidateProfileSchema(
        candidate_id="cand",
        nome_politico="Nome",
        cargo="Prefeito",
        partido="P",
        idade=45,
        temas_prioritarios=["saude"],
        municipios_base=["SAO PAULO"],
    )
    theme_schema = CandidateThemeSchema(candidate_id="cand", tema="saude", prioridade="alta", legitimidade_percebida=0.9)
    territory_schema = TerritoryProfileSchema(
        ano=2024,
        uf="SP",
        cod_municipio_tse="71072",
        cod_municipio_ibge="3550308",
        municipio="SAO PAULO",
        zona=1,
        secao=10,
    )
    result_schema = ElectoralResultSchema(
        ano=2024,
        turno=1,
        uf="SP",
        municipio="SAO PAULO",
        zona=1,
        secao=10,
        candidate_id="cand",
        votos_nominais=100,
        total_aptos=200,
        comparecimento=0.8,
        percentual_votos=0.5,
    )
    base_schema = ElectoralBaseStrengthSchema(
        candidate_id="cand",
        municipio="SAO PAULO",
        zona=1,
        secao=10,
        base_strength_score=0.8,
        retention_score=0.7,
        expansion_score=0.6,
        competition_score=0.5,
    )
    finance_schema = CampaignFinanceSummarySchema(
        candidate_id="cand",
        municipio="SAO PAULO",
        receita_total=10000,
        despesa_total=8000,
        custo_por_voto_estimado=4.5,
        intensidade_financeira=0.7,
    )

    assert isinstance(candidate_profile_to_domain(candidate_schema), CandidateProfile)
    assert isinstance(candidate_theme_to_domain(theme_schema), CandidateTheme)
    assert isinstance(territory_profile_to_domain(territory_schema), TerritoryProfile)
    assert isinstance(electoral_result_to_domain(result_schema), ElectoralResult)
    assert isinstance(electoral_base_strength_to_domain(base_schema), ElectoralBaseStrength)
    assert isinstance(campaign_finance_summary_to_domain(finance_schema), CampaignFinanceSummary)
    assert EvidenceRecordSchema.model_fields["timestamp_ingestao"].is_required()


def test_phase1_contracts_reject_extra_payload_fields():
    try:
        CandidateProfileSchema(
            candidate_id="cand",
            nome_politico="Nome",
            cargo="Prefeito",
            partido="P",
            microtargeting_individual=True,
        )
    except Exception as exc:
        assert "extra_forbidden" in str(exc)
    else:
        raise AssertionError("CandidateProfileSchema deve rejeitar campos fora do contrato")


def test_catalog_contains_tier1_sources():
    catalog = build_default_catalog()
    names = {source.name for source in catalog.sources}

    assert "tse_resultados_secao_boletim_urna" in names
    assert "ibge_agregados_censo_2022" in names
    assert all(source.chaves_principais for source in catalog.sources if source.tier == 1)


def test_territorial_master_index_from_zone_fact():
    df = pd.DataFrame(
        [
            {
                "ano_eleicao": 2024,
                "uf": "SP",
                "cod_tse_municipio": "71072",
                "municipio_id_ibge7": "3550308",
                "municipio": "SAO PAULO",
                "zona_eleitoral": 1,
                "join_confidence": 0.96,
            }
        ]
    )

    master = TerritorialMasterIndexBuilder().build_from_zone_fact(df, candidate_id="cand")

    assert master.loc[0, "territorio_id"] == "2024:SP:71072:ZE1"
    assert master.loc[0, "ID_CANDIDATO"] == "cand"
    assert normalize_name("Santa Bárbara D'Oeste") == "SANTA BARBARA D OESTE"


def test_scoring_engine_generates_priority_components():
    territories = pd.DataFrame(
        [
            {"territorio_id": "A", "eleitores_aptos": 1000, "votos_validos": 800, "abstencao_pct": 0.2, "competitividade": 0.7},
            {"territorio_id": "B", "eleitores_aptos": 5000, "votos_validos": 2000, "abstencao_pct": 0.5, "competitividade": 0.4},
        ]
    )

    scored = ScoringEngine().score(territories, thematic_vector={"saude": 1.0}, capacidade_operacional=0.8)

    assert "score_prioridade_final" in scored.columns
    assert scored["score_prioridade_final"].between(0, 1).all()
    assert scored["score_explanation"].str.contains("Prioridade=").all()


def test_decision_platform_generates_recommendations_from_gold(tmp_path):
    paths = _paths(tmp_path)
    paths.gold_root.mkdir(parents=True, exist_ok=True)
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
            },
        ]
    ).to_parquet(paths.gold_root / "fact_zona_eleitoral_20260416_000000.parquet", index=False)
    req = AllocationScenarioRequest(
        candidate=CandidateProfileSchema(
            candidate_id="cand",
            nome_politico="Candidato",
            cargo="Prefeito",
            partido="P",
            temas_prioritarios=["saude"],
            municipios_base=["SAO PAULO"],
        ),
        budget_total=100000,
        top_n=2,
    )

    response = DecisionPlatformService(paths).generate_allocation_scenario(req)

    assert len(response.recommendations) == 2
    assert round(sum(rec.recurso_sugerido for rec in response.recommendations), 2) == 100000
    assert response.evidence_count >= 2
