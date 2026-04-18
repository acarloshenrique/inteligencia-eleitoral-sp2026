from __future__ import annotations

import pandas as pd

from allocation.budget_allocator import BudgetAllocator, cluster_territory
from allocation.explanation_builder import build_justification, choose_action
from allocation.recommendation_engine import RecommendationEngine
from allocation.scenario_builder import SCENARIO_PROFILES, ScenarioBuilder
from application.candidate_context_service import CandidateContextService
from application.evidence_service import EvidenceService
from domain.decision_models import CandidateProfile
from scoring.priority_score import ScoringEngine


def _candidate() -> CandidateProfile:
    return CandidateProfile(
        candidate_id="cand",
        nome_politico="Nome",
        cargo="Prefeito",
        partido="P",
        temas_prioritarios=("saude",),
        municipios_base=("SAO PAULO",),
        zonas_base=("1",),
    )


def _territories() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"territorio_id": "A", "municipio": "SAO PAULO", "zona_eleitoral": 1, "eleitores_aptos": 10000, "abstencao_pct": 0.2, "competitividade": 0.4, "data_quality_score": 0.94},
            {"territorio_id": "B", "municipio": "OSASCO", "zona_eleitoral": 2, "eleitores_aptos": 20000, "abstencao_pct": 0.6, "competitividade": 0.5, "data_quality_score": 0.90},
            {"territorio_id": "C", "municipio": "SANTOS", "zona_eleitoral": 3, "eleitores_aptos": 15000, "abstencao_pct": 0.3, "competitividade": 0.9, "data_quality_score": 0.88},
        ]
    )


def _engine() -> RecommendationEngine:
    return RecommendationEngine(
        context_service=CandidateContextService(),
        scoring_engine=ScoringEngine(),
        evidence_service=EvidenceService(),
    )


def test_scenario_builder_returns_three_budget_profiles():
    builder = ScenarioBuilder()

    conservative = builder.build(budget_total=100000, scenario="conservador", top_n=10, capacidade_operacional=0.8, janela_temporal_dias=30)
    hybrid = builder.build(budget_total=100000, scenario="hibrido", top_n=10, capacidade_operacional=0.8, janela_temporal_dias=30)
    aggressive = builder.build(budget_total=100000, scenario="agressivo", top_n=10, capacidade_operacional=0.8, janela_temporal_dias=30)

    assert set(SCENARIO_PROFILES) == {"conservador", "hibrido", "agressivo"}
    assert conservative.action_budget_split["retencao_base"] > aggressive.action_budget_split["retencao_base"]
    assert aggressive.action_budget_split["expansao_territorial"] > conservative.action_budget_split["expansao_territorial"]
    assert round(sum(hybrid.action_budget_split.values()), 6) == 1.0


def test_budget_allocator_allocates_by_action_cluster_and_preserves_total():
    scenario = ScenarioBuilder().build(budget_total=90000, scenario="hibrido", top_n=3, capacidade_operacional=0.7, janela_temporal_dias=45)
    scored = pd.DataFrame(
        [
            {"territorio_id": "A", "score_prioridade_final": 0.9, "base_eleitoral_score": 0.8, "potencial_expansao_score": 0.2, "concorrencia_score": 0.3, "custo_eficiencia_score": 0.7, "tipo_recomendacao": "retencao_base"},
            {"territorio_id": "B", "score_prioridade_final": 0.8, "base_eleitoral_score": 0.2, "potencial_expansao_score": 0.9, "concorrencia_score": 0.4, "custo_eficiencia_score": 0.6, "tipo_recomendacao": "expansao_territorial"},
            {"territorio_id": "C", "score_prioridade_final": 0.7, "base_eleitoral_score": 0.3, "potencial_expansao_score": 0.3, "concorrencia_score": 0.9, "custo_eficiencia_score": 0.5, "tipo_recomendacao": "reforco_area_competitiva"},
        ]
    )

    allocated = BudgetAllocator().allocate(scored, scenario=scenario)

    assert round(float(allocated["recurso_sugerido"].sum()), 2) == 90000.00
    assert set(allocated["cluster_territorial"]) >= {"base_consolidada", "expansao_prioritaria", "competitivo"}
    assert allocated["percentual_orcamento_sugerido"].between(0, 1).all()


def test_choose_action_and_justification_are_decision_oriented():
    scenario = ScenarioBuilder().build(budget_total=100000, scenario="agressivo", top_n=5, capacidade_operacional=0.8, janela_temporal_dias=20)
    row = pd.Series(
        {
            "municipio": "OSASCO",
            "zona_eleitoral": 2,
            "base_eleitoral_score": 0.2,
            "potencial_expansao_score": 0.8,
            "concorrencia_score": 0.4,
            "afinidade_tematica_score": 0.6,
            "custo_eficiencia_score": 0.7,
            "score_prioridade_final": 0.75,
            "recurso_sugerido": 20000,
            "percentual_orcamento_sugerido": 0.2,
        }
    )

    action = choose_action(row, scenario=scenario)
    row["tipo_recomendacao"] = action
    row["cluster_territorial"] = cluster_territory(row)
    justification = build_justification(row, scenario=scenario)

    assert action == "expansao_territorial"
    assert "OSASCO zona 2" in justification
    assert "Acao recomendada" in justification
    assert "R$ 20,000" in justification


def test_recommendation_engine_generates_allocation_scenarios_with_evidence():
    engine = _engine()

    result = engine.recommend_scenario(
        candidate=_candidate(),
        territories=_territories(),
        budget_total=120000,
        top_n=3,
        capacidade_operacional=0.75,
        janela_temporal_dias=35,
        scenario_name="hibrido",
    )

    assert result.scenario.name == "hibrido"
    assert len(result.recommendations) == 3
    assert round(sum(rec.recurso_sugerido for rec in result.recommendations), 2) == 120000.00
    assert all(rec.tipo_recomendacao for rec in result.recommendations)
    assert all(rec.justificativa for rec in result.recommendations)
    assert all(rec.evidencias for rec in result.recommendations)
    assert result.summary["by_action"]
    assert result.summary["by_cluster"]


def test_recommendation_engine_generates_conservative_hybrid_aggressive_outputs():
    results = _engine().recommend_all_scenarios(
        candidate=_candidate(),
        territories=_territories(),
        budget_total=150000,
        top_n=3,
        capacidade_operacional=0.7,
        janela_temporal_dias=45,
    )

    assert set(results) == {"conservador", "hibrido", "agressivo"}
    assert all(round(sum(rec.recurso_sugerido for rec in result.recommendations), 2) == 150000.00 for result in results.values())
    assert results["conservador"].summary["scenario"] == "conservador"
    assert results["agressivo"].summary["scenario"] == "agressivo"
