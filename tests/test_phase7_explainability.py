from __future__ import annotations

import pandas as pd

from allocation.recommendation_engine import RecommendationEngine
from allocation.scenario_builder import ScenarioBuilder
from application.candidate_context_service import CandidateContextService
from application.evidence_service import EvidenceService
from application.explanation_service import ExplanationService
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
    )


def _territories() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "territorio_id": "2024:SP:71072:ZE1",
                "municipio": "SAO PAULO",
                "zona_eleitoral": 1,
                "eleitores_aptos": 10000,
                "abstencao_pct": 0.2,
                "competitividade": 0.9,
                "data_quality_score": 0.72,
                "join_confidence": 0.78,
                "source_name": "fact_zona_eleitoral",
                "ingestion_run_id": "run_123",
                "lake_layer": "gold",
            }
        ]
    )


def test_evidence_service_links_score_allocation_lineage_and_quality():
    row = pd.Series(
        {
            "score_explanation": "Prioridade=0.72",
            "cenario": "hibrido",
            "recurso_sugerido": 10000,
            "tipo_recomendacao": "reforco_area_competitiva",
            "source_name": "fact_zona_eleitoral",
            "ingestion_run_id": "run_123",
            "lake_layer": "gold",
            "data_quality_score": 0.9,
            "join_confidence": 0.92,
        }
    )

    evidences = EvidenceService().build_recommendation_evidence(row, territorio_id="T1")

    labels = {e.dataset for e in evidences}
    assert "territorial_priority_scores" in labels
    assert "allocation_scenario" in labels
    assert "fact_zona_eleitoral" in labels
    assert "data_quality_join_confidence" in labels
    assert all(e.chave_registro == "T1" for e in evidences)


def test_explanation_service_answers_audit_questions():
    scenario = ScenarioBuilder().build(
        budget_total=100000,
        scenario="hibrido",
        top_n=10,
        capacidade_operacional=0.7,
        janela_temporal_dias=45,
    )
    row = pd.Series(
        {
            "territorio_id": "T1",
            "municipio": "SAO PAULO",
            "zona_eleitoral": 1,
            "tipo_recomendacao": "reforco_area_competitiva",
            "score_prioridade_final": 0.72,
            "base_eleitoral_score": 0.3,
            "afinidade_tematica_score": 0.8,
            "potencial_expansao_score": 0.7,
            "custo_eficiencia_score": 0.35,
            "concorrencia_score": 0.9,
            "capacidade_operacional_score": 0.7,
            "data_quality_score": 0.72,
            "join_confidence": 0.78,
            "source_name": "fact_zona_eleitoral",
            "ingestion_run_id": "run_123",
            "lake_layer": "gold",
            "score_weights_version": "unit",
        }
    )
    evidences = EvidenceService().build_recommendation_evidence(row, territorio_id="T1")
    confidence = EvidenceService().confidence_from_evidence(
        evidences,
        data_quality_score=0.72,
        join_confidence=0.78,
        score_completeness=1.0,
    )

    audit = ExplanationService().build_recommendation_audit(
        row,
        scenario=scenario,
        evidences=evidences,
        confidence_score=confidence,
        base_justification="Base justification.",
    )

    assert "por combinar" in audit.why_prioritized
    assert audit.supporting_bases
    assert 0 < audit.confidence_score <= 1
    assert "alta pressao competitiva local" in audit.counter_factors
    assert "eficiencia de custo abaixo do ideal" in audit.counter_factors
    assert audit.provenance["ingestion_run_id"] == "run_123"
    assert "Fatores contra" in audit.detailed_justification
    assert "Bases de suporte" in audit.detailed_justification


def test_recommendation_engine_returns_auditable_recommendations():
    engine = RecommendationEngine(
        context_service=CandidateContextService(),
        scoring_engine=ScoringEngine(),
        evidence_service=EvidenceService(),
    )

    result = engine.recommend_scenario(
        candidate=_candidate(),
        territories=_territories(),
        budget_total=50000,
        top_n=1,
        capacidade_operacional=0.7,
        janela_temporal_dias=45,
        scenario_name="hibrido",
    )
    rec = result.recommendations[0]

    assert rec.evidencias
    assert rec.confidence_score > 0
    assert rec.fatores_positivos
    assert rec.fatores_contra
    assert rec.provenance["territorio_id"] == rec.territorio_id
    assert rec.provenance["source_name"] == "fact_zona_eleitoral"
    assert "Por que priorizar" in rec.justificativa
    assert "Bases de suporte" in rec.justificativa
    assert "Confianca da recomendacao" in rec.justificativa
    assert "Fatores contra" in rec.justificativa
