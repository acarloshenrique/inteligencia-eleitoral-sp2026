from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from allocation.budget_allocator import BudgetAllocator
from allocation.explanation_builder import build_justification, build_scenario_summary, choose_action
from allocation.scenario_builder import Scenario, ScenarioBuilder, normalize_action_type
from application.candidate_context_service import CandidateContextService
from application.evidence_service import EvidenceService
from application.explanation_service import ExplanationService
from domain.decision_models import AllocationRecommendation, CandidateProfile
from scoring.priority_score import ScoringEngine


@dataclass(frozen=True)
class AllocationScenarioResult:
    scenario: Scenario
    recommendations: list[AllocationRecommendation]
    scored: pd.DataFrame
    allocated: pd.DataFrame
    summary: dict[str, object]


@dataclass
class RecommendationEngine:
    context_service: CandidateContextService
    scoring_engine: ScoringEngine
    evidence_service: EvidenceService
    explanation_service: ExplanationService | None = None
    budget_allocator: BudgetAllocator | None = None
    scenario_builder: ScenarioBuilder | None = None

    def __post_init__(self) -> None:
        if self.budget_allocator is None:
            self.budget_allocator = BudgetAllocator()
        if self.scenario_builder is None:
            self.scenario_builder = ScenarioBuilder()
        if self.explanation_service is None:
            self.explanation_service = ExplanationService()

    def recommend(
        self,
        *,
        candidate: CandidateProfile,
        territories: pd.DataFrame,
        budget_total: float,
        top_n: int = 20,
        capacidade_operacional: float = 0.7,
        janela_temporal_dias: int = 45,
        scenario_name: str = "hibrido",
    ) -> tuple[list[AllocationRecommendation], pd.DataFrame]:
        result = self.recommend_scenario(
            candidate=candidate,
            territories=territories,
            budget_total=budget_total,
            top_n=top_n,
            capacidade_operacional=capacidade_operacional,
            janela_temporal_dias=janela_temporal_dias,
            scenario_name=scenario_name,
        )
        return result.recommendations, result.scored

    def recommend_scenario(
        self,
        *,
        candidate: CandidateProfile,
        territories: pd.DataFrame,
        budget_total: float,
        top_n: int = 20,
        capacidade_operacional: float = 0.7,
        janela_temporal_dias: int = 45,
        scenario_name: str = "hibrido",
    ) -> AllocationScenarioResult:
        scenario_builder = self.scenario_builder
        budget_allocator = self.budget_allocator
        if scenario_builder is None or budget_allocator is None:
            raise RuntimeError("recommendation engine dependencies were not initialized")
        scenario = scenario_builder.build(
            budget_total=budget_total,
            scenario=scenario_name,
            top_n=top_n,
            capacidade_operacional=capacidade_operacional,
            janela_temporal_dias=janela_temporal_dias,
        )
        context = self.context_service.build_context(candidate, territories)
        mapped = (
            context.consolidated_base_map
            if not context.consolidated_base_map.empty
            else self.context_service.base_mapper.map_base(candidate, territories)
        )
        scored = self.scoring_engine.score(
            mapped,
            thematic_vector=context.thematic_vector,
            capacidade_operacional=scenario.capacidade_operacional,
        )
        scored = scored.copy()
        scored["tipo_recomendacao"] = scored.apply(lambda row: choose_action(row, scenario=scenario), axis=1)
        allocated = budget_allocator.allocate(scored, scenario=scenario)
        recommendations = self._recommendations_from_allocated(candidate, allocated, scenario=scenario)
        return AllocationScenarioResult(
            scenario=scenario,
            recommendations=recommendations,
            scored=scored,
            allocated=allocated,
            summary=build_scenario_summary(allocated, scenario),
        )

    def recommend_all_scenarios(
        self,
        *,
        candidate: CandidateProfile,
        territories: pd.DataFrame,
        budget_total: float,
        top_n: int = 20,
        capacidade_operacional: float = 0.7,
        janela_temporal_dias: int = 45,
    ) -> dict[str, AllocationScenarioResult]:
        return {
            scenario_name: self.recommend_scenario(
                candidate=candidate,
                territories=territories,
                budget_total=budget_total,
                top_n=top_n,
                capacidade_operacional=capacidade_operacional,
                janela_temporal_dias=janela_temporal_dias,
                scenario_name=scenario_name,
            )
            for scenario_name in ["conservador", "hibrido", "agressivo"]
        }

    def _recommendations_from_allocated(
        self,
        candidate: CandidateProfile,
        allocated: pd.DataFrame,
        *,
        scenario: Scenario,
    ) -> list[AllocationRecommendation]:
        recs: list[AllocationRecommendation] = []
        for _, row in allocated.iterrows():
            territorio_id = str(row.get("territorio_id") or row.get("zona_id") or row.get("municipio") or "")
            evidences = self.evidence_service.build_recommendation_evidence(row, territorio_id=territorio_id)
            required_scores = [
                "score_prioridade_final",
                "base_eleitoral_score",
                "afinidade_tematica_score",
                "potencial_expansao_score",
                "custo_eficiencia_score",
                "concorrencia_score",
            ]
            score_completeness = sum(1 for col in required_scores if col in row and pd.notna(row[col])) / len(
                required_scores
            )
            confidence = self.evidence_service.confidence_from_evidence(
                evidences,
                data_quality_score=float(row.get("data_quality_score", 0.8) or 0.8),
                join_confidence=float(row.get("join_confidence", 0.8) or 0.8),
                score_completeness=score_completeness,
            )
            base_justification = build_justification(row, scenario=scenario)
            explanation_service = self.explanation_service
            if explanation_service is None:
                raise RuntimeError("explanation service was not initialized")
            audit = explanation_service.build_recommendation_audit(
                row,
                scenario=scenario,
                evidences=evidences,
                confidence_score=confidence,
                base_justification=base_justification,
            )
            recs.append(
                AllocationRecommendation(
                    candidate_id=candidate.candidate_id,
                    territorio_id=territorio_id,
                    tipo_recomendacao=normalize_action_type(str(row.get("tipo_recomendacao", "presenca_fisica"))),
                    score_prioridade=float(row["score_prioridade_final"]),
                    score_aderencia_tematica=float(row["afinidade_tematica_score"]),
                    score_expansao=float(row["potencial_expansao_score"]),
                    score_competicao=float(row["concorrencia_score"]),
                    score_eficiencia=float(row["custo_eficiencia_score"]),
                    recurso_sugerido=float(row["recurso_sugerido"]),
                    percentual_orcamento_sugerido=float(row["percentual_orcamento_sugerido"]),
                    justificativa=audit.detailed_justification,
                    confidence_score=audit.confidence_score,
                    evidencias=tuple(evidences),
                    fatores_positivos=tuple(audit.positive_factors),
                    fatores_contra=tuple(audit.counter_factors),
                    provenance=audit.provenance,
                )
            )
        return recs
