from __future__ import annotations

from pathlib import Path
from typing import cast

import pandas as pd

from allocation.recommendation_engine import RecommendationEngine
from allocation.scenario_builder import ScenarioBuilder, ScenarioName
from api.decision_contracts import (
    AllocationScenarioRequest,
    AllocationScenarioResponse,
    CandidateProfileSchema,
    PrioritizedTerritoriesResponse,
    PrioritizedTerritoryItem,
    RecommendationExplanationResponse,
    TerritoryScoreResponse,
)
from application.candidate_context_service import CandidateContextService
from application.candidate_registry_service import CandidateRegistryService
from application.decision_mappers import (
    allocation_recommendation_to_schema,
    candidate_profile_to_domain,
)
from application.evidence_service import EvidenceService
from config.settings import AppPaths
from scoring.priority_score import ScoringEngine


def find_latest_zone_fact(paths: AppPaths) -> Path | None:
    candidates = sorted(
        paths.gold_root.glob("fact_zona_eleitoral_*.parquet"), key=lambda p: p.stat().st_mtime, reverse=True
    )
    return candidates[0] if candidates else None


def load_zone_territories(paths: AppPaths) -> pd.DataFrame:
    latest = find_latest_zone_fact(paths)
    if latest is None:
        return pd.DataFrame(
            [
                {
                    "territorio_id": "demo:SP:00000:ZE1",
                    "municipio": "DEMO",
                    "cod_tse_municipio": "00000",
                    "zona_eleitoral": 1,
                    "eleitores_aptos": 10000,
                    "votos_validos": 7000,
                    "abstencao_pct": 0.25,
                    "competitividade": 0.7,
                    "data_quality_score": 0.55,
                }
            ]
        )
    return pd.read_parquet(latest)


class DecisionPlatformService:
    def __init__(self, paths: AppPaths):
        self.paths = paths
        self.scenario_builder = ScenarioBuilder()
        self.registry = CandidateRegistryService(paths)
        self.engine = RecommendationEngine(
            context_service=CandidateContextService(),
            scoring_engine=ScoringEngine(),
            evidence_service=EvidenceService(),
        )

    def generate_allocation_scenario(self, req: AllocationScenarioRequest) -> AllocationScenarioResponse:
        scenario = self.scenario_builder.build(
            budget_total=req.budget_total,
            scenario=req.scenario,
            top_n=req.top_n,
            capacidade_operacional=req.capacidade_operacional,
            janela_temporal_dias=req.janela_temporal_dias,
        )
        candidate = candidate_profile_to_domain(req.candidate)
        territories = load_zone_territories(self.paths)
        scenario_result = self.engine.recommend_scenario(
            candidate=candidate,
            territories=territories,
            budget_total=scenario.budget_total,
            top_n=scenario.top_n,
            capacidade_operacional=scenario.capacidade_operacional,
            janela_temporal_dias=scenario.janela_temporal_dias,
            scenario_name=scenario.name,
        )
        recs = scenario_result.recommendations
        scored = scenario_result.scored
        score_items = self.engine.scoring_engine.breakdowns(
            scored.sort_values("score_prioridade_final", ascending=False).head(req.top_n)
        )
        return AllocationScenarioResponse(
            candidate_id=candidate.candidate_id,
            scenario=scenario.name,
            budget_total=scenario.budget_total,
            recommendations=[allocation_recommendation_to_schema(rec) for rec in recs],
            scores=[_score_to_schema(item) for item in score_items],
            evidence_count=sum(len(rec.evidencias) for rec in recs),
        )

    def generate_allocation_for_candidate_id(
        self,
        *,
        candidate_id: str,
        budget_total: float = 200000.0,
        capacidade_operacional: float = 0.7,
        janela_temporal_dias: int = 45,
        top_n: int = 20,
        scenario: str = "hibrido",
    ) -> AllocationScenarioResponse:
        candidate = self.registry.get(candidate_id)
        if candidate is None:
            raise ValueError("candidate_id nao encontrado")
        req = AllocationScenarioRequest(
            candidate=CandidateProfileSchema(
                candidate_id=candidate.candidate_id,
                nome_politico=candidate.nome_politico,
                cargo=candidate.cargo,
                partido=candidate.partido,
                idade=candidate.idade,
                faixa_etaria=candidate.faixa_etaria,
                origem_territorial=candidate.origem_territorial,
                incumbente=candidate.incumbente,
                biografia_resumida=candidate.biografia_resumida,
                temas_prioritarios=list(candidate.temas_prioritarios),
                temas_secundarios=list(candidate.temas_secundarios),
                historico_eleitoral=list(candidate.historico_eleitoral),
                municipios_base=list(candidate.municipios_base),
                zonas_base=list(candidate.zonas_base),
                observacoes_estrategicas=candidate.observacoes_estrategicas,
            ),
            budget_total=budget_total,
            capacidade_operacional=capacidade_operacional,
            janela_temporal_dias=janela_temporal_dias,
            top_n=top_n,
            scenario=cast(ScenarioName, scenario),
        )
        return self.generate_allocation_scenario(req)

    def list_prioritized_territories(
        self,
        *,
        candidate_id: str,
        tenant_id: str,
        budget_total: float = 200000.0,
        capacidade_operacional: float = 0.7,
        janela_temporal_dias: int = 45,
        top_n: int = 20,
        scenario: str = "hibrido",
    ) -> PrioritizedTerritoriesResponse:
        response = self.generate_allocation_for_candidate_id(
            candidate_id=candidate_id,
            budget_total=budget_total,
            capacidade_operacional=capacidade_operacional,
            janela_temporal_dias=janela_temporal_dias,
            top_n=top_n,
            scenario=scenario,
        )
        items = []
        for rec in response.recommendations:
            municipio = str(rec.provenance.get("municipio", ""))
            zona_raw = rec.provenance.get("zona")
            items.append(
                PrioritizedTerritoryItem(
                    territorio_id=rec.territorio_id,
                    municipio=municipio,
                    zona=int(str(zona_raw)) if str(zona_raw or "").isdigit() else None,
                    cluster_territorial=str(rec.provenance.get("cluster_territorial", "")),
                    tipo_recomendacao=rec.tipo_recomendacao,
                    score_prioridade=rec.score_prioridade,
                    recurso_sugerido=rec.recurso_sugerido,
                    percentual_orcamento_sugerido=rec.percentual_orcamento_sugerido,
                    confidence_score=rec.confidence_score,
                    justificativa=rec.justificativa,
                )
            )
        return PrioritizedTerritoriesResponse(
            candidate_id=candidate_id,
            scenario=response.scenario,
            items=items,
            total_budget=response.budget_total,
            tenant_id=tenant_id,
        )

    def get_territory_score(
        self,
        *,
        candidate_id: str,
        territorio_id: str,
        tenant_id: str,
        scenario: str = "hibrido",
    ) -> TerritoryScoreResponse:
        response = self.generate_allocation_for_candidate_id(candidate_id=candidate_id, top_n=200, scenario=scenario)
        score = next((item for item in response.scores if item.territorio_id == territorio_id), None)
        if score is None:
            raise ValueError("territorio_id nao encontrado")
        return TerritoryScoreResponse(
            candidate_id=candidate_id, territorio_id=territorio_id, score=score, tenant_id=tenant_id
        )

    def get_recommendation_explanation(
        self,
        *,
        candidate_id: str,
        territorio_id: str,
        tenant_id: str,
        scenario: str = "hibrido",
    ) -> RecommendationExplanationResponse:
        response = self.generate_allocation_for_candidate_id(candidate_id=candidate_id, top_n=200, scenario=scenario)
        rec = next((item for item in response.recommendations if item.territorio_id == territorio_id), None)
        if rec is None:
            raise ValueError("territorio_id nao encontrado")
        return RecommendationExplanationResponse(
            candidate_id=candidate_id,
            territorio_id=territorio_id,
            tipo_recomendacao=rec.tipo_recomendacao,
            why_prioritized=str(rec.provenance.get("why_prioritized", rec.justificativa)),
            supporting_bases=[f"{e.fonte}:{e.dataset}" for e in rec.evidencias],
            confidence_score=rec.confidence_score,
            positive_factors=rec.fatores_positivos,
            counter_factors=rec.fatores_contra,
            provenance=rec.provenance,
            evidencias=rec.evidencias,
            detailed_justification=rec.justificativa,
            tenant_id=tenant_id,
        )


def _score_to_schema(item):
    from api.decision_contracts import ScoreBreakdownSchema

    return ScoreBreakdownSchema(
        territorio_id=item.territorio_id,
        base_eleitoral_score=item.base_eleitoral_score,
        afinidade_tematica_score=item.afinidade_tematica_score,
        potencial_expansao_score=item.potencial_expansao_score,
        custo_eficiencia_score=item.custo_eficiencia_score,
        concorrencia_score=item.concorrencia_score,
        capacidade_operacional_score=item.capacidade_operacional_score,
        score_prioridade_final=item.score_prioridade_final,
        explicacoes=item.explicacoes,
    )
