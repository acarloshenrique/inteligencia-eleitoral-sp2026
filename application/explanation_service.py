from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import pandas as pd

from allocation.scenario_builder import Scenario
from domain.decision_models import EvidenceRecord


@dataclass(frozen=True)
class RecommendationAudit:
    territorio_id: str
    why_prioritized: str
    supporting_bases: list[str]
    confidence_score: float
    positive_factors: list[str] = field(default_factory=list)
    counter_factors: list[str] = field(default_factory=list)
    provenance: dict[str, Any] = field(default_factory=dict)
    detailed_justification: str = ""


class ExplanationService:
    def build_recommendation_audit(
        self,
        row: pd.Series,
        *,
        scenario: Scenario,
        evidences: list[EvidenceRecord],
        confidence_score: float,
        base_justification: str,
    ) -> RecommendationAudit:
        territorio_id = str(row.get("territorio_id") or row.get("zona_id") or row.get("municipio") or "")
        positive = self.positive_factors(row)
        counter = self.counter_factors(row)
        supporting = self.supporting_bases(evidences)
        why = self.why_prioritized(row, scenario=scenario)
        provenance = self.provenance(row, scenario=scenario, evidences=evidences)
        provenance["why_prioritized"] = why
        detailed = self.detailed_justification(
            base_justification=base_justification,
            why=why,
            supporting=supporting,
            confidence_score=confidence_score,
            counter_factors=counter,
        )
        return RecommendationAudit(
            territorio_id=territorio_id,
            why_prioritized=why,
            supporting_bases=supporting,
            confidence_score=confidence_score,
            positive_factors=positive,
            counter_factors=counter,
            provenance=provenance,
            detailed_justification=detailed,
        )

    def why_prioritized(self, row: pd.Series, *, scenario: Scenario) -> str:
        municipio = str(row.get("municipio") or row.get("MUNICIPIO") or "territorio")
        zona = row.get("zona_eleitoral", row.get("ZONA", ""))
        action = str(row.get("tipo_recomendacao", "presenca_fisica"))
        return (
            f"{municipio} zona {zona} entrou no cenario {scenario.name} por combinar "
            f"score final {float(row.get('score_prioridade_final', 0.0) or 0.0):.2f}, "
            f"acao recomendada {action} e adequacao ao split estrategico do orcamento."
        )

    def positive_factors(self, row: pd.Series) -> list[str]:
        factors: list[str] = []
        if float(row.get("base_eleitoral_score", 0.0) or 0.0) >= 0.65:
            factors.append("base eleitoral agregada forte")
        if float(row.get("afinidade_tematica_score", 0.0) or 0.0) >= 0.65:
            factors.append("alta aderencia tematica")
        if float(row.get("potencial_expansao_score", 0.0) or 0.0) >= 0.65:
            factors.append("potencial de expansao territorial")
        if float(row.get("custo_eficiencia_score", 0.0) or 0.0) >= 0.60:
            factors.append("boa eficiencia relativa de custo")
        if float(row.get("capacidade_operacional_score", 0.0) or 0.0) >= 0.70:
            factors.append("capacidade operacional adequada")
        return factors or ["prioridade composta acima dos demais territorios selecionados"]

    def counter_factors(self, row: pd.Series) -> list[str]:
        factors: list[str] = []
        if float(row.get("concorrencia_score", 0.0) or 0.0) >= 0.70:
            factors.append("alta pressao competitiva local")
        if float(row.get("custo_eficiencia_score", 0.0) or 0.0) < 0.40:
            factors.append("eficiencia de custo abaixo do ideal")
        if float(row.get("base_eleitoral_score", 0.0) or 0.0) < 0.35:
            factors.append("base eleitoral agregada ainda fraca")
        if float(row.get("join_confidence", 1.0) or 1.0) < 0.80:
            factors.append("confianca de join territorial abaixo do ideal")
        if float(row.get("data_quality_score", 1.0) or 1.0) < 0.75:
            factors.append("qualidade da base abaixo do ideal")
        return factors

    def supporting_bases(self, evidences: list[EvidenceRecord]) -> list[str]:
        bases = []
        for evidence in evidences:
            label = f"{evidence.fonte}:{evidence.dataset}"
            if label not in bases:
                bases.append(label)
        return bases

    def provenance(self, row: pd.Series, *, scenario: Scenario, evidences: list[EvidenceRecord]) -> dict[str, Any]:
        return {
            "generated_at_utc": datetime.now(UTC).isoformat(),
            "scenario": scenario.name,
            "budget_total": scenario.budget_total,
            "janela_temporal_dias": scenario.janela_temporal_dias,
            "capacidade_operacional": scenario.capacidade_operacional,
            "territorio_id": str(row.get("territorio_id") or row.get("zona_id") or row.get("municipio") or ""),
            "municipio": str(row.get("municipio") or row.get("MUNICIPIO") or ""),
            "zona": row.get("zona_eleitoral", row.get("ZONA", None)),
            "cluster_territorial": str(row.get("cluster_territorial", "")),
            "score_weights_version": str(row.get("score_weights_version", "nao_informado")),
            "source_name": str(row.get("source_name", "nao_informado")),
            "ingestion_run_id": str(row.get("ingestion_run_id", "nao_informado")),
            "lake_layer": str(row.get("lake_layer", "gold")),
            "data_quality_score": float(row.get("data_quality_score", 0.8) or 0.8),
            "join_confidence": float(row.get("join_confidence", 0.8) or 0.8),
            "evidence_ids": [evidence.evidence_id for evidence in evidences],
        }

    def detailed_justification(
        self,
        *,
        base_justification: str,
        why: str,
        supporting: list[str],
        confidence_score: float,
        counter_factors: list[str],
    ) -> str:
        counter = "; ".join(counter_factors) if counter_factors else "nenhum fator contrario material nos limiares atuais"
        bases = "; ".join(supporting) if supporting else "sem base vinculada"
        return (
            f"{base_justification} Por que priorizar: {why} "
            f"Bases de suporte: {bases}. Confianca da recomendacao: {confidence_score:.2f}. "
            f"Fatores contra: {counter}."
        )
