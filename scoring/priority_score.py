from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from domain.decision_models import ScoreBreakdown
from scoring.base_strength import compute_base_strength
from scoring.competition import compute_competition
from scoring.config import DEFAULT_SCORE_WEIGHTS, ScoreWeights, ScoringPersistenceResult
from scoring.cost_efficiency import compute_cost_efficiency
from scoring.expansion import compute_expansion
from scoring.explanations import explain_components, explanation_to_summary, explanations_to_dict
from scoring.persistence import GoldScoreWriter
from scoring.thematic_affinity import compute_thematic_affinity
from scoring.utils import clamp01

DEFAULT_WEIGHTS = DEFAULT_SCORE_WEIGHTS.as_dict()


@dataclass(frozen=True)
class ScoringEngine:
    weights: dict[str, float] | ScoreWeights = field(default_factory=lambda: DEFAULT_SCORE_WEIGHTS)
    writer: GoldScoreWriter = field(default_factory=GoldScoreWriter)

    def score(
        self,
        territories: pd.DataFrame,
        *,
        thematic_vector: dict[str, float],
        capacidade_operacional: float = 0.7,
    ) -> pd.DataFrame:
        df = territories.copy().reset_index(drop=True)
        weights = self._weights_dict()
        df["base_eleitoral_score"] = compute_base_strength(df)
        df["afinidade_tematica_score"] = compute_thematic_affinity(df, thematic_vector)
        df["potencial_expansao_score"] = compute_expansion(df)
        df["custo_eficiencia_score"] = compute_cost_efficiency(df)
        df["concorrencia_score"] = compute_competition(df)
        df["capacidade_operacional_score"] = clamp01(capacidade_operacional)
        score = pd.Series([0.0] * len(df), index=df.index, dtype=float)
        for col, weight in weights.items():
            score = score + weight * pd.to_numeric(df[col], errors="coerce").fillna(0.0)
        df["score_prioridade_final"] = score.clip(0, 1)
        df["score_component_details"] = df.apply(lambda row: self._component_details(row, weights), axis=1)
        df["score_explanation"] = df.apply(lambda row: _explain_row(row, weights), axis=1)
        df["score_weights_version"] = self.weights_version
        return df

    @property
    def weights_version(self) -> str:
        weights = self._weights_dict()
        return "|".join(f"{key}:{weights[key]:.4f}" for key in sorted(weights))

    def persist_gold(self, scored: pd.DataFrame, *, gold_root: Path, dataset_version: str) -> ScoringPersistenceResult:
        return self.writer.write(
            scored,
            gold_root=gold_root,
            dataset_version=dataset_version,
            weights=self._weights_dict(),
        )

    def breakdowns(self, scored: pd.DataFrame) -> list[ScoreBreakdown]:
        items: list[ScoreBreakdown] = []
        for _, row in scored.iterrows():
            territorio_id = str(row.get("territorio_id") or row.get("zona_id") or row.get("municipio") or "")
            details = row.get("score_component_details", {})
            items.append(
                ScoreBreakdown(
                    territorio_id=territorio_id,
                    base_eleitoral_score=float(row["base_eleitoral_score"]),
                    afinidade_tematica_score=float(row["afinidade_tematica_score"]),
                    potencial_expansao_score=float(row["potencial_expansao_score"]),
                    custo_eficiencia_score=float(row["custo_eficiencia_score"]),
                    concorrencia_score=float(row["concorrencia_score"]),
                    capacidade_operacional_score=float(row["capacidade_operacional_score"]),
                    score_prioridade_final=float(row["score_prioridade_final"]),
                    explicacoes={"summary": str(row.get("score_explanation", "")), "components": details},
                )
            )
        return items

    def _weights_dict(self) -> dict[str, float]:
        if isinstance(self.weights, ScoreWeights):
            return self.weights.as_dict()
        return ScoreWeights.model_validate(self.weights).as_dict()

    def _component_details(self, row: pd.Series, weights: dict[str, float]) -> dict[str, dict[str, float | str]]:
        explanations = explain_components(row.to_dict(), weights)
        return explanations_to_dict(explanations)


def _explain_row(row: pd.Series, weights: dict[str, float]) -> str:
    explanations = explain_components(row.to_dict(), weights)
    return explanation_to_summary(explanations, final_score=float(row["score_prioridade_final"]))
