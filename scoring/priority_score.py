from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from domain.decision_models import ScoreBreakdown
from scoring.base_strength import compute_base_strength
from scoring.competition import compute_competition
from scoring.config import (
    DEFAULT_SCORE_WEIGHTS,
    GRANULARITY_KEYS,
    SCORE_COLUMNS,
    ScoreWeights,
    ScoringPersistenceResult,
)
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

    def score_by_granularity(
        self,
        territories: pd.DataFrame,
        *,
        thematic_vector: dict[str, float],
        capacidade_operacional: float = 0.7,
        granularities: tuple[str, ...] = ("municipio", "zona", "secao"),
    ) -> pd.DataFrame:
        scored = self.score(
            territories,
            thematic_vector=thematic_vector,
            capacidade_operacional=capacidade_operacional,
        )
        frames: list[pd.DataFrame] = []
        for granularity in granularities:
            frames.append(self._aggregate_granularity(scored, granularity))
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

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

    def _aggregate_granularity(self, scored: pd.DataFrame, granularity: str) -> pd.DataFrame:
        if granularity not in GRANULARITY_KEYS:
            raise ValueError(f"Granularidade invalida: {granularity}")
        df = scored.copy()
        group_cols = [column for column in GRANULARITY_KEYS[granularity] if column in df.columns]
        if "candidate_id" in df.columns:
            group_cols = ["candidate_id", *group_cols]
        if not group_cols:
            raise ValueError(f"Sem colunas canonicas para granularidade {granularity}")
        for column in SCORE_COLUMNS:
            if column not in df.columns:
                df[column] = 0.0
        aggregations: dict[str, tuple[str, str]] = {
            column: (column, "mean") for column in [*SCORE_COLUMNS, "score_prioridade_final"] if column in df.columns
        }
        for optional in ["join_confidence", "data_quality_score", "source_coverage_score", "confidence_score"]:
            if optional in df.columns:
                aggregations[optional] = (optional, "mean")
        for optional in ["eleitores_aptos", "total_aptos", "votos_validos", "votos_nominais", "votos", "votos_total"]:
            if optional in df.columns:
                aggregations[optional] = (optional, "sum")
        if "territorio_id" in df.columns:
            aggregations["territorios_origem"] = ("territorio_id", "nunique")
        out = df.groupby(group_cols, dropna=False).agg(**aggregations).reset_index()
        out["score_prioridade_final"] = self._final_from_components(out)
        out["score_component_details"] = out.apply(lambda row: self._component_details(row, self._weights_dict()), axis=1)
        out["score_explanation"] = out.apply(lambda row: _explain_row(row, self._weights_dict()), axis=1)
        out["score_granularity"] = granularity
        out["score_weights_version"] = self.weights_version
        out["score_record_id"] = out.apply(lambda row: self._score_record_id(row, group_cols, granularity), axis=1)
        rank_cols = ["candidate_id"] if "candidate_id" in out.columns else []
        if rank_cols:
            out["score_rank"] = (
                out.groupby(rank_cols, dropna=False)["score_prioridade_final"]
                .rank(method="first", ascending=False)
                .astype(int)
            )
        else:
            out["score_rank"] = out["score_prioridade_final"].rank(method="first", ascending=False).astype(int)
        return out.sort_values(["score_granularity", "score_rank"])

    def _final_from_components(self, df: pd.DataFrame) -> pd.Series:
        weights = self._weights_dict()
        score = pd.Series([0.0] * len(df), index=df.index, dtype=float)
        for col, weight in weights.items():
            score = score + weight * pd.to_numeric(df[col], errors="coerce").fillna(0.0)
        return score.clip(0, 1)

    def _score_record_id(self, row: pd.Series, group_cols: list[str], granularity: str) -> str:
        parts = [granularity]
        for column in group_cols:
            value = str(row.get(column, "") or "").strip()
            if value:
                parts.append(value)
        return ":".join(parts)


def _explain_row(row: pd.Series, weights: dict[str, float]) -> str:
    explanations = explain_components(row.to_dict(), weights)
    return explanation_to_summary(explanations, final_score=float(row["score_prioridade_final"]))
