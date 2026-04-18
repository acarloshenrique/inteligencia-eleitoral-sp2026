from __future__ import annotations

import pandas as pd
from pydantic import BaseModel, Field, field_validator


class ZoneAllocationConfig(BaseModel):
    municipal_score_weight: float = Field(default=0.35, ge=0, le=1)
    electoral_weight_weight: float = Field(default=0.25, ge=0, le=1)
    abstention_opportunity_weight: float = Field(default=0.15, ge=0, le=1)
    competitiveness_weight: float = Field(default=0.15, ge=0, le=1)
    media_efficiency_weight: float = Field(default=0.10, ge=0, le=1)
    min_data_quality_score: float = Field(default=0.70, ge=0, le=1)
    min_join_confidence: float = Field(default=0.80, ge=0, le=1)
    max_zone_share_of_municipality: float = Field(default=0.45, gt=0, le=1)
    min_zone_budget: float = Field(default=1000.0, ge=0)

    @field_validator(
        "municipal_score_weight",
        "electoral_weight_weight",
        "abstention_opportunity_weight",
        "competitiveness_weight",
        "media_efficiency_weight",
    )
    @classmethod
    def validate_weight(cls, value: float) -> float:
        return float(value)


ZONE_ALLOCATION_COLUMNS = [
    "ranking_zona",
    "territorio_id",
    "municipio",
    "municipio_id_ibge7",
    "zona_eleitoral",
    "cluster_municipal",
    "score_zona",
    "score_potencial_eleitoral",
    "score_oportunidade",
    "score_eficiencia_midia",
    "score_custo",
    "score_risco",
    "eleitores_aptos",
    "abstencao_pct",
    "competitividade",
    "volatilidade_historica",
    "verba_sugerida",
    "canal_ideal",
    "mensagem_ideal",
    "justificativa",
    "data_quality_score",
    "join_confidence",
]


def build_zone_features(zones: pd.DataFrame, municipal_scores: pd.DataFrame) -> pd.DataFrame:
    if zones.empty:
        return pd.DataFrame()
    required = {"municipio", "zona_eleitoral", "eleitores_aptos", "abstencao_pct", "competitividade"}
    missing = sorted(required - set(zones.columns))
    if missing:
        raise ValueError(f"zonas sem colunas obrigatorias: {missing}")

    df = zones.copy()
    if "municipio_id_ibge7" not in df.columns:
        df["municipio_id_ibge7"] = df.groupby("municipio", sort=False).ngroup().add(1).astype(str).str.zfill(7)
    if "territorio_id" not in df.columns:
        df["territorio_id"] = df["municipio_id_ibge7"].astype(str) + "-ZE" + df["zona_eleitoral"].astype(str)

    score_cols = [
        col for col in ["municipio", "cluster", "indice_final", "ranking_final"] if col in municipal_scores.columns
    ]
    mun = municipal_scores[score_cols].copy() if score_cols else pd.DataFrame()
    if not mun.empty:
        mun = mun.rename(columns={"cluster": "cluster_municipal", "indice_final": "score_municipal"})
        df = df.merge(mun, on="municipio", how="left")
    if "cluster_municipal" not in df.columns:
        df["cluster_municipal"] = "Indefinido"
    if "score_municipal" not in df.columns:
        df["score_municipal"] = 50.0

    df["eleitores_aptos"] = pd.to_numeric(df["eleitores_aptos"], errors="coerce").fillna(0.0)
    municipio_total = df.groupby("municipio")["eleitores_aptos"].transform("sum").replace(0, 1)
    df["peso_eleitoral_no_municipio"] = (df["eleitores_aptos"] / municipio_total).clip(0, 1)
    df["abstencao_pct"] = pd.to_numeric(df["abstencao_pct"], errors="coerce").fillna(0.0).clip(0, 1)
    df["competitividade"] = pd.to_numeric(df["competitividade"], errors="coerce").fillna(0.0).clip(0, 1)
    df["volatilidade_historica"] = (
        pd.to_numeric(df.get("volatilidade_historica", 0.25), errors="coerce").fillna(0.25).clip(0, 1)
    )
    df["data_quality_score"] = (
        pd.to_numeric(df.get("data_quality_score", 0.85), errors="coerce").fillna(0.85).clip(0, 1)
    )
    df["join_confidence"] = pd.to_numeric(df.get("join_confidence", 0.90), errors="coerce").fillna(0.90).clip(0, 1)
    df["media_efficiency_proxy"] = (0.55 + (1 - df["abstencao_pct"]) * 0.25 + df["competitividade"] * 0.20).clip(0, 1)
    return df


def score_zone_allocation(
    zones: pd.DataFrame,
    municipal_scores: pd.DataFrame,
    *,
    budget_total: int | float,
    config: ZoneAllocationConfig | None = None,
) -> pd.DataFrame:
    config = config or ZoneAllocationConfig()
    features = build_zone_features(zones, municipal_scores)
    if features.empty:
        return pd.DataFrame(columns=ZONE_ALLOCATION_COLUMNS)

    eligible = features[
        (features["data_quality_score"] >= config.min_data_quality_score)
        & (features["join_confidence"] >= config.min_join_confidence)
    ].copy()
    if eligible.empty:
        return pd.DataFrame(columns=ZONE_ALLOCATION_COLUMNS)

    municipal_component = (pd.to_numeric(eligible["score_municipal"], errors="coerce").fillna(50.0) / 100).clip(0, 1)
    electoral_component = (
        eligible["peso_eleitoral_no_municipio"].clip(0, config.max_zone_share_of_municipality)
        / config.max_zone_share_of_municipality
    )
    opportunity_component = eligible["abstencao_pct"]
    competitiveness_component = eligible["competitividade"]
    media_component = eligible["media_efficiency_proxy"]

    eligible["score_potencial_eleitoral"] = municipal_component.round(3)
    eligible["score_oportunidade"] = opportunity_component.round(3)
    eligible["score_eficiencia_midia"] = media_component.round(3)
    eligible["score_custo"] = (1 - eligible["peso_eleitoral_no_municipio"].clip(0, 1) * 0.45).round(3)
    eligible["score_risco"] = (
        (1 - eligible["data_quality_score"]) * 0.55 + (1 - eligible["join_confidence"]) * 0.45
    ).round(3)

    weighted = (
        municipal_component * config.municipal_score_weight
        + electoral_component * config.electoral_weight_weight
        + opportunity_component * config.abstention_opportunity_weight
        + competitiveness_component * config.competitiveness_weight
        + media_component * config.media_efficiency_weight
    )
    eligible["score_zona"] = (weighted * 100 * eligible["data_quality_score"] * eligible["join_confidence"]).round(2)

    total_score = eligible["score_zona"].sum()
    if total_score <= 0:
        eligible["verba_sugerida"] = 0.0
    else:
        eligible["verba_sugerida"] = (eligible["score_zona"] / total_score * float(budget_total)).round(0)
        eligible.loc[eligible["verba_sugerida"] < config.min_zone_budget, "verba_sugerida"] = config.min_zone_budget

    eligible = eligible.sort_values(["score_zona", "eleitores_aptos"], ascending=[False, False]).reset_index(drop=True)
    eligible["ranking_zona"] = eligible.index + 1
    eligible["canal_ideal"] = (
        eligible["cluster_municipal"].map({"Diamante": "Meta Ads", "Alavanca": "Google Ads"}).fillna("WhatsApp")
    )
    eligible["mensagem_ideal"] = eligible["abstencao_pct"].apply(
        lambda value: "mobilizacao e comparecimento" if value >= 0.22 else "persuasao e reforco"
    )
    eligible["justificativa"] = (
        "Zona priorizada por peso eleitoral, oportunidade de abstencao, competitividade e qualidade do match territorial."
    )

    return eligible.rename(columns={"cluster_municipal": "cluster_municipal"})[ZONE_ALLOCATION_COLUMNS]
