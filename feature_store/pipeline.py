from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from feature_store.registry import FEATURE_REGISTRY


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def numeric(series: pd.Series, default: float = 0.0) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(default)


def normalized(series: pd.Series, *, invert: bool = False, default: float = 0.0) -> pd.Series:
    values = numeric(series, default)
    if values.empty:
        return values
    min_value = float(values.min())
    max_value = float(values.max())
    if max_value == min_value:
        out = pd.Series([0.5] * len(values), index=values.index)
    else:
        out = (values - min_value) / (max_value - min_value)
    return 1 - out if invert else out


def clamp01_series(series: pd.Series) -> pd.Series:
    return numeric(series).clip(0, 1)


def first_existing(df: pd.DataFrame, aliases: list[str]) -> str | None:
    normalized_columns = {str(column).lower(): str(column) for column in df.columns}
    for alias in aliases:
        if alias.lower() in normalized_columns:
            return normalized_columns[alias.lower()]
    return None


def series_first(df: pd.DataFrame, aliases: list[str], default: Any = "") -> pd.Series:
    column = first_existing(df, aliases)
    if column is None:
        return pd.Series([default] * len(df), index=df.index)
    return df[column]


@dataclass(frozen=True)
class FeatureComputationResult:
    features: pd.DataFrame
    feature_version: str
    computed_at_utc: str
    lineage: list[str]


class AnalyticalFeatureStore:
    def compute(
        self,
        *,
        gold_tables: dict[str, pd.DataFrame],
        feature_version: str,
    ) -> FeatureComputationResult:
        computed_at = utc_now_iso()
        base = self._base_frame(gold_tables)
        features = base.copy()
        features = self._add_electoral_base_features(features, gold_tables)
        features = self._add_competition_features(features, gold_tables)
        features = self._add_territorial_features(features, gold_tables)
        features = self._add_thematic_features(features, gold_tables)
        features = self._add_efficiency_features(features, gold_tables)
        features = self._add_operational_features(features, gold_tables)
        features["feature_version"] = feature_version
        features["computed_at_utc"] = computed_at
        features["feature_lineage"] = ";".join(self.lineage(gold_tables))
        return FeatureComputationResult(
            features=features,
            feature_version=feature_version,
            computed_at_utc=computed_at,
            lineage=self.lineage(gold_tables),
        )

    def scoring_frame(self, features: pd.DataFrame) -> pd.DataFrame:
        out = features.copy()
        out["base_context_score"] = out["retention_score"]
        out["votos_validos"] = out["historical_vote_share"]
        out["eleitores_aptos"] = out["territory_size"]
        out["custo_eficiencia_score"] = out["spend_result_elasticity"]
        out["concorrencia_score"] = out["competitive_intensity"]
        out["data_quality_score"] = (1.0 - out["logistical_complexity"]).clip(0, 1)
        return out

    def lineage(self, gold_tables: dict[str, pd.DataFrame]) -> list[str]:
        available = set(gold_tables)
        declared = {source for feature in FEATURE_REGISTRY for source in feature.lineage}
        return sorted(available.union(declared))

    def _base_frame(self, gold_tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
        priority = gold_tables.get("gold_priority_score")
        base_strength = gold_tables.get("gold_electoral_base_strength")
        if priority is not None and not priority.empty:
            source = priority
        elif base_strength is not None and not base_strength.empty:
            source = base_strength
        else:
            return pd.DataFrame(columns=["candidate_id", "territorio_id"])
        base = pd.DataFrame(
            {
                "candidate_id": series_first(source, ["candidate_id"], "").astype(str),
                "territorio_id": series_first(source, ["territorio_id"], "").astype(str),
            }
        ).drop_duplicates(["candidate_id", "territorio_id"])
        for column in ["ano_eleicao", "uf", "cod_municipio_tse", "municipio_nome", "zona", "territorial_cluster_id"]:
            if column in source.columns:
                values = source[["candidate_id", "territorio_id", column]].drop_duplicates(
                    ["candidate_id", "territorio_id"]
                )
                base = base.merge(values, on=["candidate_id", "territorio_id"], how="left")
        return base.reset_index(drop=True)

    def _add_electoral_base_features(
        self, features: pd.DataFrame, gold_tables: dict[str, pd.DataFrame]
    ) -> pd.DataFrame:
        base = gold_tables.get("gold_electoral_base_strength", pd.DataFrame())
        out = features.copy()
        if base.empty:
            return self._fill(
                out,
                ["historical_vote_share", "territorial_concentration", "electoral_volatility", "retention_score"],
                0.0,
            )
        enriched = base.copy()
        votes = numeric(series_first(enriched, ["votos_nominais"], 0))
        total_aptos = numeric(series_first(enriched, ["total_aptos"], 0))
        enriched["historical_vote_share"] = (votes / total_aptos.replace(0, pd.NA)).fillna(0.0).clip(0, 1)
        total_by_candidate = votes.groupby(enriched["candidate_id"].astype(str)).transform("sum").replace(0, pd.NA)
        enriched["territorial_concentration"] = (votes / total_by_candidate).fillna(0.0).clip(0, 1)
        volatility = (
            enriched.groupby(["candidate_id", "territorio_id"], dropna=False)["historical_vote_share"]
            .std()
            .fillna(0.0)
            .reset_index(name="electoral_volatility")
        )
        enriched["retention_score"] = clamp01_series(
            series_first(enriched, ["retention_score", "base_strength_score"], 0.0)
        )
        enriched["stronghold_presence"] = enriched["retention_score"].ge(0.7)
        cols = [
            "candidate_id",
            "territorio_id",
            "historical_vote_share",
            "territorial_concentration",
            "retention_score",
            "stronghold_presence",
        ]
        out = out.merge(
            enriched[cols].drop_duplicates(["candidate_id", "territorio_id"]),
            on=["candidate_id", "territorio_id"],
            how="left",
        )
        out = out.merge(volatility, on=["candidate_id", "territorio_id"], how="left")
        return self._fill(
            out, ["historical_vote_share", "territorial_concentration", "electoral_volatility", "retention_score"], 0.0
        )

    def _add_competition_features(self, features: pd.DataFrame, gold_tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
        out = features.copy()
        base = gold_tables.get("gold_electoral_base_strength", pd.DataFrame())
        competition = gold_tables.get("gold_competition_landscape", pd.DataFrame())
        if not base.empty:
            tmp = base.copy()
            tmp["base_strength_score"] = clamp01_series(series_first(tmp, ["base_strength_score"], 0))
            tmp["share"] = tmp.groupby("territorio_id")["base_strength_score"].transform(
                lambda values: values / values.sum() if float(values.sum()) > 0 else 0
            )
            grouped = (
                tmp.groupby("territorio_id", dropna=False)
                .agg(
                    relevant_competitor_count=("base_strength_score", lambda values: int((values >= 0.1).sum())),
                    vote_fragmentation=("share", lambda values: float(1.0 - (values**2).sum())),
                )
                .reset_index()
            )
            out = out.merge(grouped, on="territorio_id", how="left")
        if competition is not None and not competition.empty:
            comp = competition[["territorio_id"]].copy()
            comp["competitive_intensity"] = clamp01_series(series_first(competition, ["competition_score"], 0.5))
            out = out.merge(comp.drop_duplicates("territorio_id"), on="territorio_id", how="left")
        out["competitor_incumbency_pressure"] = 0.0
        return self._fill(out, ["relevant_competitor_count", "vote_fragmentation", "competitive_intensity"], 0.0)

    def _add_territorial_features(self, features: pd.DataFrame, gold_tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
        out = features.copy()
        territory = gold_tables.get("gold_territory_profile", pd.DataFrame())
        if territory.empty:
            return self._fill(
                out,
                ["population_density_proxy", "urbanization_proxy", "institutional_presence", "social_indicator_score"],
                0.0,
            )
        frame = territory[["territorio_id"]].copy()
        density_source = series_first(territory, ["densidade", "secoes"], 0)
        frame["population_density_proxy"] = normalized(density_source)
        frame["urbanization_proxy"] = normalized(
            numeric(series_first(territory, ["secoes"], 0)) + numeric(series_first(territory, ["locais_votacao"], 0))
        )
        frame["institutional_presence"] = normalized(series_first(territory, ["locais_votacao"], 0))
        frame["social_indicator_score"] = clamp01_series(
            series_first(territory, ["data_quality_score", "source_coverage_avg"], 0.5)
        )
        out = out.merge(frame.drop_duplicates("territorio_id"), on="territorio_id", how="left")
        return self._fill(
            out,
            ["population_density_proxy", "urbanization_proxy", "institutional_presence", "social_indicator_score"],
            0.0,
        )

    def _add_thematic_features(self, features: pd.DataFrame, gold_tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
        out = features.copy()
        thematic = gold_tables.get("gold_thematic_affinity", pd.DataFrame())
        competition = gold_tables.get("gold_competition_landscape", pd.DataFrame())
        if thematic.empty:
            return self._fill(
                out,
                ["candidate_territory_thematic_affinity", "thematic_coherence", "theme_competitive_saturation"],
                0.5,
            )
        grouped = (
            thematic.groupby("territorio_id", dropna=False)
            .agg(
                candidate_territory_thematic_affinity=("thematic_affinity_score", "mean"),
                thematic_std=("thematic_affinity_score", "std"),
                theme_count=("tema", "nunique"),
            )
            .reset_index()
        )
        grouped["thematic_coherence"] = (1.0 - grouped["thematic_std"].fillna(0.0)).clip(0, 1)
        if competition is not None and not competition.empty:
            comp = competition[["territorio_id"]].copy()
            comp["competitive_intensity"] = clamp01_series(series_first(competition, ["competition_score"], 0.5))
            grouped = grouped.merge(comp.drop_duplicates("territorio_id"), on="territorio_id", how="left")
        else:
            grouped["competitive_intensity"] = 0.5
        grouped["theme_competitive_saturation"] = (
            grouped["candidate_territory_thematic_affinity"] * grouped["competitive_intensity"]
        ).clip(0, 1)
        out = out.merge(
            grouped[
                [
                    "territorio_id",
                    "candidate_territory_thematic_affinity",
                    "thematic_coherence",
                    "theme_competitive_saturation",
                ]
            ],
            on="territorio_id",
            how="left",
        )
        return self._fill(
            out, ["candidate_territory_thematic_affinity", "thematic_coherence", "theme_competitive_saturation"], 0.5
        )

    def _add_efficiency_features(self, features: pd.DataFrame, gold_tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
        out = features.copy()
        finance = gold_tables.get("gold_campaign_finance_efficiency", pd.DataFrame())
        if finance.empty:
            return self._fill(
                out, ["estimated_cost_per_vote", "spend_result_elasticity", "local_financial_intensity"], 0.0
            )
        frame = finance[["candidate_id"]].copy()
        frame["estimated_cost_per_vote"] = numeric(series_first(finance, ["custo_por_voto_estimado"], 0))
        frame["spend_result_elasticity"] = clamp01_series(series_first(finance, ["finance_efficiency_score"], 0.5))
        frame["local_financial_intensity"] = normalized(series_first(finance, ["despesa_total"], 0))
        out = out.merge(frame.drop_duplicates("candidate_id"), on="candidate_id", how="left")
        return self._fill(out, ["estimated_cost_per_vote", "spend_result_elasticity", "local_financial_intensity"], 0.0)

    def _add_operational_features(self, features: pd.DataFrame, gold_tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
        out = features.copy()
        out["base_proximity"] = clamp01_series(out.get("retention_score", pd.Series([0.0] * len(out), index=out.index)))
        territory = gold_tables.get("gold_territory_profile", pd.DataFrame())
        if not territory.empty:
            frame = territory[["territorio_id"]].copy()
            frame["territory_size"] = normalized(series_first(territory, ["secoes"], 0))
            quality = clamp01_series(series_first(territory, ["data_quality_score"], 0.5))
            frame["logistical_complexity"] = (0.6 * frame["territory_size"] + 0.4 * (1.0 - quality)).clip(0, 1)
            places = numeric(series_first(territory, ["locais_votacao"], 0))
            sections = numeric(series_first(territory, ["secoes"], 1)).replace(0, 1)
            frame["polling_place_centrality"] = (places / sections).clip(0, 1)
            out = out.merge(frame.drop_duplicates("territorio_id"), on="territorio_id", how="left")
        return self._fill(
            out, ["base_proximity", "logistical_complexity", "territory_size", "polling_place_centrality"], 0.0
        )

    def _fill(self, df: pd.DataFrame, columns: list[str], value: Any) -> pd.DataFrame:
        out = df.copy()
        for column in columns:
            if column not in out.columns:
                out[column] = value
            out[column] = out[column].fillna(value)
        return out


def read_gold_tables(paths: dict[str, Path]) -> dict[str, pd.DataFrame]:
    tables: dict[str, pd.DataFrame] = {}
    for name, path in paths.items():
        if path.suffix.lower() == ".parquet":
            tables[name] = pd.read_parquet(path)
        elif path.suffix.lower() == ".json":
            tables[name] = pd.read_json(path)
        else:
            tables[name] = pd.read_csv(path, sep=None, engine="python", dtype=str)
    return tables
