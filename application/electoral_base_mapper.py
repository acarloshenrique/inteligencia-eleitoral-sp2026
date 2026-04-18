from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Any

import pandas as pd

from domain.decision_models import CandidateProfile


@dataclass(frozen=True)
class TerritorialSignalMaps:
    consolidated_base_map: pd.DataFrame
    plausible_expansion_map: pd.DataFrame
    territorial_coherence_score: float
    municipios_base: set[str]
    zonas_base: set[str]


def normalize_territory_name(value: Any) -> str:
    text = str(value or "").strip().upper()
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^A-Z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _series(df: pd.DataFrame, candidates: list[str], default: Any = "") -> pd.Series:
    for column in candidates:
        if column in df.columns:
            return df[column]
    return pd.Series([default] * len(df), index=df.index)


def _numeric(df: pd.DataFrame, candidates: list[str], default: float = 0.0) -> pd.Series:
    return pd.to_numeric(_series(df, candidates, default), errors="coerce").fillna(default)


def _normalize01(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce").fillna(0.0)
    min_v = float(numeric.min()) if len(numeric) else 0.0
    max_v = float(numeric.max()) if len(numeric) else 0.0
    if max_v == min_v:
        return pd.Series([0.5] * len(numeric), index=numeric.index)
    return ((numeric - min_v) / (max_v - min_v)).clip(0, 1)


class ElectoralBaseMapper:
    """Build aggregated base and expansion maps. No individual-level targeting is performed."""

    def map_base(self, candidate: CandidateProfile, territories: pd.DataFrame) -> pd.DataFrame:
        return self.build_signal_maps(candidate, territories).consolidated_base_map

    def build_signal_maps(
        self,
        candidate: CandidateProfile,
        territories: pd.DataFrame,
        historical_results: pd.DataFrame | None = None,
    ) -> TerritorialSignalMaps:
        base = self._prepare_territories(territories)
        municipios_base = self._candidate_municipios(candidate, historical_results)
        zonas_base = self._candidate_zones(candidate, historical_results)
        base["candidate_base_flag"] = base["municipio_normalizado"].isin(municipios_base)
        base["candidate_zone_base_flag"] = base["zona_key"].isin(zonas_base)
        historical = self._historical_base_scores(base, candidate, historical_results)
        base = base.merge(historical, on=["municipio_normalizado", "zona_key"], how="left")
        base["historical_base_score"] = base["historical_base_score"].fillna(0.0)
        base["base_context_score"] = (
            base["candidate_base_flag"].astype(float) * 0.40
            + base["candidate_zone_base_flag"].astype(float) * 0.25
            + base["historical_base_score"] * 0.35
        ).clip(0, 1)
        base.loc[base["candidate_base_flag"] & base["historical_base_score"].eq(0), "base_context_score"] = base.loc[
            base["candidate_base_flag"] & base["historical_base_score"].eq(0), "base_context_score"
        ].clip(lower=0.55)
        base.loc[base["candidate_zone_base_flag"], "base_context_score"] = base.loc[
            base["candidate_zone_base_flag"], "base_context_score"
        ].clip(lower=0.75)
        expansion = self._build_expansion_map(base)
        coherence = self._territorial_coherence(base, municipios_base, zonas_base)
        return TerritorialSignalMaps(
            consolidated_base_map=base,
            plausible_expansion_map=expansion,
            territorial_coherence_score=coherence,
            municipios_base=municipios_base,
            zonas_base=zonas_base,
        )

    def _prepare_territories(self, territories: pd.DataFrame) -> pd.DataFrame:
        df = territories.copy().reset_index(drop=True)
        df["municipio"] = _series(df, ["municipio", "MUNICIPIO"], "").astype(str)
        df["municipio_normalizado"] = df["municipio"].map(normalize_territory_name)
        df["zona_key"] = _series(df, ["zona_eleitoral", "ZONA"], "").astype(str).str.strip()
        df["territorio_id"] = _series(df, ["territorio_id", "zona_id"], "").astype(str)
        empty_id = df["territorio_id"].str.len().eq(0)
        df.loc[empty_id, "territorio_id"] = df.loc[empty_id, "municipio_normalizado"] + ":ZE" + df.loc[empty_id, "zona_key"]
        df["eleitores_aptos"] = _numeric(df, ["eleitores_aptos", "total_aptos"], 0.0)
        df["abstencao_pct"] = _numeric(df, ["abstencao_pct"], 0.0).clip(0, 1)
        df["competitividade"] = _numeric(df, ["competitividade", "concorrencia_local"], 0.5).clip(0, 1)
        return df

    def _candidate_municipios(self, candidate: CandidateProfile, historical_results: pd.DataFrame | None) -> set[str]:
        municipios = {normalize_territory_name(m) for m in candidate.municipios_base if normalize_territory_name(m)}
        if historical_results is not None and not historical_results.empty:
            history = historical_results.copy()
            if "candidate_id" in history.columns:
                history = history[history["candidate_id"].astype(str).eq(candidate.candidate_id)]
            if "municipio" in history.columns:
                municipios.update(history["municipio"].map(normalize_territory_name).dropna().tolist())
        return {m for m in municipios if m}

    def _candidate_zones(self, candidate: CandidateProfile, historical_results: pd.DataFrame | None) -> set[str]:
        zonas = {str(z).strip() for z in candidate.zonas_base if str(z).strip()}
        if historical_results is not None and not historical_results.empty:
            history = historical_results.copy()
            if "candidate_id" in history.columns:
                history = history[history["candidate_id"].astype(str).eq(candidate.candidate_id)]
            zone_col = next((col for col in ["zona", "zona_eleitoral", "ZONA"] if col in history.columns), None)
            if zone_col:
                zonas.update(history[zone_col].astype(str).str.strip().tolist())
        return {z for z in zonas if z}

    def _historical_base_scores(
        self,
        territories: pd.DataFrame,
        candidate: CandidateProfile,
        historical_results: pd.DataFrame | None,
    ) -> pd.DataFrame:
        keys = territories[["municipio_normalizado", "zona_key"]].drop_duplicates()
        if historical_results is None or historical_results.empty:
            keys["historical_base_score"] = 0.0
            return keys
        history = historical_results.copy()
        if "candidate_id" in history.columns:
            history = history[history["candidate_id"].astype(str).eq(candidate.candidate_id)]
        if history.empty:
            keys["historical_base_score"] = 0.0
            return keys
        history["municipio_normalizado"] = _series(history, ["municipio", "MUNICIPIO"], "").map(normalize_territory_name)
        history["zona_key"] = _series(history, ["zona", "zona_eleitoral", "ZONA"], "").astype(str).str.strip()
        if "percentual_votos" in history.columns:
            score = _numeric(history, ["percentual_votos"], 0.0).clip(0, 1)
        else:
            votos = _numeric(history, ["votos_nominais", "votos_validos"], 0.0)
            aptos = _numeric(history, ["total_aptos", "eleitores_aptos"], 1.0).replace(0, 1)
            score = (votos / aptos).clip(0, 1)
        history["historical_base_score"] = score
        return (
            history.groupby(["municipio_normalizado", "zona_key"], dropna=False)
            .agg(historical_base_score=("historical_base_score", "max"))
            .reset_index()
        )

    def _build_expansion_map(self, base: pd.DataFrame) -> pd.DataFrame:
        expansion = base.copy()
        scale = _normalize01(expansion["eleitores_aptos"])
        weak_base = 1 - expansion["base_context_score"].clip(0, 1)
        expansion["expansion_plausibility_score"] = (
            0.35 * weak_base + 0.25 * scale + 0.25 * expansion["abstencao_pct"] + 0.15 * expansion["competitividade"]
        ).clip(0, 1)
        return expansion.sort_values("expansion_plausibility_score", ascending=False).reset_index(drop=True)

    def _territorial_coherence(self, base: pd.DataFrame, municipios_base: set[str], zonas_base: set[str]) -> float:
        if base.empty:
            return 0.0
        manual_signal = 0.5 * bool(municipios_base) + 0.25 * bool(zonas_base)
        coverage = float((base["candidate_base_flag"] | base["candidate_zone_base_flag"]).mean())
        history_signal = float(base["historical_base_score"].max()) if "historical_base_score" in base else 0.0
        return max(0.0, min(1.0, manual_signal + 0.15 * coverage + 0.10 * history_signal))
