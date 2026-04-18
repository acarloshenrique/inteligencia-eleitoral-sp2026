from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from application.serving_service import ServingDataNotFoundError, ServingOutputService
from config.settings import AppPaths


@dataclass(frozen=True)
class MunicipalStrategyView:
    municipio: str
    snapshot_id: str
    metrics: dict[str, Any]
    priority_ranking: pd.DataFrame
    recommendations: list[str]
    rag_context: dict[str, Any]
    missing_fields: list[str]
    source_paths: list[str]


def numeric(series: pd.Series, default: float = 0.0) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(default)


def first_existing(df: pd.DataFrame, aliases: list[str]) -> str | None:
    columns = {str(column).lower(): str(column) for column in df.columns}
    for alias in aliases:
        if alias.lower() in columns:
            return columns[alias.lower()]
    return None


class MunicipalStrategyService:
    def __init__(self, paths: AppPaths) -> None:
        self.paths = paths
        self.serving = ServingOutputService(paths)

    def municipalities(self) -> list[str]:
        frames = self._load_frames()
        names: set[str] = set()
        for df in frames.values():
            if not isinstance(df, pd.DataFrame):
                continue
            column = first_existing(df, ["municipio_nome", "municipio", "NM_MUNICIPIO"])
            if column:
                names.update(str(value).strip() for value in df[column].dropna().unique() if str(value).strip())
        return sorted(names)

    def build(self, municipio: str, *, top_n: int = 20) -> MunicipalStrategyView:
        frames = self._load_frames()
        ranking = self._filter_municipio(frames.get("serving_territory_ranking", pd.DataFrame()), municipio)
        recs = self._filter_municipio(frames.get("serving_allocation_recommendations", pd.DataFrame()), municipio)
        zone_fact = self._filter_municipio(frames.get("fact_zona_eleitoral", pd.DataFrame()), municipio)
        priority_gold = self._filter_municipio(frames.get("gold_priority_score", pd.DataFrame()), municipio)
        base = ranking if not ranking.empty else priority_gold
        metrics = self._metrics(base=base, zone_fact=zone_fact)
        missing = self._missing_fields(base=base, zone_fact=zone_fact)
        output = self._ranking_table(base=base, recs=recs, top_n=top_n)
        recommendations = self._recommendations(metrics=metrics, ranking=output, recs=recs)
        return MunicipalStrategyView(
            municipio=municipio,
            snapshot_id=str(frames.get("_snapshot_id", "")),
            metrics=metrics,
            priority_ranking=output,
            recommendations=recommendations,
            rag_context=self._rag_context(base=base, zone_fact=zone_fact),
            missing_fields=missing,
            source_paths=[str(path) for path in frames.get("_source_paths", [])],
        )

    def _load_frames(self) -> dict[str, Any]:
        frames: dict[str, Any] = {"_source_paths": []}
        try:
            result = self.serving.read_output("serving_territory_ranking", tenant_id=self.paths.tenant_id, limit=0)
            frames["serving_territory_ranking"] = pd.DataFrame(result.records)
            frames["_snapshot_id"] = result.snapshot_id
            frames["_source_paths"].append(result.path)
        except ServingDataNotFoundError:
            frames["serving_territory_ranking"] = pd.DataFrame()
        try:
            result = self.serving.read_output(
                "serving_allocation_recommendations", tenant_id=self.paths.tenant_id, limit=0
            )
            frames["serving_allocation_recommendations"] = pd.DataFrame(result.records)
            frames["_source_paths"].append(result.path)
        except ServingDataNotFoundError:
            frames["serving_allocation_recommendations"] = pd.DataFrame()
        gold_priority = self._latest_parquet("lake/gold/marts", "gold_priority_score.parquet")
        if gold_priority:
            frames["gold_priority_score"] = pd.read_parquet(gold_priority)
            frames["_source_paths"].append(gold_priority)
        zone_fact = self._latest_parquet("data_lake/gold", "fact_zona_eleitoral_*.parquet")
        if zone_fact:
            frames["fact_zona_eleitoral"] = pd.read_parquet(zone_fact)
            frames["_source_paths"].append(zone_fact)
        return frames

    def _latest_parquet(self, root: str, pattern: str) -> Path | None:
        base = Path(root)
        if not base.is_absolute():
            base = self.paths.data_root / base
        if not base.exists():
            return None
        candidates = sorted(base.rglob(pattern), key=lambda path: path.stat().st_mtime, reverse=True)
        return candidates[0] if candidates else None

    def _filter_municipio(self, df: pd.DataFrame, municipio: str) -> pd.DataFrame:
        if df.empty:
            return df
        column = first_existing(df, ["municipio_nome", "municipio", "NM_MUNICIPIO"])
        if column is None:
            return pd.DataFrame(columns=df.columns)
        return df[df[column].astype(str).str.upper().str.strip().eq(municipio.upper().strip())].copy()

    def _metrics(self, *, base: pd.DataFrame, zone_fact: pd.DataFrame) -> dict[str, Any]:
        metrics: dict[str, Any] = {
            "territorios": int(len(base)),
            "score_prioridade_medio": self._mean(base, ["score_prioridade_final", "score_prioridade"]),
            "score_prioridade_max": self._max(base, ["score_prioridade_final", "score_prioridade"]),
            "score_disputabilidade": self._mean(base, ["score_disputabilidade"]),
            "competitividade": self._mean(zone_fact, ["competitividade", "competition_score"]),
            "margem": self._mean(base, ["margem", "leader_margin_score"]),
            "volatilidade": self._mean(zone_fact, ["volatilidade_historica", "volatilidade"]),
            "partido_dominante": self._first(base, ["partido_dominante", "leader_party"]),
            "votos_hist_pt": self._sum(base, ["votos_hist_pt", "votos_pt"]),
            "votos_hist_pl": self._sum(base, ["votos_hist_pl", "votos_pl"]),
            "confianca_media": self._mean(base, ["confidence_score", "join_confidence"]),
        }
        return metrics

    def _ranking_table(self, *, base: pd.DataFrame, recs: pd.DataFrame, top_n: int) -> pd.DataFrame:
        if base.empty:
            return pd.DataFrame()
        out = base.copy()
        if not recs.empty and "territorio_id" in out.columns and "territorio_id" in recs.columns:
            rec_cols = [
                column
                for column in ["territorio_id", "tipo_acao_sugerida", "recurso_sugerido", "justificativa"]
                if column in recs.columns and column not in out.columns
            ]
            if rec_cols:
                out = out.merge(recs[["territorio_id", *rec_cols]].drop_duplicates("territorio_id"), on="territorio_id")
        sort_col = first_existing(out, ["score_prioridade_final", "score_prioridade", "rank"])
        if sort_col:
            out = out.sort_values(sort_col, ascending=sort_col == "rank")
        preferred = [
            "rank",
            "territorio_id",
            "zona",
            "secao",
            "local_votacao",
            "score_prioridade_final",
            "score_disputabilidade",
            "competition_score",
            "join_confidence",
            "confidence_score",
            "tipo_acao_sugerida",
            "recurso_sugerido",
            "justificativa",
        ]
        columns = [column for column in preferred if column in out.columns]
        return out[columns].head(top_n) if columns else out.head(top_n)

    def _recommendations(self, *, metrics: dict[str, Any], ranking: pd.DataFrame, recs: pd.DataFrame) -> list[str]:
        if not recs.empty and "justificativa" in recs.columns:
            return [str(value) for value in recs["justificativa"].dropna().astype(str).head(3) if str(value).strip()]
        score = float(metrics.get("score_prioridade_max") or 0.0)
        competition = float(metrics.get("competitividade") or 0.0)
        volatility = float(metrics.get("volatilidade") or 0.0)
        actions: list[str] = []
        if score >= 0.65:
            actions.append("Priorizar as secoes no topo do ranking para presença física e mobilização local.")
        if competition >= 0.7:
            actions.append(
                "Tratar o municipio como disputa sensível: reforçar mensagem comparativa e operação de campo."
            )
        if volatility >= 0.2:
            actions.append("Usar testes de mensagem por zona; volatilidade sugere resposta diferente por território.")
        if ranking.empty:
            actions.append("Not found in repo: ranking gold/serving para este município.")
        return actions or ["Manter monitoramento; não há sinal gold suficiente para recomendação agressiva."]

    def _rag_context(self, *, base: pd.DataFrame, zone_fact: pd.DataFrame) -> dict[str, Any]:
        context: dict[str, Any] = {}
        for df in [base, zone_fact]:
            for column in [
                "contexto_rag",
                "rag_context",
                "score_explanation",
                "join_strategy",
                "source_dataset",
                "source_name",
            ]:
                if column in df.columns and column not in context:
                    values = [str(value) for value in df[column].dropna().astype(str).head(5) if str(value).strip()]
                    if values:
                        context[column] = values
        return context

    def _missing_fields(self, *, base: pd.DataFrame, zone_fact: pd.DataFrame) -> list[str]:
        available = set(base.columns).union(set(zone_fact.columns))
        expected = [
            "score_disputabilidade",
            "margem",
            "volatilidade_historica",
            "partido_dominante",
            "votos_hist_pt",
            "votos_hist_pl",
            "contexto_rag",
        ]
        return [field for field in expected if field not in available]

    def _mean(self, df: pd.DataFrame, aliases: list[str]) -> float | None:
        column = first_existing(df, aliases)
        if column is None or df.empty:
            return None
        values = numeric(df[column])
        return round(float(values.mean()), 6) if len(values) else None

    def _max(self, df: pd.DataFrame, aliases: list[str]) -> float | None:
        column = first_existing(df, aliases)
        if column is None or df.empty:
            return None
        values = numeric(df[column])
        return round(float(values.max()), 6) if len(values) else None

    def _sum(self, df: pd.DataFrame, aliases: list[str]) -> float | None:
        column = first_existing(df, aliases)
        if column is None or df.empty:
            return None
        return round(float(numeric(df[column]).sum()), 6)

    def _first(self, df: pd.DataFrame, aliases: list[str]) -> str | None:
        column = first_existing(df, aliases)
        if column is None or df.empty:
            return None
        values = [str(value) for value in df[column].dropna().astype(str) if str(value).strip()]
        return values[0] if values else None
