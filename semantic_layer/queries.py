from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from semantic_layer.models import SemanticMetric
from semantic_layer.registry import build_semantic_registry


class SemanticQueryError(ValueError):
    pass


class SemanticQueryService:
    def __init__(self, tables: dict[str, pd.DataFrame] | None = None):
        self.registry = build_semantic_registry()
        self.tables = tables or {}

    @classmethod
    def from_parquet_paths(cls, paths: dict[str, Path]) -> "SemanticQueryService":
        return cls({name: pd.read_parquet(path) for name, path in paths.items()})

    def metric_frame(
        self,
        metric_id: str,
        *,
        filters: dict[str, Any] | None = None,
        limit: int | None = None,
    ) -> pd.DataFrame:
        metric = self._metric(metric_id)
        table = self._table(metric.source_table)
        frame = table.copy()
        frame = self._apply_filters(frame, filters or {})
        frame = self._ensure_metric_value(frame, metric)
        columns = self._dimension_columns(frame, metric) + ["metric_id", "metric_name", "metric_value"]
        out = frame[columns].copy()
        if limit is not None:
            out = out.head(limit)
        return out.reset_index(drop=True)

    def territory_ranking(
        self,
        *,
        candidate_id: str | None = None,
        limit: int = 20,
    ) -> pd.DataFrame:
        filters = {"candidate_id": candidate_id} if candidate_id else {}
        frame = self.metric_frame("prioridade_territorial", filters=filters)
        return frame.sort_values("metric_value", ascending=False).head(limit).reset_index(drop=True)

    def allocation_recommendations(
        self,
        *,
        candidate_id: str | None = None,
        scenario_id: str | None = None,
        limit: int = 50,
    ) -> pd.DataFrame:
        table = self._table("gold_allocation_recommendations").copy()
        filters = {}
        if candidate_id:
            filters["candidate_id"] = candidate_id
        if scenario_id:
            filters["scenario_id"] = scenario_id
        table = self._apply_filters(table, filters)
        sort_col = "recurso_sugerido" if "recurso_sugerido" in table.columns else "score_prioridade_final"
        return table.sort_values(sort_col, ascending=False).head(limit).reset_index(drop=True)

    def entity_dimensions(self, entity_id: str) -> list[str]:
        entity = self.registry.entity(entity_id)
        if entity is None:
            raise SemanticQueryError(f"Unknown semantic entity: {entity_id}")
        return entity.dimensions

    def metric_catalog(self) -> pd.DataFrame:
        return pd.DataFrame([metric.model_dump(mode="json") for metric in self.registry.metrics])

    def entity_catalog(self) -> pd.DataFrame:
        return pd.DataFrame([entity.model_dump(mode="json") for entity in self.registry.entities])

    def _metric(self, metric_id: str) -> SemanticMetric:
        metric = self.registry.metric(metric_id)
        if metric is None:
            raise SemanticQueryError(f"Unknown semantic metric: {metric_id}")
        return metric

    def _table(self, table_name: str) -> pd.DataFrame:
        if table_name not in self.tables:
            raise SemanticQueryError(f"Table not loaded for semantic query: {table_name}")
        return self.tables[table_name]

    def _apply_filters(self, frame: pd.DataFrame, filters: dict[str, Any]) -> pd.DataFrame:
        out = frame
        for column, value in filters.items():
            if value is None or column not in out.columns:
                continue
            out = out[out[column].astype(str).eq(str(value))]
        return out

    def _ensure_metric_value(self, frame: pd.DataFrame, metric: SemanticMetric) -> pd.DataFrame:
        out = frame.copy()
        if metric.metric_id == "aderencia_tematica":
            grouped = (
                out.groupby("territorio_id", dropna=False)["thematic_affinity_score"]
                .mean()
                .reset_index(name="metric_value")
            )
            grouped["metric_id"] = metric.metric_id
            grouped["metric_name"] = metric.name
            return grouped
        if metric.metric_id == "confianca_recomendacao" and "confidence_score" not in out.columns:
            priority = pd.to_numeric(out.get("score_prioridade_final", 0.0), errors="coerce").fillna(0.0)
            pct = pd.to_numeric(out.get("percentual_orcamento_sugerido", 0.0), errors="coerce").fillna(0.0)
            out["metric_value"] = (priority * 0.8 + pct.clip(0, 1) * 0.2).clip(0, 1)
        else:
            source_column = metric.source_columns[0]
            if source_column not in out.columns:
                raise SemanticQueryError(f"Metric source column missing: {source_column}")
            out["metric_value"] = pd.to_numeric(out[source_column], errors="coerce")
        out["metric_id"] = metric.metric_id
        out["metric_name"] = metric.name
        return out

    def _dimension_columns(self, frame: pd.DataFrame, metric: SemanticMetric) -> list[str]:
        candidates = [
            "candidate_id",
            "territorio_id",
            "scenario_id",
            "uf",
            "municipio_nome",
            "zona",
            "tema",
            "territorial_cluster_id",
        ]
        return [column for column in candidates if column in frame.columns]
