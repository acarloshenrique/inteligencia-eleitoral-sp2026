from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from feature_store.models import FeatureSetManifest
from feature_store.pipeline import FeatureComputationResult
from feature_store.registry import FEATURE_REGISTRY


class FeatureStoreWriter:
    def write(
        self,
        result: FeatureComputationResult,
        *,
        output_dir: Path,
        feature_set_id: str = "territorial_recommendation_features",
    ) -> FeatureSetManifest:
        root = output_dir / result.feature_version / feature_set_id
        root.mkdir(parents=True, exist_ok=True)
        parquet_path = root / "features.parquet"
        registry_path = root / "feature_registry.json"
        manifest_path = root / "manifest.json"
        sql_path = root / "duckdb_feature_examples.sql"
        result.features.to_parquet(parquet_path, index=False)
        registry_payload = [feature.model_dump(mode="json") for feature in FEATURE_REGISTRY]
        registry_path.write_text(json.dumps(registry_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        duckdb_path = self._write_duckdb(result.features, root)
        self._write_sql_examples(sql_path)
        manifest = FeatureSetManifest(
            feature_set_id=feature_set_id,
            feature_version=result.feature_version,
            rows=int(len(result.features)),
            features=[feature.feature_name for feature in FEATURE_REGISTRY],
            lineage=result.lineage,
            output_path=str(parquet_path),
            duckdb_path=str(duckdb_path) if duckdb_path else None,
            computed_at_utc=result.computed_at_utc,
            quality=self._quality(result.features),
        )
        manifest_path.write_text(
            json.dumps(manifest.model_dump(mode="json"), ensure_ascii=False, indent=2), encoding="utf-8"
        )
        return manifest

    def _quality(self, features: pd.DataFrame) -> dict[str, Any]:
        quality: dict[str, Any] = {
            "rows": int(len(features)),
            "columns": int(len(features.columns)),
            "candidate_count": int(features["candidate_id"].nunique()) if "candidate_id" in features else 0,
            "territory_count": int(features["territorio_id"].nunique()) if "territorio_id" in features else 0,
        }
        for spec in FEATURE_REGISTRY:
            if spec.feature_name not in features.columns:
                quality[f"{spec.feature_name}_missing"] = 1
                continue
            if spec.dtype in {"float", "int"}:
                values = pd.to_numeric(features[spec.feature_name], errors="coerce")
                quality[f"{spec.feature_name}_nulls"] = int(values.isna().sum())
                quality[f"{spec.feature_name}_min"] = (
                    round(float(values.min()), 6) if not values.dropna().empty else 0.0
                )
                quality[f"{spec.feature_name}_max"] = (
                    round(float(values.max()), 6) if not values.dropna().empty else 0.0
                )
        return quality

    def _write_duckdb(self, features: pd.DataFrame, root: Path) -> Path | None:
        try:
            import duckdb
        except ImportError:
            return None
        path = root / "feature_store.duckdb"
        with duckdb.connect(str(path)) as con:
            con.register("_features", features)
            con.execute("CREATE OR REPLACE TABLE territorial_recommendation_features AS SELECT * FROM _features")
        return path

    def _write_sql_examples(self, path: Path) -> None:
        sql = """-- Feature store consumption examples
-- Scoring frame by candidate and territory
SELECT
  candidate_id,
  territorio_id,
  retention_score AS base_context_score,
  competitive_intensity AS concorrencia_score,
  spend_result_elasticity AS custo_eficiencia_score,
  candidate_territory_thematic_affinity AS afinidade_tematica_feature
FROM territorial_recommendation_features;

-- Operationally difficult high-potential territories
SELECT candidate_id, territorio_id, retention_score, logistical_complexity, polling_place_centrality
FROM territorial_recommendation_features
WHERE retention_score >= 0.6 OR candidate_territory_thematic_affinity >= 0.7
ORDER BY logistical_complexity DESC;
"""
        path.write_text(sql, encoding="utf-8")
