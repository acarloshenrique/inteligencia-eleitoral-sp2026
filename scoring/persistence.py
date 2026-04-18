from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from scoring.config import ScoringPersistenceResult


class GoldScoreWriter:
    def write(
        self,
        scored: pd.DataFrame,
        *,
        gold_root: Path,
        dataset_version: str,
        weights: dict[str, float],
    ) -> ScoringPersistenceResult:
        output_dir = gold_root / "scoring" / dataset_version
        output_dir.mkdir(parents=True, exist_ok=True)
        parquet_path = output_dir / "territorial_priority_scores.parquet"
        scored.to_parquet(parquet_path, index=False)
        duckdb_path = self._write_duckdb(scored, output_dir)
        quality = self._quality(scored)
        manifest_path = output_dir / "manifest.json"
        manifest = {
            "dataset": "territorial_priority_scores",
            "dataset_version": dataset_version,
            "created_at_utc": datetime.now(UTC).isoformat(),
            "parquet_path": str(parquet_path),
            "duckdb_path": str(duckdb_path) if duckdb_path else None,
            "rows": int(len(scored)),
            "weights": weights,
            "quality": quality,
            "schema": {col: str(dtype) for col, dtype in scored.dtypes.items()},
        }
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        return ScoringPersistenceResult(
            parquet_path=parquet_path,
            duckdb_path=duckdb_path,
            manifest_path=manifest_path,
            rows=int(len(scored)),
            dataset_version=dataset_version,
            quality=quality,
        )

    def _write_duckdb(self, scored: pd.DataFrame, output_dir: Path) -> Path | None:
        try:
            import duckdb
        except ImportError:
            return None
        duckdb_path = output_dir / "territorial_priority_scores.duckdb"
        with duckdb.connect(str(duckdb_path)) as con:
            con.register("_scores", scored)
            con.execute("CREATE OR REPLACE TABLE territorial_priority_scores AS SELECT * FROM _scores")
        return duckdb_path

    def _quality(self, scored: pd.DataFrame) -> dict[str, Any]:
        if scored.empty:
            return {"rows": 0, "score_min": 0.0, "score_max": 0.0, "score_mean": 0.0, "null_scores": 0}
        score = pd.to_numeric(scored["score_prioridade_final"], errors="coerce")
        return {
            "rows": int(len(scored)),
            "score_min": round(float(score.min()), 6),
            "score_max": round(float(score.max()), 6),
            "score_mean": round(float(score.mean()), 6),
            "null_scores": int(score.isna().sum()),
        }
