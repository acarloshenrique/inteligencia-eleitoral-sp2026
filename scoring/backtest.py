from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class BacktestResult:
    metrics: dict[str, Any]
    detail: pd.DataFrame


class ScoreBacktester:
    def run(
        self,
        predicted: pd.DataFrame,
        *,
        actual: pd.DataFrame | None = None,
        key_columns: list[str] | None = None,
        predicted_score_col: str = "score_prioridade_final",
        actual_score_col: str = "actual_score",
        top_n: int = 20,
    ) -> BacktestResult:
        if predicted.empty:
            return BacktestResult(metrics={"rows": 0, "status": "empty"}, detail=pd.DataFrame())
        keys = key_columns or self._infer_keys(predicted, actual)
        frame = predicted.copy()
        if actual is not None and not actual.empty:
            frame = frame.merge(actual[keys + [actual_score_col]], on=keys, how="inner")
        elif actual_score_col not in frame.columns:
            actual_score_col = self._infer_actual_column(frame)
        if actual_score_col not in frame.columns:
            return BacktestResult(
                metrics={
                    "rows": int(len(frame)),
                    "status": "actual_not_found",
                    "message": "Not found in repo: coluna de resultado observado para backtest.",
                },
                detail=frame,
            )
        frame = frame.dropna(subset=[predicted_score_col, actual_score_col]).copy()
        if frame.empty:
            return BacktestResult(metrics={"rows": 0, "status": "no_overlap"}, detail=frame)
        frame["predicted_rank"] = pd.to_numeric(frame[predicted_score_col], errors="coerce").rank(
            method="first", ascending=False
        )
        frame["actual_rank"] = pd.to_numeric(frame[actual_score_col], errors="coerce").rank(
            method="first", ascending=False
        )
        frame["absolute_rank_error"] = (frame["predicted_rank"] - frame["actual_rank"]).abs()
        top_pred = set(frame.nsmallest(top_n, "predicted_rank").index)
        top_actual = set(frame.nsmallest(top_n, "actual_rank").index)
        precision = len(top_pred & top_actual) / max(len(top_pred), 1)
        rank_corr = frame["predicted_rank"].corr(frame["actual_rank"], method="spearman")
        metrics = {
            "status": "ok",
            "rows": int(len(frame)),
            "top_n": int(top_n),
            "top_n_precision": round(float(precision), 6),
            "spearman_rank_correlation": round(float(rank_corr), 6) if pd.notna(rank_corr) else 0.0,
            "mean_absolute_rank_error": round(float(frame["absolute_rank_error"].mean()), 6),
            "predicted_score_col": predicted_score_col,
            "actual_score_col": actual_score_col,
            "key_columns": keys,
        }
        return BacktestResult(metrics=metrics, detail=frame.sort_values("predicted_rank"))

    def write(self, result: BacktestResult, output_dir: Path) -> dict[str, str]:
        output_dir.mkdir(parents=True, exist_ok=True)
        metrics_path = output_dir / "backtest_metrics.json"
        detail_path = output_dir / "backtest_detail.parquet"
        payload = {"generated_at_utc": datetime.now(UTC).isoformat(), **result.metrics}
        metrics_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        result.detail.to_parquet(detail_path, index=False)
        return {"metrics": str(metrics_path), "detail": str(detail_path)}

    def _infer_keys(self, predicted: pd.DataFrame, actual: pd.DataFrame | None) -> list[str]:
        candidates = [
            "score_record_id",
            "territorio_id",
            "zona_id",
            "municipio_nome",
            "zona",
            "secao",
        ]
        source = predicted if actual is None or actual.empty else predicted[predicted.columns.intersection(actual.columns)]
        keys = [column for column in candidates if column in source.columns]
        return keys[:1] if keys and keys[0] in {"score_record_id", "territorio_id", "zona_id"} else keys

    def _infer_actual_column(self, frame: pd.DataFrame) -> str:
        for column in [
            "actual_score",
            "resultado_real_score",
            "votos_observados",
            "votos_total",
            "votos_nominais",
            "votos",
            "votos_validos",
        ]:
            if column in frame.columns:
                return column
        return "actual_score"
