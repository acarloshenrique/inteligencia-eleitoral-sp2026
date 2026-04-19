from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from scoring.backtest import ScoreBacktester
from scoring.config import SCORE_COLUMNS, load_score_weights
from scoring.priority_score import ScoringEngine


@dataclass(frozen=True)
class CalibratedScoringResult:
    scored_path: Path
    backtest_metrics_path: Path
    rows: int
    granularities: list[str]
    weights_version: str
    backtest_metrics: dict[str, Any]


class CalibratedScoringService:
    def __init__(self, paths) -> None:
        self.paths = paths

    def run(
        self,
        *,
        input_path: Path | None = None,
        weights_path: Path | None = None,
        dataset_version: str = "calibrated",
        thematic_vector: dict[str, float] | None = None,
        capacidade_operacional: float = 0.7,
        granularities: tuple[str, ...] = ("municipio", "zona", "secao"),
        top_n: int = 20,
    ) -> CalibratedScoringResult:
        source_path = input_path or self._find_latest_master()
        if source_path is None:
            raise FileNotFoundError("Not found in repo: master index/gold para scoring calibravel.")
        territories = pd.read_parquet(source_path)
        territories = self._prepare_territories(territories)
        weights = load_score_weights(weights_path or self._default_weights_path())
        engine = ScoringEngine(weights=weights)
        scored = engine.score_by_granularity(
            territories,
            thematic_vector=thematic_vector or {"geral": 1.0},
            capacidade_operacional=capacidade_operacional,
            granularities=granularities,
        )
        output_dir = self.paths.lakehouse_root / "gold" / "marts" / dataset_version / "gold_calibrated_priority_scores"
        output_dir.mkdir(parents=True, exist_ok=True)
        scored_path = output_dir / "gold_calibrated_priority_scores.parquet"
        scored.to_parquet(scored_path, index=False)
        backtest = ScoreBacktester().run(
            scored,
            predicted_score_col="score_prioridade_final",
            top_n=top_n,
        )
        backtest_paths = ScoreBacktester().write(
            backtest,
            self.paths.lakehouse_root / "quality" / dataset_version / "score_backtest",
        )
        return CalibratedScoringResult(
            scored_path=scored_path,
            backtest_metrics_path=Path(backtest_paths["metrics"]),
            rows=int(len(scored)),
            granularities=list(granularities),
            weights_version=engine.weights_version,
            backtest_metrics=backtest.metrics,
        )

    def _default_weights_path(self) -> Path:
        tenant_path = (
            self.paths.data_root
            / "data"
            / "tenants"
            / self.paths.tenant_id
            / "config"
            / "scoring_weights.yaml"
        )
        if tenant_path.exists():
            return tenant_path
        return self.paths.data_root / "config" / "scoring_weights.yaml"

    def _find_latest_master(self) -> Path | None:
        candidates: list[Path] = []
        for root in [self.paths.lakehouse_root, self.paths.lake_root, self.paths.gold_root]:
            if root.exists():
                candidates.extend(
                    path
                    for path in root.rglob("*.parquet")
                    if "gold_territorial_electoral_master_index" in path.name
                    or "territorial_electoral_master_index" in str(path.parent)
                )
        return sorted(candidates, key=lambda path: path.stat().st_mtime, reverse=True)[0] if candidates else None

    def _prepare_territories(self, frame: pd.DataFrame) -> pd.DataFrame:
        out = frame.copy()
        out["base_context_score"] = self._normalized(out, ["votos_total", "votos_nominais", "votos", "votos_validos"])
        out["competitividade"] = self._competition_proxy(out)
        out["custo_operacional_estimado"] = 1.0 - self._normalized(out, ["source_coverage_score", "join_confidence"])
        out["abstencao_pct"] = 0.0
        if "source_coverage_score" in out.columns:
            out["data_quality_score"] = pd.to_numeric(out["source_coverage_score"], errors="coerce").fillna(0.0)
        if "join_confidence" in out.columns:
            out["confidence_score"] = pd.to_numeric(out["join_confidence"], errors="coerce").fillna(0.0)
        out["indicadores_tematicos"] = [{"geral": 0.5} for _ in range(len(out))]
        for column in SCORE_COLUMNS:
            if column in out.columns:
                out = out.drop(columns=[column])
        return out

    def _normalized(self, frame: pd.DataFrame, aliases: list[str]) -> pd.Series:
        column = next((name for name in aliases if name in frame.columns), None)
        if column is None:
            return pd.Series([0.5] * len(frame), index=frame.index)
        values = pd.to_numeric(frame[column], errors="coerce").fillna(0.0)
        max_value = float(values.max()) if len(values) else 0.0
        if max_value <= 0:
            return pd.Series([0.5] * len(frame), index=frame.index)
        return (values / max_value).clip(0, 1)

    def _competition_proxy(self, frame: pd.DataFrame) -> pd.Series:
        if "competition_score" in frame.columns:
            return pd.to_numeric(frame["competition_score"], errors="coerce").fillna(0.5).clip(0, 1)
        if "join_confidence" in frame.columns:
            confidence = pd.to_numeric(frame["join_confidence"], errors="coerce").fillna(0.5)
            return (1.0 - (confidence - confidence.mean()).abs()).clip(0, 1)
        return pd.Series([0.5] * len(frame), index=frame.index)
