from __future__ import annotations

from pathlib import Path

import pandas as pd

from application.calibrated_scoring_service import CalibratedScoringService
from config.settings import AppPaths
from scoring.backtest import ScoreBacktester


def _paths(root: Path) -> AppPaths:
    return AppPaths(
        data_root=root,
        ingestion_root=root / "ingestion",
        lake_root=root / "data_lake",
        bronze_root=root / "data_lake" / "bronze",
        silver_root=root / "data_lake" / "silver",
        gold_root=root / "data_lake" / "gold",
        gold_reports_root=root / "data_lake" / "gold" / "reports",
        gold_serving_root=root / "data_lake" / "gold" / "serving",
        catalog_root=root / "data_lake" / "catalog",
        chromadb_path=root / "chromadb",
        runtime_reports_root=root / "reports",
        ts="test",
        metadata_db_path=root / "metadata.sqlite3",
        artifact_root=root / "artifacts",
        tenant_id="default",
        tenant_root=root,
    )


def test_score_backtester_computes_rank_metrics() -> None:
    predicted = pd.DataFrame(
        [
            {"score_record_id": "a", "score_prioridade_final": 0.9, "actual_score": 100},
            {"score_record_id": "b", "score_prioridade_final": 0.5, "actual_score": 20},
            {"score_record_id": "c", "score_prioridade_final": 0.7, "actual_score": 80},
        ]
    )

    result = ScoreBacktester().run(predicted, top_n=2)

    assert result.metrics["status"] == "ok"
    assert result.metrics["top_n_precision"] == 1.0
    assert result.metrics["spearman_rank_correlation"] > 0


def test_calibrated_scoring_service_persists_scores_and_backtest(tmp_path: Path) -> None:
    input_path = tmp_path / "master.parquet"
    pd.DataFrame(
        [
            {
                "master_record_id": "m1",
                "candidate_id": "cand_1",
                "ano_eleicao": 2024,
                "uf": "SP",
                "cod_municipio_tse": "71072",
                "cod_municipio_ibge": "3550308",
                "municipio_nome": "SAO PAULO",
                "zona": "0001",
                "secao": "0001",
                "votos": 100,
                "join_confidence": 0.9,
                "source_coverage_score": 0.8,
            },
            {
                "master_record_id": "m2",
                "candidate_id": "cand_1",
                "ano_eleicao": 2024,
                "uf": "SP",
                "cod_municipio_tse": "71072",
                "cod_municipio_ibge": "3550308",
                "municipio_nome": "SAO PAULO",
                "zona": "0002",
                "secao": "0002",
                "votos": 20,
                "join_confidence": 0.8,
                "source_coverage_score": 0.7,
            },
        ]
    ).to_parquet(input_path, index=False)

    result = CalibratedScoringService(_paths(tmp_path)).run(
        input_path=input_path,
        dataset_version="unit",
        granularities=("municipio", "zona", "secao"),
    )

    assert result.scored_path.exists()
    assert result.backtest_metrics_path.exists()
    assert result.rows == 5
    scored = pd.read_parquet(result.scored_path)
    assert set(scored["score_granularity"]) == {"municipio", "zona", "secao"}
    assert scored["score_prioridade_final"].between(0, 1).all()
