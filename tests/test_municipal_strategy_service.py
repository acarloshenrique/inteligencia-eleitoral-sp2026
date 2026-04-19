from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from application.municipal_strategy_service import MunicipalStrategyService
from config.settings import AppPaths


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


def test_municipal_strategy_uses_serving_and_marks_missing_fields(tmp_path: Path) -> None:
    serving_root = tmp_path / "lake" / "serving" / "campaign_id=campanha" / "snapshot_id=s1"
    ranking_dir = serving_root / "serving_territory_ranking"
    zone_dir = serving_root / "serving_municipality_zone_detail"
    recs_dir = serving_root / "serving_allocation_recommendations"
    ranking_dir.mkdir(parents=True)
    zone_dir.mkdir(parents=True)
    recs_dir.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "candidate_id": "aggregate",
                "municipio_nome": "SAO PAULO",
                "territorio_id": "2024:SP:71072:Z0001:S0001",
                "zona": "0001",
                "secao": "0001",
                "score_prioridade_final": 0.8,
                "confidence_score": 0.9,
            }
        ]
    ).to_parquet(ranking_dir / "serving_territory_ranking.parquet", index=False)
    pd.DataFrame(
        [
            {
                "candidate_id": "aggregate",
                "municipio_nome": "SAO PAULO",
                "zona_id": "2024:SP:71072:Z0001",
                "zona": "0001",
                "secoes": 12,
                "score_prioridade_final": 0.82,
                "score_disputabilidade": 0.71,
                "join_confidence": 0.9,
                "data_quality_score": 0.88,
                "recomendacao_curta": "priorizar reforco em area competitiva",
            }
        ]
    ).to_parquet(zone_dir / "serving_municipality_zone_detail.parquet", index=False)
    pd.DataFrame(
        [
            {
                "candidate_id": "aggregate",
                "municipio_nome": "SAO PAULO",
                "territorio_id": "2024:SP:71072:Z0001:S0001",
                "justificativa": "Prioridade alta para operação local.",
            }
        ]
    ).to_parquet(recs_dir / "serving_allocation_recommendations.parquet", index=False)
    (serving_root / "serving_manifest.json").write_text(json.dumps({"warnings": []}), encoding="utf-8")

    zone_root = tmp_path / "data_lake" / "gold"
    zone_root.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "municipio": "SAO PAULO",
                "competitividade": 0.7,
                "volatilidade_historica": 0.25,
            }
        ]
    ).to_parquet(zone_root / "fact_zona_eleitoral_test.parquet", index=False)

    view = MunicipalStrategyService(_paths(tmp_path)).build("SAO PAULO", top_n=5)

    assert view.metrics["score_prioridade_max"] == 0.82
    assert view.metrics["competitividade"] == 0.7
    assert not view.zone_ranking.empty
    assert view.zone_ranking.iloc[0]["zona"] == "0001"
    assert view.recommendations == ["Prioridade alta para operação local."]
    assert "votos_hist_pt" in view.missing_fields
