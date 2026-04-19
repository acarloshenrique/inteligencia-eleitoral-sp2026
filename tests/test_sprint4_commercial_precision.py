from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from application.commercial_precision_service import CommercialPrecisionService
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


def _recommendations() -> pd.DataFrame:
    rows = []
    for candidate_id in ("cand_1", "cand_2"):
        for scenario_id in ("conservador", "hibrido", "agressivo"):
            rows.append(
                {
                    "candidate_id": candidate_id,
                    "scenario_id": scenario_id,
                    "ano_eleicao": 2024,
                    "uf": "SP",
                    "territorio_id": f"zona:{candidate_id}:001",
                    "municipio_nome": "SAO PAULO",
                    "zona": "0001",
                    "score_prioridade_final": 0.8 if candidate_id == "cand_1" else 0.6,
                    "confidence_score": 0.9,
                    "join_confidence": 0.88,
                    "tipo_recomendacao": "retencao_base",
                    "recurso_sugerido": 10000.0,
                    "percentual_orcamento_sugerido": 0.10,
                    "justificativa": "score e base eleitoral agregada",
                }
            )
    return pd.DataFrame(rows)


def test_commercial_precision_builds_multi_candidate_snapshot_and_demo(tmp_path: Path) -> None:
    operational_path = tmp_path / "operational.parquet"
    scores_path = tmp_path / "scores.parquet"
    recs = _recommendations()
    recs.to_parquet(operational_path, index=False)
    recs.drop(columns=["scenario_id", "tipo_recomendacao", "recurso_sugerido"]).to_parquet(scores_path, index=False)

    result = CommercialPrecisionService(_paths(tmp_path)).run(
        dataset_version="unit",
        tenant_id="tenant_a",
        campaign_id="campanha_sp_2026",
        snapshot_id="snap_unit",
        operational_path=operational_path,
        scores_path=scores_path,
    )

    assert result.rows == 6
    assert result.candidate_count == 2
    assert result.readiness_score >= 0.90
    assert result.multi_candidate_summary_path.exists()
    assert result.campaign_snapshots_path.exists()
    assert result.scenario_comparison_path.exists()
    assert result.demo_workbook_path.read_bytes().startswith(b"PK")
    assert result.demo_markdown_path.read_text(encoding="utf-8").startswith("# Demo comercial SP 2024/2026")

    summary = pd.read_parquet(result.multi_candidate_summary_path)
    assert set(summary["candidate_id"]) == {"cand_1", "cand_2"}
    comparison = pd.read_parquet(result.scenario_comparison_path)
    assert set(comparison["scenario_id"]) == {"conservador", "hibrido", "agressivo"}
    readiness = json.loads(result.readiness_json_path.read_text(encoding="utf-8"))
    assert readiness["checks"]["multi_candidate_data"] is True
    assert readiness["checks"]["real_sp_2024_data"] is True
    assert readiness["status"] == "commercial_demo_ready"
