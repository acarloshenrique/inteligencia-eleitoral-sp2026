from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from application.operational_recommendation_service import OperationalRecommendationService
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


def _scores() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "score_record_id": "zona:2024:SP:71072:0001",
                "score_granularity": "zona",
                "candidate_id": "cand_1",
                "ano_eleicao": 2024,
                "uf": "SP",
                "cod_municipio_tse": "71072",
                "cod_municipio_ibge": "3550308",
                "municipio_nome": "SAO PAULO",
                "zona": "0001",
                "base_eleitoral_score": 0.85,
                "afinidade_tematica_score": 0.70,
                "potencial_expansao_score": 0.30,
                "custo_eficiencia_score": 0.80,
                "concorrencia_score": 0.40,
                "capacidade_operacional_score": 0.75,
                "score_prioridade_final": 0.82,
                "join_confidence": 0.92,
                "data_quality_score": 0.90,
            },
            {
                "score_record_id": "zona:2024:SP:71072:0002",
                "score_granularity": "zona",
                "candidate_id": "cand_1",
                "ano_eleicao": 2024,
                "uf": "SP",
                "cod_municipio_tse": "71072",
                "cod_municipio_ibge": "3550308",
                "municipio_nome": "SAO PAULO",
                "zona": "0002",
                "base_eleitoral_score": 0.20,
                "afinidade_tematica_score": 0.60,
                "potencial_expansao_score": 0.90,
                "custo_eficiencia_score": 0.60,
                "concorrencia_score": 0.50,
                "capacidade_operacional_score": 0.75,
                "score_prioridade_final": 0.76,
                "join_confidence": 0.85,
                "data_quality_score": 0.82,
            },
        ]
    )


def test_operational_service_builds_three_scenarios_and_exports(tmp_path: Path) -> None:
    scores_path = tmp_path / "scores.parquet"
    _scores().to_parquet(scores_path, index=False)

    result = OperationalRecommendationService(_paths(tmp_path)).run(
        scores_path=scores_path,
        dataset_version="unit",
        budget_total=90000,
        top_n=2,
        score_granularity="zona",
    )

    assert result.rows == 6
    assert result.recommendations_path.exists()
    assert result.summary_path.exists()
    assert result.executive_pdf_path.read_bytes().startswith(b"%PDF")
    assert result.workbook_path.read_bytes().startswith(b"PK")

    recommendations = pd.read_parquet(result.recommendations_path)
    assert set(recommendations["scenario_id"]) == {"conservador", "hibrido", "agressivo"}
    assert {"tipo_recomendacao", "canal_ideal", "mensagem_ideal", "justificativa"}.issubset(
        recommendations.columns
    )
    by_scenario = recommendations.groupby("scenario_id")["recurso_sugerido"].sum().round(2).to_dict()
    assert by_scenario == {"agressivo": 90000.0, "conservador": 90000.0, "hibrido": 90000.0}
    summary = json.loads(result.summary_path.read_text(encoding="utf-8"))
    assert len(summary["scenarios"]) == 3
    assert summary["scenarios"][0]["by_action"]
