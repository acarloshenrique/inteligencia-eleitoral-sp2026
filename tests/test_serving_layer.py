from __future__ import annotations

import json

import pandas as pd

from serving.builder import ServingLayerBuilder
from serving.writer import ServingLayerWriter


def sample_tables() -> dict[str, pd.DataFrame]:
    return {
        "gold_priority_score": pd.DataFrame(
            [
                {
                    "candidate_id": "cand_1",
                    "territorio_id": "2024:SP:71072:Z1",
                    "territorial_cluster_id": "cluster_1",
                    "score_prioridade_final": 0.92,
                    "score_explanation": "base=0.8; tema=0.7",
                    "join_confidence": 0.9,
                    "data_quality_score": 0.88,
                },
                {
                    "candidate_id": "cand_1",
                    "territorio_id": "2024:SP:71072:Z2",
                    "territorial_cluster_id": "cluster_2",
                    "score_prioridade_final": 0.71,
                    "score_explanation": "expansao=0.7",
                    "join_confidence": 0.8,
                    "data_quality_score": 0.75,
                },
            ]
        ),
        "gold_territory_profile": pd.DataFrame(
            [
                {
                    "territorio_id": "2024:SP:71072:Z1",
                    "uf": "SP",
                    "municipio_nome": "SAO PAULO",
                    "zona": "0001",
                    "data_quality_score": 0.88,
                },
                {
                    "territorio_id": "2024:SP:71072:Z2",
                    "uf": "SP",
                    "municipio_nome": "SAO PAULO",
                    "zona": "0002",
                    "data_quality_score": 0.75,
                },
            ]
        ),
        "gold_allocation_recommendations": pd.DataFrame(
            [
                {
                    "scenario_id": "hibrido",
                    "candidate_id": "cand_1",
                    "territorio_id": "2024:SP:71072:Z1",
                    "tipo_acao_sugerida": "retencao_base",
                    "score_prioridade_final": 0.92,
                    "recurso_sugerido": 10000.0,
                    "percentual_orcamento_sugerido": 0.4,
                    "justificativa": "Alta prioridade territorial.",
                }
            ]
        ),
        "gold_territorial_electoral_master_index": pd.DataFrame([{"master_record_id": "m1", "join_confidence": 0.9}]),
        "lake_health_report": pd.DataFrame([{"aggregate_quality_score": 0.86}]),
    }


def test_serving_builder_materializes_api_outputs() -> None:
    result = ServingLayerBuilder().build(
        tenant_id="cliente_a",
        campaign_id="campanha_2026",
        snapshot_id="s001",
        dataset_version="gold-test",
        tables=sample_tables(),
    )

    assert set(result.outputs) == {
        "serving_territory_ranking",
        "serving_allocation_recommendations",
        "serving_data_readiness",
    }
    ranking = result.outputs["serving_territory_ranking"]
    recommendations = result.outputs["serving_allocation_recommendations"]
    readiness = result.outputs["serving_data_readiness"]
    assert ranking["tenant_id"].eq("cliente_a").all()
    assert ranking.iloc[0]["rank"] == 1
    assert recommendations.iloc[0]["evidence_ids"].startswith("ev:hibrido:cand_1")
    assert float(readiness.iloc[0]["readiness_score"]) > 0.8


def test_serving_writer_persists_manifest_and_outputs(tmp_path) -> None:
    result = ServingLayerBuilder().build(
        tenant_id="cliente_a",
        campaign_id="campanha_2026",
        snapshot_id="s001",
        dataset_version="gold-test",
        tables=sample_tables(),
    )

    manifest = ServingLayerWriter(tmp_path).write(
        result=result,
        tenant_id="cliente_a",
        campaign_id="campanha_2026",
        snapshot_id="s001",
        dataset_version="gold-test",
        source_tables=list(sample_tables()),
    )

    root = tmp_path / "tenants" / "cliente_a" / "serving" / "campaign_id=campanha_2026" / "snapshot_id=s001"
    assert (root / "serving_manifest.json").exists()
    assert (root / "serving_territory_ranking" / "serving_territory_ranking.parquet").exists()
    assert manifest.row_counts["serving_territory_ranking"] == 2
    payload = json.loads((root / "serving_manifest.json").read_text(encoding="utf-8"))
    assert payload["tenant_id"] == "cliente_a"
