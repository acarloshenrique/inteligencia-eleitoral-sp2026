from __future__ import annotations

import json

import pandas as pd

from commercial.exports import CommercialExportService
from commercial.marts import CommercialMartBuilder
from commercial.snapshots import CampaignSnapshotStore, build_snapshot_spec
from commercial.strategy import (
    competitive_dataset_ranking,
    default_tenant_policy,
    exportable_artifacts,
    multi_candidate_tables,
)


def sample_gold_tables() -> dict[str, pd.DataFrame]:
    return {
        "gold_priority_score": pd.DataFrame(
            [
                {
                    "candidate_id": "cand_1",
                    "territorio_id": "SP-001-0001",
                    "municipio_nome": "SAO PAULO",
                    "zona": "001",
                    "score_prioridade_final": 0.91,
                    "score_explanation": "Base forte e boa eficiência.",
                },
                {
                    "candidate_id": "cand_2",
                    "territorio_id": "SP-002-0001",
                    "municipio_nome": "CAMPINAS",
                    "zona": "002",
                    "score_prioridade_final": 0.73,
                    "score_explanation": "Expansão plausível.",
                },
            ]
        ),
        "gold_allocation_recommendations": pd.DataFrame(
            [
                {
                    "scenario_id": "hibrido",
                    "candidate_id": "cand_1",
                    "territorio_id": "SP-001-0001",
                    "tipo_acao_sugerida": "retencao_base",
                    "score_prioridade_final": 0.91,
                    "recurso_sugerido": 50000,
                    "percentual_orcamento_sugerido": 0.25,
                    "justificativa": "Alta prioridade com confiança adequada.",
                }
            ]
        ),
        "gold_territorial_electoral_master_index": pd.DataFrame(
            [
                {"master_record_id": "m1", "join_confidence": 0.95},
                {"master_record_id": "m2", "join_confidence": 0.85},
            ]
        ),
        "lake_health_report": pd.DataFrame([{"aggregate_quality_score": 0.88}]),
    }


def test_commercial_strategy_prioritizes_critical_assets() -> None:
    ranking = competitive_dataset_ranking()
    assert ranking[0].impact == "critical"
    assert "gold_priority_score" in multi_candidate_tables()
    assert "premium_report_tables.xlsx" in exportable_artifacts()
    policy = default_tenant_policy(tenant_id="cliente_a", data_root="lake")
    assert policy.logical_filters == {"tenant_id": "cliente_a"}
    assert policy.storage_root.endswith("lake/tenants/cliente_a")


def test_commercial_marts_support_multi_candidate_snapshots() -> None:
    result = CommercialMartBuilder().build(
        tenant_id="cliente_a",
        campaign_id="campanha_2026",
        snapshot_id="s001",
        gold_tables=sample_gold_tables(),
    )

    demo = result.marts["commercial_demo_summary"]
    pitch = result.marts["commercial_pitch_metrics"]

    assert set(result.marts) == {"commercial_demo_summary", "premium_report_tables", "commercial_pitch_metrics"}
    assert demo["tenant_id"].eq("cliente_a").all()
    assert demo["campaign_id"].eq("campanha_2026").all()
    assert int(pitch.loc[0, "candidates_supported"]) == 2
    assert round(float(pitch.loc[0, "avg_join_confidence"]), 2) == 0.9


def test_snapshot_store_writes_tenant_scoped_parquet(tmp_path) -> None:
    result = CommercialMartBuilder().build(
        tenant_id="cliente_a",
        campaign_id="campanha_2026",
        snapshot_id="s001",
        gold_tables=sample_gold_tables(),
    )
    spec = build_snapshot_spec(
        tenant_id="cliente_a",
        campaign_id="campanha_2026",
        candidate_ids=["cand_2", "cand_1", "cand_1"],
        snapshot_id="s001",
        dataset_version="gold-test",
        source_tables=["gold_priority_score"],
    )

    written = CampaignSnapshotStore(tmp_path).write_snapshot(spec=spec, marts=result.marts)

    assert written["snapshot"].exists()
    assert written["commercial_demo_summary"].exists()
    assert "tenants" in written["snapshot"].parts
    loaded = CampaignSnapshotStore(tmp_path).read_snapshot_manifest(
        tenant_id="cliente_a",
        campaign_id="campanha_2026",
        snapshot_id="s001",
    )
    assert loaded.candidate_ids == ["cand_1", "cand_2"]


def test_export_service_writes_sales_artifacts(tmp_path) -> None:
    result = CommercialMartBuilder().build(
        tenant_id="cliente_a",
        campaign_id="campanha_2026",
        snapshot_id="s001",
        gold_tables=sample_gold_tables(),
    )

    manifest = CommercialExportService().export(
        marts=result.marts,
        output_dir=tmp_path,
        tenant_id="cliente_a",
        campaign_id="campanha_2026",
        snapshot_id="s001",
    )

    assert (tmp_path / "commercial_demo_summary.md").exists()
    assert (tmp_path / "premium_report_tables.xlsx").exists()
    assert (tmp_path / "commercial_export_manifest.json").exists()
    assert manifest.row_counts["commercial_demo_summary"] == 2
    payload = json.loads((tmp_path / "commercial_export_manifest.json").read_text(encoding="utf-8"))
    assert payload["tenant_id"] == "cliente_a"
