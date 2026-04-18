from __future__ import annotations

from pathlib import Path

import pandas as pd

from feature_store import AnalyticalFeatureStore, FeatureStoreWriter, feature_by_name, features_by_family
from feature_store.registry import FEATURE_REGISTRY


def _gold_tables() -> dict[str, pd.DataFrame]:
    return {
        "gold_electoral_base_strength": pd.DataFrame(
            [
                {
                    "candidate_id": "123",
                    "territorio_id": "2024:SP:71072:Z0001",
                    "ano_eleicao": 2024,
                    "uf": "SP",
                    "cod_municipio_tse": "71072",
                    "municipio_nome": "Sao Paulo",
                    "zona": "0001",
                    "territorial_cluster_id": "SP:3550308:Z0001",
                    "votos_nominais": 100,
                    "total_aptos": 200,
                    "base_strength_score": 0.8,
                    "retention_score": 0.8,
                    "source_coverage_score": 0.8,
                    "join_confidence": 0.95,
                },
                {
                    "candidate_id": "456",
                    "territorio_id": "2024:SP:71072:Z0001",
                    "ano_eleicao": 2024,
                    "uf": "SP",
                    "cod_municipio_tse": "71072",
                    "municipio_nome": "Sao Paulo",
                    "zona": "0001",
                    "territorial_cluster_id": "SP:3550308:Z0001",
                    "votos_nominais": 50,
                    "total_aptos": 200,
                    "base_strength_score": 0.4,
                    "retention_score": 0.4,
                    "source_coverage_score": 0.7,
                    "join_confidence": 0.9,
                },
            ]
        ),
        "gold_competition_landscape": pd.DataFrame(
            [{"territorio_id": "2024:SP:71072:Z0001", "competition_score": 0.7, "candidate_count": 2}]
        ),
        "gold_territory_profile": pd.DataFrame(
            [
                {
                    "territorio_id": "2024:SP:71072:Z0001",
                    "secoes": 2,
                    "locais_votacao": 1,
                    "data_quality_score": 0.85,
                }
            ]
        ),
        "gold_thematic_affinity": pd.DataFrame(
            [
                {"territorio_id": "2024:SP:71072:Z0001", "tema": "saude", "thematic_affinity_score": 0.8},
                {"territorio_id": "2024:SP:71072:Z0001", "tema": "educacao", "thematic_affinity_score": 0.6},
            ]
        ),
        "gold_campaign_finance_efficiency": pd.DataFrame(
            [
                {
                    "candidate_id": "123",
                    "custo_por_voto_estimado": 5.0,
                    "finance_efficiency_score": 0.7,
                    "despesa_total": 500.0,
                },
                {
                    "candidate_id": "456",
                    "custo_por_voto_estimado": 10.0,
                    "finance_efficiency_score": 0.3,
                    "despesa_total": 600.0,
                },
            ]
        ),
    }


def test_feature_registry_has_all_required_families_and_lineage() -> None:
    families = {feature.family for feature in FEATURE_REGISTRY}
    assert families == {"base_eleitoral", "competicao", "territorial", "tematica", "eficiencia", "operacional"}
    assert feature_by_name("historical_vote_share") is not None
    assert features_by_family("base_eleitoral")
    for feature in FEATURE_REGISTRY:
        assert feature.definition
        assert feature.lineage
        assert feature.recomputation_policy


def test_feature_store_computes_reusable_features() -> None:
    result = AnalyticalFeatureStore().compute(gold_tables=_gold_tables(), feature_version="test_v1")
    features = result.features

    assert len(features) == 2
    assert "historical_vote_share" in features.columns
    assert "vote_fragmentation" in features.columns
    assert "candidate_territory_thematic_affinity" in features.columns
    assert "estimated_cost_per_vote" in features.columns
    assert "logistical_complexity" in features.columns
    assert features["historical_vote_share"].between(0, 1).all()
    assert features["competitive_intensity"].between(0, 1).all()
    assert features["feature_version"].eq("test_v1").all()
    assert result.lineage


def test_scoring_frame_maps_features_without_running_final_score() -> None:
    store = AnalyticalFeatureStore()
    result = store.compute(gold_tables=_gold_tables(), feature_version="test_v1")

    scoring_frame = store.scoring_frame(result.features)

    assert {"base_context_score", "concorrencia_score", "custo_eficiencia_score", "data_quality_score"}.issubset(
        scoring_frame.columns
    )
    assert scoring_frame["base_context_score"].between(0, 1).all()


def test_feature_store_writer_persists_versioned_outputs(tmp_path: Path) -> None:
    result = AnalyticalFeatureStore().compute(gold_tables=_gold_tables(), feature_version="test_v1")

    manifest = FeatureStoreWriter().write(result, output_dir=tmp_path / "semantic" / "feature_store")

    output_path = Path(manifest.output_path)
    assert output_path.exists()
    assert output_path.name == "features.parquet"
    assert manifest.feature_version == "test_v1"
    assert manifest.rows == 2
    assert "historical_vote_share" in manifest.features
    assert (output_path.parent / "feature_registry.json").exists()
    assert (output_path.parent / "manifest.json").exists()
    assert (output_path.parent / "duckdb_feature_examples.sql").exists()


def test_feature_store_supports_recomputation_versions() -> None:
    store = AnalyticalFeatureStore()
    first = store.compute(gold_tables=_gold_tables(), feature_version="v1")
    second = store.compute(gold_tables=_gold_tables(), feature_version="v2")

    assert first.feature_version == "v1"
    assert second.feature_version == "v2"
    assert (
        first.features.drop(columns=["feature_version", "computed_at_utc"]).shape
        == second.features.drop(columns=["feature_version", "computed_at_utc"]).shape
    )
