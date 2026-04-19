from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from application.gold_marts import GOLD_TABLE_SPECS, GoldMartBuilder, GoldMartWriter


def _master() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "master_record_id": "m1",
                "ano_eleicao": 2024,
                "uf": "SP",
                "cod_municipio_tse": "71072",
                "cod_municipio_ibge": "3550308",
                "municipio_nome": "Sao Paulo",
                "zona": "0001",
                "secao": "0025",
                "local_votacao": "Escola A",
                "candidate_id": "123",
                "numero_candidato": "13",
                "partido": "PT",
                "cd_setor": "355030801000001",
                "territorial_cluster_id": "SP:3550308:Z0001",
                "join_strategy": "base_resultados_secao;exact_tse_ibge_code;exact_sector",
                "join_confidence": 0.95,
                "source_coverage_score": 0.8,
            },
            {
                "master_record_id": "m2",
                "ano_eleicao": 2024,
                "uf": "SP",
                "cod_municipio_tse": "71072",
                "cod_municipio_ibge": "3550308",
                "municipio_nome": "Sao Paulo",
                "zona": "0001",
                "secao": "0026",
                "local_votacao": "Escola B",
                "candidate_id": "456",
                "numero_candidato": "45",
                "partido": "PSD",
                "cd_setor": "355030801000002",
                "territorial_cluster_id": "SP:3550308:Z0001",
                "join_strategy": "base_resultados_secao;exact_tse_ibge_code;exact_sector",
                "join_confidence": 0.9,
                "source_coverage_score": 0.7,
            },
        ]
    )


def _results() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ano_eleicao": 2024,
                "uf": "SP",
                "cod_municipio_tse": "71072",
                "zona": "0001",
                "secao": "0025",
                "candidate_id": "123",
                "votos_nominais": 100,
                "total_aptos": 200,
            },
            {
                "ano_eleicao": 2024,
                "uf": "SP",
                "cod_municipio_tse": "71072",
                "zona": "0001",
                "secao": "0026",
                "candidate_id": "456",
                "votos_nominais": 60,
                "total_aptos": 200,
            },
        ]
    )


def test_gold_specs_cover_required_commercial_marts() -> None:
    assert {
        "gold_candidate_context",
        "gold_territory_profile",
        "gold_electoral_base_strength",
        "gold_competition_landscape",
        "gold_campaign_finance_efficiency",
        "gold_thematic_affinity",
        "gold_priority_score",
        "gold_allocation_inputs",
        "gold_allocation_recommendations",
        "gold_territorial_clusters",
        "gold_candidate_comparisons",
        "gold_zone_priority_score",
        "gold_section_master_index_quality",
    }.issubset(GOLD_TABLE_SPECS)
    for spec in GOLD_TABLE_SPECS.values():
        assert spec.grain
        assert spec.business_definition
        assert spec.metric_definitions
        assert spec.source_lineage
        assert spec.refresh_policy
        assert spec.consumers
        assert spec.data_quality_checks


def test_gold_builder_creates_all_marts_with_scores_and_recommendations() -> None:
    tables = GoldMartBuilder().build_all(
        master_index=_master(),
        electoral_results=_results(),
        campaign_finance=pd.DataFrame(
            [
                {"candidate_id": "123", "receita_total": 1000.0, "despesa_total": 500.0},
                {"candidate_id": "456", "receita_total": 800.0, "despesa_total": 600.0},
            ]
        ),
        thematic_signals=pd.DataFrame(
            [
                {"territorio_id": "2024:SP:71072:Z0001", "tema": "saude", "thematic_affinity_score": 0.8},
            ]
        ),
        budget_total=50000,
        scenario_id="baseline",
    )

    assert set(tables) == set(GOLD_TABLE_SPECS)
    priority = tables["gold_priority_score"]
    assert not priority.empty
    assert priority["score_prioridade_final"].between(0, 1).all()
    assert "score_explanation" in priority.columns
    recommendations = tables["gold_allocation_recommendations"]
    assert round(float(recommendations["recurso_sugerido"].sum()), 2) == 50000.0
    assert recommendations["percentual_orcamento_sugerido"].between(0, 1).all()
    zone_priority = tables["gold_zone_priority_score"]
    section_quality = tables["gold_section_master_index_quality"]
    assert not zone_priority.empty
    assert {"zona_id", "score_disputabilidade", "recomendacao_curta"}.issubset(zone_priority.columns)
    assert section_quality["section_quality_score"].between(0, 1).all()
    assert "quality_limitation" in section_quality.columns


def test_gold_writer_persists_manifests_and_duckdb_examples(tmp_path: Path) -> None:
    tables = GoldMartBuilder().build_all(master_index=_master(), electoral_results=_results(), budget_total=10000)

    result = GoldMartWriter().write_all(tables, output_dir=tmp_path / "gold" / "marts", dataset_version="test_v1")

    assert len(result.outputs) == len(GOLD_TABLE_SPECS)
    assert result.sql_examples_path.exists()
    for output in result.outputs:
        assert Path(output.parquet_path).exists()
        assert Path(output.manifest_path).exists()
        manifest = json.loads(Path(output.manifest_path).read_text(encoding="utf-8"))
        assert manifest["spec"]["grain"]
        assert manifest["spec"]["business_definition"]
        assert manifest["quality"]["rows"] == output.rows


def test_gold_competition_and_candidate_comparison_are_consistent() -> None:
    tables = GoldMartBuilder().build_all(master_index=_master(), electoral_results=_results())

    competition = tables["gold_competition_landscape"]
    comparisons = tables["gold_candidate_comparisons"]
    territory_profile = tables["gold_territory_profile"]

    assert len(territory_profile) == 2
    assert territory_profile["territorio_id"].str.contains(":S").all()
    assert competition["candidate_count"].eq(1).all()
    assert set(comparisons["leader_candidate_id"]) == {"123", "456"}


def test_gold_allocation_inputs_keep_quality_and_join_fields() -> None:
    tables = GoldMartBuilder().build_all(master_index=_master(), electoral_results=_results())
    allocation_inputs = tables["gold_allocation_inputs"]

    assert {"data_quality_score", "join_confidence", "allocation_weight", "tipo_acao_sugerida"}.issubset(
        allocation_inputs.columns
    )
    assert allocation_inputs["allocation_weight"].ge(0).all()
