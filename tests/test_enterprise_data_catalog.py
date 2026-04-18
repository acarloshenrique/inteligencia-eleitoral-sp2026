from __future__ import annotations

from pathlib import Path

from data_catalog.enterprise_io import (
    read_enterprise_catalog,
    read_prioritization_table,
    write_enterprise_catalog,
    write_prioritization_table,
)
from data_catalog.enterprise_registry import (
    build_enterprise_catalog,
    catalog_gaps,
    coverage_summary,
    datasets_by_product_capability,
    enterprise_dataset_by_id,
    prioritization_table,
    required_capabilities_present,
)
from data_catalog.models import ProductCapability

REQUIRED_CAPABILITIES: tuple[ProductCapability, ...] = (
    "contexto_candidato",
    "forca_eleitoral",
    "competicao",
    "eficiencia_gasto",
    "tematica_territorial",
    "capacidade_operacional",
)


def test_enterprise_catalog_contains_required_tiers_and_priority_sources() -> None:
    catalog = build_enterprise_catalog()
    dataset_ids = {dataset.dataset_id for dataset in catalog.datasets}
    tiers = {dataset.tier for dataset in catalog.datasets}

    assert tiers == {1, 2, 3}
    assert {
        "tse_boletim_urna_secao_bronze",
        "tse_resultados_eleitorais_secao_silver",
        "tse_perfil_eleitorado_secao_silver",
        "tse_eleitorado_local_votacao_silver",
        "tse_cadastro_candidatos_silver",
        "tse_prestacao_contas_candidato_silver",
        "ibge_malha_setores_censitarios_bronze",
        "ibge_censo_2022_agregados_setor_silver",
        "inep_censo_escolar_municipio_silver",
        "datasus_cnes_municipio_silver",
        "tesouro_siconfi_municipio_silver",
        "ssp_sp_criminalidade_municipio_silver",
        "territorial_auxiliar_crosswalk_gold",
        "tse_redes_sociais_candidatos_silver",
        "meta_ad_library_campanha_silver",
        "sinais_digitais_competitivos_municipio_silver",
    }.issubset(dataset_ids)


def test_required_metadata_is_complete() -> None:
    catalog = build_enterprise_catalog()

    for dataset in catalog.datasets:
        assert dataset.dataset_id
        assert dataset.nome
        assert dataset.descricao
        assert dataset.chave_primaria
        assert dataset.technical_metadata.owner_logico
        assert dataset.technical_metadata.storage_path.startswith("lake/")
        assert dataset.business_metadata.perguntas_respondidas
        assert dataset.quality_rules
        assert 1 <= dataset.prioridade_produto <= 100
        assert 0 <= dataset.score_confiabilidade <= 1
        assert set(dataset.product_capabilities).issubset(dataset.business_metadata.funcionalidades_suportadas)


def test_each_product_capability_has_at_least_one_dataset() -> None:
    assert required_capabilities_present(REQUIRED_CAPABILITIES)

    for capability in REQUIRED_CAPABILITIES:
        assert datasets_by_product_capability(capability)


def test_prioritization_table_is_sorted_and_business_ready() -> None:
    rows = prioritization_table()
    priorities = [row.prioridade_produto for row in rows]

    assert priorities == sorted(priorities, reverse=True)
    assert rows[0].impacto_comercial == "critical"
    assert rows[0].capacidades_produto
    assert any(row.lacunas_bloqueantes for row in rows)


def test_catalog_gaps_identifies_unfinished_or_risky_sources() -> None:
    gaps = catalog_gaps()
    gap_ids = {dataset.dataset_id for dataset in gaps}

    assert "ibge_malha_setores_censitarios_bronze" in gap_ids
    assert "meta_ad_library_campanha_silver" in gap_ids
    assert "sinais_digitais_competitivos_municipio_silver" in gap_ids


def test_gold_datasets_declare_dependencies() -> None:
    catalog = build_enterprise_catalog()

    for dataset in catalog.datasets:
        if dataset.camada_alvo == "gold":
            assert dataset.dependencies
            assert dataset.business_metadata.documentacao_negocio

    fact = enterprise_dataset_by_id("gold_fact_territorio_eleitoral")
    assert fact is not None
    assert {"tse_resultados_eleitorais_secao_silver", "territorial_auxiliar_crosswalk_gold"}.issubset(
        {dependency.dataset_id for dependency in fact.dependencies}
    )


def test_coverage_summary_exposes_product_questions() -> None:
    summary = coverage_summary()

    assert "secao" in summary["granularidade"]
    assert "zona" in summary["granularidade"]
    assert any(value.startswith("BR:BR") for value in summary["geografica"])


def test_enterprise_catalog_json_roundtrip(tmp_path: Path) -> None:
    catalog_path = tmp_path / "catalog.enterprise.json"
    prioritization_path = tmp_path / "prioritization.enterprise.json"

    write_enterprise_catalog(catalog_path)
    write_prioritization_table(prioritization_path)

    catalog = read_enterprise_catalog(catalog_path)
    rows = read_prioritization_table(prioritization_path)

    assert len(catalog.datasets) >= 16
    assert len(rows) == len(catalog.datasets)
    assert rows[0].dataset_id == "tse_boletim_urna_secao_bronze"
