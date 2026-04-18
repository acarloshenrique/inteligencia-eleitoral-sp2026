from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pandas as pd
import pytest

from config.settings import Settings
from lakehouse import BaseLakehouseIngestion, BaseLakehouseTransformation, LakehouseConfig, LakehouseLayout
from lakehouse.base import ContractValidationError
from lakehouse.examples import generate_example_lakehouse
from lakehouse.registry import build_electoral_lakehouse_catalog


def test_settings_creates_canonical_lakehouse_layers(tmp_path: Path) -> None:
    settings = Settings(DATA_ROOT=str(tmp_path / "data"), TENANT_ID="cliente-lake")
    paths = settings.build_paths()

    assert paths.lakehouse_root == tmp_path / "data" / "tenants" / "cliente-lake" / "lake"
    assert paths.semantic_root.exists()
    assert paths.serving_root.exists()
    assert paths.lakehouse_catalog_root.exists()
    assert paths.bronze_root.name == "bronze"  # legacy data_lake compatibility remains available


def test_lakehouse_catalog_declares_required_metadata_and_layers() -> None:
    catalog = build_electoral_lakehouse_catalog()
    layers = {dataset.layer for dataset in catalog.datasets}

    assert layers == {"bronze", "silver", "gold", "semantic", "serving"}
    assert catalog.by_id("gold_fact_territorio_eleitoral") is not None
    assert all(dataset.owner for dataset in catalog.datasets)
    assert all(dataset.granularity for dataset in catalog.datasets)
    assert all(dataset.primary_key for dataset in catalog.datasets)
    assert all(dataset.schema_definition for dataset in catalog.datasets)
    assert all(dataset.business_documentation for dataset in catalog.by_layer("gold"))


def test_base_ingestion_preserves_raw_with_hash_manifest(tmp_path: Path) -> None:
    catalog = build_electoral_lakehouse_catalog()
    contract = catalog.by_id("raw_tse_resultados_secao_boletim_urna")
    assert contract is not None
    input_path = tmp_path / "raw.csv"
    input_path.write_text(
        "ANO_ELEICAO,SIGLA_UF,COD_MUN_TSE,ZONA,SECAO\n2024,SP,71072,1,10\n",
        encoding="utf-8",
    )

    result = BaseLakehouseIngestion(LakehouseConfig(root=tmp_path / "lake")).preserve_raw(
        input_path=input_path,
        contract=contract,
        dataset_version="v2024",
        run_id="run_raw",
        partition_values={"ANO_ELEICAO": 2024, "SIGLA_UF": "SP"},
    )

    assert result.output_path.exists()
    assert result.manifest_path.exists()
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["dataset_id"] == contract.dataset_id
    assert manifest["source_hash_sha256"]
    assert manifest["quality"]["raw_preserved"] is True
    assert "ano_eleicao=2024" in str(result.output_path).lower()


def test_base_transformation_writes_parquet_duckdb_manifest_and_lineage(tmp_path: Path) -> None:
    catalog = build_electoral_lakehouse_catalog()
    contract = catalog.by_id("gold_fact_territorio_eleitoral")
    assert contract is not None
    df = pd.DataFrame(
        [
            {
                "territorio_id": "2024:SP:71072:ZE1:S10",
                "ANO_ELEICAO": 2024,
                "SIGLA_UF": "SP",
                "COD_MUN_TSE": "71072",
                "COD_MUN_IBGE": "3550308",
                "MUNICIPIO": "SAO PAULO",
                "ZONA": 1,
                "SECAO": 10,
                "LOCAL_VOTACAO": "ESCOLA A",
                "CD_SETOR": "355030800001",
                "eleitores_aptos": 1000,
                "votos_validos": 700,
                "abstencao_pct": 0.22,
                "competitividade": 0.71,
                "join_confidence": 0.96,
                "data_quality_score": 0.94,
            }
        ]
    )

    result = BaseLakehouseTransformation(LakehouseConfig(root=tmp_path / "lake")).write_dataframe(
        df=df,
        contract=contract,
        dataset_version="v2024",
        run_id="run_gold",
        inputs=["silver_tse_resultados_secao"],
        partition_values={"ANO_ELEICAO": 2024, "SIGLA_UF": "SP"},
        operation="build_gold_fact",
        business_rule="Consolidar territorio eleitoral para scoring e dashboards.",
    )

    assert result.output_path.exists()
    if importlib.util.find_spec("duckdb") is not None:
        assert result.duckdb_path is not None and result.duckdb_path.exists()
    else:
        assert result.duckdb_path is None
    assert result.lineage_path is not None and result.lineage_path.exists()
    assert pd.read_parquet(result.output_path).loc[0, "territorio_id"] == "2024:SP:71072:ZE1:S10"
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["output_format"] == "parquet"
    assert manifest["quality"]["primary_key_unique"] is True
    lineage = json.loads(result.lineage_path.read_text(encoding="utf-8"))
    assert lineage["inputs"] == ["silver_tse_resultados_secao"]
    assert lineage["operation"] == "build_gold_fact"


def test_transformation_rejects_primary_key_duplicates(tmp_path: Path) -> None:
    catalog = build_electoral_lakehouse_catalog()
    contract = catalog.by_id("semantic_metricas_prioridade_territorial")
    assert contract is not None
    df = pd.DataFrame(
        [
            {
                "territorio_id": "t1",
                "candidate_id": "c1",
                "scenario": "hibrido",
                "score_version": "v1",
                "score_prioridade_final": 0.7,
                "confidence_score": 0.8,
            },
            {
                "territorio_id": "t1",
                "candidate_id": "c1",
                "scenario": "hibrido",
                "score_version": "v1",
                "score_prioridade_final": 0.8,
                "confidence_score": 0.9,
            },
        ]
    )

    with pytest.raises(ContractValidationError, match="chave primaria duplicada"):
        BaseLakehouseTransformation(LakehouseConfig(root=tmp_path / "lake")).write_dataframe(
            df=df,
            contract=contract,
            dataset_version="v1",
            run_id="dup",
        )


def test_repository_catalog_json_is_loadable() -> None:
    payload = json.loads(Path("lake/catalog/datasets.json").read_text(encoding="utf-8"))
    assert payload["version"] == "electoral_lakehouse_catalog_v1"
    assert {item["layer"] for item in payload["datasets"]} == {"bronze", "silver", "gold", "semantic", "serving"}
    assert all("schema" in item for item in payload["datasets"])


def test_example_generator_materializes_bronze_silver_gold(tmp_path: Path) -> None:
    results = generate_example_lakehouse(tmp_path / "lake")

    assert [item.layer for item in results] == ["bronze", "silver", "gold"]
    assert all(item.output_path.exists() for item in results)
    assert all(item.manifest_path.exists() for item in results)
    assert results[1].lineage_path is not None and results[1].lineage_path.exists()
    assert results[2].lineage_path is not None and results[2].lineage_path.exists()
