from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from config.settings import AppPaths
from data_catalog.io import read_catalog, write_catalog
from data_catalog.sources import build_default_catalog, source_by_name
from ingestion import DatasetValidator, KeyHarmonizer, LayeredIngestionPipeline


def _paths(tmp_path: Path) -> AppPaths:
    lake = tmp_path / "data_lake"
    for folder in [
        tmp_path / "ingestion",
        lake / "bronze",
        lake / "silver",
        lake / "gold",
        lake / "catalog",
        lake / "gold" / "reports",
        lake / "gold" / "serving",
        tmp_path / "chromadb",
        tmp_path / "metadata",
        tmp_path / "artifacts",
    ]:
        folder.mkdir(parents=True, exist_ok=True)
    return AppPaths(
        data_root=tmp_path,
        ingestion_root=tmp_path / "ingestion",
        lake_root=lake,
        bronze_root=lake / "bronze",
        silver_root=lake / "silver",
        gold_root=lake / "gold",
        gold_reports_root=lake / "gold" / "reports",
        gold_serving_root=lake / "gold" / "serving",
        catalog_root=lake / "catalog",
        chromadb_path=tmp_path / "chromadb",
        runtime_reports_root=tmp_path / "runtime_reports",
        ts="20260416_000000",
        metadata_db_path=tmp_path / "metadata" / "jobs.sqlite3",
        artifact_root=tmp_path / "artifacts",
        tenant_id="default",
        tenant_root=tmp_path,
    )


def test_catalog_roundtrip_and_priority_tiers(tmp_path):
    catalog = build_default_catalog()
    path = write_catalog(tmp_path / "catalog" / "sources.json", catalog)
    loaded = read_catalog(path)

    assert len(loaded.sources) >= 13
    assert {source.tier for source in loaded.sources} == {1, 2, 3}
    assert source_by_name("tse_resultados_secao_boletim_urna") is not None
    assert all(source.estrategia_normalizacao for source in loaded.sources)


def test_harmonizer_maps_tse_keys_to_canonical_contract():
    source = source_by_name("tse_resultados_secao_boletim_urna")
    assert source is not None
    raw = pd.DataFrame(
        [
            {
                "ANO_ELEICAO": "2024",
                "SG_UF": "sp",
                "CD_MUNICIPIO": "71072",
                "NM_MUNICIPIO": "São Paulo",
                "NR_ZONA": "001",
                "NR_SECAO": "002",
            }
        ]
    )

    harmonized = KeyHarmonizer().harmonize(raw, source)
    report = DatasetValidator().validate(harmonized, source)

    assert report.ok
    assert harmonized.loc[0, "SIGLA_UF"] == "SP"
    assert harmonized.loc[0, "COD_MUN_TSE"] == "71072"
    assert harmonized.loc[0, "MUNICIPIO_NORMALIZADO"] == "SAO PAULO"


def test_layered_ingestion_pipeline_writes_bronze_silver_gold_manifest_and_duckdb(tmp_path):
    paths = _paths(tmp_path)
    source = source_by_name("tse_resultados_secao_boletim_urna")
    assert source is not None
    input_path = tmp_path / "votacao.csv"
    input_path.write_text(
        "ANO_ELEICAO,SG_UF,CD_MUNICIPIO,NM_MUNICIPIO,NR_ZONA,NR_SECAO,QT_VOTOS\n"
        "2024,SP,71072,SAO PAULO,1,10,100\n",
        encoding="utf-8",
    )

    result = LayeredIngestionPipeline(paths).ingest_source(source=source, input_path=input_path, run_id="run_a")

    assert result.status == "ok"
    assert result.bronze_path.exists()
    assert result.silver_path.exists()
    assert result.gold_path.exists()
    assert result.manifest_path.exists()
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["status"] == "ok"
    assert manifest["quality"]["rows"] == 1
    assert manifest["gold"]["path"].endswith("tse_resultados_secao_boletim_urna.parquet")
    gold = pd.read_parquet(result.gold_path)
    assert gold.loc[0, "COD_MUN_TSE"] == "71072"
    assert gold.loc[0, "lake_layer"] == "gold"


def test_layered_ingestion_pipeline_is_idempotent_for_same_run_id(tmp_path):
    paths = _paths(tmp_path)
    source = source_by_name("tse_resultados_secao_boletim_urna")
    assert source is not None
    input_path = tmp_path / "votacao.csv"
    input_path.write_text(
        "ANO_ELEICAO,SG_UF,CD_MUNICIPIO,NM_MUNICIPIO,NR_ZONA,NR_SECAO\n"
        "2024,SP,71072,SAO PAULO,1,10\n",
        encoding="utf-8",
    )
    pipeline = LayeredIngestionPipeline(paths)

    first = pipeline.ingest_source(source=source, input_path=input_path, run_id="same_run")
    second = pipeline.ingest_source(source=source, input_path=input_path, run_id="same_run")

    assert first.status == second.status == "ok"
    assert first.gold_path == second.gold_path
    assert pd.read_parquet(second.gold_path).shape[0] == 1


def test_layered_ingestion_pipeline_persists_failure_report(tmp_path):
    paths = _paths(tmp_path)
    source = source_by_name("tse_resultados_secao_boletim_urna")
    assert source is not None
    input_path = tmp_path / "invalid.csv"
    input_path.write_text("foo,bar\n1,2\n", encoding="utf-8")

    result = LayeredIngestionPipeline(paths).ingest_source(source=source, input_path=input_path, run_id="bad_run")

    assert result.status == "failed"
    assert result.failure_path is not None
    failure = json.loads(result.failure_path.read_text(encoding="utf-8"))
    assert failure["status"] == "failed"
    assert "chaves principais ausentes" in failure["error"]
