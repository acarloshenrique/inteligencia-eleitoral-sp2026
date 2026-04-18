import importlib
import sys

import pandas as pd
import pytest

from config.settings import AppPaths


def _paths(tmp_path, ts="20260316_1855"):
    lake = tmp_path / "data_lake"
    gold = lake / "gold"
    reports = gold / "reports"
    serving = gold / "serving"
    runtime_reports = tmp_path / "runtime_reports"
    for folder in [
        tmp_path / "ingestion",
        lake / "bronze",
        lake / "silver",
        gold,
        reports,
        serving,
        lake / "catalog",
        tmp_path / "chromadb",
        runtime_reports,
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
        gold_root=gold,
        gold_reports_root=reports,
        gold_serving_root=serving,
        catalog_root=lake / "catalog",
        chromadb_path=tmp_path / "chromadb",
        runtime_reports_root=runtime_reports,
        ts=ts,
        metadata_db_path=tmp_path / "metadata" / "jobs.sqlite3",
        artifact_root=tmp_path / "artifacts",
        tenant_id="tenant_a",
        tenant_root=tmp_path,
    )


def test_storage_import_does_not_require_streamlit(monkeypatch):
    sys.modules.pop("infrastructure.storage", None)
    monkeypatch.setitem(sys.modules, "streamlit", None)

    storage = importlib.import_module("infrastructure.storage")

    assert hasattr(storage, "carrega_dados")
    assert hasattr(storage, "carrega_db")


def test_carrega_dados_uses_framework_agnostic_cache(tmp_path):
    from infrastructure.storage import carrega_dados, clear_storage_cache

    clear_storage_cache()
    paths = _paths(tmp_path)
    df = pd.DataFrame(
        {
            "municipio": ["Cidade A"],
            "score_territorial_qt": [80.0],
            "VS_qt": [50.0],
        }
    )
    df.to_parquet(paths.gold_root / f"df_mun_{paths.ts}.parquet", index=False)

    loaded = carrega_dados(paths)
    loaded_again = carrega_dados(paths)

    assert loaded is loaded_again
    assert loaded.loc[0, "cluster"] == "Consolidacao"
    assert "ranking_final" in loaded.columns


def test_carrega_db_registers_municipios_without_streamlit(tmp_path):
    pytest.importorskip("duckdb")
    from infrastructure.storage import carrega_db, clear_storage_cache, tem_tabela

    clear_storage_cache()
    paths = _paths(tmp_path)
    df_mun = pd.DataFrame({"municipio": ["Cidade A"], "cluster": ["Diamante"]})

    db = carrega_db(paths, df_mun)

    assert tem_tabela(db, "municipios") is True
    assert db.execute("select count(*) as total from municipios").fetchone()[0] == 1


def test_carrega_dados_returns_demo_frame_when_gold_is_absent_in_dev(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from infrastructure import storage

    storage.clear_storage_cache()
    paths = _paths(tmp_path)
    monkeypatch.setattr(storage, "get_settings", lambda: SimpleNamespace(app_env="dev", require_data=False))

    loaded = storage.carrega_dados(paths)

    assert not loaded.empty
    assert {"ranking_final", "municipio", "cluster", "indice_final"}.issubset(loaded.columns)


def test_build_demo_zone_tables_adds_governed_zone_metadata():
    from infrastructure.storage import build_demo_zone_tables

    df = pd.DataFrame(
        [
            {
                "ranking_final": 1,
                "municipio": "Cidade A",
                "cluster": "Diamante",
                "indice_final": 90.0,
                "pop_censo2022": 100000,
            }
        ]
    )

    tables = build_demo_zone_tables(df)

    assert {"dim_territorio_eleitoral", "fact_zona_eleitoral", "features_zona_eleitoral"}.issubset(tables)
    fact = tables["fact_zona_eleitoral"]
    assert {"territorio_id", "zona_eleitoral", "data_quality_score", "join_confidence"}.issubset(fact.columns)
    assert len(fact) == 3
