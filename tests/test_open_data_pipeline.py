import json
from pathlib import Path
import sys
import tempfile

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config.settings import AppPaths
from infrastructure.open_data_pipeline import OpenDataInputs, run_open_data_crosswalk_pipeline


def test_open_data_crosswalk_pipeline_enriches_and_catalogs():
    with tempfile.TemporaryDirectory() as tmp:
        data_root = Path(tmp)
        ingestion_root = data_root / "ingestion"
        lake_root = data_root / "lake"
        bronze_root = lake_root / "bronze"
        silver_root = lake_root / "silver"
        gold_root = lake_root / "gold"
        gold_reports_root = gold_root / "reports"
        gold_serving_root = gold_root / "serving"
        catalog_root = gold_root / "_catalog"
        chroma = data_root / "chromadb"
        runtime_reports_root = data_root / "runtime_rel"
        for path in [ingestion_root, bronze_root, silver_root, gold_root, gold_reports_root, gold_serving_root, catalog_root, chroma, runtime_reports_root]:
            path.mkdir(parents=True, exist_ok=True)

        base_path = ingestion_root / "df_mun_20260408_010101.parquet"
        pd.DataFrame(
            [
                {"ranking_final": 1, "municipio": "Sao Paulo", "codigo_tse": "71072", "indice_final": 90.1, "ano": 2026, "turno": 1},
                {"ranking_final": 2, "municipio": "Campinas", "indice_final": 88.3, "ano": 2026, "turno": 1},
                {"ranking_final": 3, "municipio": "SP Capital", "indice_final": 85.0, "ano": 2026, "turno": 1},
                {"ranking_final": 4, "municipio": "Campina", "indice_final": 80.0, "ano": 2026, "turno": 1},
                {"ranking_final": 5, "municipio": "Cidade Sem Match", "indice_final": 50.0, "ano": 2026, "turno": 1},
            ]
        ).to_parquet(base_path, index=False)

        mapping_path = data_root / "open_data" / "raw" / "municipios_tse_ibge.csv"
        mapping_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            [
                {
                    "codigo_tse": "71072",
                    "codigo_ibge": "3550308",
                    "nome_municipio": "Sao Paulo",
                    "aliases": "S. Paulo;SP Capital",
                },
                {"codigo_tse": "62919", "codigo_ibge": "3509502", "nome_municipio": "Campinas", "aliases": ""},
            ]
        ).to_csv(mapping_path, index=False)

        socio_path = data_root / "open_data" / "raw" / "indicadores.csv"
        pd.DataFrame(
            [
                {"codigo_ibge": "3550308", "idhm": 0.81},
                {"codigo_ibge": "3509502", "idhm": 0.80},
            ]
        ).to_csv(socio_path, index=False)

        paths = AppPaths(
            data_root=data_root,
            ingestion_root=ingestion_root,
            lake_root=lake_root,
            bronze_root=bronze_root,
            silver_root=silver_root,
            gold_root=gold_root,
            gold_reports_root=gold_reports_root,
            gold_serving_root=gold_serving_root,
            catalog_root=catalog_root,
            chromadb_path=chroma,
            runtime_reports_root=runtime_reports_root,
            ts="20260408_010101",
            metadata_db_path=data_root / "metadata" / "jobs.sqlite3",
            artifact_root=data_root / "artifacts",
        )
        result = run_open_data_crosswalk_pipeline(
            paths=paths,
            inputs=OpenDataInputs(base_parquet_path=base_path, mapping_csv_path=mapping_path, socio_csv_path=socio_path),
            pipeline_version="open_data_test",
        )

        assert 0.79 <= result["join_rate"] <= 0.81

        published = Path(result["published_path"])
        assert published.exists()
        df = pd.read_parquet(published)
        assert set(df["join_status"].tolist()) == {"matched", "manual_review"}
        assert set(df["join_method"].dropna().tolist()) >= {"exact_code", "exact_name", "historical_alias", "fuzzy_score", "manual_review"}
        assert "join_confidence" in df.columns
        assert "coverage" in df.columns
        assert "data_quality_score" in df.columns
        assert df["coverage"].between(0, 1).all()
        assert df["data_quality_score"].between(0, 1).all()
        assert "needs_review" in df.columns
        assert "idhm" in df.columns
        assert "municipio_id_ibge7" in df.columns
        assert "canonical_key" in df.columns
        assert df.loc[df["municipio"] == "Sao Paulo", "canonical_key"].iloc[0] == "3550308:2026:00:1"
        assert df.loc[df["municipio"] == "SP Capital", "join_method"].iloc[0] == "historical_alias"
        assert df.loc[df["municipio"] == "Campina", "join_method"].iloc[0] == "fuzzy_score"
        assert float(df.loc[df["municipio"] == "Campina", "join_confidence"].iloc[0]) >= 0.9
        assert df.loc[df["municipio"] == "Cidade Sem Match", "join_status"].iloc[0] == "manual_review"
        assert bool(df.loc[df["municipio"] == "Cidade Sem Match", "needs_review"].iloc[0]) is True
        assert bool(df.loc[df["municipio"] == "Sao Paulo", "needs_review"].iloc[0]) is False

        manifest = json.loads(Path(result["manifest_path"]).read_text(encoding="utf-8"))
        dataset_manifest = manifest["dataset_manifest"]
        assert dataset_manifest["fonte"] == "df_municipios_enriched"
        assert dataset_manifest["hash_arquivo"]
        assert "municipio" in dataset_manifest["schema_detectado"]
        assert dataset_manifest["qualidade_carga"]["matched_rows"] == 4
        assert dataset_manifest["qualidade_carga"]["manual_review_rows"] == 1
        assert dataset_manifest["schema_detectado"]["data_quality_score"]
        assert dataset_manifest["versao_parser"] == "open_data_test"
        assert manifest["quality"]["rows"] == 5
        assert manifest["quality"]["matched_rows"] == 4
        assert manifest["quality"]["manual_review_rows"] == 1
        assert manifest["source_of_truth"]["join_key"] == "municipio_id_ibge7"
        assert manifest["matching"]["contract_fields"] == ["join_status", "join_method", "join_confidence", "needs_review"]
        assert Path(manifest["matching"]["manual_review_queue_path"]).exists()
        review_queue = pd.read_parquet(manifest["matching"]["manual_review_queue_path"])
        assert len(review_queue) == 1
        assert bool(review_queue["needs_review"].iloc[0]) is True

        latest_catalog = catalog_root / "datasets_latest.json"
        latest_payload = json.loads(latest_catalog.read_text(encoding="utf-8"))
        assert latest_payload["df_municipios_enriched"]["dataset_version"] == result["run_id"]
        assert latest_payload["df_municipios_enriched"]["quality"]["data_quality_score_avg"] > 0
        assert "coverage" in latest_payload["df_municipios_enriched"]
        assert latest_payload["dim_municipio"]["dataset_version"] == result["run_id"]

        dim_path = Path(result["dim_municipio_path"])
        assert dim_path.exists()
