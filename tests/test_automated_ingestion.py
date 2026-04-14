import json
from pathlib import Path
import sys
import tempfile

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config.settings import AppPaths
from infrastructure.automated_ingestion import run_automated_ingestion


def _paths(root: Path) -> AppPaths:
    ingestion_root = root / "ingestion"
    lake_root = root / "lake"
    bronze_root = lake_root / "bronze"
    silver_root = lake_root / "silver"
    gold_root = lake_root / "gold"
    gold_reports_root = gold_root / "reports"
    gold_serving_root = gold_root / "serving"
    catalog_root = gold_root / "_catalog"
    chroma = root / "chromadb"
    runtime_reports_root = root / "runtime_rel"
    for p in [
        ingestion_root,
        bronze_root,
        silver_root,
        gold_root,
        gold_reports_root,
        gold_serving_root,
        catalog_root,
        chroma,
        runtime_reports_root,
    ]:
        p.mkdir(parents=True, exist_ok=True)
    return AppPaths(
        data_root=root,
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
        ts="20260410_120000",
        metadata_db_path=root / "metadata" / "jobs.sqlite3",
        artifact_root=root / "artifacts",
    )


def test_automated_ingestion_downloads_validates_and_promotes(monkeypatch):
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        paths = _paths(root)
        catalog_path = root / "catalog.json"
        catalog_path.write_text(
            json.dumps(
                {
                    "pipeline": "open_data",
                    "pipeline_version": "auto_v1",
                    "domains": [
                        {
                            "domain": "eleitoral_oficial",
                            "assets": [
                                {
                                    "name": "base",
                                    "role": "base_parquet",
                                    "url": "https://example.com/base.parquet",
                                    "file_name": "base.parquet",
                                    "format": "parquet",
                                    "required": True,
                                },
                                {
                                    "name": "mapping",
                                    "role": "mapping_csv",
                                    "url": "https://example.com/mapping.csv",
                                    "file_name": "mapping.csv",
                                    "format": "csv",
                                    "required": True,
                                },
                                {
                                    "name": "historico_municipio",
                                    "role": "resultado_municipio_csv",
                                    "url": "https://example.com/historico_municipio.csv",
                                    "file_name": "historico_municipio.csv",
                                    "format": "csv",
                                    "required": False,
                                },
                            ],
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )

        def _fake_download(*, asset, output_dir, timeout_seconds=30):
            target = output_dir / asset.file_name
            output_dir.mkdir(parents=True, exist_ok=True)
            if asset.file_name.endswith(".parquet"):
                pd.DataFrame([{"ranking_final": 1, "municipio": "Sao Paulo", "ano": 2026, "turno": 1}]).to_parquet(
                    target, index=False
                )
            else:
                pd.DataFrame(
                    [{"codigo_tse": "71072", "codigo_ibge": "3550308", "nome_municipio": "Sao Paulo", "aliases": ""}]
                ).to_csv(target, index=False)
            return {
                "asset": asset.name,
                "status": "downloaded",
                "path": str(target),
                "downloaded_at_utc": "2026-04-10T12:00:00+00:00",
            }

        def _fake_pipeline(*, paths, inputs, pipeline_version):
            published = paths.gold_root / "df_mun_enriched_20260410_120000.parquet"
            pd.DataFrame([{"municipio": "Sao Paulo"}]).to_parquet(published, index=False)
            manifest = paths.ingestion_root / "pipeline_runs" / "open_data" / "fake" / "manifest.json"
            manifest.parent.mkdir(parents=True, exist_ok=True)
            manifest.write_text("{}", encoding="utf-8")
            return {
                "run_id": "20260410_120000",
                "manifest_path": str(manifest),
                "published_path": str(published),
                "dim_municipio_path": str(paths.silver_root / "dim_municipio_20260410_120000.parquet"),
                "join_rate": 1.0,
            }

        monkeypatch.setattr("infrastructure.automated_ingestion.download_asset_incremental", _fake_download)
        monkeypatch.setattr("infrastructure.automated_ingestion.run_open_data_crosswalk_pipeline", _fake_pipeline)

        result = run_automated_ingestion(paths=paths, catalog_path=catalog_path)

        manifest = json.loads(Path(result["manifest_path"]).read_text(encoding="utf-8"))
        assert result["pipeline"] == "open_data"
        assert len(result["downloads"]) == 3
        assert manifest["pipeline"] == "open_data"
        assert manifest["dominios"]["eleitoral_oficial"]["total_rows"] == 3
        assert result["downloads"][0]["dominio_fonte"] == "eleitoral_oficial"
        assert Path(result["downloads"][0]["bronze_path"]).exists()
        assert any(item["role"] == "resultado_municipio_csv" for item in manifest["downloads"])
        assert manifest["promotion_result"]["run_id"] == "20260410_120000"
        assert manifest["downloads"][0]["fonte"] in {"base", "mapping"}
