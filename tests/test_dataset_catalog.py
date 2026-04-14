import json
from pathlib import Path
import sys
import tempfile

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config.settings import AppPaths
from infrastructure.dataset_catalog import build_dataset_metadata, register_dataset_version


def test_register_dataset_version_writes_catalog_and_latest_index():
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

        dataset_path = gold_root / "df_mun_20260407_010101.parquet"
        pd.DataFrame([{"ranking_final": 1, "municipio": "A"}]).to_parquet(dataset_path, index=False)

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
            ts="20260407_010101",
            metadata_db_path=data_root / "metadata" / "jobs.sqlite3",
            artifact_root=data_root / "artifacts",
        )

        metadata = build_dataset_metadata(
            dataset_name="df_municipios",
            dataset_version="20260407_010101",
            dataset_path=dataset_path,
            pipeline_version="vtest",
            run_id="20260407_010101",
        )
        refs = register_dataset_version(paths, metadata)

        catalog_path = Path(refs["catalog_path"])
        latest_path = Path(refs["latest_index_path"])
        assert catalog_path.exists()
        assert latest_path.exists()

        catalog_lines = [ln for ln in catalog_path.read_text(encoding="utf-8").splitlines() if ln.strip()]
        assert len(catalog_lines) == 1
        entry = json.loads(catalog_lines[0])
        assert entry["dataset_name"] == "df_municipios"
        assert entry["dataset_version"] == "20260407_010101"
        assert len(entry["sha256"]) == 64
        assert entry["rows"] == 1
        assert entry["source"]["name"] == "df_municipios"
        assert entry["version"]["dataset"] == "20260407_010101"
        assert "schema" in entry and entry["schema"]["municipio"]
        assert "coverage" in entry and entry["coverage"]["municipios_cobertos"] == 1
        assert "quality" in entry and entry["quality"]["status"] == "ok"
        assert entry["lgpd_classification"] == "public_open_data_or_derived_aggregate"

        latest_data = json.loads(latest_path.read_text(encoding="utf-8"))
        assert latest_data["df_municipios"]["dataset_version"] == "20260407_010101"
