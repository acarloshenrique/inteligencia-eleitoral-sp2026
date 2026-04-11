import json
from pathlib import Path
import sys
import tempfile

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from config.settings import AppPaths
from infrastructure.data_pipeline import DagNode, PipelineError, SimpleDag, run_versioned_data_pipeline


def _sample_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ranking_final": 2,
                "municipio": "Cidade B",
                "indice_final": 80.0,
                "score_territorial_qt": 65.0,
                "VS_qt": 75.0,
                "PD_qt": 55.0,
                "pop_censo2022": 50000,
            },
            {
                "ranking_final": 1,
                "municipio": "Cidade A",
                "indice_final": 90.0,
                "score_territorial_qt": 80.0,
                "VS_qt": 80.0,
                "PD_qt": 70.0,
                "pop_censo2022": 90000,
            },
        ]
    )


def test_simple_dag_detects_cycle():
    dag = SimpleDag(
        [
            DagNode(name="a", deps=("c",), fn=lambda ctx: {}),
            DagNode(name="b", deps=("a",), fn=lambda ctx: {}),
            DagNode(name="c", deps=("b",), fn=lambda ctx: {}),
        ]
    )
    try:
        dag.run({})
        assert False, "Esperava PipelineError por ciclo"
    except PipelineError:
        assert True


def test_data_pipeline_runs_all_steps_and_publishes_outputs():
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
        for p in [ingestion_root, bronze_root, silver_root, gold_root, gold_reports_root, gold_serving_root, catalog_root, chroma, runtime_reports_root]:
            p.mkdir(parents=True, exist_ok=True)

        input_path = ingestion_root / "df_mun_20260407_000000.parquet"
        _sample_df().to_parquet(input_path, index=False)

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
            ts="20260407_000000",
            metadata_db_path=data_root / "metadata" / "jobs.sqlite3",
            artifact_root=data_root / "artifacts",
        )
        result = run_versioned_data_pipeline(paths=paths, input_path=input_path, pipeline_version="vtest")

        assert result["dag_order"] == ["ingest", "validate", "transform", "publish"]
        manifest_path = Path(result["manifest_path"])
        assert manifest_path.exists()

        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        assert manifest["pipeline_version"] == "vtest"
        dataset_manifest = manifest["dataset_manifest"]
        assert dataset_manifest["fonte"] == "df_municipios"
        assert dataset_manifest["hash_arquivo"]
        assert "ranking_final" in dataset_manifest["schema_detectado"]
        assert "cobertura_territorial" in dataset_manifest
        assert "periodo_referencia" in dataset_manifest
        assert "qualidade_carga" in dataset_manifest
        assert dataset_manifest["versao_parser"] == "vtest"
        assert manifest["steps"]["publish"]["rows"] == 2
        assert manifest["steps"]["publish"]["dataset_metadata"]["dataset_name"] == "df_municipios"
        assert manifest["steps"]["publish"]["dataset_metadata"]["dataset_version"] == result["run_id"]
        assert len(manifest["steps"]["publish"]["dataset_metadata"]["sha256"]) == 64

        published_path = Path(result["publish"]["published_path"])
        assert published_path.exists()
        published_df = pd.read_parquet(published_path)
        assert list(published_df["ranking_final"]) == [1, 2]

        latest_path = Path(result["publish"]["latest_path"])
        assert latest_path.exists()

        catalog_path = Path(result["publish"]["catalog_path"])
        assert catalog_path.exists()
        lines = [ln for ln in catalog_path.read_text(encoding="utf-8").splitlines() if ln.strip()]
        assert len(lines) == 1
        catalog_entry = json.loads(lines[0])
        assert catalog_entry["dataset_name"] == "df_municipios"
        assert catalog_entry["dataset_version"] == result["run_id"]
        assert catalog_entry["path"] == str(published_path)
        assert catalog_entry["rows"] == 2

        latest_index = Path(result["publish"]["catalog_latest_index_path"])
        assert latest_index.exists()
        latest_data = json.loads(latest_index.read_text(encoding="utf-8"))
        assert latest_data["df_municipios"]["dataset_version"] == result["run_id"]
