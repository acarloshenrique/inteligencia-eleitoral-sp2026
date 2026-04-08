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
        pasta_est = data_root / "outputs" / "estado_sessao"
        pasta_rel = data_root / "outputs" / "relatorios"
        chroma = data_root / "chromadb"
        runtime_rel = data_root / "runtime_rel"
        for path in [pasta_est, pasta_rel, chroma, runtime_rel]:
            path.mkdir(parents=True, exist_ok=True)

        base_path = pasta_est / "df_mun_20260408_010101.parquet"
        pd.DataFrame(
            [
                {"ranking_final": 1, "municipio": "Sao Paulo", "indice_final": 90.1, "ano": 2026, "turno": 1},
                {"ranking_final": 2, "municipio": "Campinas", "indice_final": 88.3, "ano": 2026, "turno": 1},
                {"ranking_final": 3, "municipio": "Cidade Sem Match", "indice_final": 50.0, "ano": 2026, "turno": 1},
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
            pasta_est=pasta_est,
            pasta_rel=pasta_rel,
            chromadb_path=chroma,
            runtime_rel=runtime_rel,
            ts="20260408_010101",
            metadata_db_path=data_root / "metadata" / "jobs.sqlite3",
            artifact_root=data_root / "artifacts",
        )
        result = run_open_data_crosswalk_pipeline(
            paths=paths,
            inputs=OpenDataInputs(base_parquet_path=base_path, mapping_csv_path=mapping_path, socio_csv_path=socio_path),
            pipeline_version="open_data_test",
        )

        assert 0.65 <= result["join_rate"] <= 0.67

        published = Path(result["published_path"])
        assert published.exists()
        df = pd.read_parquet(published)
        assert set(df["join_status"].tolist()) == {"matched", "no_match"}
        assert "idhm" in df.columns
        assert "municipio_id_ibge7" in df.columns
        assert "canonical_key" in df.columns
        assert df.loc[df["municipio"] == "Sao Paulo", "canonical_key"].iloc[0] == "3550308:2026:00:1"

        manifest = json.loads(Path(result["manifest_path"]).read_text(encoding="utf-8"))
        assert manifest["quality"]["rows"] == 3
        assert manifest["quality"]["matched_rows"] == 2
        assert manifest["source_of_truth"]["join_key"] == "municipio_id_ibge7"

        latest_catalog = data_root / "outputs" / "catalog" / "datasets_latest.json"
        latest_payload = json.loads(latest_catalog.read_text(encoding="utf-8"))
        assert latest_payload["df_municipios_enriched"]["dataset_version"] == result["run_id"]
        assert latest_payload["dim_municipio"]["dataset_version"] == result["run_id"]

        dim_path = Path(result["dim_municipio_path"])
        assert dim_path.exists()
