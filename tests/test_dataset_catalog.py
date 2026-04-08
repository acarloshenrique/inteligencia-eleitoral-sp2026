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
        pasta_est = data_root / "outputs" / "estado_sessao"
        pasta_rel = data_root / "outputs" / "relatorios"
        chroma = data_root / "chromadb"
        runtime_rel = data_root / "runtime_rel"
        for p in [pasta_est, pasta_rel, chroma, runtime_rel]:
            p.mkdir(parents=True, exist_ok=True)

        dataset_path = pasta_est / "df_mun_20260407_010101.parquet"
        pd.DataFrame([{"ranking_final": 1, "municipio": "A"}]).to_parquet(dataset_path, index=False)

        paths = AppPaths(
            data_root=data_root,
            pasta_est=pasta_est,
            pasta_rel=pasta_rel,
            chromadb_path=chroma,
            runtime_rel=runtime_rel,
            ts="20260407_010101",
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

        latest_data = json.loads(latest_path.read_text(encoding="utf-8"))
        assert latest_data["df_municipios"]["dataset_version"] == "20260407_010101"
