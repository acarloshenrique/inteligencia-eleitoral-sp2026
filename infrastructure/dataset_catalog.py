from __future__ import annotations

from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
from typing import Any

import pandas as pd

from config.settings import AppPaths


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(65536)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _catalog_paths(paths: AppPaths) -> tuple[Path, Path]:
    catalog_dir = paths.data_root / "outputs" / "catalog"
    catalog_dir.mkdir(parents=True, exist_ok=True)
    return catalog_dir / "datasets_catalog.jsonl", catalog_dir / "datasets_latest.json"


def build_dataset_metadata(
    *,
    dataset_name: str,
    dataset_version: str,
    dataset_path: Path,
    pipeline_version: str,
    run_id: str,
) -> dict[str, Any]:
    df = pd.read_parquet(dataset_path)
    return {
        "dataset_name": dataset_name,
        "dataset_version": dataset_version,
        "timestamp_utc": datetime.now(UTC).isoformat(),
        "pipeline_version": pipeline_version,
        "run_id": run_id,
        "path": str(dataset_path),
        "sha256": _sha256_file(dataset_path),
        "rows": int(len(df)),
        "columns": [str(c) for c in df.columns.tolist()],
        "dtypes": {str(col): str(dtype) for col, dtype in df.dtypes.items()},
        "size_bytes": int(dataset_path.stat().st_size),
    }


def register_dataset_version(paths: AppPaths, metadata: dict[str, Any]) -> dict[str, str]:
    catalog_path, latest_path = _catalog_paths(paths)

    with catalog_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(metadata, ensure_ascii=False) + "\n")

    latest_map: dict[str, Any] = {}
    if latest_path.exists():
        latest_map = json.loads(latest_path.read_text(encoding="utf-8"))

    latest_map[metadata["dataset_name"]] = metadata
    latest_path.write_text(json.dumps(latest_map, ensure_ascii=False, indent=2), encoding="utf-8")

    return {"catalog_path": str(catalog_path), "latest_index_path": str(latest_path)}
