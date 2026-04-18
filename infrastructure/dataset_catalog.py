from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from config.settings import AppPaths
from infrastructure.load_manifest import _detect_reference_period, _detect_schema, _detect_territorial_coverage


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
    catalog_dir = paths.catalog_root
    catalog_dir.mkdir(parents=True, exist_ok=True)
    return catalog_dir / "datasets_catalog.jsonl", catalog_dir / "datasets_latest.json"


def _quality_summary(df: pd.DataFrame) -> dict[str, Any]:
    if df.empty:
        null_pct = 0.0
    else:
        null_pct = float(df.isna().sum().sum() / max(1, df.shape[0] * max(1, df.shape[1]))) * 100.0
    summary: dict[str, Any] = {
        "status": "ok" if not df.empty else "warning",
        "rows": int(len(df)),
        "columns": int(len(df.columns)),
        "null_pct": round(null_pct, 3),
    }
    if "join_confidence" in df.columns:
        values = pd.to_numeric(df["join_confidence"], errors="coerce").dropna()
        summary["join_confidence_avg"] = round(float(values.mean()), 6) if not values.empty else 0.0
    if "data_quality_score" in df.columns:
        values = pd.to_numeric(df["data_quality_score"], errors="coerce").dropna()
        summary["data_quality_score_avg"] = round(float(values.mean()), 6) if not values.empty else 0.0
        summary["data_quality_score_min"] = round(float(values.min()), 6) if not values.empty else 0.0
    if "coverage" in df.columns:
        values = pd.to_numeric(df["coverage"], errors="coerce").dropna()
        summary["coverage_avg"] = round(float(values.mean()), 6) if not values.empty else 0.0
    return summary


def build_dataset_metadata(
    *,
    dataset_name: str,
    dataset_version: str,
    dataset_path: Path,
    pipeline_version: str,
    run_id: str,
    source: dict[str, Any] | None = None,
    quality: dict[str, Any] | None = None,
    coverage: dict[str, Any] | None = None,
) -> dict[str, Any]:
    df = pd.read_parquet(dataset_path)
    schema = _detect_schema(df)
    coverage_payload = coverage or _detect_territorial_coverage(df)
    quality_payload = quality or _quality_summary(df)
    collected_at = datetime.now(UTC).isoformat()
    source_payload = source or {
        "name": dataset_name,
        "type": "derived_dataset",
        "path": str(dataset_path),
        "run_id": run_id,
    }
    return {
        "dataset_name": dataset_name,
        "dataset_version": dataset_version,
        "timestamp_utc": collected_at,
        "pipeline_version": pipeline_version,
        "run_id": run_id,
        "path": str(dataset_path),
        "sha256": _sha256_file(dataset_path),
        "rows": int(len(df)),
        "columns": [str(c) for c in df.columns.tolist()],
        "dtypes": {str(col): str(dtype) for col, dtype in df.dtypes.items()},
        "size_bytes": int(dataset_path.stat().st_size),
        "source": source_payload,
        "data": {
            "cataloged_at_utc": collected_at,
            "reference_period": _detect_reference_period(df),
        },
        "version": {
            "dataset": dataset_version,
            "pipeline": pipeline_version,
            "run_id": run_id,
        },
        "schema": schema,
        "coverage": coverage_payload,
        "quality": quality_payload,
        "lgpd_classification": "public_open_data_or_derived_aggregate",
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
