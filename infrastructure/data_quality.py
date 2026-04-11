from __future__ import annotations

from datetime import UTC, datetime
import json
from pathlib import Path
from typing import Any

import pandas as pd


def compute_join_success(df: pd.DataFrame) -> float:
    if "join_status" not in df.columns or df.empty:
        return 0.0
    matched = (df["join_status"].astype(str) == "matched").sum()
    return float(matched / len(df))


def compute_null_critical(df: pd.DataFrame, critical_columns: list[str]) -> float:
    if df.empty:
        return 0.0
    existing = [c for c in critical_columns if c in df.columns]
    if not existing:
        return 0.0
    null_rows = df[existing].isna().any(axis=1).sum()
    return float(null_rows / len(df))


def compute_update_delay_days(bronze_assets: list[dict[str, Any]]) -> dict[str, float]:
    now = datetime.now(UTC)
    delays: dict[str, float] = {}
    for asset in bronze_assets:
        source = str(asset.get("source", "unknown"))
        last_modified = str(asset.get("source_last_modified_utc", "")).strip()
        if not last_modified:
            delays[source] = 0.0
            continue
        try:
            dt = datetime.fromisoformat(last_modified)
            delays[source] = max(0.0, (now - dt).total_seconds() / 86400.0)
        except ValueError:
            delays[source] = 0.0
    return delays


def compute_drift_score(
    *,
    current_df: pd.DataFrame,
    previous_path: Path | None,
    feature_columns: list[str],
) -> dict[str, float]:
    if previous_path is None or not previous_path.exists() or current_df.empty:
        return {"drift_score": 0.0, "drift_alert": 0.0}
    prev = pd.read_parquet(previous_path)
    if prev.empty:
        return {"drift_score": 0.0, "drift_alert": 0.0}
    deltas: list[float] = []
    for col in feature_columns:
        if col not in current_df.columns or col not in prev.columns:
            continue
        cur_mean = pd.to_numeric(current_df[col], errors="coerce").dropna().mean()
        prev_mean = pd.to_numeric(prev[col], errors="coerce").dropna().mean()
        if pd.isna(cur_mean) or pd.isna(prev_mean):
            continue
        baseline = abs(prev_mean) if abs(prev_mean) > 1e-9 else 1.0
        deltas.append(abs(float(cur_mean - prev_mean)) / baseline)
    if not deltas:
        return {"drift_score": 0.0, "drift_alert": 0.0}
    drift_score = float(sum(deltas) / len(deltas))
    return {"drift_score": drift_score, "drift_alert": 1.0 if drift_score >= 0.2 else 0.0}


def find_previous_dataset_path(paths, dataset_name: str, current_run_id: str) -> Path | None:
    latest_file = paths.catalog_root / "datasets_catalog.jsonl"
    if not latest_file.exists():
        return None
    lines = [ln for ln in latest_file.read_text(encoding="utf-8").splitlines() if ln.strip()]
    for line in reversed(lines):
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if obj.get("dataset_name") != dataset_name:
            continue
        if obj.get("dataset_version") == current_run_id:
            continue
        p = Path(str(obj.get("path", "")))
        if p.exists():
            return p
    return None
