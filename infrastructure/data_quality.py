from __future__ import annotations

import json
from datetime import UTC, datetime
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


def apply_row_quality_scores(
    df: pd.DataFrame,
    *,
    critical_columns: list[str],
    source_columns: list[str] | None = None,
) -> pd.DataFrame:
    """Attach row-level coverage and quality scores for governed lake outputs."""
    out = df.copy()
    if out.empty:
        out["coverage"] = pd.Series(dtype=float)
        out["data_quality_score"] = pd.Series(dtype=float)
        return out

    critical_existing = [col for col in critical_columns if col in out.columns]
    source_existing = [col for col in (source_columns or []) if col in out.columns]
    coverage_columns = source_existing or critical_existing or list(out.columns)

    if coverage_columns:
        non_null = out[coverage_columns].notna() & (
            out[coverage_columns].astype(str).apply(lambda col: col.str.strip()) != ""
        )
        out["coverage"] = non_null.sum(axis=1).astype(float) / max(1, len(coverage_columns))
    else:
        out["coverage"] = 1.0

    if "join_confidence" in out.columns:
        join_confidence = pd.to_numeric(out["join_confidence"], errors="coerce").fillna(0.0).clip(0.0, 1.0)
    else:
        join_confidence = pd.Series([1.0] * len(out), index=out.index, dtype=float)

    if critical_existing:
        critical_non_null = out[critical_existing].notna() & (
            out[critical_existing].astype(str).apply(lambda col: col.str.strip()) != ""
        )
        critical_completeness = critical_non_null.sum(axis=1).astype(float) / max(1, len(critical_existing))
    else:
        critical_completeness = pd.Series([1.0] * len(out), index=out.index, dtype=float)

    needs_review = (
        out["needs_review"].fillna(False).astype(bool)
        if "needs_review" in out.columns
        else pd.Series([False] * len(out), index=out.index)
    )
    review_penalty = needs_review.map({True: 0.75, False: 1.0}).astype(float)
    out["data_quality_score"] = (
        (0.45 * join_confidence) + (0.35 * critical_completeness) + (0.20 * out["coverage"])
    ) * review_penalty
    out["data_quality_score"] = out["data_quality_score"].clip(0.0, 1.0).round(6)
    out["coverage"] = out["coverage"].clip(0.0, 1.0).round(6)
    return out


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
