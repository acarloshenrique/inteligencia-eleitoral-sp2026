from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from data_quality.models import QualityCheckResult, QualityDimension, QualityStatus


def _status(score: float, *, warn_below: float, fail_below: float) -> QualityStatus:
    if score < fail_below:
        return "fail"
    if score < warn_below:
        return "warn"
    return "pass"


def _result(
    *,
    check_id: str,
    dimension: QualityDimension,
    score: float,
    message: str,
    threshold: float | str | int | None = None,
    observed_value: float | str | int | None = None,
    warn_below: float = 0.95,
    fail_below: float = 0.8,
) -> QualityCheckResult:
    score = max(0.0, min(1.0, float(score)))
    return QualityCheckResult(
        check_id=check_id,
        dimension=dimension,
        status=_status(score, warn_below=warn_below, fail_below=fail_below),
        score=round(score, 6),
        observed_value=observed_value,
        threshold=threshold,
        message=message,
    )


def completeness_check(df: pd.DataFrame, *, required_columns: list[str]) -> QualityCheckResult:
    if df.empty:
        return _result(
            check_id="completeness_required_columns",
            dimension="completeness",
            score=0.0,
            observed_value=0,
            threshold=1.0,
            message="Dataset is empty.",
        )
    existing = [column for column in required_columns if column in df.columns]
    if not existing:
        return _result(
            check_id="completeness_required_columns",
            dimension="completeness",
            score=0.0,
            observed_value=0,
            threshold=len(required_columns),
            message="No required columns were found.",
        )
    non_empty = df[existing].notna() & (df[existing].astype(str).apply(lambda col: col.str.strip()) != "")
    score = float(non_empty.sum().sum() / max(1, len(df) * len(existing)))
    return _result(
        check_id="completeness_required_columns",
        dimension="completeness",
        score=score,
        observed_value=round(score, 6),
        threshold=0.95,
        message=f"Completeness across required columns: {existing}.",
    )


def uniqueness_check(df: pd.DataFrame, *, primary_key: list[str]) -> QualityCheckResult:
    existing = [column for column in primary_key if column in df.columns]
    if df.empty or not existing:
        return _result(
            check_id="uniqueness_primary_key",
            dimension="uniqueness",
            score=0.0,
            observed_value=0,
            threshold=1.0,
            message="Primary key cannot be evaluated.",
        )
    unique_rows = int(len(df.drop_duplicates(existing)))
    score = unique_rows / max(1, len(df))
    return _result(
        check_id="uniqueness_primary_key",
        dimension="uniqueness",
        score=score,
        observed_value=round(score, 6),
        threshold=1.0,
        message=f"Uniqueness on primary key: {existing}.",
        warn_below=1.0,
        fail_below=0.98,
    )


def key_validity_check(df: pd.DataFrame, *, key_columns: list[str]) -> QualityCheckResult:
    existing = [column for column in key_columns if column in df.columns]
    if df.empty or not existing:
        return _result(
            check_id="validity_keys",
            dimension="validity",
            score=0.0,
            message="No key columns available for validity check.",
        )
    valid_count = 0
    total = 0
    for column in existing:
        values = df[column].astype(str).str.strip()
        total += len(values)
        if column in {"uf", "SIGLA_UF"}:
            valid_count += int(values.str.match(r"^[A-Z]{2}$").sum())
        elif column in {"ano_eleicao", "ANO_ELEICAO", "ano"}:
            valid_count += int(values.str.match(r"^(19|20)\d{2}$").sum())
        elif column in {"join_confidence", "source_coverage_score"}:
            numeric = pd.to_numeric(values, errors="coerce")
            valid_count += int(numeric.between(0, 1).sum())
        else:
            valid_count += int(values.ne("").sum())
    score = valid_count / max(1, total)
    return _result(
        check_id="validity_keys",
        dimension="validity",
        score=score,
        observed_value=round(score, 6),
        threshold=0.98,
        message=f"Validity for key columns: {existing}.",
    )


def freshness_check(metadata: dict[str, Any], *, max_age_days: int = 30) -> QualityCheckResult:
    timestamp = str(metadata.get("generated_at_utc") or metadata.get("collected_at_utc") or "").strip()
    if not timestamp:
        return _result(
            check_id="freshness_metadata",
            dimension="freshness",
            score=0.5,
            observed_value="missing_timestamp",
            threshold=max_age_days,
            message="Freshness timestamp not found.",
            warn_below=0.9,
            fail_below=0.4,
        )
    try:
        created = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        if created.tzinfo is None:
            created = created.replace(tzinfo=UTC)
        age_days = max(0.0, (datetime.now(UTC) - created.astimezone(UTC)).total_seconds() / 86400)
    except ValueError:
        return _result(
            check_id="freshness_metadata",
            dimension="freshness",
            score=0.0,
            observed_value=timestamp,
            threshold=max_age_days,
            message="Freshness timestamp is invalid.",
        )
    score = max(0.0, 1.0 - age_days / max(max_age_days, 1))
    return _result(
        check_id="freshness_metadata",
        dimension="freshness",
        score=score,
        observed_value=round(age_days, 3),
        threshold=max_age_days,
        message="Dataset freshness based on metadata timestamp.",
        warn_below=0.4,
        fail_below=0.1,
    )


def referential_integrity_check(
    child: pd.DataFrame,
    parent: pd.DataFrame | None,
    *,
    join_keys: list[str],
) -> QualityCheckResult:
    existing = [
        column for column in join_keys if column in child.columns and parent is not None and column in parent.columns
    ]
    if child.empty or parent is None or parent.empty or not existing:
        return _result(
            check_id="referential_integrity",
            dimension="referential_integrity",
            score=0.5,
            message="Referential integrity could not be fully evaluated.",
            warn_below=0.9,
            fail_below=0.4,
        )
    parent_keys = parent[existing].drop_duplicates()
    matched = child[existing].merge(parent_keys, on=existing, how="inner")
    score = len(matched.drop_duplicates()) / max(1, len(child[existing].drop_duplicates()))
    return _result(
        check_id="referential_integrity",
        dimension="referential_integrity",
        score=score,
        observed_value=round(score, 6),
        threshold=0.95,
        message=f"Referential integrity on keys: {existing}.",
    )


def territorial_coverage_check(df: pd.DataFrame) -> QualityCheckResult:
    candidates = ["uf", "cod_municipio_tse", "cod_municipio_ibge", "zona", "secao"]
    existing = [column for column in candidates if column in df.columns]
    if df.empty or not existing:
        return _result(
            check_id="territorial_coverage",
            dimension="territorial_coverage",
            score=0.0,
            message="No territorial coverage columns found.",
        )
    non_empty = df[existing].notna() & (df[existing].astype(str).apply(lambda col: col.str.strip()) != "")
    score = float(non_empty.sum().sum() / max(1, len(df) * len(existing)))
    return _result(
        check_id="territorial_coverage",
        dimension="territorial_coverage",
        score=score,
        observed_value=round(score, 6),
        threshold=0.9,
        message=f"Territorial coverage across {existing}.",
    )


def temporal_coverage_check(df: pd.DataFrame) -> QualityCheckResult:
    column = next((candidate for candidate in ["ano_eleicao", "ANO_ELEICAO", "ano"] if candidate in df.columns), None)
    if df.empty or column is None:
        return _result(
            check_id="temporal_coverage",
            dimension="temporal_coverage",
            score=0.0,
            message="No temporal coverage column found.",
        )
    years = pd.to_numeric(df[column], errors="coerce").dropna().astype(int)
    score = 1.0 if not years.empty else 0.0
    return _result(
        check_id="temporal_coverage",
        dimension="temporal_coverage",
        score=score,
        observed_value=f"{int(years.min())}-{int(years.max())}" if not years.empty else "none",
        threshold="at least one valid year",
        message="Temporal coverage based on election year.",
    )


def joinability_check(df: pd.DataFrame) -> QualityCheckResult:
    if "join_confidence" in df.columns:
        score = (
            float(pd.to_numeric(df["join_confidence"], errors="coerce").fillna(0.0).clip(0, 1).mean())
            if len(df)
            else 0.0
        )
        return _result(
            check_id="joinability_confidence",
            dimension="joinability",
            score=score,
            observed_value=round(score, 6),
            threshold=0.85,
            message="Joinability based on join_confidence.",
            warn_below=0.85,
            fail_below=0.6,
        )
    keys = ["cod_municipio_tse", "cod_municipio_ibge", "candidate_id", "territorio_id"]
    existing = [column for column in keys if column in df.columns]
    if df.empty or not existing:
        return _result(
            check_id="joinability_keys",
            dimension="joinability",
            score=0.0,
            message="No joinability keys available.",
        )
    score = float(
        (df[existing].notna() & df[existing].astype(str).ne("")).sum().sum() / max(1, len(df) * len(existing))
    )
    return _result(
        check_id="joinability_keys",
        dimension="joinability",
        score=score,
        observed_value=round(score, 6),
        threshold=0.85,
        message=f"Joinability based on keys: {existing}.",
    )


def distribution_check(df: pd.DataFrame, *, numeric_columns: list[str]) -> QualityCheckResult:
    existing = [column for column in numeric_columns if column in df.columns]
    if df.empty or not existing:
        return _result(
            check_id="distribution_numeric",
            dimension="distribution",
            score=0.5,
            message="No numeric columns available for distribution check.",
            warn_below=0.6,
            fail_below=0.2,
        )
    valid = 0
    for column in existing:
        values = pd.to_numeric(df[column], errors="coerce").dropna()
        if values.empty:
            continue
        if float(values.max()) >= float(values.min()):
            valid += 1
    score = valid / max(1, len(existing))
    return _result(
        check_id="distribution_numeric",
        dimension="distribution",
        score=score,
        observed_value=round(score, 6),
        threshold=1.0,
        message=f"Distribution sanity for numeric columns: {existing}.",
    )


def drift_check(current: pd.DataFrame, previous_path: Path | None, *, numeric_columns: list[str]) -> QualityCheckResult:
    if previous_path is None or not previous_path.exists() or current.empty:
        return _result(
            check_id="drift_numeric_mean",
            dimension="drift",
            score=1.0,
            observed_value="baseline_missing",
            threshold=0.2,
            message="No previous baseline available; drift check passes as initial run.",
        )
    previous = pd.read_parquet(previous_path)
    deltas: list[float] = []
    for column in numeric_columns:
        if column not in current.columns or column not in previous.columns:
            continue
        cur = pd.to_numeric(current[column], errors="coerce").dropna()
        prev = pd.to_numeric(previous[column], errors="coerce").dropna()
        if cur.empty or prev.empty:
            continue
        prev_mean = float(prev.mean())
        baseline = abs(prev_mean) if abs(prev_mean) > 1e-9 else 1.0
        deltas.append(abs(float(cur.mean()) - prev_mean) / baseline)
    if not deltas:
        return _result(
            check_id="drift_numeric_mean",
            dimension="drift",
            score=1.0,
            observed_value="no_comparable_columns",
            threshold=0.2,
            message="No comparable numeric columns for drift.",
        )
    drift = sum(deltas) / len(deltas)
    score = max(0.0, 1.0 - drift)
    return _result(
        check_id="drift_numeric_mean",
        dimension="drift",
        score=score,
        observed_value=round(drift, 6),
        threshold=0.2,
        message="Drift based on relative change in numeric means.",
        warn_below=0.8,
        fail_below=0.6,
    )
