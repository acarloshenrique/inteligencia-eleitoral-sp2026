from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

QualityDimension = Literal[
    "completeness",
    "uniqueness",
    "validity",
    "freshness",
    "referential_integrity",
    "territorial_coverage",
    "temporal_coverage",
    "joinability",
    "distribution",
    "drift",
]

QualityStatus = Literal["pass", "warn", "fail"]


class QualityCheckResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    check_id: str
    dimension: QualityDimension
    status: QualityStatus
    score: float = Field(ge=0, le=1)
    observed_value: float | str | int | None = None
    threshold: float | str | int | None = None
    message: str


class DatasetQualityReport(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dataset_id: str
    row_count: int
    quality_score: float = Field(ge=0, le=1)
    production_readiness: Literal["production_ready", "limited_use", "not_ready"]
    checks: list[QualityCheckResult]
    limitations: list[str] = Field(default_factory=list)
    reliable_joins: list[str] = Field(default_factory=list)
    generated_at_utc: str


class LakeHealthReport(BaseModel):
    model_config = ConfigDict(extra="forbid")

    report_version: str = "lake_health_report_v1"
    datasets: list[DatasetQualityReport]
    aggregate_quality_score: float = Field(ge=0, le=1)
    production_ready_datasets: list[str]
    limited_datasets: list[str]
    not_ready_datasets: list[str]
    trusted_joins: list[str]
    generated_at_utc: str
