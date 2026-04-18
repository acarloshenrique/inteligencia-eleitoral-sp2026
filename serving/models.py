from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

ServingAudience = Literal["api", "ui", "report", "recommendation_engine", "commercial_demo"]
ServingFormat = Literal["parquet", "csv", "json"]


def default_serving_formats() -> list[ServingFormat]:
    return ["parquet", "csv"]


class ServingOutputSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    output_id: str
    description: str
    grain: str
    primary_key: list[str]
    source_tables: list[str]
    audiences: list[ServingAudience]
    formats: list[ServingFormat] = Field(default_factory=default_serving_formats)
    quality_rules: list[str]
    lgpd_classification: str = "derived_aggregate_or_campaign_confidential"
    join_limitations: list[str] = Field(default_factory=list)


class ServingOutputManifest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tenant_id: str
    campaign_id: str
    snapshot_id: str
    dataset_version: str
    generated_at_utc: str
    outputs: dict[str, dict[str, str]]
    row_counts: dict[str, int]
    quality: dict[str, dict[str, float | int | str | bool]]
    source_tables: list[str]
    warnings: list[str] = Field(default_factory=list)


class ServingOutputWriteResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    output_id: str
    rows: int
    parquet_path: str
    csv_path: str
    json_path: str
