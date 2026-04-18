from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

CommercialImpact = Literal["critical", "high", "medium", "low"]
ExportFormat = Literal["parquet", "json", "csv", "xlsx", "markdown"]


class TenantIsolationPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tenant_id: str
    storage_root: str
    logical_filters: dict[str, str] = Field(default_factory=dict)
    pii_policy: str = "minimize_personal_data_and_never_log_voter_level_data"
    export_watermark: str = "tenant_id + campaign_id + snapshot_id required"


class CommercialAssetSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    asset_id: str
    name: str
    description: str
    impact: CommercialImpact
    source_tables: list[str]
    supported_outputs: list[ExportFormat]
    commercial_use_cases: list[str]
    demo_readiness: bool
    premium_report_ready: bool
    multi_tenant_required: bool = True
    limitations: list[str] = Field(default_factory=list)


class CampaignSnapshotSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tenant_id: str
    campaign_id: str
    candidate_ids: list[str]
    snapshot_id: str
    dataset_version: str
    generated_at_utc: str
    source_tables: list[str]


class CommercialExportManifest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tenant_id: str
    campaign_id: str
    snapshot_id: str
    exported_files: dict[str, str]
    generated_at_utc: str
    row_counts: dict[str, int]
    notes: list[str] = Field(default_factory=list)
