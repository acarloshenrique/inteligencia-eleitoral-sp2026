from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

LakeLayer = Literal["bronze", "silver", "gold", "semantic", "serving"]
IncrementalStrategy = Literal["snapshot", "append", "upsert_by_key", "merge_by_partition"]
SensitivityClass = Literal[
    "public_open_data_aggregated",
    "public_open_data_personal",
    "campaign_operational_confidential",
    "derived_aggregate",
]


class PartitionPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    columns: list[str] = Field(default_factory=list)
    description: str = "Sem particionamento explicito."


class DatasetContract(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dataset_id: str = Field(min_length=1)
    layer: LakeLayer
    owner: str = Field(min_length=1)
    entity: str = Field(min_length=1)
    fact_or_dimension: Literal["fact", "dimension", "metric_view", "source_snapshot", "serving_output"]
    business_description: str = Field(min_length=1)
    source_name: str = Field(min_length=1)
    source_url: str = ""
    schema_version: str = "v1"
    dataset_version: str = "v1"
    granularity: str = Field(min_length=1)
    primary_key: list[str] = Field(default_factory=list)
    schema_definition: dict[str, str] = Field(default_factory=dict, alias="schema")
    required_columns: list[str] = Field(default_factory=list)
    partition_policy: PartitionPolicy = Field(default_factory=PartitionPolicy)
    incremental_strategy: IncrementalStrategy = "snapshot"
    coverage: dict[str, str] = Field(default_factory=dict)
    quality_rules: list[str] = Field(default_factory=list)
    lineage_inputs: list[str] = Field(default_factory=list)
    lgpd_classification: SensitivityClass = "public_open_data_aggregated"
    business_documentation: str = ""

    @model_validator(mode="after")
    def validate_declared_columns(self) -> "DatasetContract":
        missing_required = [column for column in self.required_columns if column not in self.schema_definition]
        if missing_required:
            raise ValueError(f"required_columns ausentes do schema: {missing_required}")
        missing_keys = [column for column in self.primary_key if column not in self.schema_definition]
        if missing_keys:
            raise ValueError(f"primary_key ausente do schema: {missing_keys}")
        return self


class LakehouseCatalog(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: str = "electoral_lakehouse_catalog_v1"
    owner: str = "data_platform"
    datasets: list[DatasetContract]

    def by_id(self, dataset_id: str) -> DatasetContract | None:
        normalized = dataset_id.strip().lower()
        return next((dataset for dataset in self.datasets if dataset.dataset_id.lower() == normalized), None)

    def by_layer(self, layer: LakeLayer) -> list[DatasetContract]:
        return [dataset for dataset in self.datasets if dataset.layer == layer]
