from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

FeatureFamily = Literal[
    "base_eleitoral",
    "competicao",
    "territorial",
    "tematica",
    "eficiencia",
    "operacional",
]


class FeatureSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    feature_name: str
    family: FeatureFamily
    definition: str
    grain: str
    dtype: Literal["float", "int", "string", "bool"]
    version: str = "v1"
    lineage: list[str]
    recomputation_policy: str
    owner: str = "data-products"
    quality_rules: list[str] = Field(default_factory=list)


class FeatureSetManifest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    manifest_version: str = "feature_store_manifest_v1"
    feature_set_id: str
    feature_version: str
    rows: int
    features: list[str]
    lineage: list[str]
    output_path: str
    duckdb_path: str | None = None
    computed_at_utc: str
    quality: dict[str, float | int | str] = Field(default_factory=dict)
