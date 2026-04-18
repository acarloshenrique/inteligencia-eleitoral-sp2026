from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from lakehouse.contracts import LakeLayer


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class IngestionManifest(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    manifest_version: str = "lakehouse_manifest_v1"
    dataset_id: str
    layer: LakeLayer
    dataset_version: str
    run_id: str
    status: Literal["ok", "failed"] = "ok"
    created_at_utc: str = Field(default_factory=utc_now_iso)
    source_name: str
    source_url: str = ""
    source_hash_sha256: str = ""
    source_bytes: int = 0
    output_path: str
    output_format: str = "parquet"
    output_rows: int = 0
    schema_definition: dict[str, str] = Field(default_factory=dict, alias="schema")
    primary_key: list[str] = Field(default_factory=list)
    coverage: dict[str, str] = Field(default_factory=dict)
    partition_values: dict[str, str] = Field(default_factory=dict)
    quality: dict[str, Any] = Field(default_factory=dict)
    lineage_inputs: list[str] = Field(default_factory=list)
    error: str = ""

    def write(self, path: Path) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(self.model_dump(mode="json", by_alias=True), ensure_ascii=False, indent=2), encoding="utf-8"
        )
        return path


class LineageRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    lineage_version: str = "lakehouse_lineage_v1"
    transformation_id: str
    dataset_id: str
    layer: LakeLayer
    dataset_version: str
    created_at_utc: str = Field(default_factory=utc_now_iso)
    inputs: list[str] = Field(default_factory=list)
    outputs: list[str] = Field(default_factory=list)
    operation: str
    business_rule: str = ""
    code_version: str = "local"
    quality: dict[str, Any] = Field(default_factory=dict)
    evidence: list[dict[str, Any]] = Field(default_factory=list)

    def write(self, path: Path) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(self.model_dump(mode="json", by_alias=True), ensure_ascii=False, indent=2), encoding="utf-8"
        )
        return path
