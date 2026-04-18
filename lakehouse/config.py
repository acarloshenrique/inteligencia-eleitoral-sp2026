from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from config.settings import AppPaths
from lakehouse.contracts import DatasetContract, LakeLayer

LAKE_LAYERS: tuple[LakeLayer, ...] = ("bronze", "silver", "gold", "semantic", "serving")
SUPPORT_DIRS: tuple[str, ...] = ("catalog", "manifests", "lineage", "duckdb", "examples")


def _slug(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9_=-]+", "_", str(value).strip().lower())
    return re.sub(r"_+", "_", slug).strip("_") or "unknown"


@dataclass(frozen=True)
class LakehouseConfig:
    root: Path
    default_format: str = "parquet"
    duckdb_filename: str = "electoral_lakehouse.duckdb"

    @classmethod
    def from_paths(cls, paths: AppPaths) -> "LakehouseConfig":
        root = getattr(paths, "lakehouse_root", paths.lake_root)
        return cls(root=root)


class LakehouseLayout:
    def __init__(self, config: LakehouseConfig):
        self.config = config

    def ensure(self) -> dict[str, Path]:
        created: dict[str, Path] = {}
        for name in [*LAKE_LAYERS, *SUPPORT_DIRS]:
            folder = self.config.root / name
            folder.mkdir(parents=True, exist_ok=True)
            created[name] = folder
        return created

    def layer_root(self, layer: LakeLayer) -> Path:
        if layer not in LAKE_LAYERS:
            raise ValueError(f"camada lakehouse invalida: {layer}")
        path = self.config.root / layer
        path.mkdir(parents=True, exist_ok=True)
        return path

    @property
    def catalog_root(self) -> Path:
        path = self.config.root / "catalog"
        path.mkdir(parents=True, exist_ok=True)
        return path

    @property
    def manifest_root(self) -> Path:
        path = self.config.root / "manifests"
        path.mkdir(parents=True, exist_ok=True)
        return path

    @property
    def lineage_root(self) -> Path:
        path = self.config.root / "lineage"
        path.mkdir(parents=True, exist_ok=True)
        return path

    @property
    def duckdb_root(self) -> Path:
        path = self.config.root / "duckdb"
        path.mkdir(parents=True, exist_ok=True)
        return path


class LakehouseNaming:
    def dataset_dir(
        self,
        *,
        layout: LakehouseLayout,
        contract: DatasetContract,
        dataset_version: str,
        partition_values: dict[str, Any] | None = None,
    ) -> Path:
        root = layout.layer_root(contract.layer)
        parts = [
            root,
            Path(_slug(contract.entity)),
            Path(_slug(contract.dataset_id)),
            Path(f"dataset_version={_slug(dataset_version)}"),
        ]
        for column in contract.partition_policy.columns:
            value = (partition_values or {}).get(column, "unknown")
            parts.append(Path(f"{_slug(column)}={_slug(str(value))}"))
        path = Path(*parts)
        path.mkdir(parents=True, exist_ok=True)
        return path

    def parquet_path(
        self,
        *,
        layout: LakehouseLayout,
        contract: DatasetContract,
        dataset_version: str,
        partition_values: dict[str, Any] | None = None,
    ) -> Path:
        folder = self.dataset_dir(
            layout=layout,
            contract=contract,
            dataset_version=dataset_version,
            partition_values=partition_values,
        )
        return folder / f"{_slug(contract.dataset_id)}.parquet"

    def raw_path(
        self,
        *,
        layout: LakehouseLayout,
        contract: DatasetContract,
        dataset_version: str,
        original_name: str,
        partition_values: dict[str, Any] | None = None,
    ) -> Path:
        folder = self.dataset_dir(
            layout=layout,
            contract=contract,
            dataset_version=dataset_version,
            partition_values=partition_values,
        )
        return folder / original_name
