from __future__ import annotations

import json
import logging
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

import pandas as pd

from lakehouse.config import LakehouseConfig, LakehouseLayout, LakehouseNaming
from lakehouse.contracts import DatasetContract, LakeLayer
from lakehouse.manifest import IngestionManifest, LineageRecord, sha256_file

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LakehouseWriteResult:
    dataset_id: str
    layer: LakeLayer
    dataset_version: str
    output_path: Path
    manifest_path: Path
    lineage_path: Path | None
    duckdb_path: Path | None
    rows: int


class ContractValidationError(ValueError):
    pass


class BaseLakehouseIngestion:
    def __init__(self, config: LakehouseConfig, naming: LakehouseNaming | None = None):
        self.config = config
        self.layout = LakehouseLayout(config)
        self.naming = naming or LakehouseNaming()
        self.layout.ensure()

    def preserve_raw(
        self,
        *,
        input_path: Path,
        contract: DatasetContract,
        dataset_version: str,
        run_id: str | None = None,
        partition_values: dict[str, Any] | None = None,
    ) -> LakehouseWriteResult:
        if contract.layer != "bronze":
            raise ContractValidationError("preserve_raw exige contrato da camada bronze")
        if not input_path.exists():
            raise FileNotFoundError(input_path)
        run_id = run_id or str(uuid4())
        output_path = self.naming.raw_path(
            layout=self.layout,
            contract=contract,
            dataset_version=dataset_version,
            original_name=input_path.name,
            partition_values=partition_values,
        )
        shutil.copy2(input_path, output_path)
        manifest = IngestionManifest(
            dataset_id=contract.dataset_id,
            layer=contract.layer,
            dataset_version=dataset_version,
            run_id=run_id,
            source_name=contract.source_name,
            source_url=contract.source_url,
            source_hash_sha256=sha256_file(input_path),
            source_bytes=input_path.stat().st_size,
            output_path=str(output_path),
            output_format=input_path.suffix.lstrip(".") or "raw",
            output_rows=0,
            schema=contract.schema_definition,
            primary_key=contract.primary_key,
            coverage=contract.coverage,
            partition_values={str(k): str(v) for k, v in (partition_values or {}).items()},
            quality={"raw_preserved": True, "destructive_transform": False},
        )
        manifest_path = (
            self.layout.manifest_root / contract.layer / contract.dataset_id / dataset_version / f"{run_id}.json"
        )
        manifest.write(manifest_path)
        logger.info(
            "lakehouse_raw_preserved",
            extra={"dataset_id": contract.dataset_id, "run_id": run_id, "output_path": str(output_path)},
        )
        return LakehouseWriteResult(
            dataset_id=contract.dataset_id,
            layer=contract.layer,
            dataset_version=dataset_version,
            output_path=output_path,
            manifest_path=manifest_path,
            lineage_path=None,
            duckdb_path=None,
            rows=0,
        )


class BaseLakehouseTransformation:
    def __init__(self, config: LakehouseConfig, naming: LakehouseNaming | None = None):
        self.config = config
        self.layout = LakehouseLayout(config)
        self.naming = naming or LakehouseNaming()
        self.layout.ensure()

    def write_dataframe(
        self,
        *,
        df: pd.DataFrame,
        contract: DatasetContract,
        dataset_version: str,
        run_id: str | None = None,
        inputs: list[str] | None = None,
        partition_values: dict[str, Any] | None = None,
        operation: str = "transform",
        business_rule: str = "",
        write_duckdb: bool = True,
    ) -> LakehouseWriteResult:
        if contract.layer == "bronze":
            raise ContractValidationError(
                "write_dataframe e destinado a silver/gold/semantic/serving; use preserve_raw para bronze"
            )
        run_id = run_id or str(uuid4())
        self._validate_contract(df, contract)
        output_path = self.naming.parquet_path(
            layout=self.layout,
            contract=contract,
            dataset_version=dataset_version,
            partition_values=partition_values,
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path, index=False)
        duckdb_path = self._write_duckdb(df, contract) if write_duckdb else None
        quality = self._quality_report(df, contract)
        manifest = IngestionManifest(
            dataset_id=contract.dataset_id,
            layer=contract.layer,
            dataset_version=dataset_version,
            run_id=run_id,
            source_name=contract.source_name,
            source_url=contract.source_url,
            source_hash_sha256=sha256_file(output_path),
            source_bytes=output_path.stat().st_size,
            output_path=str(output_path),
            output_format="parquet",
            output_rows=int(len(df)),
            schema={column: str(dtype) for column, dtype in df.dtypes.items()},
            primary_key=contract.primary_key,
            coverage=contract.coverage,
            partition_values={str(k): str(v) for k, v in (partition_values or {}).items()},
            quality=quality,
            lineage_inputs=inputs or contract.lineage_inputs,
        )
        manifest_path = (
            self.layout.manifest_root / contract.layer / contract.dataset_id / dataset_version / f"{run_id}.json"
        )
        manifest.write(manifest_path)
        lineage = LineageRecord(
            transformation_id=run_id,
            dataset_id=contract.dataset_id,
            layer=contract.layer,
            dataset_version=dataset_version,
            inputs=inputs or contract.lineage_inputs,
            outputs=[str(output_path)],
            operation=operation,
            business_rule=business_rule or contract.business_documentation,
            quality=quality,
            evidence=[{"manifest_path": str(manifest_path), "contract_schema_version": contract.schema_version}],
        )
        lineage_path = (
            self.layout.lineage_root / contract.layer / contract.dataset_id / dataset_version / f"{run_id}.json"
        )
        lineage.write(lineage_path)
        logger.info(
            "lakehouse_dataframe_written",
            extra={
                "dataset_id": contract.dataset_id,
                "layer": contract.layer,
                "rows": len(df),
                "output_path": str(output_path),
            },
        )
        return LakehouseWriteResult(
            dataset_id=contract.dataset_id,
            layer=contract.layer,
            dataset_version=dataset_version,
            output_path=output_path,
            manifest_path=manifest_path,
            lineage_path=lineage_path,
            duckdb_path=duckdb_path,
            rows=int(len(df)),
        )

    def _validate_contract(self, df: pd.DataFrame, contract: DatasetContract) -> None:
        missing = [column for column in contract.required_columns if column not in df.columns]
        if missing:
            raise ContractValidationError(f"colunas obrigatorias ausentes em {contract.dataset_id}: {missing}")
        key_missing = [column for column in contract.primary_key if column not in df.columns]
        if key_missing:
            raise ContractValidationError(f"chave primaria ausente em {contract.dataset_id}: {key_missing}")
        if contract.primary_key and df.duplicated(contract.primary_key).any():
            raise ContractValidationError(f"chave primaria duplicada em {contract.dataset_id}: {contract.primary_key}")

    def _quality_report(self, df: pd.DataFrame, contract: DatasetContract) -> dict[str, Any]:
        null_counts = {
            column: int(df[column].isna().sum()) for column in contract.required_columns if column in df.columns
        }
        key_coverage = 1.0
        if contract.primary_key:
            present = df[contract.primary_key].notna().all(axis=1)
            key_coverage = round(float(present.mean()), 6) if len(df) else 0.0
        return {
            "rows": int(len(df)),
            "columns": int(len(df.columns)),
            "required_null_counts": null_counts,
            "primary_key_coverage": key_coverage,
            "primary_key_unique": bool(not df.duplicated(contract.primary_key).any()) if contract.primary_key else True,
            "quality_rules": contract.quality_rules,
        }

    def _write_duckdb(self, df: pd.DataFrame, contract: DatasetContract) -> Path | None:
        try:
            import duckdb
        except ImportError:
            return None
        duckdb_path = self.layout.duckdb_root / self.config.duckdb_filename
        table_name = contract.dataset_id.replace("-", "_")
        with duckdb.connect(str(duckdb_path)) as con:
            con.register("_lake_df", df)
            con.execute(f'CREATE OR REPLACE TABLE "{table_name}" AS SELECT * FROM _lake_df')  # noqa: S608
        return duckdb_path


def write_catalog_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path
