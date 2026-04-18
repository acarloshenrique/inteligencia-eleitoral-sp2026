from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Callable

import pandas as pd

from config.settings import AppPaths
from data_catalog.models import DataSourceSpec
from ingestion.downloader import DownloadedAsset, SourceDownloader, sha256_file
from ingestion.harmonizer import KeyHarmonizer
from ingestion.normalizer import DatasetNormalizer
from ingestion.validator import DatasetValidator, ValidationReport
from ingestion.writer import ParquetDuckDBWriter, WrittenDataset

logger = logging.getLogger(__name__)
Normalizer = Callable[[Path, DataSourceSpec], pd.DataFrame]


@dataclass(frozen=True)
class IngestionResult:
    source_name: str
    run_id: str
    bronze_path: Path
    silver_path: Path
    gold_path: Path
    manifest_path: Path
    rows: int
    status: str
    duckdb_path: Path | None = None
    failure_path: Path | None = None


def _ts_now() -> str:
    return datetime.now(UTC).strftime("%Y%m%d_%H%M%S")


def default_csv_normalizer(path: Path, source: DataSourceSpec) -> pd.DataFrame:
    return DatasetNormalizer().normalize(path, source)


class LayeredIngestionPipeline:
    def __init__(
        self,
        paths: AppPaths,
        *,
        downloader: SourceDownloader | None = None,
        normalizer: DatasetNormalizer | None = None,
        validator: DatasetValidator | None = None,
        harmonizer: KeyHarmonizer | None = None,
        writer: ParquetDuckDBWriter | None = None,
    ):
        self.paths = paths
        self.downloader = downloader or SourceDownloader()
        self.normalizer = normalizer or DatasetNormalizer()
        self.validator = validator or DatasetValidator()
        self.harmonizer = harmonizer or KeyHarmonizer()
        self.writer = writer or ParquetDuckDBWriter()

    def ingest_local_file(
        self,
        *,
        source: DataSourceSpec,
        input_path: Path,
        normalizer: Normalizer | None = None,
        run_id: str | None = None,
    ) -> IngestionResult:
        return self.ingest_source(source=source, input_path=input_path, normalizer=normalizer, run_id=run_id)

    def ingest_source(
        self,
        *,
        source: DataSourceSpec,
        input_path: Path | None = None,
        normalizer: Normalizer | None = None,
        run_id: str | None = None,
    ) -> IngestionResult:
        run_id = run_id or _ts_now()
        run_root = self.paths.ingestion_root / "pipeline_runs" / "layered_ingestion_v1" / source.name / run_id
        bronze_dir = self.paths.bronze_root / source.name / run_id
        silver_dir = self.paths.silver_root / source.name / run_id
        gold_dir = self.paths.gold_root / source.name / run_id
        for folder in [run_root, bronze_dir, silver_dir, gold_dir]:
            folder.mkdir(parents=True, exist_ok=True)

        logger.info(
            "ingestion_started",
            extra={
                "source_name": source.name,
                "run_id": run_id,
                "tier": source.tier,
                "input_path": str(input_path or source.url),
            },
        )
        try:
            asset = self.downloader.download(source=source, destination_dir=bronze_dir, input_path=input_path)
            raw_df = (
                normalizer(asset.path, source)
                if normalizer is not None
                else self.normalizer.normalize(asset.path, source)
            )
            silver_df = self.harmonizer.harmonize(raw_df, source)
            validation_report = self.validator.validate(silver_df, source)
            if not validation_report.ok:
                raise ValueError("; ".join(validation_report.errors))
            silver_df = self._with_lineage(silver_df, source=source, run_id=run_id, layer="silver")
            silver_written = self.writer.write(df=silver_df, source=source, destination_dir=silver_dir)

            gold_df = self._publish_gold(silver_df, source)
            gold_written = self.writer.write(df=gold_df, source=source, destination_dir=gold_dir)

            manifest_path = self._write_manifest(
                run_root=run_root,
                source=source,
                run_id=run_id,
                asset=asset,
                validation=validation_report,
                silver=silver_written,
                gold=gold_written,
            )
            logger.info(
                "ingestion_finished",
                extra={"source_name": source.name, "run_id": run_id, "rows": gold_written.rows, "status": "ok"},
            )
            return IngestionResult(
                source_name=source.name,
                run_id=run_id,
                bronze_path=asset.path,
                silver_path=silver_written.parquet_path,
                gold_path=gold_written.parquet_path,
                manifest_path=manifest_path,
                rows=gold_written.rows,
                status="ok",
                duckdb_path=gold_written.duckdb_path,
            )
        except Exception as exc:
            failure_path = self._write_failure(run_root=run_root, source=source, run_id=run_id, error=exc)
            logger.exception(
                "ingestion_failed",
                extra={"source_name": source.name, "run_id": run_id, "failure_path": str(failure_path)},
            )
            return IngestionResult(
                source_name=source.name,
                run_id=run_id,
                bronze_path=bronze_dir,
                silver_path=silver_dir,
                gold_path=gold_dir,
                manifest_path=failure_path,
                rows=0,
                status="failed",
                failure_path=failure_path,
            )

    def _with_lineage(self, df: pd.DataFrame, *, source: DataSourceSpec, run_id: str, layer: str) -> pd.DataFrame:
        out = df.copy()
        out["source_name"] = source.name
        out["source_tier"] = int(source.tier)
        out["ingestion_run_id"] = run_id
        out["lake_layer"] = layer
        out["ingested_at_utc"] = datetime.now(UTC).isoformat()
        out["lgpd_classification"] = source.lgpd_classification
        return out

    def _publish_gold(self, silver_df: pd.DataFrame, source: DataSourceSpec) -> pd.DataFrame:
        gold = silver_df.copy()
        gold["lake_layer"] = "gold"
        key_cols = [key for key in source.chaves_principais if key in gold.columns]
        if key_cols:
            gold = gold.drop_duplicates(key_cols).reset_index(drop=True)
        return gold

    def _write_manifest(
        self,
        *,
        run_root: Path,
        source: DataSourceSpec,
        run_id: str,
        asset: DownloadedAsset,
        validation: ValidationReport,
        silver: WrittenDataset,
        gold: WrittenDataset,
    ) -> Path:
        manifest = {
            "pipeline_version": "layered_ingestion_v1",
            "source": source.model_dump(mode="json"),
            "run_id": run_id,
            "status": "ok",
            "created_at_utc": datetime.now(UTC).isoformat(),
            "bronze": {
                "path": str(asset.path),
                "sha256": asset.sha256,
                "bytes": asset.bytes_size,
                "source_url": asset.source_url,
            },
            "silver": {
                "path": str(silver.parquet_path),
                "duckdb_path": str(silver.duckdb_path) if silver.duckdb_path else None,
                "rows": silver.rows,
            },
            "gold": {
                "path": str(gold.parquet_path),
                "duckdb_path": str(gold.duckdb_path) if gold.duckdb_path else None,
                "rows": gold.rows,
            },
            "validation": asdict(validation),
            "schema": gold.schema,
            "quality": {
                "rows": gold.rows,
                "missing_keys": validation.missing_keys,
                "null_key_counts": validation.null_key_counts,
                "input_sha256": sha256_file(asset.path),
            },
        }
        manifest_path = run_root / "manifest.json"
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        return manifest_path

    def _write_failure(self, *, run_root: Path, source: DataSourceSpec, run_id: str, error: Exception) -> Path:
        payload = {
            "pipeline_version": "layered_ingestion_v1",
            "source_name": source.name,
            "run_id": run_id,
            "status": "failed",
            "error_type": type(error).__name__,
            "error": str(error),
            "created_at_utc": datetime.now(UTC).isoformat(),
        }
        failure_path = run_root / "failure.json"
        failure_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return failure_path
