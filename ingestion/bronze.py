from __future__ import annotations

import json
import logging
import shutil
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field

from lakehouse.manifest import sha256_file

logger = logging.getLogger(__name__)

BronzeStatus = Literal["ok", "skipped_duplicate", "failed"]


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def utc_partition() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def _slug(value: str | int | None) -> str:
    text = str(value or "unknown").strip().lower()
    allowed = [char if char.isalnum() else "_" for char in text]
    return "_".join("".join(allowed).split("_")).strip("_") or "unknown"


class BronzeIngestionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dataset_id: str = Field(min_length=1)
    source: str = Field(min_length=1)
    source_url: str = Field(min_length=1)
    formato: str = Field(min_length=1)
    reference_period: str = Field(min_length=1)
    ano: int | None = None
    uf: str | None = None
    municipio: str | None = None
    local_path: Path | None = None
    expected_sha256: str | None = None
    metadata: dict[str, str] = Field(default_factory=dict)


class IntegrityValidationResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ok: bool
    sha256: str
    size_bytes: int
    errors: list[str] = Field(default_factory=list)


class SourceManifest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    manifest_version: str = "bronze_source_manifest_v1"
    run_id: str
    dataset_id: str
    source: str
    source_url: str
    collected_at_utc: str = Field(default_factory=utc_now_iso)
    sha256: str = ""
    size_bytes: int = 0
    formato: str
    reference_period: str
    ano: int | None = None
    uf: str | None = None
    municipio: str | None = None
    status: BronzeStatus
    output_path: str = ""
    duplicate_of: str | None = None
    attempt_count: int = 0
    error: str = ""
    metadata: dict[str, str] = Field(default_factory=dict)

    def write(self, path: Path) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.model_dump(mode="json"), ensure_ascii=False, indent=2), encoding="utf-8")
        return path


class IngestionReport(BaseModel):
    model_config = ConfigDict(extra="forbid")

    report_version: str = "bronze_ingestion_report_v1"
    run_id: str
    dataset_id: str
    source: str
    status: BronzeStatus
    started_at_utc: str
    finished_at_utc: str
    output_path: str = ""
    manifest_path: str = ""
    error_report_path: str = ""
    sha256: str = ""
    size_bytes: int = 0
    attempt_count: int = 0
    errors: list[str] = Field(default_factory=list)

    def write(self, path: Path) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.model_dump(mode="json"), ensure_ascii=False, indent=2), encoding="utf-8")
        return path


@dataclass(frozen=True)
class DownloadedRawAsset:
    path: Path
    source_url: str
    attempt_count: int


class DownloadClient:
    def __init__(self, *, retries: int = 3, backoff_seconds: float = 0.25, timeout_seconds: int = 60):
        self.retries = max(1, retries)
        self.backoff_seconds = max(0.0, backoff_seconds)
        self.timeout_seconds = timeout_seconds

    def download(self, request: BronzeIngestionRequest, destination_dir: Path) -> DownloadedRawAsset:
        destination_dir.mkdir(parents=True, exist_ok=True)
        if request.local_path is not None:
            return self._copy_local(request, destination_dir)

        parsed = urlparse(request.source_url)
        if parsed.scheme not in {"http", "https"}:
            raise ValueError("source_url must use http or https when local_path is not provided")

        filename = Path(parsed.path).name or f"{request.dataset_id}.download"
        destination = destination_dir / filename
        last_error: Exception | None = None
        for attempt in range(1, self.retries + 1):
            try:
                req = Request(  # noqa: S310
                    request.source_url,
                    headers={"User-Agent": "inteligencia-eleitoral-bronze/1.0"},
                )
                with urlopen(req, timeout=self.timeout_seconds) as response:  # noqa: S310
                    destination.write_bytes(response.read())
                return DownloadedRawAsset(path=destination, source_url=request.source_url, attempt_count=attempt)
            except Exception as exc:  # pragma: no cover - exercised with integration/network tests.
                last_error = exc
                logger.warning(
                    "bronze_download_retry",
                    extra={"dataset_id": request.dataset_id, "attempt": attempt, "error": str(exc)},
                )
                if attempt < self.retries:
                    time.sleep(self.backoff_seconds * attempt)
        raise RuntimeError(f"download failed after {self.retries} attempts: {last_error}") from last_error

    def _copy_local(self, request: BronzeIngestionRequest, destination_dir: Path) -> DownloadedRawAsset:
        source_path = request.local_path
        if source_path is None or not source_path.exists():
            raise FileNotFoundError(f"local_path not found: {source_path}")
        destination = destination_dir / source_path.name
        if not destination.exists() or sha256_file(destination) != sha256_file(source_path):
            shutil.copy2(source_path, destination)
        return DownloadedRawAsset(path=destination, source_url=str(source_path), attempt_count=1)


class FileIntegrityValidator:
    def validate(
        self,
        path: Path,
        *,
        expected_sha256: str | None = None,
        min_bytes: int = 1,
    ) -> IntegrityValidationResult:
        errors: list[str] = []
        if not path.exists():
            return IntegrityValidationResult(ok=False, sha256="", size_bytes=0, errors=[f"file not found: {path}"])
        size = path.stat().st_size
        file_hash = sha256_file(path)
        if size < min_bytes:
            errors.append(f"file is smaller than minimum size: {size} < {min_bytes}")
        if expected_sha256 and file_hash.lower() != expected_sha256.lower():
            errors.append("sha256 does not match expected value")
        return IntegrityValidationResult(ok=not errors, sha256=file_hash, size_bytes=size, errors=errors)


class RawDatasetWriter:
    def __init__(self, *, bronze_root: Path, manifest_root: Path | None = None):
        self.bronze_root = bronze_root
        self.manifest_root = manifest_root or bronze_root.parent / "manifests" / "bronze"
        self.index_path = self.manifest_root / "hash_index.json"

    def write(
        self,
        *,
        request: BronzeIngestionRequest,
        asset_path: Path,
        integrity: IntegrityValidationResult,
        run_id: str,
        attempt_count: int,
    ) -> tuple[SourceManifest, Path]:
        index = self._read_index()
        index_key = self._index_key(request, integrity.sha256)
        duplicate_of = index.get(index_key)
        status: BronzeStatus = "skipped_duplicate" if duplicate_of else "ok"
        output_path = (
            Path(duplicate_of) if duplicate_of else self._raw_output_path(request, asset_path, integrity.sha256)
        )
        if duplicate_of is None:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(asset_path, output_path)
            index[index_key] = str(output_path)
            self._write_index(index)

        manifest = SourceManifest(
            run_id=run_id,
            dataset_id=request.dataset_id,
            source=request.source,
            source_url=request.source_url,
            sha256=integrity.sha256,
            size_bytes=integrity.size_bytes,
            formato=request.formato,
            reference_period=request.reference_period,
            ano=request.ano,
            uf=request.uf,
            municipio=request.municipio,
            status=status,
            output_path=str(output_path),
            duplicate_of=duplicate_of,
            attempt_count=attempt_count,
            metadata=request.metadata,
        )
        manifest_path = self._manifest_path(request, run_id)
        manifest.write(manifest_path)
        return manifest, manifest_path

    def write_failure(
        self,
        *,
        request: BronzeIngestionRequest,
        run_id: str,
        error: Exception,
        attempt_count: int = 0,
    ) -> Path:
        manifest = SourceManifest(
            run_id=run_id,
            dataset_id=request.dataset_id,
            source=request.source,
            source_url=request.source_url,
            formato=request.formato,
            reference_period=request.reference_period,
            ano=request.ano,
            uf=request.uf,
            municipio=request.municipio,
            status="failed",
            attempt_count=attempt_count,
            error=str(error),
            metadata=request.metadata,
        )
        path = self._manifest_path(request, run_id, filename="failure.json")
        manifest.write(path)
        return path

    def _raw_output_path(self, request: BronzeIngestionRequest, asset_path: Path, file_hash: str) -> Path:
        year = str(request.ano or request.reference_period)
        uf = request.uf or "BR"
        suffix = asset_path.suffix or f".{request.formato.strip('.')}"
        filename = f"{file_hash[:16]}_{_slug(asset_path.stem)}{suffix}"
        return (
            self.bronze_root
            / _slug(request.source)
            / _slug(request.dataset_id)
            / _slug(year)
            / _slug(uf)
            / f"ingested_at={utc_partition()}"
            / filename
        )

    def _manifest_path(self, request: BronzeIngestionRequest, run_id: str, filename: str = "manifest.json") -> Path:
        year = str(request.ano or request.reference_period)
        uf = request.uf or "BR"
        return (
            self.manifest_root
            / _slug(request.source)
            / _slug(request.dataset_id)
            / _slug(year)
            / _slug(uf)
            / f"run_id={run_id}"
            / filename
        )

    def _read_index(self) -> dict[str, str]:
        if not self.index_path.exists():
            return {}
        return json.loads(self.index_path.read_text(encoding="utf-8"))

    def _write_index(self, index: dict[str, str]) -> None:
        self.index_path.parent.mkdir(parents=True, exist_ok=True)
        self.index_path.write_text(json.dumps(index, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    def _index_key(self, request: BronzeIngestionRequest, file_hash: str) -> str:
        return f"{_slug(request.source)}:{_slug(request.dataset_id)}:{file_hash}"


class BaseIngestionJob:
    def __init__(
        self,
        *,
        bronze_root: Path,
        run_root: Path | None = None,
        download_client: DownloadClient | None = None,
        validator: FileIntegrityValidator | None = None,
        writer: RawDatasetWriter | None = None,
    ):
        self.bronze_root = bronze_root
        self.run_root = run_root or bronze_root.parent / "pipeline_runs" / "bronze"
        self.download_client = download_client or DownloadClient()
        self.validator = validator or FileIntegrityValidator()
        self.writer = writer or RawDatasetWriter(bronze_root=bronze_root)

    def run(self, request: BronzeIngestionRequest, *, run_id: str | None = None) -> IngestionReport:
        run_id = run_id or str(uuid4())
        started = utc_now_iso()
        staging_dir = self.run_root / _slug(request.dataset_id) / run_id / "downloads"
        report_path = self.run_root / _slug(request.dataset_id) / run_id / "report.json"
        try:
            logger.info(
                "bronze_ingestion_started",
                extra={"dataset_id": request.dataset_id, "source": request.source, "run_id": run_id},
            )
            asset = self.download_client.download(request, staging_dir)
            integrity = self.validator.validate(asset.path, expected_sha256=request.expected_sha256)
            if not integrity.ok:
                raise ValueError("; ".join(integrity.errors))
            manifest, manifest_path = self.writer.write(
                request=request,
                asset_path=asset.path,
                integrity=integrity,
                run_id=run_id,
                attempt_count=asset.attempt_count,
            )
            report = IngestionReport(
                run_id=run_id,
                dataset_id=request.dataset_id,
                source=request.source,
                status=manifest.status,
                started_at_utc=started,
                finished_at_utc=utc_now_iso(),
                output_path=manifest.output_path,
                manifest_path=str(manifest_path),
                sha256=manifest.sha256,
                size_bytes=manifest.size_bytes,
                attempt_count=manifest.attempt_count,
            )
            report.write(report_path)
            logger.info(
                "bronze_ingestion_finished",
                extra={"dataset_id": request.dataset_id, "run_id": run_id, "status": report.status},
            )
            return report
        except Exception as exc:
            failure_manifest = self.writer.write_failure(request=request, run_id=run_id, error=exc)
            report = IngestionReport(
                run_id=run_id,
                dataset_id=request.dataset_id,
                source=request.source,
                status="failed",
                started_at_utc=started,
                finished_at_utc=utc_now_iso(),
                manifest_path=str(failure_manifest),
                error_report_path=str(failure_manifest),
                errors=[str(exc)],
            )
            report.write(report_path)
            logger.exception(
                "bronze_ingestion_failed",
                extra={"dataset_id": request.dataset_id, "run_id": run_id, "failure_manifest": str(failure_manifest)},
            )
            return report
