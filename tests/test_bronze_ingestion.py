from __future__ import annotations

import json
from pathlib import Path

from ingestion.bronze import BaseIngestionJob, BronzeIngestionRequest, FileIntegrityValidator
from ingestion.bronze_sources import ALL_BRONZE_DATASETS, IBGEBronzeIngestionJob, TSEBronzeIngestionJob
from lakehouse.manifest import sha256_file


def _sample_file(tmp_path: Path, name: str = "sample.zip", content: bytes = b"bronze-data") -> Path:
    path = tmp_path / name
    path.write_bytes(content)
    return path


def test_bronze_job_preserves_raw_file_and_manifest(tmp_path: Path) -> None:
    source_file = _sample_file(tmp_path)
    request = BronzeIngestionRequest(
        dataset_id="boletim_urna",
        source="tse",
        source_url="https://example.test/boletim.zip",
        formato="zip",
        reference_period="2024",
        ano=2024,
        uf="SP",
        local_path=source_file,
    )
    job = BaseIngestionJob(bronze_root=tmp_path / "lake" / "bronze")

    report = job.run(request, run_id="run-ok")

    assert report.status == "ok"
    assert Path(report.output_path).exists()
    assert Path(report.manifest_path).exists()
    assert "lake" in report.output_path
    manifest = json.loads(Path(report.manifest_path).read_text(encoding="utf-8"))
    assert manifest["dataset_id"] == "boletim_urna"
    assert manifest["source_url"] == "https://example.test/boletim.zip"
    assert manifest["sha256"] == sha256_file(source_file)
    assert manifest["size_bytes"] == source_file.stat().st_size
    assert manifest["reference_period"] == "2024"
    assert manifest["uf"] == "SP"


def test_bronze_job_deduplicates_by_hash(tmp_path: Path) -> None:
    source_file = _sample_file(tmp_path, content=b"same-content")
    request = BronzeIngestionRequest(
        dataset_id="boletim_urna",
        source="tse",
        source_url="https://example.test/boletim.zip",
        formato="zip",
        reference_period="2024",
        ano=2024,
        uf="SP",
        local_path=source_file,
    )
    job = BaseIngestionJob(bronze_root=tmp_path / "lake" / "bronze")

    first = job.run(request, run_id="run-1")
    second = job.run(request, run_id="run-2")

    assert first.status == "ok"
    assert second.status == "skipped_duplicate"
    assert second.output_path == first.output_path
    manifest = json.loads(Path(second.manifest_path).read_text(encoding="utf-8"))
    assert manifest["duplicate_of"] == first.output_path


def test_bronze_job_reports_integrity_failure(tmp_path: Path) -> None:
    source_file = _sample_file(tmp_path)
    request = BronzeIngestionRequest(
        dataset_id="candidatos",
        source="tse",
        source_url="https://example.test/candidatos.zip",
        formato="zip",
        reference_period="2024",
        ano=2024,
        local_path=source_file,
        expected_sha256="0" * 64,
    )
    job = BaseIngestionJob(bronze_root=tmp_path / "lake" / "bronze")

    report = job.run(request, run_id="bad-hash")

    assert report.status == "failed"
    assert report.errors
    assert Path(report.error_report_path).exists()
    failure = json.loads(Path(report.error_report_path).read_text(encoding="utf-8"))
    assert failure["status"] == "failed"
    assert "sha256" in failure["error"]


def test_file_integrity_validator_accepts_expected_hash(tmp_path: Path) -> None:
    source_file = _sample_file(tmp_path, content=b"hash-me")
    expected = sha256_file(source_file)

    result = FileIntegrityValidator().validate(source_file, expected_sha256=expected)

    assert result.ok
    assert result.sha256 == expected
    assert result.size_bytes == len(b"hash-me")


def test_source_specific_jobs_build_governed_requests(tmp_path: Path) -> None:
    source_file = _sample_file(tmp_path)
    tse_job = TSEBronzeIngestionJob(bronze_root=tmp_path / "lake" / "bronze")
    ibge_job = IBGEBronzeIngestionJob(bronze_root=tmp_path / "lake" / "bronze")

    tse_report = tse_job.run_dataset("boletim_urna", ano=2024, uf="SP", local_path=source_file)
    ibge_report = ibge_job.run_dataset("malha_setores", ano=2022, uf="SP", local_path=source_file)

    assert tse_report.status == "ok"
    assert ibge_report.status == "ok"
    assert ALL_BRONZE_DATASETS["tse.boletim_urna"].source == "tse"
    assert ALL_BRONZE_DATASETS["ibge.malha_setores"].source == "ibge"
