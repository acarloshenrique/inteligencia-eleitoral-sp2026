from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from data_quality import DataQualityReportWriter, DataQualityRunner, DatasetQualitySuite
from data_quality.checks import drift_check


def _master() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "master_record_id": "m1",
                "ano_eleicao": 2024,
                "uf": "SP",
                "cod_municipio_tse": "71072",
                "zona": "0001",
                "secao": "0025",
                "join_confidence": 0.95,
                "source_coverage_score": 0.8,
            },
            {
                "master_record_id": "m2",
                "ano_eleicao": 2024,
                "uf": "SP",
                "cod_municipio_tse": "71072",
                "zona": "0001",
                "secao": "0026",
                "join_confidence": 0.9,
                "source_coverage_score": 0.7,
            },
        ]
    )


def _priority() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "candidate_id": "123",
                "territorio_id": "2024:SP:71072:Z0001",
                "score_prioridade_final": 0.8,
                "base_strength_score": 0.7,
                "competition_score": 0.6,
            }
        ]
    )


def _territory_profile() -> pd.DataFrame:
    return pd.DataFrame([{"territorio_id": "2024:SP:71072:Z0001", "data_quality_score": 0.9}])


def test_data_quality_runner_scores_dataset_and_readiness() -> None:
    runner = DataQualityRunner()

    report = runner.run_dataset(
        _master(),
        suite=DatasetQualitySuite(
            dataset_id="gold_territorial_electoral_master_index",
            required_columns=["master_record_id", "ano_eleicao", "uf", "cod_municipio_tse", "join_confidence"],
            primary_key=["master_record_id"],
            key_columns=["ano_eleicao", "uf", "cod_municipio_tse", "join_confidence"],
            numeric_columns=["join_confidence", "source_coverage_score"],
        ),
        metadata={"generated_at_utc": datetime.now(UTC).isoformat()},
    )

    assert report.quality_score >= 0.8
    assert report.production_readiness in {"production_ready", "limited_use"}
    assert any(check.dimension == "completeness" for check in report.checks)
    assert report.reliable_joins


def test_lake_health_report_identifies_ready_limited_and_trusted_joins() -> None:
    report = DataQualityRunner().run_lake(
        {
            "gold_territorial_electoral_master_index": _master(),
            "gold_priority_score": _priority(),
            "gold_territory_profile": _territory_profile(),
        },
        metadata={
            "gold_territorial_electoral_master_index": {"generated_at_utc": datetime.now(UTC).isoformat()},
            "gold_priority_score": {"generated_at_utc": datetime.now(UTC).isoformat()},
            "gold_territory_profile": {"generated_at_utc": datetime.now(UTC).isoformat()},
        },
    )

    assert report.aggregate_quality_score > 0
    assert "gold_territorial_electoral_master_index" in (
        report.production_ready_datasets + report.limited_datasets + report.not_ready_datasets
    )
    assert report.trusted_joins


def test_data_quality_reports_write_json_and_markdown(tmp_path: Path) -> None:
    report = DataQualityRunner().run_lake({"gold_territorial_electoral_master_index": _master()})
    writer = DataQualityReportWriter()

    json_path = writer.write_json(report, tmp_path / "lake_health_report.json")
    md_path = writer.write_markdown(report, tmp_path / "lake_health_report.md")

    assert json_path.exists()
    assert md_path.exists()
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    markdown = md_path.read_text(encoding="utf-8")
    assert payload["report_version"] == "lake_health_report_v1"
    assert "Dataset Status" in markdown
    assert "Trusted Joins" in markdown


def test_drift_detection_warns_on_distribution_shift(tmp_path: Path) -> None:
    previous = pd.DataFrame({"score_prioridade_final": [0.1, 0.2, 0.3]})
    current = pd.DataFrame({"score_prioridade_final": [0.9, 0.95, 1.0]})
    previous_path = tmp_path / "previous.parquet"
    previous.to_parquet(previous_path, index=False)

    result = drift_check(current, previous_path, numeric_columns=["score_prioridade_final"])

    assert result.dimension == "drift"
    assert result.status in {"warn", "fail"}
    assert float(result.observed_value) > 0.2


def test_quality_runner_marks_bad_dataset_not_ready() -> None:
    bad = pd.DataFrame([{"candidate_id": "", "territorio_id": "", "score_prioridade_final": None}])

    report = DataQualityRunner().run_dataset(
        bad,
        suite=DatasetQualitySuite(
            dataset_id="bad_priority",
            required_columns=["candidate_id", "territorio_id", "score_prioridade_final"],
            primary_key=["candidate_id", "territorio_id"],
            key_columns=["candidate_id", "territorio_id", "score_prioridade_final"],
            numeric_columns=["score_prioridade_final"],
        ),
    )

    assert report.production_readiness == "not_ready"
    assert report.limitations
