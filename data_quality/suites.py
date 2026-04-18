from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

import pandas as pd

from data_quality.checks import (
    completeness_check,
    distribution_check,
    drift_check,
    freshness_check,
    joinability_check,
    key_validity_check,
    referential_integrity_check,
    temporal_coverage_check,
    territorial_coverage_check,
    uniqueness_check,
)
from data_quality.models import DatasetQualityReport, LakeHealthReport, QualityCheckResult


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


@dataclass(frozen=True)
class DatasetQualitySuite:
    dataset_id: str
    required_columns: list[str]
    primary_key: list[str]
    key_columns: list[str] = field(default_factory=list)
    numeric_columns: list[str] = field(default_factory=list)
    reference_dataset_id: str | None = None
    reference_join_keys: list[str] = field(default_factory=list)
    max_age_days: int = 30
    previous_path: Path | None = None


DEFAULT_SUITES: dict[str, DatasetQualitySuite] = {
    "gold_territorial_electoral_master_index": DatasetQualitySuite(
        dataset_id="gold_territorial_electoral_master_index",
        required_columns=[
            "master_record_id",
            "ano_eleicao",
            "uf",
            "cod_municipio_tse",
            "zona",
            "secao",
            "join_confidence",
            "source_coverage_score",
        ],
        primary_key=["master_record_id"],
        key_columns=["ano_eleicao", "uf", "cod_municipio_tse", "zona", "secao", "join_confidence"],
        numeric_columns=["join_confidence", "source_coverage_score"],
    ),
    "gold_priority_score": DatasetQualitySuite(
        dataset_id="gold_priority_score",
        required_columns=["candidate_id", "territorio_id", "score_prioridade_final"],
        primary_key=["candidate_id", "territorio_id"],
        key_columns=["candidate_id", "territorio_id", "score_prioridade_final"],
        numeric_columns=["score_prioridade_final", "base_strength_score", "competition_score"],
        reference_dataset_id="gold_territory_profile",
        reference_join_keys=["territorio_id"],
    ),
    "gold_allocation_recommendations": DatasetQualitySuite(
        dataset_id="gold_allocation_recommendations",
        required_columns=["scenario_id", "candidate_id", "territorio_id", "recurso_sugerido"],
        primary_key=["scenario_id", "candidate_id", "territorio_id"],
        key_columns=["candidate_id", "territorio_id"],
        numeric_columns=["recurso_sugerido", "percentual_orcamento_sugerido", "score_prioridade_final"],
        reference_dataset_id="gold_priority_score",
        reference_join_keys=["candidate_id", "territorio_id"],
    ),
}


class DataQualityRunner:
    def run_dataset(
        self,
        df: pd.DataFrame,
        *,
        suite: DatasetQualitySuite,
        metadata: dict[str, Any] | None = None,
        reference_df: pd.DataFrame | None = None,
    ) -> DatasetQualityReport:
        metadata = metadata or {}
        checks: list[QualityCheckResult] = [
            completeness_check(df, required_columns=suite.required_columns),
            uniqueness_check(df, primary_key=suite.primary_key),
            key_validity_check(df, key_columns=suite.key_columns or suite.primary_key),
            freshness_check(metadata, max_age_days=suite.max_age_days),
            territorial_coverage_check(df),
            temporal_coverage_check(df),
            joinability_check(df),
            distribution_check(df, numeric_columns=suite.numeric_columns),
            drift_check(df, suite.previous_path, numeric_columns=suite.numeric_columns),
        ]
        if suite.reference_dataset_id and suite.reference_join_keys:
            checks.append(referential_integrity_check(df, reference_df, join_keys=suite.reference_join_keys))
        score = self._score(checks)
        readiness = self._readiness(score, checks)
        return DatasetQualityReport(
            dataset_id=suite.dataset_id,
            row_count=int(len(df)),
            quality_score=score,
            production_readiness=readiness,
            checks=checks,
            limitations=self._limitations(checks),
            reliable_joins=self._reliable_joins(checks, suite),
            generated_at_utc=utc_now_iso(),
        )

    def run_lake(
        self,
        datasets: dict[str, pd.DataFrame],
        *,
        suites: dict[str, DatasetQualitySuite] | None = None,
        metadata: dict[str, dict[str, Any]] | None = None,
    ) -> LakeHealthReport:
        suites = suites or DEFAULT_SUITES
        metadata = metadata or {}
        reports: list[DatasetQualityReport] = []
        for dataset_id, df in datasets.items():
            suite = suites.get(dataset_id) or self._infer_suite(dataset_id, df)
            reference_df = datasets.get(suite.reference_dataset_id or "")
            reports.append(
                self.run_dataset(
                    df,
                    suite=suite,
                    metadata=metadata.get(dataset_id, {}),
                    reference_df=reference_df,
                )
            )
        aggregate = round(sum(report.quality_score for report in reports) / max(1, len(reports)), 6)
        return LakeHealthReport(
            datasets=reports,
            aggregate_quality_score=aggregate,
            production_ready_datasets=[r.dataset_id for r in reports if r.production_readiness == "production_ready"],
            limited_datasets=[r.dataset_id for r in reports if r.production_readiness == "limited_use"],
            not_ready_datasets=[r.dataset_id for r in reports if r.production_readiness == "not_ready"],
            trusted_joins=sorted({join for report in reports for join in report.reliable_joins}),
            generated_at_utc=utc_now_iso(),
        )

    def _infer_suite(self, dataset_id: str, df: pd.DataFrame) -> DatasetQualitySuite:
        primary_key = [
            column for column in ["master_record_id", "candidate_id", "territorio_id"] if column in df.columns
        ]
        if not primary_key:
            primary_key = [str(df.columns[0])] if len(df.columns) else ["row_id"]
        required = list(
            dict.fromkeys(primary_key + [column for column in ["ano_eleicao", "uf"] if column in df.columns])
        )
        numeric_cols = [column for column in df.columns if pd.api.types.is_numeric_dtype(df[column])]
        return DatasetQualitySuite(
            dataset_id=dataset_id,
            required_columns=required,
            primary_key=primary_key,
            key_columns=required,
            numeric_columns=numeric_cols,
        )

    def _score(self, checks: list[QualityCheckResult]) -> float:
        weights = {
            "completeness": 0.16,
            "uniqueness": 0.14,
            "validity": 0.12,
            "freshness": 0.10,
            "referential_integrity": 0.12,
            "territorial_coverage": 0.10,
            "temporal_coverage": 0.08,
            "joinability": 0.12,
            "distribution": 0.04,
            "drift": 0.02,
        }
        total_weight = sum(weights.get(check.dimension, 0.05) for check in checks)
        if total_weight <= 0:
            return 0.0
        return round(
            sum(check.score * weights.get(check.dimension, 0.05) for check in checks) / total_weight,
            6,
        )

    def _readiness(self, score: float, checks: list[QualityCheckResult]) -> Literal["production_ready", "limited_use", "not_ready"]:
        if any(
            check.status == "fail" and check.dimension in {"completeness", "uniqueness", "validity"} for check in checks
        ):
            return "not_ready"
        if score >= 0.9 and all(check.status != "fail" for check in checks):
            return "production_ready"
        if score >= 0.7:
            return "limited_use"
        return "not_ready"

    def _limitations(self, checks: list[QualityCheckResult]) -> list[str]:
        return [check.message for check in checks if check.status in {"warn", "fail"}]

    def _reliable_joins(self, checks: list[QualityCheckResult], suite: DatasetQualitySuite) -> list[str]:
        joinability = next((check for check in checks if check.dimension == "joinability"), None)
        referential = next((check for check in checks if check.dimension == "referential_integrity"), None)
        reliable: list[str] = []
        if joinability is not None and joinability.score >= 0.85:
            reliable.append(f"{suite.dataset_id}:joinability")
        if referential is not None and referential.score >= 0.9:
            reliable.append(f"{suite.dataset_id}->{suite.reference_dataset_id}:{','.join(suite.reference_join_keys)}")
        return reliable
