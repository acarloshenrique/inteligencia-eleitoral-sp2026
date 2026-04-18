from __future__ import annotations

import json
from pathlib import Path

from data_quality.models import DatasetQualityReport, LakeHealthReport


class DataQualityReportWriter:
    def write_json(self, report: LakeHealthReport, path: Path) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(report.model_dump(mode="json"), ensure_ascii=False, indent=2), encoding="utf-8")
        return path

    def write_markdown(self, report: LakeHealthReport, path: Path) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        lines = [
            "# Lake Health Report",
            "",
            f"- Generated at: `{report.generated_at_utc}`",
            f"- Aggregate quality score: `{report.aggregate_quality_score:.3f}`",
            f"- Production ready: `{len(report.production_ready_datasets)}`",
            f"- Limited use: `{len(report.limited_datasets)}`",
            f"- Not ready: `{len(report.not_ready_datasets)}`",
            "",
            "## Dataset Status",
            "",
            "| Dataset | Score | Readiness | Rows | Limitations |",
            "| --- | ---: | --- | ---: | --- |",
        ]
        for dataset in report.datasets:
            limitations = "; ".join(dataset.limitations) if dataset.limitations else "none"
            lines.append(
                f"| `{dataset.dataset_id}` | {dataset.quality_score:.3f} | {dataset.production_readiness} | {dataset.row_count} | {limitations} |"
            )
        lines.extend(["", "## Trusted Joins", ""])
        if report.trusted_joins:
            lines.extend(f"- `{join}`" for join in report.trusted_joins)
        else:
            lines.append("- None")
        lines.extend(["", "## Check Details", ""])
        for dataset in report.datasets:
            lines.extend([f"### {dataset.dataset_id}", ""])
            for check in dataset.checks:
                lines.append(
                    f"- `{check.dimension}` / `{check.check_id}`: **{check.status}** "
                    f"score={check.score:.3f}; observed={check.observed_value}; threshold={check.threshold}; {check.message}"
                )
            lines.append("")
        path.write_text("\n".join(lines), encoding="utf-8")
        return path


def dataset_summary(report: DatasetQualityReport) -> dict[str, object]:
    return {
        "dataset_id": report.dataset_id,
        "quality_score": report.quality_score,
        "production_readiness": report.production_readiness,
        "row_count": report.row_count,
        "limitations": report.limitations,
        "reliable_joins": report.reliable_joins,
    }
