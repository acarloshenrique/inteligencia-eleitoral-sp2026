from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from application.commercial_precision_service import CommercialPrecisionService
from config.settings import get_settings


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Sprint 4 commercial precision artifacts.")
    parser.add_argument("--dataset-version", default="sprint4_commercial_precision")
    parser.add_argument("--tenant-id", default="default")
    parser.add_argument("--campaign-id", default="campanha_sp_2026")
    parser.add_argument("--snapshot-id", default=None)
    parser.add_argument("--operational-path", type=Path, default=None)
    parser.add_argument("--scores-path", type=Path, default=None)
    args = parser.parse_args()

    result = CommercialPrecisionService(get_settings().build_paths()).run(
        dataset_version=args.dataset_version,
        tenant_id=args.tenant_id,
        campaign_id=args.campaign_id,
        snapshot_id=args.snapshot_id,
        operational_path=args.operational_path,
        scores_path=args.scores_path,
    )
    print(
        json.dumps(
            {
                "root": str(result.root),
                "multi_candidate_summary_path": str(result.multi_candidate_summary_path),
                "campaign_snapshots_path": str(result.campaign_snapshots_path),
                "scenario_comparison_path": str(result.scenario_comparison_path),
                "readiness_json_path": str(result.readiness_json_path),
                "readiness_markdown_path": str(result.readiness_markdown_path),
                "demo_workbook_path": str(result.demo_workbook_path),
                "demo_markdown_path": str(result.demo_markdown_path),
                "rows": result.rows,
                "candidate_count": result.candidate_count,
                "readiness_score": result.readiness_score,
                "generated_at_utc": result.generated_at_utc,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
