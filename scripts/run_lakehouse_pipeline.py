from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from application.lakehouse_orchestrator import LakehouseOrchestrator
from config.settings import get_settings


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the electoral lakehouse pipeline from gold/master to serving.")
    parser.add_argument("--tenant-id", default=None)
    parser.add_argument("--campaign-id", default="campanha_demo")
    parser.add_argument("--snapshot-id", default=None)
    parser.add_argument("--dataset-version", default=None)
    parser.add_argument("--budget-total", type=float, default=200000.0)
    parser.add_argument("--scenario-id", default="hibrido")
    args = parser.parse_args()

    paths = get_settings().build_paths()
    now = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    tenant_id = args.tenant_id or paths.tenant_id
    dataset_version = args.dataset_version or f"run_{now}"
    snapshot_id = args.snapshot_id or dataset_version
    result = LakehouseOrchestrator(paths).run(
        tenant_id=tenant_id,
        campaign_id=args.campaign_id,
        snapshot_id=snapshot_id,
        dataset_version=dataset_version,
        budget_total=args.budget_total,
        scenario_id=args.scenario_id,
    )
    coverage = LakehouseOrchestrator(paths).coverage_by_zone_section()
    print(json.dumps({"pipeline": result, "coverage": coverage}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
