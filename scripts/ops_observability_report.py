from __future__ import annotations

import argparse
import json

from config.settings import get_settings
from infrastructure.metadata_db import MetadataDb
from infrastructure.observability import AlertThresholds, build_observability_snapshot


def main() -> int:
    parser = argparse.ArgumentParser(description="Exibe snapshot de observabilidade operacional por tenant.")
    parser.add_argument("--limit", type=int, default=500)
    args = parser.parse_args()

    settings = get_settings()
    paths = settings.build_paths()
    db = MetadataDb(paths.metadata_db_path)
    snapshot = build_observability_snapshot(
        db,
        tenant_id=paths.tenant_id,
        thresholds=AlertThresholds(
            error_rate=settings.ops_alert_error_rate_threshold,
            latency_p95_ms=settings.ops_alert_latency_p95_ms,
            daily_cost_usd=settings.ops_alert_daily_cost_usd,
        ),
        limit=args.limit,
    )
    print(json.dumps(snapshot, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
