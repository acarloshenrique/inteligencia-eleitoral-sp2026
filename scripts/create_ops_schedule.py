from __future__ import annotations

import argparse
import json

from config.settings import get_settings
from infrastructure.operation_scheduler import build_default_schedule, write_schedule_manifest


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Cria manifesto operacional multi-tenant para ingestao, atualizacao e alertas."
    )
    parser.add_argument("--daily-hour", type=int, default=None)
    parser.add_argument("--weekly-day", default=None)
    parser.add_argument("--weekly-hour", type=int, default=None)
    args = parser.parse_args()

    settings = get_settings()
    paths = settings.build_paths()
    schedules = build_default_schedule(
        tenant_id=paths.tenant_id,
        daily_hour=args.daily_hour if args.daily_hour is not None else settings.ops_daily_ingestion_hour,
        weekly_day=args.weekly_day if args.weekly_day is not None else settings.ops_weekly_update_day,
        weekly_hour=args.weekly_hour if args.weekly_hour is not None else settings.ops_weekly_update_hour,
    )
    manifest = write_schedule_manifest(paths, schedules)
    print(json.dumps({"tenant_id": paths.tenant_id, "manifest_path": str(manifest)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
