from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import shutil

from config.settings import get_settings
from infrastructure.metadata_db import MetadataDb


def _cleanup_old_runs(root: Path, retention_days: int) -> int:
    if not root.exists():
        return 0
    cutoff_ts = datetime.now(UTC).timestamp() - retention_days * 86400
    removed = 0
    for version_dir in root.glob("*"):
        if not version_dir.is_dir():
            continue
        for run_dir in version_dir.glob("*"):
            try:
                mtime = run_dir.stat().st_mtime
            except FileNotFoundError:
                continue
            if mtime < cutoff_ts:
                shutil.rmtree(run_dir, ignore_errors=True)
                removed += 1
    return removed


def main() -> int:
    settings = get_settings()
    paths = settings.build_paths()
    db = MetadataDb(paths.metadata_db_path)
    db_removed = db.purge_older_than_days(settings.retention_days)
    runs_removed = _cleanup_old_runs(paths.data_root / "outputs" / "pipeline_runs", settings.retention_days)
    print(
        {
            "retention_days": settings.retention_days,
            "db_removed": db_removed,
            "pipeline_runs_removed": runs_removed,
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
