from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import shutil

from config.settings import get_settings


def main() -> int:
    settings = get_settings()
    paths = settings.build_paths()
    src = paths.metadata_db_path
    if not src.exists():
        raise FileNotFoundError(f"metadata db nao encontrado: {src}")
    backup_dir = paths.data_root / "backups" / "metadata"
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    dst = backup_dir / f"jobs_{stamp}.sqlite3"
    shutil.copy2(src, dst)
    print({"backup_path": str(dst)})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
