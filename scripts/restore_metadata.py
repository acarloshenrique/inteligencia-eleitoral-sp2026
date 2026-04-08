from __future__ import annotations

import argparse
from pathlib import Path
import shutil

from config.settings import get_settings


def main() -> int:
    parser = argparse.ArgumentParser(description="Restaura banco de metadados a partir de backup.")
    parser.add_argument("--from-backup", required=True, help="Arquivo sqlite de backup.")
    args = parser.parse_args()

    settings = get_settings()
    paths = settings.build_paths()
    src = Path(args.from_backup)
    if not src.exists():
        raise FileNotFoundError(f"backup nao encontrado: {src}")
    paths.metadata_db_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, paths.metadata_db_path)
    print({"restored_to": str(paths.metadata_db_path), "from_backup": str(src)})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
