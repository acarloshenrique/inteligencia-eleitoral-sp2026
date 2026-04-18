from __future__ import annotations

import json
from pathlib import Path

from data_catalog.models import DataCatalog
from data_catalog.sources import build_default_catalog


def write_catalog(path: Path, catalog: DataCatalog | None = None) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = (catalog or build_default_catalog()).model_dump(mode="json")
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def read_catalog(path: Path) -> DataCatalog:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return DataCatalog.model_validate(payload)
