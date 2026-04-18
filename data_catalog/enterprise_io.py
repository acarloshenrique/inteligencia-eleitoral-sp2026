from __future__ import annotations

import json
from pathlib import Path

from data_catalog.enterprise_registry import build_enterprise_catalog, prioritization_table
from data_catalog.models import EnterpriseDataCatalog, PrioritizationRow


def write_enterprise_catalog(path: Path, catalog: EnterpriseDataCatalog | None = None) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = (catalog or build_enterprise_catalog()).model_dump(mode="json")
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def read_enterprise_catalog(path: Path) -> EnterpriseDataCatalog:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return EnterpriseDataCatalog.model_validate(payload)


def write_prioritization_table(path: Path, catalog: EnterpriseDataCatalog | None = None) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = [row.model_dump(mode="json") for row in prioritization_table(catalog)]
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def read_prioritization_table(path: Path) -> list[PrioritizationRow]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return [PrioritizationRow.model_validate(row) for row in payload]
