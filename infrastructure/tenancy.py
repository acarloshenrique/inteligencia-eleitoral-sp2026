from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

_TENANT_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{1,62}$")
DEFAULT_TENANT_ID = "default"


@dataclass(frozen=True)
class TenantContext:
    tenant_id: str
    tenant_root: Path


def normalize_tenant_id(raw: str | None) -> str:
    value = (raw or DEFAULT_TENANT_ID).strip().lower().replace(" ", "-")
    if value == DEFAULT_TENANT_ID:
        return DEFAULT_TENANT_ID
    if not _TENANT_RE.fullmatch(value):
        raise ValueError("TENANT_ID invalido. Use letras minusculas, numeros, hifen ou underscore; 2 a 63 caracteres.")
    return value


def tenant_root_for(data_root: Path, tenant_id: str) -> Path:
    tenant_id = normalize_tenant_id(tenant_id)
    if tenant_id == DEFAULT_TENANT_ID:
        return data_root
    return data_root / "tenants" / tenant_id


def build_tenant_context(data_root: Path, tenant_id: str | None) -> TenantContext:
    normalized = normalize_tenant_id(tenant_id)
    root = tenant_root_for(data_root, normalized).resolve()
    root.mkdir(parents=True, exist_ok=True)
    return TenantContext(tenant_id=normalized, tenant_root=root)


def ensure_tenant_path(tenant_root: Path, candidate: Path) -> Path:
    resolved = candidate.resolve()
    root = tenant_root.resolve()
    if resolved != root and root not in resolved.parents:
        raise ValueError("path fora do tenant ativo")
    return resolved
