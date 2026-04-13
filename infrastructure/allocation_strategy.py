from __future__ import annotations

from copy import deepcopy
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator

from config.settings import AppPaths


class DigitalBaseWeights(BaseModel):
    meta_fb_ig: float = Field(ge=0)
    youtube: float = Field(ge=0)
    tiktok: float = Field(ge=0)
    whatsapp: float = Field(ge=0)
    google_ads: float = Field(ge=0)


class DigitalSensitivityWeights(BaseModel):
    meta_fb_ig_low_pd_bonus: float = Field(ge=0)
    tiktok_high_pd_bonus: float = Field(ge=0)
    whatsapp_low_pd_bonus: float = Field(ge=0)
    small_city_bias: float = Field(ge=0)
    meta_min: float = Field(ge=0)


class OfflineChannelMix(BaseModel):
    evento_presencial: float = Field(ge=0)
    radio_local: float = Field(ge=0)
    impresso: float = Field(ge=0)

    @model_validator(mode="after")
    def validate_positive_total(self):
        if self.evento_presencial + self.radio_local + self.impresso <= 0:
            raise ValueError("offline channel mix precisa ter soma positiva")
        return self


class OfflineByPdWeights(BaseModel):
    high_pd_threshold: float = Field(ge=0, le=100)
    mid_pd_threshold: float = Field(ge=0, le=100)
    high_pd: OfflineChannelMix
    mid_pd: OfflineChannelMix
    low_pd: OfflineChannelMix

    @model_validator(mode="after")
    def validate_threshold_order(self):
        if self.high_pd_threshold < self.mid_pd_threshold:
            raise ValueError("high_pd_threshold deve ser maior ou igual a mid_pd_threshold")
        return self


class ChannelWeights(BaseModel):
    digital_base: DigitalBaseWeights
    digital_sensitivity: DigitalSensitivityWeights
    offline_by_pd: OfflineByPdWeights


class ScoreModularWeights(BaseModel):
    potencial_eleitoral: float = Field(ge=0)
    oportunidade: float = Field(ge=0)
    eficiencia_midia: float = Field(ge=0)
    custo: float = Field(ge=0)
    risco_invertido: float = Field(ge=0)

    def normalized(self) -> dict[str, float]:
        raw = self.model_dump()
        total = sum(float(v) for v in raw.values())
        if total <= 0:
            raise ValueError("score_modular_weights precisa ter soma positiva")
        return {k: float(v) / total for k, v in raw.items()}


class RiskWeights(BaseModel):
    volatilidade_historica: float = Field(ge=0)
    qualidade_dados: float = Field(ge=0)

    def normalized(self) -> dict[str, float]:
        raw = self.model_dump()
        total = sum(float(v) for v in raw.values())
        if total <= 0:
            raise ValueError("risk_weights precisa ter soma positiva")
        return {k: float(v) / total for k, v in raw.items()}


class AllocationStrategy(BaseModel):
    version: str = "local"
    default_budget: float = Field(default=50_000.0, ge=0)
    cluster_weights: dict[str, float]
    office_caps: dict[str, float]
    statewide_offices: set[str]
    channel_weights: ChannelWeights
    score_modular_weights: ScoreModularWeights
    risk_weights: RiskWeights
    source_path: Path | None = None
    tenant_override_path: Path | None = None

    @field_validator("cluster_weights", "office_caps")
    @classmethod
    def validate_numeric_mapping(cls, value: dict[str, float]) -> dict[str, float]:
        if not value:
            raise ValueError("configuracao nao pode ser vazia")
        return {str(k): float(v) for k, v in value.items()}


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _read_yaml(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    return payload or {}


def _default_strategy_path() -> Path:
    return Path(__file__).resolve().parents[1] / "config" / "allocation_weights.yaml"


def resolve_strategy_paths(paths: AppPaths, explicit_path: Path | None = None) -> tuple[Path, Path | None]:
    base_path = explicit_path or _default_strategy_path()
    tenant_root = paths.tenant_root or paths.data_root
    tenant_path = tenant_root / "config" / "allocation_weights.yaml"
    return base_path, tenant_path if tenant_path.exists() else None


def _mtime(path: Path | None) -> float:
    return path.stat().st_mtime if path is not None and path.exists() else 0.0


@lru_cache(maxsize=128)
def _load_allocation_strategy_cached(
    base_path_raw: str,
    tenant_override_path_raw: str | None,
    base_mtime: float,
    tenant_mtime: float,
) -> AllocationStrategy:
    del base_mtime, tenant_mtime
    base_path = Path(base_path_raw)
    tenant_override_path = Path(tenant_override_path_raw) if tenant_override_path_raw else None
    data = _read_yaml(base_path)
    if tenant_override_path is not None:
        data = _deep_merge(data, _read_yaml(tenant_override_path))
    strategy = AllocationStrategy.model_validate(data)
    return strategy.model_copy(update={"source_path": base_path, "tenant_override_path": tenant_override_path})


def load_allocation_strategy(paths: AppPaths, explicit_path: Path | None = None) -> AllocationStrategy:
    base_path, tenant_override_path = resolve_strategy_paths(paths, explicit_path)
    return _load_allocation_strategy_cached(
        str(base_path.resolve()),
        str(tenant_override_path.resolve()) if tenant_override_path else None,
        _mtime(base_path),
        _mtime(tenant_override_path),
    )
