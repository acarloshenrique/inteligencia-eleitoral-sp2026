from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, model_validator

SCORE_COLUMNS = [
    "base_eleitoral_score",
    "afinidade_tematica_score",
    "potencial_expansao_score",
    "custo_eficiencia_score",
    "concorrencia_score",
    "capacidade_operacional_score",
]


class ScoreWeights(BaseModel):
    model_config = ConfigDict(extra="forbid")

    base_eleitoral_score: float = 0.30
    afinidade_tematica_score: float = 0.20
    potencial_expansao_score: float = 0.15
    custo_eficiencia_score: float = 0.15
    concorrencia_score: float = -0.10
    capacidade_operacional_score: float = 0.10

    @model_validator(mode="after")
    def validate_non_zero_model(self):
        total_abs = sum(abs(value) for value in self.model_dump().values())
        if total_abs <= 0:
            raise ValueError("score weights cannot all be zero")
        return self

    def as_dict(self) -> dict[str, float]:
        return {key: float(value) for key, value in self.model_dump().items()}


DEFAULT_SCORE_WEIGHTS = ScoreWeights()


def load_score_weights(path: Path | None = None) -> ScoreWeights:
    if path is None or not path.exists():
        return DEFAULT_SCORE_WEIGHTS
    suffix = path.suffix.lower()
    if suffix == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
    else:
        payload = _read_simple_yaml(path)
    return ScoreWeights.model_validate(payload)


def _read_simple_yaml(path: Path) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if not line or ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.strip()
        value = value.strip().strip("\"'")
        try:
            payload[key] = float(value)
        except ValueError:
            payload[key] = value
    return payload


@dataclass(frozen=True)
class ComponentExplanation:
    component: str
    value: float
    weight: float
    contribution: float
    rationale: str


@dataclass(frozen=True)
class ScoringPersistenceResult:
    parquet_path: Path
    duckdb_path: Path | None
    manifest_path: Path
    rows: int
    dataset_version: str
    quality: dict[str, Any] = field(default_factory=dict)
