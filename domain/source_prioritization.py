from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

SourcePriority = Literal["A", "B"]


@dataclass(frozen=True)
class SourceSpec:
    key: str
    nome: str
    prioridade: SourcePriority
    area: str
    cobertura_municipal: float
    atualizacao_dias: int
    licenca_aberta: bool
    schema_quality: float
    endpoint: str
    notes: str = ""


@dataclass(frozen=True)
class SourceEvaluation:
    source: SourceSpec
    accepted: bool
    score: float
    reasons: tuple[str, ...]
