from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, cast

ScenarioName = Literal["conservador", "hibrido", "agressivo"]
ActionType = Literal[
    "retencao_base",
    "expansao_territorial",
    "presenca_fisica",
    "ativacao_lideranca_local",
    "comunicacao_programatica",
    "reforco_area_competitiva",
]

SCENARIO_PROFILES: dict[ScenarioName, dict[ActionType, float]] = {
    "conservador": {
        "retencao_base": 0.38,
        "expansao_territorial": 0.16,
        "presenca_fisica": 0.16,
        "ativacao_lideranca_local": 0.14,
        "comunicacao_programatica": 0.08,
        "reforco_area_competitiva": 0.08,
    },
    "hibrido": {
        "retencao_base": 0.24,
        "expansao_territorial": 0.24,
        "presenca_fisica": 0.14,
        "ativacao_lideranca_local": 0.12,
        "comunicacao_programatica": 0.13,
        "reforco_area_competitiva": 0.13,
    },
    "agressivo": {
        "retencao_base": 0.14,
        "expansao_territorial": 0.34,
        "presenca_fisica": 0.10,
        "ativacao_lideranca_local": 0.10,
        "comunicacao_programatica": 0.16,
        "reforco_area_competitiva": 0.16,
    },
}


def normalize_action_type(action: str) -> ActionType:
    if action in {
        "retencao_base",
        "expansao_territorial",
        "presenca_fisica",
        "ativacao_lideranca_local",
        "comunicacao_programatica",
        "reforco_area_competitiva",
    }:
        return cast(ActionType, action)
    return "presenca_fisica"


@dataclass(frozen=True)
class Scenario:
    name: ScenarioName
    budget_total: float
    top_n: int
    capacidade_operacional: float
    janela_temporal_dias: int
    action_budget_split: dict[ActionType, float] = field(default_factory=dict)

    def action_budget(self, action: str) -> float:
        action_key = normalize_action_type(action)
        return float(self.budget_total) * float(self.action_budget_split.get(action_key, 0.0))


class ScenarioBuilder:
    def build(
        self,
        *,
        budget_total: float,
        scenario: str,
        top_n: int,
        capacidade_operacional: float,
        janela_temporal_dias: int,
    ) -> Scenario:
        scenario_name: ScenarioName = scenario if scenario in SCENARIO_PROFILES else "hibrido"  # type: ignore[assignment]
        split = SCENARIO_PROFILES[scenario_name].copy()
        total = sum(split.values()) or 1.0
        normalized = {action: value / total for action, value in split.items()}
        return Scenario(
            name=scenario_name,
            budget_total=max(0.0, float(budget_total)),
            top_n=max(1, int(top_n)),
            capacidade_operacional=max(0.0, min(1.0, float(capacidade_operacional))),
            janela_temporal_dias=max(1, int(janela_temporal_dias)),
            action_budget_split=normalized,
        )
