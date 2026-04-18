from __future__ import annotations

import pandas as pd

from allocation.scenario_builder import Scenario, normalize_action_type


def allocate_budget(scored: pd.DataFrame, budget_total: float, *, top_n: int) -> pd.DataFrame:
    from allocation.scenario_builder import ScenarioBuilder

    scenario = ScenarioBuilder().build(
        budget_total=budget_total,
        scenario="hibrido",
        top_n=top_n,
        capacidade_operacional=0.7,
        janela_temporal_dias=45,
    )
    return BudgetAllocator().allocate(scored, scenario=scenario)


class BudgetAllocator:
    def allocate(self, scored: pd.DataFrame, *, scenario: Scenario) -> pd.DataFrame:
        if scored.empty:
            out = scored.copy()
            out["percentual_orcamento_sugerido"] = []
            out["recurso_sugerido"] = []
            return out
        selected = scored.sort_values("score_prioridade_final", ascending=False).head(scenario.top_n).copy()
        if "tipo_recomendacao" not in selected.columns:
            selected["tipo_recomendacao"] = "presenca_fisica"
        selected["allocation_weight"] = selected.apply(lambda row: self._allocation_weight(row, scenario), axis=1)
        total_weight = float(selected["allocation_weight"].sum())
        if total_weight <= 0:
            selected["percentual_orcamento_sugerido"] = 1.0 / max(len(selected), 1)
        else:
            selected["percentual_orcamento_sugerido"] = selected["allocation_weight"] / total_weight
        selected["recurso_sugerido"] = selected["percentual_orcamento_sugerido"] * float(scenario.budget_total)
        selected["cenario"] = scenario.name
        selected["janela_temporal_dias"] = scenario.janela_temporal_dias
        selected["capacidade_operacional_informada"] = scenario.capacidade_operacional
        selected["cluster_territorial"] = selected.apply(cluster_territory, axis=1)
        return selected

    def _allocation_weight(self, row: pd.Series, scenario: Scenario) -> float:
        action = normalize_action_type(str(row.get("tipo_recomendacao", "presenca_fisica")))
        action_bias = scenario.action_budget_split.get(action, 0.05)
        priority = float(row.get("score_prioridade_final", 0.0) or 0.0)
        efficiency = float(row.get("custo_eficiencia_score", 0.5) or 0.5)
        operational = scenario.capacidade_operacional
        time_pressure = min(1.0, max(0.25, 45.0 / max(float(scenario.janela_temporal_dias), 1.0)))
        return max(
            0.0,
            priority * (0.55 + 0.25 * efficiency + 0.20 * operational) * (0.75 + 0.25 * time_pressure) * action_bias,
        )


def cluster_territory(row: pd.Series) -> str:
    base = float(row.get("base_eleitoral_score", 0.0) or 0.0)
    expansion = float(row.get("potencial_expansao_score", 0.0) or 0.0)
    competition = float(row.get("concorrencia_score", 0.0) or 0.0)
    priority = float(row.get("score_prioridade_final", 0.0) or 0.0)
    if base >= 0.70:
        return "base_consolidada"
    if expansion >= 0.70:
        return "expansao_prioritaria"
    if competition >= 0.70:
        return "competitivo"
    if priority >= 0.60:
        return "oportunidade_eficiente"
    return "monitoramento"
