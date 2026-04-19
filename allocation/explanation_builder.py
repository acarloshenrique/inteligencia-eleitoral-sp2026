from __future__ import annotations

import pandas as pd

from allocation.budget_allocator import cluster_territory
from allocation.scenario_builder import ActionType, Scenario


def choose_action(row: pd.Series, *, scenario: Scenario | None = None) -> ActionType:
    base = float(row.get("base_eleitoral_score", 0.0) or 0.0)
    expansion = float(row.get("potencial_expansao_score", 0.0) or 0.0)
    competition = float(row.get("concorrencia_score", 0.0) or 0.0)
    affinity = float(row.get("afinidade_tematica_score", 0.0) or 0.0)
    efficiency = float(row.get("custo_eficiencia_score", 0.0) or 0.0)
    capacidade = (
        scenario.capacidade_operacional
        if scenario is not None
        else float(row.get("capacidade_operacional_score", 0.7) or 0.7)
    )

    if base >= 0.75:
        return "retencao_base"
    if competition >= 0.75:
        return "reforco_area_competitiva"
    if expansion >= 0.70:
        return "expansao_territorial"
    if affinity >= 0.70:
        return "comunicacao_programatica"
    if capacidade >= 0.55 and efficiency >= 0.55:
        return "ativacao_lideranca_local"
    return "presenca_fisica"


def build_justification(row: pd.Series, *, scenario: Scenario | None = None) -> str:
    municipio = str(row.get("municipio") or row.get("municipio_nome") or row.get("MUNICIPIO") or "territorio")
    zona = row.get("zona_eleitoral", row.get("zona", row.get("ZONA", "")))
    action = str(row.get("tipo_recomendacao") or choose_action(row, scenario=scenario))
    cluster = str(row.get("cluster_territorial") or cluster_territory(row))
    scenario_name = scenario.name if scenario is not None else str(row.get("cenario", "hibrido"))
    budget = float(row.get("recurso_sugerido", 0.0) or 0.0)
    pct = float(row.get("percentual_orcamento_sugerido", 0.0) or 0.0)
    return (
        f"{municipio} zona {zona} foi priorizado no cenario {scenario_name} como {cluster}. "
        f"Acao recomendada: {action}. Score final {float(row['score_prioridade_final']):.2f}; "
        f"base {float(row['base_eleitoral_score']):.2f}, expansao {float(row['potencial_expansao_score']):.2f}, "
        f"afinidade tematica {float(row['afinidade_tematica_score']):.2f}, competicao {float(row['concorrencia_score']):.2f} "
        f"e eficiencia {float(row['custo_eficiencia_score']):.2f}. "
        f"Sugestao de verba: R$ {budget:,.0f} ({pct:.1%} do orcamento selecionado)."
    )


def build_scenario_summary(allocated: pd.DataFrame, scenario: Scenario) -> dict[str, object]:
    if allocated.empty:
        return {
            "scenario": scenario.name,
            "budget_total": scenario.budget_total,
            "recommendations": 0,
            "by_action": {},
            "by_cluster": {},
        }
    return {
        "scenario": scenario.name,
        "budget_total": scenario.budget_total,
        "recommendations": int(len(allocated)),
        "by_action": allocated.groupby("tipo_recomendacao")["recurso_sugerido"].sum().round(2).to_dict(),
        "by_cluster": allocated.groupby("cluster_territorial")["recurso_sugerido"].sum().round(2).to_dict(),
    }
