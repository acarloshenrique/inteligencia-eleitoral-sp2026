from __future__ import annotations

from scoring.config import ComponentExplanation

RATIONALES = {
    "base_eleitoral_score": "Forca territorial consolidada a partir de base declarada e/ou desempenho historico agregado.",
    "afinidade_tematica_score": "Aderencia entre temas do candidato e indicadores tematicos agregados do territorio.",
    "potencial_expansao_score": "Potencial de crescimento em territorios com escala, abstencao e base ainda fraca.",
    "custo_eficiencia_score": "Eficiencia relativa considerando custo operacional estimado ou escala eleitoral.",
    "concorrencia_score": "Pressao competitiva local; entra como redutor no score final.",
    "capacidade_operacional_score": "Capacidade informada para executar a campanha no territorio durante a janela definida.",
}


def explain_components(row: dict, weights: dict[str, float]) -> list[ComponentExplanation]:
    explanations: list[ComponentExplanation] = []
    for component, weight in weights.items():
        value = float(row.get(component, 0.0) or 0.0)
        explanations.append(
            ComponentExplanation(
                component=component,
                value=value,
                weight=float(weight),
                contribution=value * float(weight),
                rationale=RATIONALES.get(component, "Componente configurado do score de prioridade."),
            )
        )
    return explanations


def explanation_to_summary(explanations: list[ComponentExplanation], final_score: float) -> str:
    parts = [f"{item.component}={item.value:.2f}*{item.weight:.2f} ({item.contribution:+.3f})" for item in explanations]
    return f"Prioridade={final_score:.3f}; score_prioridade_final={final_score:.3f}; " + "; ".join(parts)


def explanations_to_dict(explanations: list[ComponentExplanation]) -> dict[str, dict[str, float | str]]:
    return {
        item.component: {
            "value": item.value,
            "weight": item.weight,
            "contribution": item.contribution,
            "rationale": item.rationale,
        }
        for item in explanations
    }
