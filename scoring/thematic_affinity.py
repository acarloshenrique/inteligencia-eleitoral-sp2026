from __future__ import annotations

import pandas as pd


def compute_thematic_affinity(df: pd.DataFrame, thematic_vector: dict[str, float]) -> pd.Series:
    if not thematic_vector:
        return pd.Series([0.5] * len(df), index=df.index)
    scores = []
    for _, row in df.iterrows():
        indicators = row.get("indicadores_tematicos", {})
        if not isinstance(indicators, dict):
            indicators = {}
        total_weight = sum(thematic_vector.values()) or 1.0
        value = (
            sum(float(indicators.get(theme, 0.5)) * weight for theme, weight in thematic_vector.items()) / total_weight
        )
        scores.append(max(0.0, min(1.0, value)))
    return pd.Series(scores, index=df.index)
