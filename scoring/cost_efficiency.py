from __future__ import annotations

import pandas as pd

from scoring.utils import normalized_series


def compute_cost_efficiency(df: pd.DataFrame) -> pd.Series:
    if "custo_operacional_estimado" in df.columns:
        return normalized_series(df["custo_operacional_estimado"], invert=True).clip(0, 1)
    if "eleitores_aptos" in df.columns:
        return normalized_series(df["eleitores_aptos"]).clip(0, 1)
    return pd.Series([0.5] * len(df), index=df.index)
