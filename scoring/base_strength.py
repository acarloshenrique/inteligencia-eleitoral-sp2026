from __future__ import annotations

import pandas as pd

from scoring.utils import normalized_series


def compute_base_strength(df: pd.DataFrame) -> pd.Series:
    if "base_context_score" in df.columns:
        return pd.to_numeric(df["base_context_score"], errors="coerce").fillna(0.35).clip(0, 1)
    if "votos_validos" in df.columns:
        return normalized_series(df["votos_validos"])
    if "eleitores_aptos" in df.columns:
        return normalized_series(df["eleitores_aptos"])
    return pd.Series([0.5] * len(df), index=df.index)
