from __future__ import annotations

import pandas as pd

from scoring.utils import normalized_series


def compute_expansion(df: pd.DataFrame) -> pd.Series:
    turnout_gap = pd.Series([0.5] * len(df), index=df.index)
    if "abstencao_pct" in df.columns:
        turnout_gap = pd.to_numeric(df["abstencao_pct"], errors="coerce").fillna(0.0).clip(0, 1)
    scale = (
        normalized_series(df["eleitores_aptos"])
        if "eleitores_aptos" in df.columns
        else pd.Series([0.5] * len(df), index=df.index)
    )
    base_source = (
        df["base_context_score"] if "base_context_score" in df.columns else pd.Series([0.35] * len(df), index=df.index)
    )
    base = pd.to_numeric(base_source, errors="coerce").fillna(0.35)
    return (0.45 * turnout_gap + 0.35 * scale + 0.20 * (1 - base.clip(0, 1))).clip(0, 1)
