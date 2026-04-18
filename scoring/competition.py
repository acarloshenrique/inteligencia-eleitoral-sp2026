from __future__ import annotations

import pandas as pd


def compute_competition(df: pd.DataFrame) -> pd.Series:
    if "competitividade" in df.columns:
        return pd.to_numeric(df["competitividade"], errors="coerce").fillna(0.5).clip(0, 1)
    if "concorrencia_local" in df.columns:
        return pd.to_numeric(df["concorrencia_local"], errors="coerce").fillna(0.5).clip(0, 1)
    return pd.Series([0.5] * len(df), index=df.index)
