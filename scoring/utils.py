from __future__ import annotations

import pandas as pd


def clamp01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def normalized_series(series: pd.Series, *, invert: bool = False) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce").fillna(0.0)
    min_v = float(numeric.min()) if len(numeric) else 0.0
    max_v = float(numeric.max()) if len(numeric) else 0.0
    if max_v == min_v:
        out = pd.Series([0.5] * len(numeric), index=numeric.index)
    else:
        out = (numeric - min_v) / (max_v - min_v)
    return 1 - out if invert else out
