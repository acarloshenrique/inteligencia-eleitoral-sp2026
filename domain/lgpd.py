from __future__ import annotations

from hashlib import sha256
from typing import Iterable

import pandas as pd

DEFAULT_ALLOWED_COLUMNS = [
    "ranking_final",
    "municipio",
    "cluster",
    "indice_final",
    "PD_qt",
    "pop_censo2022",
]


def minimize_dataframe(df: pd.DataFrame, allowed_columns: Iterable[str] = DEFAULT_ALLOWED_COLUMNS) -> pd.DataFrame:
    allowed = [c for c in allowed_columns if c in df.columns]
    if not allowed:
        return df.copy()
    return df[allowed].copy()


def anonymize_value(value: str, salt: str) -> str:
    raw = f"{salt}:{value}".encode("utf-8")
    return sha256(raw).hexdigest()[:16]


def anonymize_columns(df: pd.DataFrame, columns: Iterable[str], salt: str) -> pd.DataFrame:
    out = df.copy()
    for col in columns:
        if col in out.columns:
            out[col] = out[col].astype(str).map(lambda v: anonymize_value(v, salt))
    return out
