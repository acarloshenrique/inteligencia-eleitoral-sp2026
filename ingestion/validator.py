from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from data_catalog.models import DataSourceSpec


@dataclass(frozen=True)
class ValidationReport:
    ok: bool
    rows: int
    missing_keys: list[str] = field(default_factory=list)
    null_key_counts: dict[str, int] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)


class DatasetValidator:
    def validate(self, df: pd.DataFrame, source: DataSourceSpec) -> ValidationReport:
        missing = [key for key in source.chaves_principais if key not in df.columns]
        null_counts = {key: int(df[key].isna().sum()) for key in source.chaves_principais if key in df.columns}
        errors: list[str] = []
        if df.empty:
            errors.append("dataset vazio")
        if missing:
            errors.append(f"chaves principais ausentes: {', '.join(missing)}")
        return ValidationReport(
            ok=not errors,
            rows=int(len(df)),
            missing_keys=missing,
            null_key_counts=null_counts,
            errors=errors,
        )
