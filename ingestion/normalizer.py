from __future__ import annotations

import zipfile
from pathlib import Path

import pandas as pd

from data_catalog.models import DataSourceSpec


class DatasetNormalizer:
    def normalize(self, path: Path, source: DataSourceSpec) -> pd.DataFrame:
        suffix = path.suffix.lower()
        if suffix == ".parquet":
            df = pd.read_parquet(path)
        elif suffix == ".json":
            df = pd.read_json(path)
        elif suffix == ".zip":
            df = self._read_zip_csv(path)
        else:
            df = pd.read_csv(path, sep=None, engine="python", dtype=str)
        return self._standardize_columns(df, source)

    def _read_zip_csv(self, path: Path) -> pd.DataFrame:
        with zipfile.ZipFile(path) as archive:
            names = [name for name in archive.namelist() if name.lower().endswith(".csv")]
            if not names:
                raise ValueError(f"zip sem CSV: {path}")
            with archive.open(names[0]) as handle:
                return pd.read_csv(handle, sep=";", encoding="latin1", dtype=str, low_memory=False)

    def _standardize_columns(self, df: pd.DataFrame, source: DataSourceSpec) -> pd.DataFrame:
        del source
        out = df.copy()
        out.columns = [str(col).strip() for col in out.columns]
        return out
