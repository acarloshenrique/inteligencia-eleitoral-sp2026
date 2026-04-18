from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from data_catalog.models import DataSourceSpec


@dataclass(frozen=True)
class WrittenDataset:
    parquet_path: Path
    duckdb_path: Path | None
    rows: int
    schema: dict[str, str]


class ParquetDuckDBWriter:
    def write(
        self, *, df: pd.DataFrame, source: DataSourceSpec, destination_dir: Path, table_name: str | None = None
    ) -> WrittenDataset:
        destination_dir.mkdir(parents=True, exist_ok=True)
        parquet_path = destination_dir / f"{source.name}.parquet"
        df.to_parquet(parquet_path, index=False)
        duckdb_path = self._write_duckdb(df=df, destination_dir=destination_dir, table_name=table_name or source.name)
        return WrittenDataset(
            parquet_path=parquet_path,
            duckdb_path=duckdb_path,
            rows=int(len(df)),
            schema={col: str(dtype) for col, dtype in df.dtypes.items()},
        )

    def _write_duckdb(self, *, df: pd.DataFrame, destination_dir: Path, table_name: str) -> Path | None:
        try:
            import duckdb
        except ImportError:
            return None
        safe_table = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in table_name.lower())
        duckdb_path = destination_dir / "dataset.duckdb"
        with duckdb.connect(str(duckdb_path)) as con:
            con.register("_df", df)
            con.execute(f"CREATE OR REPLACE TABLE {safe_table} AS SELECT * FROM _df")  # noqa: S608 - nome saneado localmente.
        return duckdb_path
