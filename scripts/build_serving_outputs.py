from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from serving.builder import ServingLayerBuilder
from serving.writer import ServingLayerWriter


def parse_table(value: str) -> tuple[str, Path]:
    if "=" not in value:
        raise argparse.ArgumentTypeError("Use name=path for --table.")
    name, path = value.split("=", 1)
    return name, Path(path)


def load_table(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return pd.read_parquet(path)
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix == ".json":
        return pd.read_json(path)
    raise ValueError(f"Unsupported table format: {path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build serving outputs for API/UI/recommendation consumers.")
    parser.add_argument("--tenant-id", required=True)
    parser.add_argument("--campaign-id", required=True)
    parser.add_argument("--snapshot-id", required=True)
    parser.add_argument("--dataset-version", required=True)
    parser.add_argument("--lake-root", type=Path, default=Path("lake"))
    parser.add_argument("--table", action="append", default=[], type=parse_table)
    args = parser.parse_args()

    tables = {name: load_table(path) for name, path in args.table}
    result = ServingLayerBuilder().build(
        tenant_id=args.tenant_id,
        campaign_id=args.campaign_id,
        snapshot_id=args.snapshot_id,
        dataset_version=args.dataset_version,
        tables=tables,
    )
    manifest = ServingLayerWriter(args.lake_root).write(
        result=result,
        tenant_id=args.tenant_id,
        campaign_id=args.campaign_id,
        snapshot_id=args.snapshot_id,
        dataset_version=args.dataset_version,
        source_tables=list(tables),
    )
    print(manifest.model_dump_json(indent=2))


if __name__ == "__main__":
    main()
