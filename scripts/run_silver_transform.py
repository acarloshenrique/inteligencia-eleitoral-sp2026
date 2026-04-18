from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from config.settings import get_settings
from ingestion.silver import MunicipalCrosswalk, SilverDatasetTransformer, SilverDatasetWriter, utc_now_iso


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Transform raw/bronze data into governed silver parquet.")
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument("--input-path", required=True, type=Path)
    parser.add_argument("--source-dataset", default=None)
    parser.add_argument("--crosswalk-path", type=Path, default=None)
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--ingestion-timestamp", default=None)
    return parser.parse_args()


def _read_table(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return pd.read_parquet(path)
    if suffix == ".json":
        return pd.read_json(path)
    return pd.read_csv(path, sep=None, engine="python", dtype=str)


def main() -> None:
    args = parse_args()
    paths = get_settings().build_paths()
    df = _read_table(args.input_path)
    crosswalk = MunicipalCrosswalk.from_dataframe(_read_table(args.crosswalk_path)) if args.crosswalk_path else None
    transformer = SilverDatasetTransformer.for_dataset(args.dataset_id, crosswalk=crosswalk)
    result = transformer.transform(
        df,
        source_dataset=args.source_dataset or args.dataset_id,
        source_file=str(args.input_path),
        ingestion_timestamp=args.ingestion_timestamp or utc_now_iso(),
    )
    output_dir = args.output_dir or (paths.lakehouse_root / "silver" / args.dataset_id)
    parquet_path, quality_path = SilverDatasetWriter().write(
        result, destination_dir=output_dir, dataset_id=args.dataset_id
    )
    print(
        json.dumps(
            {
                "status": result.quality.status,
                "parquet_path": str(parquet_path),
                "quality_path": str(quality_path),
                "quality": result.quality.model_dump(mode="json"),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
