from __future__ import annotations

import argparse
import json
from pathlib import Path

from config.settings import get_settings
from feature_store.pipeline import AnalyticalFeatureStore, read_gold_tables
from feature_store.writer import FeatureStoreWriter


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build analytical feature store for territorial recommendation.")
    parser.add_argument("--feature-version", required=True)
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument(
        "--gold-table", action="append", default=[], help="table_name=path. Can be passed multiple times."
    )
    return parser.parse_args()


def parse_gold_table_args(items: list[str]) -> dict[str, Path]:
    parsed: dict[str, Path] = {}
    for item in items:
        if "=" not in item:
            raise ValueError("--gold-table must use table_name=path")
        name, path = item.split("=", 1)
        parsed[name.strip()] = Path(path.strip())
    return parsed


def main() -> None:
    args = parse_args()
    paths = get_settings().build_paths()
    output_dir = args.output_dir or (paths.lakehouse_root / "semantic" / "feature_store")
    gold_tables = read_gold_tables(parse_gold_table_args(args.gold_table))
    result = AnalyticalFeatureStore().compute(gold_tables=gold_tables, feature_version=args.feature_version)
    manifest = FeatureStoreWriter().write(result, output_dir=output_dir)
    print(json.dumps(manifest.model_dump(mode="json"), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
