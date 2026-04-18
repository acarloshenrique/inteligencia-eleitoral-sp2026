from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from application.gold_marts import GoldMartBuilder, GoldMartWriter
from config.settings import get_settings


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build commercial gold analytical marts.")
    parser.add_argument("--master-index", required=True, type=Path)
    parser.add_argument("--dataset-version", required=True)
    parser.add_argument("--candidate-profiles", type=Path, default=None)
    parser.add_argument("--electoral-results", type=Path, default=None)
    parser.add_argument("--campaign-finance", type=Path, default=None)
    parser.add_argument("--thematic-signals", type=Path, default=None)
    parser.add_argument("--budget-total", type=float, default=100000.0)
    parser.add_argument("--scenario-id", default="baseline")
    parser.add_argument("--output-dir", type=Path, default=None)
    return parser.parse_args()


def read_table(path: Path | None) -> pd.DataFrame | None:
    if path is None:
        return None
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return pd.read_parquet(path)
    if suffix == ".json":
        return pd.read_json(path)
    return pd.read_csv(path, sep=None, engine="python", dtype=str)


def main() -> None:
    args = parse_args()
    paths = get_settings().build_paths()
    output_dir = args.output_dir or (paths.lakehouse_root / "gold" / "marts")
    tables = GoldMartBuilder().build_all(
        master_index=read_table(args.master_index),
        candidate_profiles=read_table(args.candidate_profiles),
        electoral_results=read_table(args.electoral_results),
        campaign_finance=read_table(args.campaign_finance),
        thematic_signals=read_table(args.thematic_signals),
        budget_total=args.budget_total,
        scenario_id=args.scenario_id,
    )
    result = GoldMartWriter().write_all(tables, output_dir=output_dir, dataset_version=args.dataset_version)
    print(
        json.dumps(
            {
                "duckdb_path": str(result.duckdb_path) if result.duckdb_path else None,
                "sql_examples_path": str(result.sql_examples_path),
                "outputs": [output.model_dump(mode="json") for output in result.outputs],
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
