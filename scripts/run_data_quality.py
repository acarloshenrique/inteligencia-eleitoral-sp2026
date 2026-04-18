from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from data_quality import DataQualityReportWriter, DataQualityRunner


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run lake data quality checks and generate health reports.")
    parser.add_argument("--dataset", action="append", default=[], help="dataset_id=path. Can be passed multiple times.")
    parser.add_argument("--output-dir", type=Path, default=Path("output/data_quality"))
    parser.add_argument("--fail-under", type=float, default=0.0)
    return parser.parse_args()


def parse_dataset_args(items: list[str]) -> dict[str, Path]:
    parsed: dict[str, Path] = {}
    for item in items:
        if "=" not in item:
            raise ValueError("--dataset must use dataset_id=path")
        dataset_id, path = item.split("=", 1)
        parsed[dataset_id.strip()] = Path(path.strip())
    return parsed


def read_table(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    if path.suffix.lower() == ".json":
        return pd.read_json(path)
    return pd.read_csv(path, sep=None, engine="python", dtype=str)


def main() -> None:
    args = parse_args()
    datasets = {dataset_id: read_table(path) for dataset_id, path in parse_dataset_args(args.dataset).items()}
    report = DataQualityRunner().run_lake(datasets)
    writer = DataQualityReportWriter()
    json_path = writer.write_json(report, args.output_dir / "lake_health_report.json")
    md_path = writer.write_markdown(report, args.output_dir / "lake_health_report.md")
    print(
        json.dumps(
            {
                "aggregate_quality_score": report.aggregate_quality_score,
                "json_path": str(json_path),
                "markdown_path": str(md_path),
                "production_ready_datasets": report.production_ready_datasets,
                "limited_datasets": report.limited_datasets,
                "not_ready_datasets": report.not_ready_datasets,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    if args.fail_under and report.aggregate_quality_score < args.fail_under:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
