from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from commercial.exports import CommercialExportService
from commercial.marts import CommercialMartBuilder
from commercial.snapshots import CampaignSnapshotStore, build_snapshot_spec


def load_table(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return pd.read_parquet(path)
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix == ".json":
        return pd.read_json(path)
    raise ValueError(f"Unsupported table format: {path}")


def parse_gold_table(value: str) -> tuple[str, Path]:
    if "=" not in value:
        raise argparse.ArgumentTypeError("Use name=path for --gold-table.")
    name, raw_path = value.split("=", 1)
    return name, Path(raw_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build commercial marts, snapshots and exports.")
    parser.add_argument("--tenant-id", required=True)
    parser.add_argument("--campaign-id", required=True)
    parser.add_argument("--snapshot-id", required=True)
    parser.add_argument("--dataset-version", default="manual")
    parser.add_argument("--data-root", type=Path, default=Path("lake"))
    parser.add_argument("--output-dir", type=Path, default=Path("output/commercial"))
    parser.add_argument("--gold-table", action="append", default=[], type=parse_gold_table)
    args = parser.parse_args()

    gold_tables = {name: load_table(path) for name, path in args.gold_table}
    result = CommercialMartBuilder().build(
        tenant_id=args.tenant_id,
        campaign_id=args.campaign_id,
        snapshot_id=args.snapshot_id,
        gold_tables=gold_tables,
    )
    candidate_ids = []
    priority = gold_tables.get("gold_priority_score", pd.DataFrame())
    if "candidate_id" in priority:
        candidate_ids = [str(value) for value in priority["candidate_id"].dropna().unique()]

    spec = build_snapshot_spec(
        tenant_id=args.tenant_id,
        campaign_id=args.campaign_id,
        candidate_ids=candidate_ids,
        snapshot_id=args.snapshot_id,
        dataset_version=args.dataset_version,
        source_tables=list(gold_tables),
    )
    CampaignSnapshotStore(args.data_root).write_snapshot(spec=spec, marts=result.marts)
    manifest = CommercialExportService().export(
        marts=result.marts,
        output_dir=args.output_dir / args.tenant_id / args.campaign_id / args.snapshot_id,
        tenant_id=args.tenant_id,
        campaign_id=args.campaign_id,
        snapshot_id=args.snapshot_id,
    )
    print(manifest.model_dump_json(indent=2))


if __name__ == "__main__":
    main()
