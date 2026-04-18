from __future__ import annotations

import argparse
import json
from pathlib import Path

from config.settings import get_settings
from ingestion.bronze import BaseIngestionJob
from ingestion.bronze_sources import ALL_BRONZE_DATASETS


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run bronze raw ingestion for governed public datasets.")
    parser.add_argument("--dataset", required=True, choices=sorted(ALL_BRONZE_DATASETS))
    parser.add_argument("--ano", required=True, type=int)
    parser.add_argument("--uf", default=None)
    parser.add_argument("--municipio", default=None)
    parser.add_argument("--local-path", type=Path, default=None)
    parser.add_argument("--expected-sha256", default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    paths = get_settings().build_paths()
    definition = ALL_BRONZE_DATASETS[args.dataset]
    request = definition.build_request(
        ano=args.ano,
        uf=args.uf,
        municipio=args.municipio,
        local_path=args.local_path,
        expected_sha256=args.expected_sha256,
    )
    job = BaseIngestionJob(bronze_root=paths.lakehouse_root / "bronze", run_root=paths.ingestion_root / "bronze")
    report = job.run(request)
    print(json.dumps(report.model_dump(mode="json"), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
