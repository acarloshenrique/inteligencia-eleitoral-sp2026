from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from infrastructure.source_prioritization import (
    load_source_catalog,
    prioritize_sources,
    render_prioritization_report,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Prioriza fontes de dados abertos por impacto e criterio de entrada.")
    parser.add_argument(
        "--catalog",
        default="config/open_data_sources.json",
        help="Catalogo JSON de fontes.",
    )
    parser.add_argument(
        "--output",
        default="docs/open_data_sources_report.json",
        help="Caminho do relatorio de priorizacao.",
    )
    args = parser.parse_args()

    catalog_path = Path(args.catalog).resolve()
    output_path = Path(args.output).resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    sources = load_source_catalog(catalog_path)
    grouped = prioritize_sources(sources)
    report = render_prioritization_report(grouped)
    output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"catalog={catalog_path}")
    print(f"accepted_a={len(report['accepted_a'])}")
    print(f"accepted_b={len(report['accepted_b'])}")
    print(f"rejected={len(report['rejected'])}")
    print(f"report={output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
