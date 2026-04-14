from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config.settings import get_settings
from infrastructure.automated_ingestion import run_automated_ingestion


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Executa ingestao automatizada: download, validacao e promocao bronze/silver/gold."
    )
    parser.add_argument("--source-catalog", default="", help="Catalogo JSON com assets remotos.")
    parser.add_argument("--pipeline", default="", help="Override do pipeline: open_data ou medallion.")
    parser.add_argument("--pipeline-version", default="", help="Override da versao do pipeline.")
    args = parser.parse_args()

    settings = get_settings()
    paths = settings.build_paths()
    catalog_raw = args.source_catalog or settings.ingestion_source_catalog_path
    if not catalog_raw:
        raise ValueError("informe --source-catalog ou configure INGESTION_SOURCE_CATALOG_PATH")

    result = run_automated_ingestion(
        paths=paths,
        catalog_path=Path(catalog_raw).resolve(),
        pipeline=args.pipeline or None,
        pipeline_version=args.pipeline_version or None,
    )
    print("Ingestao automatizada executada com sucesso")
    print(f"run_id={result['run_id']}")
    print(f"pipeline={result['pipeline']}")
    print(f"manifest={result['manifest_path']}")
    print(f"promotion_manifest={result['promotion_result']['manifest_path']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
