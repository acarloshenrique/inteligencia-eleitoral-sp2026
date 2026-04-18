from __future__ import annotations

import argparse
import json
from pathlib import Path

from application.master_index_service import MasterIndexPipeline
from config.settings import get_settings


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the gold territorial-electoral master index.")
    parser.add_argument("--resultados-secao", required=True, type=Path)
    parser.add_argument("--eleitorado-secao", type=Path, default=None)
    parser.add_argument("--locais-votacao", type=Path, default=None)
    parser.add_argument("--candidatos", type=Path, default=None)
    parser.add_argument("--prestacao-contas", type=Path, default=None)
    parser.add_argument("--setores-censitarios", type=Path, default=None)
    parser.add_argument("--municipio-crosswalk", type=Path, default=None)
    parser.add_argument("--dataset-version", required=True)
    parser.add_argument("--output-dir", type=Path, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    paths = get_settings().build_paths()
    output_dir = args.output_dir or (paths.lakehouse_root / "gold" / "territorial_electoral_master_index")
    result = MasterIndexPipeline().run_from_paths(
        resultados_secao_path=args.resultados_secao,
        eleitorado_secao_path=args.eleitorado_secao,
        locais_votacao_path=args.locais_votacao,
        candidatos_path=args.candidatos,
        prestacao_contas_path=args.prestacao_contas,
        setores_censitarios_path=args.setores_censitarios,
        municipio_crosswalk_path=args.municipio_crosswalk,
        output_dir=output_dir,
        dataset_version=args.dataset_version,
    )
    print(
        json.dumps(
            {
                "parquet_path": str(result.parquet_path) if result.parquet_path else None,
                "duckdb_path": str(result.duckdb_path) if result.duckdb_path else None,
                "manifest_path": str(result.manifest_path) if result.manifest_path else None,
                "quality": result.quality.model_dump(mode="json"),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
