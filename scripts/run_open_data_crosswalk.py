from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config.settings import get_settings
from infrastructure.automated_ingestion import run_automated_ingestion
from infrastructure.open_data_pipeline import OpenDataInputs, run_open_data_crosswalk_pipeline


def _resolve_default_base(paths) -> Path:
    preferred = paths.pasta_est / f"df_mun_{paths.ts}.parquet"
    if preferred.exists():
        return preferred
    candidates = sorted(paths.pasta_est.glob("df_mun_*.parquet"), reverse=True)
    if not candidates:
        raise FileNotFoundError(f"nenhum df_mun_*.parquet encontrado em {paths.pasta_est}")
    return candidates[0]


def _resolve_latest_bronze_asset(paths, name: str, *, required: bool) -> Path | None:
    bronze_candidates = sorted(paths.bronze_root.rglob(f"{name}.*"), reverse=True)
    if bronze_candidates:
        return bronze_candidates[0]
    download_candidates = sorted((paths.ingestion_root / "downloads").rglob(f"{name}.*"), reverse=True)
    if download_candidates:
        return download_candidates[0]
    if required:
        raise FileNotFoundError(
            f"asset '{name}' nao encontrado em {paths.bronze_root} ou {paths.ingestion_root / 'downloads'}"
        )
    return None


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Executa cruzamento de dados abertos para enriquecer o dataset municipal."
    )
    parser.add_argument("--base-parquet", default="", help="Parquet base com ranking municipal.")
    parser.add_argument(
        "--source-catalog",
        default="",
        help="Catalogo JSON opcional para executar download, validacao e promocao automatizada.",
    )
    parser.add_argument(
        "--mapping-csv",
        default="",
        help="CSV com correspondencia municipio/codigo TSE/IBGE. Quando omitido, resolve do bronze/downloads.",
    )
    parser.add_argument(
        "--socio-csv",
        default="",
        help="CSV socioeconomico opcional com chave codigo_ibge. Quando omitido, resolve do bronze/downloads.",
    )
    parser.add_argument("--ano", type=int, default=None, help="Ano de referencia para chave canonica.")
    parser.add_argument("--mes", type=int, default=None, help="Mes de referencia (quando aplicavel).")
    parser.add_argument("--turno", type=int, default=None, help="Turno eleitoral de referencia.")
    parser.add_argument("--pipeline-version", default="open_data_v1", help="Versao do pipeline.")
    args = parser.parse_args()

    settings = get_settings()
    paths = settings.build_paths()
    catalog_raw = args.source_catalog or settings.ingestion_source_catalog_path
    if catalog_raw:
        result = run_automated_ingestion(
            paths=paths,
            catalog_path=Path(catalog_raw).resolve(),
            pipeline="open_data",
            pipeline_version=args.pipeline_version,
        )
        print("Ingestao automatizada open-data executada com sucesso")
        print(f"run_id={result['run_id']}")
        print(f"manifest={result['manifest_path']}")
        print(f"promotion_manifest={result['promotion_result']['manifest_path']}")
        return 0

    base_path = Path(args.base_parquet).resolve() if args.base_parquet else _resolve_default_base(paths)
    mapping_path = (
        Path(args.mapping_csv).resolve()
        if args.mapping_csv
        else _resolve_latest_bronze_asset(paths, "municipios_tse_ibge", required=True)
    )
    socio_path = (
        Path(args.socio_csv).resolve()
        if args.socio_csv
        else _resolve_latest_bronze_asset(paths, "indicadores_municipais", required=False)
    )
    if socio_path is not None and not socio_path.exists():
        socio_path = None

    result = run_open_data_crosswalk_pipeline(
        paths=paths,
        inputs=OpenDataInputs(
            base_parquet_path=base_path,
            mapping_csv_path=mapping_path,
            socio_csv_path=socio_path,
            ano=args.ano,
            mes=args.mes,
            turno=args.turno,
        ),
        pipeline_version=args.pipeline_version,
    )
    print("Pipeline open-data executado com sucesso")
    print(f"run_id={result['run_id']}")
    print(f"manifest={result['manifest_path']}")
    print(f"published={result['published_path']}")
    print(f"join_rate={result['join_rate']:.4f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
