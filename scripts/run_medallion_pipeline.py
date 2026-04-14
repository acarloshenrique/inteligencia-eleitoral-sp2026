from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config.settings import get_settings
from infrastructure.automated_ingestion import run_automated_ingestion
from infrastructure.medallion_pipeline import MedallionInputs, run_medallion_pipeline


def _resolve_default_base(paths) -> Path:
    preferred = paths.pasta_est / f"df_mun_{paths.ts}.parquet"
    if preferred.exists():
        return preferred
    candidates = sorted(paths.pasta_est.glob("df_mun_*.parquet"), reverse=True)
    if not candidates:
        raise FileNotFoundError(f"nenhum df_mun_*.parquet encontrado em {paths.pasta_est}")
    return candidates[0]


def _resolve_latest_asset(paths, name: str, *, required: bool) -> Path | None:
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
        description="Executa pipeline Bronze/Silver/Gold para dados eleitorais e contexto."
    )
    parser.add_argument("--base-parquet", default="", help="Parquet base eleitoral.")
    parser.add_argument(
        "--source-catalog", default="", help="Catalogo JSON opcional para ingestao automatizada completa."
    )
    parser.add_argument(
        "--mapping-csv",
        default="",
        help="CSV de correspondencia TSE/IBGE. Quando omitido, resolve do bronze/downloads.",
    )
    parser.add_argument(
        "--socio-csv", default="", help="CSV socioeconomico opcional. Quando omitido, resolve do bronze/downloads."
    )
    parser.add_argument(
        "--secao-csv",
        default="",
        help="CSV de resultados por secao opcional. Quando omitido, resolve do bronze/downloads.",
    )
    parser.add_argument(
        "--ibge-csv", default="", help="CSV de indicadores IBGE. Quando omitido, resolve do bronze/downloads."
    )
    parser.add_argument(
        "--seade-csv", default="", help="CSV de indicadores SEADE. Quando omitido, resolve do bronze/downloads."
    )
    parser.add_argument(
        "--fiscal-csv",
        default="",
        help="CSV de transferencias/emendas por municipio. Quando omitido, resolve do bronze/downloads.",
    )
    parser.add_argument("--ano", type=int, default=None, help="Ano de referencia.")
    parser.add_argument("--mes", type=int, default=None, help="Mes de referencia.")
    parser.add_argument("--turno", type=int, default=None, help="Turno eleitoral.")
    parser.add_argument("--window-cycles", type=int, default=3, help="Janela fixa de ciclos eleitorais para agregacao.")
    parser.add_argument("--uf", default="SP", help="UF padrao para particionamento quando ausente na fonte.")
    parser.add_argument("--pipeline-version", default="medallion_v1", help="Versao do pipeline.")
    args = parser.parse_args()

    settings = get_settings()
    paths = settings.build_paths()
    catalog_raw = args.source_catalog or settings.ingestion_source_catalog_path
    if catalog_raw:
        result = run_automated_ingestion(
            paths=paths,
            catalog_path=Path(catalog_raw).resolve(),
            pipeline="medallion",
            pipeline_version=args.pipeline_version,
        )
        print("Ingestao automatizada medallion executada com sucesso")
        print(f"run_id={result['run_id']}")
        print(f"manifest={result['manifest_path']}")
        print(f"promotion_manifest={result['promotion_result']['manifest_path']}")
        return 0

    base_path = Path(args.base_parquet).resolve() if args.base_parquet else _resolve_default_base(paths)
    mapping_path = (
        Path(args.mapping_csv).resolve()
        if args.mapping_csv
        else _resolve_latest_asset(paths, "municipios_tse_ibge", required=True)
    )
    socio_path = (
        Path(args.socio_csv).resolve()
        if args.socio_csv
        else _resolve_latest_asset(paths, "indicadores_municipais", required=False)
    )
    secao_path = (
        Path(args.secao_csv).resolve()
        if args.secao_csv
        else _resolve_latest_asset(paths, "resultados_secao", required=False)
    )
    ibge_path = (
        Path(args.ibge_csv).resolve()
        if args.ibge_csv
        else _resolve_latest_asset(paths, "ibge_pop_renda_educacao", required=False)
    )
    seade_path = (
        Path(args.seade_csv).resolve()
        if args.seade_csv
        else _resolve_latest_asset(paths, "seade_ipvs_emprego_saude", required=False)
    )
    fiscal_path = (
        Path(args.fiscal_csv).resolve()
        if args.fiscal_csv
        else _resolve_latest_asset(paths, "transparencia_transferencias_emendas", required=False)
    )

    if socio_path is not None and not socio_path.exists():
        socio_path = None
    if secao_path is not None and not secao_path.exists():
        secao_path = None
    if ibge_path is not None and not ibge_path.exists():
        ibge_path = None
    if seade_path is not None and not seade_path.exists():
        seade_path = None
    if fiscal_path is not None and not fiscal_path.exists():
        fiscal_path = None

    result = run_medallion_pipeline(
        paths,
        MedallionInputs(
            base_parquet_path=base_path,
            mapping_csv_path=mapping_path,
            socio_csv_path=socio_path,
            secao_csv_path=secao_path,
            ibge_csv_path=ibge_path,
            seade_csv_path=seade_path,
            fiscal_csv_path=fiscal_path,
            ano=args.ano,
            mes=args.mes,
            turno=args.turno,
            window_cycles=args.window_cycles,
            uf=args.uf,
        ),
        pipeline_version=args.pipeline_version,
    )
    print("Pipeline medallion executado com sucesso")
    print(f"run_id={result['run_id']}")
    print(f"manifest={result['manifest_path']}")
    for name, path in result["published"].items():
        print(f"{name}={path}")
    print(f"serving_db={result['serving']['serving_db_path']}")
    print(f"cache={result['serving']['cache_path']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
