from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config.settings import get_settings
from infrastructure.medallion_pipeline import MedallionInputs, run_medallion_pipeline


def _resolve_default_base(paths) -> Path:
    preferred = paths.pasta_est / f"df_mun_{paths.ts}.parquet"
    if preferred.exists():
        return preferred
    candidates = sorted(paths.pasta_est.glob("df_mun_*.parquet"), reverse=True)
    if not candidates:
        raise FileNotFoundError(f"nenhum df_mun_*.parquet encontrado em {paths.pasta_est}")
    return candidates[0]


def main() -> int:
    parser = argparse.ArgumentParser(description="Executa pipeline Bronze/Silver/Gold para dados eleitorais e contexto.")
    parser.add_argument("--base-parquet", default="", help="Parquet base eleitoral.")
    parser.add_argument("--mapping-csv", default="", help="CSV de correspondencia TSE/IBGE.")
    parser.add_argument("--socio-csv", default="", help="CSV socioeconomico opcional.")
    parser.add_argument("--secao-csv", default="", help="CSV de resultados por secao opcional.")
    parser.add_argument("--ibge-csv", default="", help="CSV de indicadores IBGE (pop/renda/educacao).")
    parser.add_argument("--seade-csv", default="", help="CSV de indicadores SEADE (IPVS/emprego/saude).")
    parser.add_argument("--fiscal-csv", default="", help="CSV de transferencias/emendas por municipio.")
    parser.add_argument("--ano", type=int, default=None, help="Ano de referencia.")
    parser.add_argument("--mes", type=int, default=None, help="Mes de referencia.")
    parser.add_argument("--turno", type=int, default=None, help="Turno eleitoral.")
    parser.add_argument("--window-cycles", type=int, default=3, help="Janela fixa de ciclos eleitorais para agregacao.")
    parser.add_argument("--uf", default="SP", help="UF padrao para particionamento quando ausente na fonte.")
    parser.add_argument("--pipeline-version", default="medallion_v1", help="Versao do pipeline.")
    args = parser.parse_args()

    settings = get_settings()
    paths = settings.build_paths()
    open_data_root = paths.data_root / "open_data" / "raw"

    base_path = Path(args.base_parquet).resolve() if args.base_parquet else _resolve_default_base(paths)
    mapping_path = (
        Path(args.mapping_csv).resolve()
        if args.mapping_csv
        else (open_data_root / "municipios_tse_ibge.csv").resolve()
    )
    socio_path = Path(args.socio_csv).resolve() if args.socio_csv else (open_data_root / "indicadores_municipais.csv").resolve()
    secao_path = Path(args.secao_csv).resolve() if args.secao_csv else (open_data_root / "resultados_secao.csv").resolve()
    ibge_path = Path(args.ibge_csv).resolve() if args.ibge_csv else (open_data_root / "ibge_pop_renda_educacao.csv").resolve()
    seade_path = Path(args.seade_csv).resolve() if args.seade_csv else (open_data_root / "seade_ipvs_emprego_saude.csv").resolve()
    fiscal_path = Path(args.fiscal_csv).resolve() if args.fiscal_csv else (open_data_root / "transparencia_transferencias_emendas.csv").resolve()

    if not socio_path.exists():
        socio_path = None
    if not secao_path.exists():
        secao_path = None
    if not ibge_path.exists():
        ibge_path = None
    if not seade_path.exists():
        seade_path = None
    if not fiscal_path.exists():
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
