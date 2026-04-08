from __future__ import annotations

import argparse
from pathlib import Path

from config.settings import get_settings
from infrastructure.open_data_pipeline import OpenDataInputs, run_open_data_crosswalk_pipeline


def _resolve_default_base(paths) -> Path:
    preferred = paths.pasta_est / f"df_mun_{paths.ts}.parquet"
    if preferred.exists():
        return preferred
    candidates = sorted(paths.pasta_est.glob("df_mun_*.parquet"), reverse=True)
    if not candidates:
        raise FileNotFoundError(f"nenhum df_mun_*.parquet encontrado em {paths.pasta_est}")
    return candidates[0]


def main() -> int:
    parser = argparse.ArgumentParser(description="Executa cruzamento de dados abertos para enriquecer o dataset municipal.")
    parser.add_argument("--base-parquet", default="", help="Parquet base com ranking municipal.")
    parser.add_argument(
        "--mapping-csv",
        default="",
        help="CSV com correspondencia municipio/codigo TSE/IBGE (obrigatorio).",
    )
    parser.add_argument("--socio-csv", default="", help="CSV socioeconomico opcional com chave codigo_ibge.")
    parser.add_argument("--ano", type=int, default=None, help="Ano de referencia para chave canonica.")
    parser.add_argument("--mes", type=int, default=None, help="Mes de referencia (quando aplicavel).")
    parser.add_argument("--turno", type=int, default=None, help="Turno eleitoral de referencia.")
    parser.add_argument("--pipeline-version", default="open_data_v1", help="Versao do pipeline.")
    args = parser.parse_args()

    settings = get_settings()
    paths = settings.build_paths()
    open_data_root = paths.data_root / "open_data"

    base_path = Path(args.base_parquet).resolve() if args.base_parquet else _resolve_default_base(paths)
    mapping_path = (
        Path(args.mapping_csv).resolve()
        if args.mapping_csv
        else (open_data_root / "raw" / "municipios_tse_ibge.csv").resolve()
    )
    socio_path = (
        Path(args.socio_csv).resolve()
        if args.socio_csv
        else (open_data_root / "raw" / "indicadores_municipais.csv").resolve()
    )
    if not socio_path.exists():
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
