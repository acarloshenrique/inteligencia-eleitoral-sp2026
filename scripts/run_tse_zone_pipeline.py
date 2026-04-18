from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config.settings import get_settings
from infrastructure.tse_zone_pipeline import TSEZoneInputs, run_tse_zone_section_pipeline


def main() -> int:
    parser = argparse.ArgumentParser(description="Normaliza arquivos TSE reais por zona/secao e publica silver/gold.")
    parser.add_argument("--eleitorado", required=True, help="CSV ou ZIP TSE de eleitorado/local por secao ou zona.")
    parser.add_argument("--resultados", default="", help="CSV ou ZIP TSE de resultados por secao/zona, opcional.")
    parser.add_argument("--uf", default="SP")
    parser.add_argument("--ano", type=int, default=2024)
    parser.add_argument("--turno", type=int, default=1)
    args = parser.parse_args()

    paths = get_settings().build_paths()
    result = run_tse_zone_section_pipeline(
        paths=paths,
        inputs=TSEZoneInputs(
            eleitorado_path=Path(args.eleitorado).resolve(),
            resultados_path=Path(args.resultados).resolve() if args.resultados else None,
            uf=args.uf,
            ano_eleicao=args.ano,
            turno=args.turno,
        ),
    )
    print("Ingestao TSE zona/secao executada")
    print(f"run_id={result['run_id']}")
    print(f"manifest={result['manifest_path']}")
    print(f"fact_zona={result['fact_zona_eleitoral_path']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
