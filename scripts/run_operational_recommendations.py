from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from application.operational_recommendation_service import OperationalRecommendationService
from config.settings import get_settings


def main() -> None:
    parser = argparse.ArgumentParser(description="Build operational campaign recommendations and executive exports.")
    parser.add_argument("--scores-path", type=Path, default=None)
    parser.add_argument("--dataset-version", default="sprint3_operational_recommendations")
    parser.add_argument("--budget-total", type=float, default=200000.0)
    parser.add_argument("--top-n", type=int, default=30)
    parser.add_argument("--capacidade-operacional", type=float, default=0.7)
    parser.add_argument("--janela-temporal-dias", type=int, default=45)
    parser.add_argument("--score-granularity", default="zona", choices=["municipio", "zona", "secao"])
    args = parser.parse_args()

    result = OperationalRecommendationService(get_settings().build_paths()).run(
        scores_path=args.scores_path,
        dataset_version=args.dataset_version,
        budget_total=args.budget_total,
        top_n=args.top_n,
        capacidade_operacional=args.capacidade_operacional,
        janela_temporal_dias=args.janela_temporal_dias,
        score_granularity=args.score_granularity,
    )
    print(
        json.dumps(
            {
                "recommendations_path": str(result.recommendations_path),
                "summary_path": str(result.summary_path),
                "executive_pdf_path": str(result.executive_pdf_path),
                "workbook_path": str(result.workbook_path),
                "rows": result.rows,
                "scenarios": result.scenarios,
                "generated_at_utc": result.generated_at_utc,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
