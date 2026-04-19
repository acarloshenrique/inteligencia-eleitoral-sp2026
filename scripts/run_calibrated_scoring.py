from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from application.calibrated_scoring_service import CalibratedScoringService
from config.settings import get_settings


def main() -> None:
    parser = argparse.ArgumentParser(description="Run calibrated scoring by municipality/zone/section.")
    parser.add_argument("--input-path", type=Path, default=None)
    parser.add_argument("--weights-path", type=Path, default=Path("config/scoring_weights.yaml"))
    parser.add_argument("--dataset-version", default="sprint2_calibrated_scores")
    parser.add_argument("--capacidade-operacional", type=float, default=0.7)
    parser.add_argument("--top-n", type=int, default=20)
    args = parser.parse_args()

    paths = get_settings().build_paths()
    result = CalibratedScoringService(paths).run(
        input_path=args.input_path,
        weights_path=args.weights_path,
        dataset_version=args.dataset_version,
        capacidade_operacional=args.capacidade_operacional,
        top_n=args.top_n,
    )
    print(
        json.dumps(
            {
                "scored_path": str(result.scored_path),
                "backtest_metrics_path": str(result.backtest_metrics_path),
                "rows": result.rows,
                "granularities": result.granularities,
                "weights_version": result.weights_version,
                "backtest_metrics": result.backtest_metrics,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
