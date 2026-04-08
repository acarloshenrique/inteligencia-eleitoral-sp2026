from pathlib import Path

from config.settings import get_settings
from infrastructure.data_pipeline import run_versioned_data_pipeline


def main() -> int:
    settings = get_settings()
    paths = settings.build_paths()

    input_path = paths.pasta_est / f"df_mun_{paths.ts}.parquet"
    if not input_path.exists():
        candidates = sorted(paths.pasta_est.glob("df_mun_*.parquet"), reverse=True)
        if not candidates:
            raise FileNotFoundError(f"Nenhum df_mun_*.parquet encontrado em {paths.pasta_est}")
        input_path = candidates[0]

    result = run_versioned_data_pipeline(paths=paths, input_path=Path(input_path))
    print("Pipeline executado com sucesso")
    print(f"run_id={result['run_id']}")
    print(f"manifest={result['manifest_path']}")
    print(f"published={result['publish']['published_path']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
