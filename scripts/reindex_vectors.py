import argparse
from pathlib import Path

from config.settings import get_settings
from infrastructure.vector_index_job import run_vector_reindex_job


def _resolve_input_path(paths, explicit_input: str | None) -> Path:
    if explicit_input:
        return Path(explicit_input)
    preferred = paths.pasta_est / f"df_mun_{paths.ts}.parquet"
    if preferred.exists():
        return preferred
    candidates = sorted(paths.pasta_est.glob("df_mun_*.parquet"), reverse=True)
    if not candidates:
        raise FileNotFoundError(f"Nenhum df_mun_*.parquet encontrado em {paths.pasta_est}")
    return candidates[0]


def main() -> int:
    parser = argparse.ArgumentParser(description="Reindexacao vetorial idempotente dos municipios.")
    parser.add_argument("--input", default=None, help="Parquet de entrada (opcional).")
    parser.add_argument("--collection", default="municipios_v2", help="Nome da collection no ChromaDB.")
    parser.add_argument("--force", action="store_true", help="Forca reindexacao mesmo sem mudancas no hash.")
    args = parser.parse_args()

    settings = get_settings()
    paths = settings.build_paths()
    input_path = _resolve_input_path(paths, args.input)
    result = run_vector_reindex_job(
        chromadb_path=paths.chromadb_path,
        input_parquet=input_path,
        collection_name=args.collection,
        force=bool(args.force),
    )
    print(f"status={result['status']}")
    print(f"collection={result['collection_name']}")
    print(f"source_hash={result['source_hash']}")
    print(f"source_rows={result['source_rows']}")
    print(f"state_path={result['state_path']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
