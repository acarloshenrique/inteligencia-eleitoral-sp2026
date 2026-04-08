from __future__ import annotations

from datetime import UTC, datetime
import hashlib
import json
import logging
from pathlib import Path
from typing import Any, Callable

import pandas as pd


class VectorIndexError(RuntimeError):
    pass


logger = logging.getLogger(__name__)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(65536)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _default_embedder_factory():
    from sentence_transformers import SentenceTransformer

    return SentenceTransformer("all-MiniLM-L6-v2")


def _default_chroma_client_factory(chromadb_path: Path):
    import chromadb

    return chromadb.PersistentClient(path=str(chromadb_path))


def _load_state(state_path: Path) -> dict[str, Any]:
    if not state_path.exists():
        return {}
    try:
        return json.loads(state_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _build_documents(df: pd.DataFrame) -> tuple[list[str], list[str], list[dict[str, Any]]]:
    ids: list[str] = []
    docs: list[str] = []
    metas: list[dict[str, Any]] = []
    for idx, row in df.reset_index(drop=True).iterrows():
        municipio = str(row.get("municipio", "")).strip()
        cluster = str(row.get("cluster", "")).strip()
        indice = float(row.get("indice_final", 0.0))
        ranking = int(row.get("ranking_final", idx + 1))
        ids.append(f"mun-{ranking}-{idx}")
        docs.append(f"Municipio: {municipio}. Cluster: {cluster}. Indice final: {indice:.2f}. Ranking: {ranking}.")
        metas.append(
            {
                "municipio": municipio,
                "cluster": cluster,
                "indice_final": indice,
                "ranking_final": ranking,
            }
        )
    return ids, docs, metas


def run_vector_reindex_job(
    *,
    chromadb_path: Path,
    input_parquet: Path,
    collection_name: str = "municipios_v2",
    force: bool = False,
    embedder_factory: Callable[[], Any] | None = None,
    chroma_client_factory: Callable[[Path], Any] | None = None,
) -> dict[str, Any]:
    if not input_parquet.exists():
        raise VectorIndexError(f"Arquivo de origem nao encontrado: {input_parquet}")

    chromadb_path.mkdir(parents=True, exist_ok=True)
    state_path = chromadb_path / "index_state.json"
    source_hash = _sha256_file(input_parquet)
    source_rows = int(len(pd.read_parquet(input_parquet)))

    prev_state = _load_state(state_path)
    if (
        not force
        and prev_state.get("source_hash") == source_hash
        and prev_state.get("source_rows") == source_rows
        and prev_state.get("collection_name") == collection_name
    ):
        return {
            "status": "skipped",
            "reason": "already_indexed",
            "collection_name": collection_name,
            "source_hash": source_hash,
            "source_rows": source_rows,
            "state_path": str(state_path),
        }

    ef = embedder_factory or _default_embedder_factory
    cf = chroma_client_factory or _default_chroma_client_factory
    embedder = ef()
    client = cf(chromadb_path)

    try:
        client.delete_collection(collection_name)
    except Exception as exc:
        logger.warning("Falha ao remover colecao existente '%s': %s", collection_name, exc)
    collection = client.get_or_create_collection(collection_name)

    df = pd.read_parquet(input_parquet)
    ids, docs, metas = _build_documents(df)
    if ids:
        encoded = embedder.encode(docs)
        embeddings = encoded.tolist() if hasattr(encoded, "tolist") else list(encoded)
        batch = 128
        for i in range(0, len(ids), batch):
            j = i + batch
            collection.add(
                ids=ids[i:j],
                documents=docs[i:j],
                embeddings=embeddings[i:j],
                metadatas=metas[i:j],
            )

    new_state = {
        "updated_at_utc": datetime.now(UTC).isoformat(),
        "collection_name": collection_name,
        "source_path": str(input_parquet),
        "source_hash": source_hash,
        "source_rows": source_rows,
        "indexed_count": int(collection.count()),
    }
    state_path.write_text(json.dumps(new_state, ensure_ascii=False, indent=2), encoding="utf-8")
    return {"status": "indexed", **new_state, "state_path": str(state_path)}
