import tempfile
from pathlib import Path

import pandas as pd

from infrastructure.vector_index_job import run_vector_reindex_job


class FakeEmbedder:
    def encode(self, docs):
        return [[float(len(d) % 7), 1.0, 2.0] for d in docs]


class FakeCollection:
    def __init__(self):
        self._count = 0

    def add(self, ids, documents, embeddings, metadatas):
        self._count += len(ids)

    def count(self):
        return self._count


class FakeChromaClient:
    def __init__(self):
        self._collections = {}

    def delete_collection(self, name):
        self._collections.pop(name, None)

    def get_or_create_collection(self, name):
        if name not in self._collections:
            self._collections[name] = FakeCollection()
        return self._collections[name]


def test_vector_reindex_job_is_idempotent_without_force():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        chroma = root / "chromadb"
        chroma.mkdir(parents=True, exist_ok=True)
        source = root / "df_mun.parquet"
        pd.DataFrame(
            [
                {"ranking_final": 1, "municipio": "Cidade A", "cluster": "Diamante", "indice_final": 90.0},
                {"ranking_final": 2, "municipio": "Cidade B", "cluster": "Alavanca", "indice_final": 80.0},
            ]
        ).to_parquet(source, index=False)

        fake_client = FakeChromaClient()
        first = run_vector_reindex_job(
            chromadb_path=chroma,
            input_parquet=source,
            collection_name="municipios_v2",
            force=False,
            embedder_factory=lambda: FakeEmbedder(),
            chroma_client_factory=lambda _: fake_client,
        )
        second = run_vector_reindex_job(
            chromadb_path=chroma,
            input_parquet=source,
            collection_name="municipios_v2",
            force=False,
            embedder_factory=lambda: FakeEmbedder(),
            chroma_client_factory=lambda _: fake_client,
        )

        assert first["status"] == "indexed"
        assert first["indexed_count"] == 2
        assert second["status"] == "skipped"
        assert second["reason"] == "already_indexed"
