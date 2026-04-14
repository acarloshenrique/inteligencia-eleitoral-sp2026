from types import SimpleNamespace

import pandas as pd

from application.interfaces import AIService, AnalyticsRepository, DataStore, LLMClient, VectorStore
from infrastructure.repositories import ChromaGroqAIService, DuckDBAnalyticsRepository


class _FakeResult:
    def df(self) -> pd.DataFrame:
        return pd.DataFrame({"n": [1]})


class _FakeDb:
    def execute(self, *args, **kwargs) -> _FakeResult:
        return _FakeResult()

    def register(self, name: str, df: pd.DataFrame) -> None:
        self.registered = (name, df)


def test_duckdb_repository_implements_datastore_protocol() -> None:
    repo = DuckDBAnalyticsRepository(_FakeDb())

    assert isinstance(repo, DataStore)
    assert isinstance(repo, AnalyticsRepository)


def test_chroma_groq_service_implements_vector_and_llm_protocols(tmp_path, monkeypatch) -> None:
    class _Settings:
        rag_cost_per_1k_tokens_usd = 0.00059

    monkeypatch.setattr("infrastructure.repositories.get_settings", lambda: _Settings())
    paths = SimpleNamespace(data_root=tmp_path)

    service = ChromaGroqAIService(tmp_path / "chromadb", app_paths=paths)

    assert isinstance(service, VectorStore)
    assert isinstance(service, LLMClient)
    assert isinstance(service, AIService)
