from pathlib import Path
import tempfile

import pandas as pd
import pytest

from config.settings import AppPaths
from infrastructure.repositories import ChromaGroqAIService, DuckDBAnalyticsRepository, ParquetReportStore


def _paths(tmp: str) -> AppPaths:
    root = Path(tmp)
    pasta_est = root / "outputs" / "estado_sessao"
    pasta_rel = root / "outputs" / "relatorios"
    chroma = root / "chromadb"
    runtime_rel = root / "runtime_rel"
    for p in [pasta_est, pasta_rel, chroma, runtime_rel]:
        p.mkdir(parents=True, exist_ok=True)
    return AppPaths(
        data_root=root,
        pasta_est=pasta_est,
        pasta_rel=pasta_rel,
        chromadb_path=chroma,
        runtime_rel=runtime_rel,
        ts="20260408_000000",
        metadata_db_path=root / "metadata" / "jobs.sqlite3",
        artifact_root=root / "artifacts",
    )


@pytest.mark.integration
def test_integration_duckdb_repository_query_and_table_exists():
    duckdb = pytest.importorskip("duckdb")
    db = duckdb.connect()
    db.register("municipios", pd.DataFrame([{"municipio": "Cidade A", "indice_final": 90.0}]))
    repo = DuckDBAnalyticsRepository(db)
    assert repo.table_exists("municipios")
    df = repo.query_df("SELECT municipio FROM municipios")
    assert df["municipio"].tolist() == ["Cidade A"]


@pytest.mark.integration
def test_integration_parquet_report_store_roundtrip():
    with tempfile.TemporaryDirectory() as tmp:
        paths = _paths(tmp)
        store = ParquetReportStore(paths)
        df = pd.DataFrame([{"municipio": "Cidade A", "budget": 1000}])
        store.save_report(df, "relatorio.parquet")
        loaded = store.load_report("relatorio.parquet")
        assert loaded is not None
        assert loaded.to_dict("records") == df.to_dict("records")


@pytest.mark.integration
def test_integration_chroma_llm_service_with_fallback(monkeypatch):
    with tempfile.TemporaryDirectory() as tmp:
        paths = _paths(tmp)

        class _FakeEmbedder:
            def encode(self, texts):
                return [[1.0, 0.0, 0.5] for _ in texts]

        class _FakeCollection:
            def query(self, query_embeddings, n_results):
                return {"metadatas": [[{"municipio": "Cidade A"}, {"municipio": "Cidade B"}]]}

        class _FakeCompletions:
            def create(self, model, messages, max_tokens=1024, temperature=0.3):
                class _Usage:
                    total_tokens = 123

                class _Msg:
                    content = "Resposta fake"

                class _Choice:
                    message = _Msg()

                class _Resp:
                    choices = [_Choice()]
                    usage = _Usage()

                return _Resp()

        class _FakeLLM:
            def __init__(self):
                self.chat = type("Chat", (), {"completions": _FakeCompletions()})()

        def _fake_stack(_):
            return _FakeEmbedder(), _FakeCollection(), _FakeLLM(), True

        monkeypatch.setattr("infrastructure.repositories.carrega_stack_ia", _fake_stack)
        svc = ChromaGroqAIService(paths.chromadb_path, app_paths=paths)
        sem = svc.search_relevant("Perfil de Cidade A")
        txt, tokens = svc.complete("sys", [], "Perfil de Cidade A", "Dados:\nabc")
        assert "Cidade A" in sem
        assert txt == "Resposta fake"
        assert tokens == 123
