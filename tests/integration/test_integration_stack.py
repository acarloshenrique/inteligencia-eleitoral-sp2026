import tempfile
from pathlib import Path

import pandas as pd
import pytest

from config.settings import AppPaths
from infrastructure.repositories import ChromaGroqAIService, DuckDBAnalyticsRepository, ParquetReportStore
from infrastructure.storage import carrega_db


def _paths(tmp: str) -> AppPaths:
    root = Path(tmp)
    ingestion_root = root / "ingestion"
    lake_root = root / "lake"
    bronze_root = lake_root / "bronze"
    silver_root = lake_root / "silver"
    gold_root = lake_root / "gold"
    gold_reports_root = gold_root / "reports"
    gold_serving_root = gold_root / "serving"
    catalog_root = gold_root / "_catalog"
    chroma = root / "chromadb"
    runtime_reports_root = root / "runtime_rel"
    for p in [
        ingestion_root,
        bronze_root,
        silver_root,
        gold_root,
        gold_reports_root,
        gold_serving_root,
        catalog_root,
        chroma,
        runtime_reports_root,
    ]:
        p.mkdir(parents=True, exist_ok=True)
    return AppPaths(
        data_root=root,
        ingestion_root=ingestion_root,
        lake_root=lake_root,
        bronze_root=bronze_root,
        silver_root=silver_root,
        gold_root=gold_root,
        gold_reports_root=gold_reports_root,
        gold_serving_root=gold_serving_root,
        catalog_root=catalog_root,
        chromadb_path=chroma,
        runtime_reports_root=runtime_reports_root,
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
def test_integration_storage_registers_gold_mobilizacao_mart():
    pytest.importorskip("duckdb")
    with tempfile.TemporaryDirectory() as tmp:
        paths = _paths(tmp)
        df_mun = pd.DataFrame([{"municipio": "Cidade A", "ranking_final": 1, "indice_final": 90.0}])
        pd.DataFrame(
            [
                {
                    "municipio_id_ibge7": "3500000",
                    "ranking_medio_3ciclos": 1.0,
                    "indice_medio_3ciclos": 90.0,
                    "custo_mobilizacao_relativo": 0.25,
                    "emprego_formal": 0.6,
                    "urbanizacao_pct": 0.8,
                    "acesso_internet_pct": 0.7,
                    "estrutura_urbana_indice": 0.75,
                    "ruralidade_pct": 0.1,
                }
            ]
        ).to_parquet(paths.gold_root / "mart_custo_mobilizacao_20260410_120000.parquet", index=False)
        db = carrega_db.__wrapped__(paths, df_mun)
        repo = DuckDBAnalyticsRepository(db)
        assert repo.table_exists("mart_custo_mobilizacao")
        loaded = repo.query_df("SELECT custo_mobilizacao_relativo FROM mart_custo_mobilizacao")
        assert loaded["custo_mobilizacao_relativo"].tolist() == [0.25]


@pytest.mark.integration
def test_integration_chroma_llm_service_with_fallback(monkeypatch, mock_llm_client):
    with tempfile.TemporaryDirectory() as tmp:
        paths = _paths(tmp)

        class _FakeEmbedder:
            def encode(self, texts):
                return [[1.0, 0.0, 0.5] for _ in texts]

        class _FakeCollection:
            def query(self, query_embeddings, n_results):
                return {"metadatas": [[{"municipio": "Cidade A"}, {"municipio": "Cidade B"}]]}

        def _fake_stack(_):
            return _FakeEmbedder(), _FakeCollection(), mock_llm_client, True

        monkeypatch.setattr("infrastructure.repositories.carrega_stack_ia", _fake_stack)
        svc = ChromaGroqAIService(paths.chromadb_path, app_paths=paths)
        sem = svc.search_relevant("Perfil de Cidade A")
        txt, tokens = svc.complete("sys", [], "Perfil de Cidade A", "Dados:\nabc")
        assert "Cidade A" in sem
        assert txt == "Resposta fake"
        assert tokens == 123


@pytest.mark.integration
def test_integration_storage_registers_product_gold_marts():
    pytest.importorskip("duckdb")
    with tempfile.TemporaryDirectory() as tmp:
        paths = _paths(tmp)
        df_mun = pd.DataFrame([{"municipio": "Cidade A", "ranking_final": 1, "indice_final": 90.0}])
        pd.DataFrame(
            [
                {
                    "municipio_id_ibge7": "3500000",
                    "ranking": 1,
                    "score_alocacao": 91.0,
                    "score_potencial_eleitoral": 0.9,
                    "score_oportunidade": 0.8,
                    "score_eficiencia_midia": 0.7,
                    "score_custo": 0.6,
                    "score_risco": 0.1,
                }
            ]
        ).to_parquet(paths.gold_root / "mart_score_alocacao_modular_20260410_120000.parquet", index=False)
        pd.DataFrame(
            [
                {
                    "municipio_id_ibge7": "3500000",
                    "ranking": 1,
                    "verba_sugerida": 50000,
                    "canal_ideal": "meta_ads",
                    "mensagem_ideal": "Emprego",
                }
            ]
        ).to_parquet(paths.gold_root / "mart_recomendacao_alocacao_20260410_120000.parquet", index=False)
        db = carrega_db.__wrapped__(paths, df_mun)
        repo = DuckDBAnalyticsRepository(db)
        assert repo.table_exists("mart_score_alocacao_modular")
        assert repo.table_exists("mart_recomendacao_alocacao")
        loaded = repo.query_df("SELECT score_alocacao FROM mart_score_alocacao_modular")
        assert loaded["score_alocacao"].tolist() == [91.0]
