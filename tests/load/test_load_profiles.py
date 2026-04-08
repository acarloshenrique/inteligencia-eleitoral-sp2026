import time

import pandas as pd
import pytest

from application.use_cases_layered import executar_alocacao, responder_pergunta


class _Repo:
    def __init__(self):
        self._tables = {"municipios"}

    def table_exists(self, table: str) -> bool:
        return table in self._tables

    def query_df(self, sql: str, params=None) -> pd.DataFrame:
        if "GROUP BY cluster" in sql:
            return pd.DataFrame([{"cluster": "Diamante", "n": 1, "indice_medio": 95.0}])
        return pd.DataFrame([{"municipio": "Cidade A", "cluster": "Diamante", "indice": 95.0, "ranking_final": 1}])

    def register_table(self, name: str, df: pd.DataFrame) -> None:
        self._tables.add(name)

    def count_municipios(self) -> int:
        return 1


class _Store:
    def save_report(self, df, nome_arquivo):
        return None

    def load_report(self, nome_arquivo):
        return None


class _AI:
    def search_relevant(self, pergunta: str, n_results: int = 5) -> str:
        return "Cidade A"

    def complete(self, system_prompt: str, historico: list[dict], pergunta: str, contexto: str):
        return "Resposta", 120


def _df_mun():
    return pd.DataFrame(
        {
            "ranking_final": [1, 2],
            "municipio": ["Cidade A", "Cidade B"],
            "cluster": ["Diamante", "Alavanca"],
            "indice_final": [95.0, 85.0],
            "PD_qt": [70.0, 60.0],
            "pop_censo2022": [100000, 80000],
        }
    )


@pytest.mark.load
def test_load_chat_and_allocation_profiles():
    repo = _Repo()
    store = _Store()
    ai = _AI()

    n = 200
    t0 = time.perf_counter()
    for _ in range(n):
        executar_alocacao(repo, store, _df_mun(), 100000, "deputado_federal", 2, 0.5)
    alloc_elapsed = time.perf_counter() - t0

    t1 = time.perf_counter()
    for _ in range(n):
        responder_pergunta(repo, ai, "Compare clusters por indice medio", [])
    chat_elapsed = time.perf_counter() - t1

    assert alloc_elapsed < 8.0
    assert chat_elapsed < 3.0
