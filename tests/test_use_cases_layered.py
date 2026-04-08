import importlib.util
from pathlib import Path
import sys

import pandas as pd


MODULE_PATH = Path(__file__).resolve().parents[1] / "application" / "use_cases_layered.py"
sys.path.insert(0, str(MODULE_PATH.parents[1]))
SPEC = importlib.util.spec_from_file_location("use_cases_layered_module", MODULE_PATH)
use_cases_module = importlib.util.module_from_spec(SPEC)
assert SPEC is not None and SPEC.loader is not None
SPEC.loader.exec_module(use_cases_module)
executar_alocacao = use_cases_module.executar_alocacao
responder_pergunta = use_cases_module.responder_pergunta


class FakeRepo:
    def __init__(self):
        self.tables = {"municipios"}
        self.registered = {}
        self.last_sql = None
        self.responses = {
            "cluster": pd.DataFrame([{"cluster": "Diamante", "n": 1, "indice_medio": 95.0}]),
            "default": pd.DataFrame([{"municipio": "A", "cluster": "Diamante", "indice": 95.0, "ranking_final": 1}]),
        }

    def table_exists(self, table: str) -> bool:
        return table in self.tables

    def query_df(self, sql: str, params=None) -> pd.DataFrame:
        self.last_sql = sql
        if "GROUP BY cluster" in sql:
            return self.responses["cluster"]
        return self.responses["default"]

    def register_table(self, name: str, df: pd.DataFrame) -> None:
        self.tables.add(name)
        self.registered[name] = df.copy()

    def count_municipios(self) -> int:
        return 1


class FakeReportStore:
    def __init__(self):
        self.saved = {}

    def save_report(self, df: pd.DataFrame, nome_arquivo: str) -> None:
        self.saved[nome_arquivo] = df.copy()

    def load_report(self, nome_arquivo: str):
        return self.saved.get(nome_arquivo)


class FakeAI:
    def __init__(self):
        self.complete_calls = []

    def search_relevant(self, pergunta: str, n_results: int = 5) -> str:
        return "Cidade A, Cidade B"

    def complete(self, system_prompt: str, historico: list[dict], pergunta: str, contexto: str) -> tuple[str, int]:
        self.complete_calls.append(
            {"system_prompt": system_prompt, "historico": historico, "pergunta": pergunta, "contexto": contexto}
        )
        return "Resposta sintética", 42


def _df_mun_base():
    return pd.DataFrame(
        {
            "ranking_final": [1, 2],
            "municipio": ["Cidade A", "Cidade B"],
            "cluster": ["Diamante", "Alavanca"],
            "indice_final": [95.0, 88.0],
            "PD_qt": [70.0, 60.0],
            "pop_censo2022": [100_000, 80_000],
        }
    )


def test_executar_alocacao_persiste_e_registra_tabela():
    repo = FakeRepo()
    store = FakeReportStore()
    df_r = executar_alocacao(
        repo=repo,
        report_store=store,
        df_mun=_df_mun_base(),
        budget=100_000,
        cargo="deputado_federal",
        n=2,
        split_d=0.5,
    )

    assert not df_r.empty
    assert "alocacao" in repo.registered
    assert "ultima_alocacao.parquet" in store.saved


def test_responder_pergunta_usa_interfaces_sem_backend_real():
    repo = FakeRepo()
    ai = FakeAI()
    texto, semantico, tokens = responder_pergunta(
        repo=repo,
        ai_service=ai,
        pergunta="Compare clusters por índice médio",
        historico=[],
    )

    assert texto == "Resposta sintética"
    assert semantico == "Cidade A, Cidade B"
    assert tokens == 42
    assert "GROUP BY cluster" in repo.last_sql
    assert len(ai.complete_calls) == 1
