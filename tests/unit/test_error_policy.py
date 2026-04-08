import pandas as pd
import pytest

from application.use_cases_layered import executar_alocacao, responder_pergunta
from domain.errors import AppOperationalError


class _RepoFailRegister:
    def table_exists(self, table: str) -> bool:
        return table == "municipios"

    def query_df(self, sql: str, params=None):
        return pd.DataFrame([{"municipio": "Cidade A", "cluster": "Diamante", "indice": 95.0, "ranking_final": 1}])

    def register_table(self, name: str, df: pd.DataFrame) -> None:
        raise RuntimeError("db down")

    def count_municipios(self) -> int:
        return 1


class _Store:
    def save_report(self, df, nome_arquivo):
        return None

    def load_report(self, nome_arquivo):
        return None


class _AIError:
    def search_relevant(self, pergunta: str, n_results: int = 5):
        return "Cidade A"

    def complete(self, system_prompt: str, historico: list[dict], pergunta: str, contexto: str):
        raise RuntimeError("llm down")


def _df():
    return pd.DataFrame(
        {
            "ranking_final": [1],
            "municipio": ["Cidade A"],
            "cluster": ["Diamante"],
            "indice_final": [95.0],
            "PD_qt": [70.0],
            "pop_censo2022": [100000],
        }
    )


@pytest.mark.unit
def test_error_policy_allocation_register_failure_raises_internal_code():
    with pytest.raises(AppOperationalError) as exc:
        executar_alocacao(_RepoFailRegister(), _Store(), _df(), 100000, "deputado_federal", 1, 0.5)
    assert exc.value.code == "E-ALOC-002"


@pytest.mark.unit
def test_error_policy_chat_llm_failure_raises_internal_code():
    repo = _RepoFailRegister()
    with pytest.raises(AppOperationalError) as exc:
        responder_pergunta(repo, _AIError(), "Pergunta", [])
    assert exc.value.code == "E-CHAT-002"
