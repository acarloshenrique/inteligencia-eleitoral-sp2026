import pandas as pd

from domain.allocation import aloca as legacy_aloca
from domain.allocation import calcular_alocacao as legacy_calcular_alocacao
from domain.constants import PESOS_CLUSTER as LEGACY_PESOS_CLUSTER
from domain.constants import TETOS as LEGACY_TETOS
from domain.scoring import PESOS_CLUSTER, TETOS, aloca, calcular_alocacao


def _df():
    return pd.DataFrame(
        [
            {
                "municipio": "Cidade A",
                "ranking_final": 1,
                "indice_final": 90.0,
                "cluster": "Diamante",
                "PD_qt": 50,
                "pop_censo2022": 10000,
            },
            {
                "municipio": "Cidade B",
                "ranking_final": 2,
                "indice_final": 70.0,
                "cluster": "Descarte",
                "PD_qt": 20,
                "pop_censo2022": 50000,
            },
        ]
    )


def test_scoring_exports_legacy_allocation_contracts():
    assert LEGACY_PESOS_CLUSTER is PESOS_CLUSTER
    assert LEGACY_TETOS is TETOS
    assert legacy_aloca is aloca
    assert legacy_calcular_alocacao is calcular_alocacao


def test_aloca_and_calcular_alocacao_are_equivalent():
    kwargs = {
        "df_mun": _df(),
        "budget": 100_000,
        "cargo": "prefeito_pequeno",
        "n": 2,
        "split_d": 0.6,
    }

    direct = aloca(**kwargs)
    compat = calcular_alocacao(**kwargs)

    assert direct.to_dict("records") == compat.to_dict("records")
    assert direct["municipio"].tolist() == ["Cidade A"]
