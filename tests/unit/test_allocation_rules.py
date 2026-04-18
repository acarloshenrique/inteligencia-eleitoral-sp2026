import pandas as pd
import pytest

from domain.scoring import CARGOS_EST, PESOS_CLUSTER, TETOS, calcular_alocacao


def _df_base() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ranking_final": [1, 2, 3],
            "municipio": ["Cidade A", "Cidade B", "Cidade C"],
            "cluster": ["Diamante", "Alavanca", "Descarte"],
            "indice_final": [95.0, 85.0, 50.0],
            "PD_qt": [70.0, 45.0, 20.0],
            "pop_censo2022": [100000, 80000, 50000],
        }
    )


@pytest.mark.unit
def test_allocation_excludes_discarte_cluster():
    result = calcular_alocacao(
        df_mun=_df_base(),
        budget=100000,
        cargo="deputado_federal",
        n=3,
        split_d=0.5,
        pesos_cluster=PESOS_CLUSTER,
        tetos=TETOS,
        cargos_est=CARGOS_EST,
    )
    assert "Cidade C" not in result["municipio"].tolist()


@pytest.mark.unit
def test_allocation_respects_non_state_cap():
    df = _df_base()
    result = calcular_alocacao(
        df_mun=df,
        budget=10_000_000,
        cargo="prefeito_pequeno",
        n=2,
        split_d=0.5,
        pesos_cluster=PESOS_CLUSTER,
        tetos=TETOS,
        cargos_est=CARGOS_EST,
    )
    assert float(result["budget"].max()) <= TETOS["prefeito_pequeno"]
