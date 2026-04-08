import pandas as pd
import pytest

from domain.ranking import calcular_ranking


def _df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ranking_final": [3, 1, 2],
            "municipio": ["Cidade C", "Cidade A", "Cidade B"],
            "cluster": ["Descarte", "Diamante", "Alavanca"],
            "indice_final": [50.0, 95.0, 80.0],
        }
    )


@pytest.mark.unit
def test_ranking_filters_and_orders():
    result = calcular_ranking(_df(), clusters=["Diamante", "Alavanca"], busca="cidade")
    assert result["municipio"].tolist() == ["Cidade A", "Cidade B"]
