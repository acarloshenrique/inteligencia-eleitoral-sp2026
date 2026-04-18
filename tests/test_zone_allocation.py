import pandas as pd

from domain.scoring_zona import ZoneAllocationConfig, build_zone_features, score_zone_allocation


def _municipios():
    return pd.DataFrame(
        [
            {"ranking_final": 1, "municipio": "Cidade A", "cluster": "Diamante", "indice_final": 90.0},
            {"ranking_final": 2, "municipio": "Cidade B", "cluster": "Alavanca", "indice_final": 75.0},
        ]
    )


def _zonas():
    return pd.DataFrame(
        [
            {
                "territorio_id": "1-ZE10",
                "municipio": "Cidade A",
                "municipio_id_ibge7": "0000001",
                "zona_eleitoral": 10,
                "eleitores_aptos": 60000,
                "abstencao_pct": 0.22,
                "competitividade": 0.80,
                "volatilidade_historica": 0.20,
                "data_quality_score": 0.95,
                "join_confidence": 0.96,
            },
            {
                "territorio_id": "1-ZE11",
                "municipio": "Cidade A",
                "municipio_id_ibge7": "0000001",
                "zona_eleitoral": 11,
                "eleitores_aptos": 40000,
                "abstencao_pct": 0.18,
                "competitividade": 0.65,
                "volatilidade_historica": 0.25,
                "data_quality_score": 0.92,
                "join_confidence": 0.94,
            },
            {
                "territorio_id": "2-ZE20",
                "municipio": "Cidade B",
                "municipio_id_ibge7": "0000002",
                "zona_eleitoral": 20,
                "eleitores_aptos": 50000,
                "abstencao_pct": 0.25,
                "competitividade": 0.72,
                "volatilidade_historica": 0.30,
                "data_quality_score": 0.60,
                "join_confidence": 0.91,
            },
        ]
    )


def test_zone_features_are_computed():
    features = build_zone_features(_zonas(), _municipios())

    assert "peso_eleitoral_no_municipio" in features.columns
    assert features.loc[features["zona_eleitoral"] == 10, "peso_eleitoral_no_municipio"].iloc[0] == 0.6
    assert features["join_confidence"].min() > 0


def test_zone_allocation_blocks_low_quality_rows_and_ranks():
    result = score_zone_allocation(_zonas(), _municipios(), budget_total=100000)

    assert result["territorio_id"].tolist() == ["1-ZE10", "1-ZE11"]
    assert result["ranking_zona"].tolist() == [1, 2]
    assert result["verba_sugerida"].sum() >= 100000
    assert {"canal_ideal", "mensagem_ideal", "justificativa"}.issubset(result.columns)


def test_zone_allocation_returns_empty_when_quality_gate_blocks_all():
    cfg = ZoneAllocationConfig(min_data_quality_score=0.99, min_join_confidence=0.99)
    result = score_zone_allocation(_zonas(), _municipios(), budget_total=100000, config=cfg)

    assert result.empty
