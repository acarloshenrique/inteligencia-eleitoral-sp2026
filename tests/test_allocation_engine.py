import pandas as pd

from infrastructure.allocation_engine import build_modular_allocation_scores, recommend_allocation, simulate_budget


def test_allocation_engine_scores_simulates_and_recommends_budget():
    mart_municipio = pd.DataFrame(
        [
            {"municipio_id_ibge7": "3550308", "indice_medio_3ciclos": 90.0, "data_quality_score": 0.95},
            {"municipio_id_ibge7": "3509502", "indice_medio_3ciclos": 70.0, "data_quality_score": 0.90},
        ]
    )
    mart_potencial = pd.DataFrame(
        [
            {"municipio_id_ibge7": "3550308", "potencial_eleitoral_ajustado_social": 150.0},
            {"municipio_id_ibge7": "3509502", "potencial_eleitoral_ajustado_social": 80.0},
        ]
    )
    mart_territorial = pd.DataFrame(
        [
            {"municipio_id_ibge7": "3550308", "score_priorizacao_territorial_sp": 120.0},
            {"municipio_id_ibge7": "3509502", "score_priorizacao_territorial_sp": 60.0},
        ]
    )
    mart_custo = pd.DataFrame(
        [
            {"municipio_id_ibge7": "3550308", "custo_mobilizacao_relativo": 0.2},
            {"municipio_id_ibge7": "3509502", "custo_mobilizacao_relativo": 0.8},
        ]
    )
    mart_sensibilidade = pd.DataFrame(
        [
            {"municipio_id_ibge7": "3550308", "sensibilidade_investimento_publico": 0.02},
            {"municipio_id_ibge7": "3509502", "sensibilidade_investimento_publico": 0.01},
        ]
    )
    mart_midia = pd.DataFrame(
        [
            {"municipio_id_ibge7": "3550308", "plataforma": "meta_ads", "gasto": 1000, "ctr": 0.05, "cpc": 2.0, "conversao": 30, "performance": 0.2},
            {"municipio_id_ibge7": "3509502", "plataforma": "google_ads", "gasto": 3000, "ctr": 0.01, "cpc": 12.0, "conversao": 5, "performance": 0.02},
        ]
    )
    features = pd.DataFrame(
        [
            {"municipio_id_ibge7": "3550308", "volatilidade_historica": 0.1, "crescimento_eleitoral": 3.0},
            {"municipio_id_ibge7": "3509502", "volatilidade_historica": 0.9, "crescimento_eleitoral": -1.0},
        ]
    )

    scores = build_modular_allocation_scores(
        mart_municipio=mart_municipio,
        mart_potencial=mart_potencial,
        mart_territorial=mart_territorial,
        mart_custo=mart_custo,
        mart_sensibilidade=mart_sensibilidade,
        mart_midia=mart_midia,
        features=features,
    )
    assert {"score_potencial_eleitoral", "score_oportunidade", "score_eficiencia_midia", "score_custo", "score_risco"}.issubset(scores.columns)
    assert scores.iloc[0]["municipio_id_ibge7"] == "3550308"
    assert scores["score_alocacao"].between(0, 100).all()

    simulation = simulate_budget(scores, total_budget=50_000)
    assert round(float(simulation["verba_simulada"].sum()), 2) == 50_000.00
    assert "impacto_incremental_estimado" in simulation.columns

    messages = pd.DataFrame(
        [
            {"municipio_id_ibge7": "3550308", "ranking_mensagem_cidade": 1, "plataforma": "meta_ads", "mensagem": "Mais emprego", "tema": "emprego_e_renda", "narrativa": "prosperidade", "publico_alvo": "amplo"},
            {"municipio_id_ibge7": "3509502", "ranking_mensagem_cidade": 1, "plataforma": "google_ads", "mensagem": "Mais saude", "tema": "saude", "narrativa": "protecao", "publico_alvo": "familias_e_mulheres"},
        ]
    )
    recommendations = recommend_allocation(scores=scores, budget_simulation=simulation, mart_message=messages, total_budget=50_000)
    assert {"ranking", "verba_sugerida", "canal_ideal", "mensagem_ideal", "justificativa"}.issubset(recommendations.columns)
    assert recommendations.iloc[0]["canal_ideal"] == "meta_ads"
    assert "potencial" in recommendations.iloc[0]["justificativa"]