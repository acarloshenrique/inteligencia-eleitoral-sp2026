import pandas as pd

from infrastructure.product_reports import (
    build_executive_pdf_bytes,
    build_explainability_frame,
    build_operational_workbook_bytes,
    build_product_exports,
    build_ranking_snapshot,
)


def _scores():
    return pd.DataFrame(
        [
            {
                "ranking": 1,
                "municipio_id_ibge7": "3550308",
                "municipio": "Sao Paulo",
                "score_alocacao": 91.2,
                "score_potencial_eleitoral": 0.95,
                "score_oportunidade": 0.88,
                "score_eficiencia_midia": 0.76,
                "score_custo": 0.55,
                "score_risco": 0.12,
                "data_quality_score": 0.93,
                "join_confidence": 0.91,
                "coverage": 0.89,
            },
            {
                "ranking": 2,
                "municipio_id_ibge7": "3509502",
                "municipio": "Campinas",
                "score_alocacao": 80.1,
                "score_potencial_eleitoral": 0.78,
                "score_oportunidade": 0.66,
                "score_eficiencia_midia": 0.70,
                "score_custo": 0.80,
                "score_risco": 0.30,
                "data_quality_score": 0.88,
                "join_confidence": 0.86,
                "coverage": 0.90,
            },
        ]
    )


def _recommendations():
    return pd.DataFrame(
        [
            {
                "ranking": 1,
                "municipio_id_ibge7": "3550308",
                "verba_sugerida": 30000,
                "canal_ideal": "meta_ads",
                "mensagem_ideal": "Emprego e renda",
            },
            {
                "ranking": 2,
                "municipio_id_ibge7": "3509502",
                "verba_sugerida": 20000,
                "canal_ideal": "google_ads",
                "mensagem_ideal": "Saude perto",
            },
        ]
    )


def test_build_explainability_frame_explains_drivers_and_confidence():
    explain = build_explainability_frame(_scores(), _recommendations())
    assert {"principais_variaveis", "por_que_municipio_esta_alto", "confiabilidade"}.issubset(explain.columns)
    assert explain.iloc[0]["municipio"] == "Sao Paulo"
    assert "potencial eleitoral" in explain.iloc[0]["principais_variaveis"]
    assert explain.iloc[0]["confiabilidade"] > 0.9


def test_product_reports_generate_pdf_workbook_and_ranking():
    scores = _scores()
    recommendations = _recommendations()
    pdf = build_executive_pdf_bytes(scores=scores, recommendations=recommendations)
    workbook = build_operational_workbook_bytes({"ranking": build_ranking_snapshot(scores, recommendations)})
    exports = build_product_exports(scores=scores, recommendations=recommendations)

    assert pdf.startswith(b"%PDF")
    assert workbook.startswith(b"PK")
    assert exports["xlsx_bytes"].startswith(b"PK")
    assert b"municipio_id_ibge7" in exports["ranking_csv_bytes"]
    assert not exports["ranking"].empty
