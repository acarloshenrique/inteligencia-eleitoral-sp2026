import streamlit as st
from pydantic import ValidationError

from application.use_cases_layered import executar_alocacao, responder_pergunta
from config.settings import get_settings
from domain.contracts import DataContractError
from infrastructure.env import bootstrap_ambiente, build_paths
from infrastructure.repositories import ChromaGroqAIService, DuckDBAnalyticsRepository, ParquetReportStore
from infrastructure.storage import carrega_dados, carrega_db
from presentation.ui import (
    render_sidebar,
    render_tab_alocacao,
    render_tab_chat,
    render_tab_ranking,
    render_tab_secoes,
)


def run_app():
    st.set_page_config(
        page_title="Inteligência Eleitoral SP 2026",
        page_icon="🗳",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    try:
        get_settings()
    except ValidationError as e:
        st.error(f"Falha na validação de configuração: {e}")
        st.stop()

    paths = build_paths()
    bootstrap = bootstrap_ambiente(paths)
    if bootstrap["erros"]:
        st.error("Falha de bootstrap de ambiente:\n- " + "\n- ".join(bootstrap["erros"]))
        st.stop()

    df_mun = carrega_dados(paths)
    db = carrega_db(paths, df_mun)
    repo = DuckDBAnalyticsRepository(db)
    report_store = ParquetReportStore(paths)
    ai_service = ChromaGroqAIService(paths.chromadb_path, app_paths=paths)

    budget, cargo, n_mun, split_d, gerar = render_sidebar(bootstrap, paths.chromadb_path, repo)
    if gerar:
        try:
            with st.spinner("Calculando..."):
                df_aloc = executar_alocacao(repo, report_store, df_mun, budget, cargo, n_mun, split_d)
                st.session_state["aloc"] = df_aloc
                st.session_state["budget"] = budget
            total = df_aloc["budget"].sum() if "budget" in df_aloc.columns else 0
            st.success(f"✓ R$ {total:,.0f} alocados em {len(df_aloc)} municípios")

        except DataContractError as e:
            st.error(f"Contrato de dados violado: {e}")

    t1, t2, t3, t4 = st.tabs(["💬 Analista", "📊 Alocação", "📍 Seções de Campo", "🏆 Ranking"])

    with t1:
        render_tab_chat(lambda pergunta, hist: responder_pergunta(repo, ai_service, pergunta, hist))
    with t2:
        render_tab_alocacao(paths, report_store)
    with t3:
        render_tab_secoes(repo)
    with t4:
        render_tab_ranking(df_mun)
