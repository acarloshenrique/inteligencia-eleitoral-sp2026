import streamlit as st
from pydantic import ValidationError

from application.use_cases_layered import executar_alocacao, responder_pergunta
from domain.contracts import DataContractError
from domain.errors import AppError
from infrastructure.app_runtime import build_app_runtime, initialize_app_environment
from presentation.decision_ui import (
    render_budget_simulator,
    render_candidate_onboarding,
    render_data_catalog,
    render_electoral_base_map,
    render_evidence_and_explanations,
    render_municipal_strategy,
    render_recommendation_detail,
    render_territorial_ranking,
)
from presentation.ui import (
    apply_ui_theme,
    render_page_header,
    render_sidebar,
    render_tab_chat,
    render_tab_monitoramento,
)


def _configure_page() -> None:
    st.set_page_config(
        page_title="Inteligencia Eleitoral SP 2026",
        page_icon="IE",
        layout="wide",
        initial_sidebar_state="expanded",
    )


def _load_runtime():
    try:
        environment = initialize_app_environment()
    except ValidationError as e:
        st.error(f"Falha na validacao de configuracao: {e}")
        st.stop()

    if environment.bootstrap["erros"]:
        st.error("Falha de bootstrap de ambiente:\n- " + "\n- ".join(environment.bootstrap["erros"]))
        st.stop()
    return build_app_runtime(environment)


def _handle_allocation(runtime, budget, cargo, n_mun, split_d) -> None:
    try:
        with st.spinner("Calculando..."):
            df_aloc = executar_alocacao(
                runtime.repository,
                runtime.report_store,
                runtime.df_mun,
                budget,
                cargo,
                n_mun,
                split_d,
            )
            st.session_state["aloc"] = df_aloc
            st.session_state["budget"] = budget
        total = df_aloc["budget"].sum() if "budget" in df_aloc.columns else 0
        st.success(f"R$ {total:,.0f} alocados em {len(df_aloc)} municipios")
    except DataContractError as e:
        st.error(f"Contrato de dados violado: {e}")
    except AppError as e:
        st.error(e.to_operational_message())


def _render_tabs(runtime) -> None:
    tabs = st.tabs(
        [
            "Onboarding",
            "Mapa de base eleitoral",
            "Ranking territorial",
            "Analise municipal",
            "Simulador de orcamento",
            "Detalhe da recomendacao",
            "Evidencias e justificativas",
            "Catalogo de bases",
            "Monitoramento",
            "Assistente",
        ]
    )
    with tabs[0]:
        render_candidate_onboarding(runtime.paths)
    with tabs[1]:
        render_electoral_base_map(runtime.paths)
    with tabs[2]:
        render_territorial_ranking(runtime.paths)
    with tabs[3]:
        render_municipal_strategy(runtime.paths)
    with tabs[4]:
        render_budget_simulator(runtime.paths)
    with tabs[5]:
        render_recommendation_detail(runtime.paths)
    with tabs[6]:
        render_evidence_and_explanations(runtime.paths)
    with tabs[7]:
        render_data_catalog(runtime.paths)
    with tabs[8]:
        render_tab_monitoramento(runtime.repository, runtime.df_mun, runtime.paths)
    with tabs[9]:
        render_tab_chat(
            lambda pergunta, historico: responder_pergunta(runtime.repository, runtime.ai_service, pergunta, historico)
        )


def run_app() -> None:
    _configure_page()
    apply_ui_theme()
    runtime = _load_runtime()
    budget, cargo, n_mun, split_d, gerar = render_sidebar(
        runtime.bootstrap,
        runtime.paths.chromadb_path,
        runtime.repository,
    )
    render_page_header(runtime.bootstrap, runtime.df_mun)
    if gerar:
        _handle_allocation(runtime, budget, cargo, n_mun, split_d)
    _render_tabs(runtime)
