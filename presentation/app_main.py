import streamlit as st
from pydantic import ValidationError

from application.use_cases_layered import executar_alocacao
from domain.contracts import DataContractError
from domain.errors import AppError
from infrastructure.app_runtime import build_app_runtime, initialize_app_environment
from presentation.ui import (
    render_sidebar,
    render_tab_mensagem,
    render_tab_midia_performance,
    render_tab_monitoramento,
    render_tab_prioridade_territorial,
    render_tab_simulacao,
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
    t1, t2, t3, t4, t5 = st.tabs(
        [
            "Prioridade territorial",
            "Midia e performance",
            "Mensagem",
            "Simulacao",
            "Monitoramento",
        ]
    )
    with t1:
        render_tab_prioridade_territorial(runtime.repository)
    with t2:
        render_tab_midia_performance(runtime.repository)
    with t3:
        render_tab_mensagem(runtime.repository)
    with t4:
        render_tab_simulacao(runtime.repository)
    with t5:
        render_tab_monitoramento(runtime.repository, runtime.df_mun, runtime.paths)


def run_app() -> None:
    _configure_page()
    runtime = _load_runtime()
    budget, cargo, n_mun, split_d, gerar = render_sidebar(
        runtime.bootstrap,
        runtime.paths.chromadb_path,
        runtime.repository,
    )
    if gerar:
        _handle_allocation(runtime, budget, cargo, n_mun, split_d)
    _render_tabs(runtime)
