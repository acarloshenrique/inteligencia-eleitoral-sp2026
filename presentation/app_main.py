import streamlit as st

from application.use_cases import executar_alocacao, responder_pergunta
from infrastructure.env import bootstrap_ambiente, build_paths
from infrastructure.storage import carrega_dados, carrega_db, tem_tabela
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

    paths = build_paths()
    bootstrap = bootstrap_ambiente(paths)
    if bootstrap["erros"]:
        st.error("Falha de bootstrap de ambiente:\n- " + "\n- ".join(bootstrap["erros"]))
        st.stop()

    df_mun = carrega_dados(paths)
    db = carrega_db(paths, df_mun)

    budget, cargo, n_mun, split_d, gerar = render_sidebar(bootstrap, paths.chromadb_path, db)
    if gerar:
        with st.spinner("Calculando..."):
            df_aloc = executar_alocacao(paths, db, df_mun, budget, cargo, n_mun, split_d)
            st.session_state["aloc"] = df_aloc
            st.session_state["budget"] = budget
        total = df_aloc["budget"].sum() if "budget" in df_aloc.columns else 0
        st.success(f"✓ R$ {total:,.0f} alocados em {len(df_aloc)} municípios")

    t1, t2, t3, t4 = st.tabs(["💬 Analista", "📊 Alocação", "📍 Seções de Campo", "🏆 Ranking"])

    with t1:
        render_tab_chat(lambda pergunta, hist: responder_pergunta(paths, db, pergunta, hist))
    with t2:
        render_tab_alocacao(paths)
    with t3:
        render_tab_secoes(db, tem_tabela)
    with t4:
        render_tab_ranking(df_mun)
