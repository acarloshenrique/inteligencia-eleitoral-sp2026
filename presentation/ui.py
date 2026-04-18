import logging
from datetime import datetime

import pandas as pd
import streamlit as st

from config.settings import get_settings
from domain.scoring import TETOS
from domain.scoring_zona import score_zone_allocation
from infrastructure.metadata_db import MetadataDb
from infrastructure.observability import AlertThresholds, build_observability_snapshot

logger = logging.getLogger(__name__)


def apply_ui_theme() -> None:
    st.markdown(
        """
        <style>
          .block-container {padding-top: 1.4rem; padding-bottom: 2rem; max-width: 1320px;}
          section[data-testid="stSidebar"] {border-right: 1px solid #d7dde2;}
          div[data-testid="stMetric"] {background: #ffffff; border: 1px solid #d7dde2; border-radius: 6px; padding: 0.85rem;}
          div.stButton > button, div[data-testid="stDownloadButton"] button {border-radius: 6px; font-weight: 650;}
          .ie-hero {border: 1px solid #d7dde2; border-radius: 6px; padding: 1rem 1.1rem; background: #f7faf9; margin-bottom: 1rem;}
          .ie-hero h1 {font-size: 1.65rem; margin: 0 0 .35rem 0; letter-spacing: 0;}
          .ie-hero p {margin: 0; color: #344054; font-size: .98rem;}
          .ie-note {border-left: 4px solid #0f766e; padding: .6rem .8rem; background: #eef8f6; margin: .5rem 0 1rem 0;}
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_page_header(bootstrap: dict, df_mun: pd.DataFrame) -> None:
    source_note = "Base gold carregada"
    if any("Sem base gold" in str(aviso) for aviso in bootstrap.get("avisos", [])):
        source_note = "Modo teste: amostra demo ativa ate publicar o dataset gold"
    st.markdown(
        f"""
        <div class="ie-hero">
          <h1>Inteligencia Eleitoral SP 2026</h1>
          <p>Priorize territorios, simule verba, compare midia e gere recomendacoes operacionais com rastreabilidade.</p>
        </div>
        <div class="ie-note">{source_note}</div>
        """,
        unsafe_allow_html=True,
    )
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Municipios", f"{len(df_mun):,.0f}")
    c2.metric("Clusters", f"{df_mun['cluster'].nunique():,.0f}" if "cluster" in df_mun.columns else "0")
    c3.metric(
        "Indice medio",
        f"{df_mun['indice_final'].mean():.1f}" if "indice_final" in df_mun.columns and not df_mun.empty else "n/d",
    )
    c4.metric("Ambiente", str(bootstrap.get("app_env", "dev")))


def render_sidebar(bootstrap, chromadb_path, repo):
    settings = get_settings()
    with st.sidebar:
        st.markdown("## 🗳 Inteligência Eleitoral SP")
        st.caption(f"644 municípios · {datetime.now().strftime('%d/%m/%Y')}")
        st.caption(f"Ambiente: {bootstrap['app_env']}")
        if bootstrap["avisos"]:
            with st.expander("Bootstrap de ambiente", expanded=False):
                for aviso in bootstrap["avisos"]:
                    st.warning(aviso)
        st.divider()

        st.markdown("### ⚙ Simulador de Alocação")
        budget = st.number_input("Budget total (R$)", 10_000, 2_500_000, 200_000, 10_000, format="%d")
        cargo = st.selectbox("Cargo", list(TETOS.keys()), index=0)
        n_mun = st.slider("Top N municípios", 5, 50, 20)
        split_d = st.slider("% digital", 20, 80, 45) / 100
        gerar = st.button("▶ Gerar Alocação", width="stretch", type="primary")

        st.divider()
        st.markdown("### 💬 Perguntas rápidas")
        for p in [
            "Quais os municípios Diamante?",
            "Como alocar R$ 500k?",
            "Quais seções são Alta prioridade?",
            "Mostre o mapa tático de campo",
            "Compare clusters por índice médio",
            "Perfil de Cássia dos Coqueiros",
        ]:
            if st.button(p, width="stretch", key=p):
                st.session_state["pq"] = p

        st.divider()
        st.markdown("### 📊 Índice Multicritério")
        st.markdown("Territorial **35%** · VS **25%** · ISE **20%** · PD **20%**")
        st.divider()
        groq_ico = "✅ configurado" if settings.groq_api_key else "⚠ simulado"
        chroma_ico = "✅ índice disponível" if chromadb_path.exists() else "⚠ sem índice"
        st.caption(f"LLM: {groq_ico}")
        st.caption(f"ChromaDB: {chroma_ico}")
        st.caption(f"DuckDB: ✅ {repo.count_municipios()} mun.")

    return budget, cargo, n_mun, split_d, gerar


def render_tab_chat(responder_fn):
    if "msgs" not in st.session_state:
        st.session_state.msgs = [
            {
                "role": "assistant",
                "content": "Olá! Sou seu analista de inteligência eleitoral para SP 2026. Tenho acesso ao ranking de 644 municípios, alocação de budget, scores por seção eleitoral e estratégia de mídia. Como posso ajudar?",
            }
        ]
        st.session_state.hist = []

    for m in st.session_state.msgs:
        with st.chat_message(m["role"]):
            st.write(m["content"])

    prompt = st.session_state.pop("pq", None) or st.chat_input(
        "Pergunte sobre municípios, clusters, seções ou estratégia..."
    )

    if prompt:
        st.session_state.msgs.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.write(prompt)
        with st.chat_message("assistant"):
            with st.spinner("Analisando dados eleitorais..."):
                resp, muns, tokens = responder_fn(prompt, st.session_state.hist)
            st.write(resp)
            with st.expander("🔍 Contexto", expanded=False):
                st.caption(f"Municípios: {muns}")
                st.caption(f"Tokens: {tokens}")
        st.session_state.hist += [{"role": "user", "content": prompt}, {"role": "assistant", "content": resp}]
        st.session_state.msgs.append({"role": "assistant", "content": resp})

    if st.button("🗑 Limpar conversa", key="clear"):
        st.session_state.msgs = [{"role": "assistant", "content": "Conversa reiniciada. Como posso ajudar?"}]
        st.session_state.hist = []
        st.rerun()


def render_tab_alocacao(paths, report_store):
    df_show = st.session_state.get("aloc")
    if df_show is None:
        df_show = report_store.load_report("ultima_alocacao.parquet")

    if df_show is not None and not df_show.empty:
        bud_col = "budget" if "budget" in df_show.columns else "budget_total_mun"
        total = df_show[bud_col].sum()

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Alocado", f"R$ {total:,.0f}")
        c2.metric("Municípios", len(df_show))
        c3.metric("💎 Diamante", int((df_show["cluster"] == "Diamante").sum()))
        c4.metric("Média/município", f"R$ {total / len(df_show):,.0f}")
        st.divider()

        cols_disp = ["municipio", "cluster"]
        for c in [
            bud_col,
            "digital",
            "offline",
            "budget_digital",
            "budget_offline",
            "meta_fb_ig",
            "youtube",
            "tiktok",
            "whatsapp",
            "radio_local",
            "evento_presencial",
        ]:
            if c in df_show.columns and c not in cols_disp:
                cols_disp.append(c)

        st.dataframe(
            df_show[cols_disp].rename(
                columns={
                    bud_col: "Budget R$",
                    "budget_digital": "Digital",
                    "budget_offline": "Offline",
                    "digital": "Digital",
                    "offline": "Offline",
                    "meta_fb_ig": "Meta FB+IG",
                    "youtube": "YouTube",
                    "tiktok": "TikTok",
                    "whatsapp": "WhatsApp",
                    "radio_local": "Rádio",
                    "evento_presencial": "Evento",
                }
            ),
            width="stretch",
            hide_index=True,
            column_config={
                c: st.column_config.NumberColumn(format="R$ %.0f")
                for c in [
                    "Budget R$",
                    "Digital",
                    "Offline",
                    "Meta FB+IG",
                    "YouTube",
                    "TikTok",
                    "WhatsApp",
                    "Rádio",
                    "Evento",
                ]
            },
        )

        import io

        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as w:
            df_show.to_excel(w, index=False)
        st.download_button(
            "⬇ Baixar Excel",
            data=buf.getvalue(),
            file_name=f"alocacao_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    elif df_show is not None and df_show.empty:
        st.warning("Sem municípios elegíveis para alocação com os dados atuais.")
    else:
        st.info("Use o simulador no menu lateral para gerar uma alocação.")


def render_tab_secoes(repo):
    if repo.table_exists("secoes") and repo.table_exists("mapa_tatico"):
        col_a, col_b = st.columns([1, 1])

        with col_a:
            st.markdown("#### Mapa Tático por Município")
            df_mt = repo.query_df(
                """
                SELECT NM_MUNICIPIO as município, cluster,
                       total_secoes as seções,
                       secoes_alta as alta,
                       secoes_media as média,
                       ROUND(budget_total_mun,0) as budget,
                       ROUND(custo_por_secao_alta,0) as R_secao_alta,
                       ROUND(eleitores_por_real,2) as eleit_R
                FROM mapa_tatico ORDER BY ranking_final
                """
            ).df()
            st.dataframe(
                df_mt,
                width="stretch",
                hide_index=True,
                column_config={
                    "budget": st.column_config.NumberColumn(format="R$ %.0f"),
                    "R_secao_alta": st.column_config.NumberColumn(format="R$ %.0f"),
                },
            )

        with col_b:
            st.markdown("#### Top Seções Alta Prioridade")
            mun_opts = ["Todos"] + sorted(
                repo.query_df("SELECT DISTINCT NM_MUNICIPIO FROM secoes ORDER BY 1")["NM_MUNICIPIO"].tolist()
            )
            filtro = st.selectbox("Município", mun_opts, key="filtro_secao")
            if filtro == "Todos":
                sql_s = "SELECT NM_MUNICIPIO as município, NR_ZONA as zona, NR_SECAO as seção, eleitores_aptos as votos, ROUND(engajamento*100,1) as engaj_pct, ROUND(score_secao,1) as score, prioridade_secao as prioridade FROM secoes WHERE prioridade_secao='Alta' ORDER BY score_secao DESC LIMIT 30"
                st.dataframe(repo.query_df(sql_s), width="stretch", hide_index=True)
            else:
                sql_s = "SELECT NM_MUNICIPIO as município, NR_ZONA as zona, NR_SECAO as seção, eleitores_aptos as votos, ROUND(engajamento*100,1) as engaj_pct, ROUND(score_secao,1) as score, prioridade_secao as prioridade FROM secoes WHERE NM_MUNICIPIO=? ORDER BY score_secao DESC"
                st.dataframe(repo.query_df(sql_s, [filtro]), width="stretch", hide_index=True)

        st.divider()
        st.markdown("#### Distribuição de Prioridade por Município")
        df_dist = repo.query_df(
            """
            SELECT NM_MUNICIPIO,
                   SUM(CASE WHEN prioridade_secao='Alta'  THEN 1 ELSE 0 END) as Alta,
                   SUM(CASE WHEN prioridade_secao='Média' THEN 1 ELSE 0 END) as Média,
                   SUM(CASE WHEN prioridade_secao='Baixa' THEN 1 ELSE 0 END) as Baixa,
                   COUNT(*) as Total
            FROM secoes GROUP BY NM_MUNICIPIO ORDER BY Alta DESC
            """
        )
        st.dataframe(df_dist, width="stretch", hide_index=True)
    else:
        st.info("Rode o Sprint 2 (sprint2_execucao_v2.py) para carregar os dados de seções.")


def render_tab_mobilizacao(repo):
    if repo.table_exists("mart_custo_mobilizacao"):
        st.markdown("#### Custo de Mobilizacao")
        df = repo.query_df(
            """
            SELECT
                municipio_id_ibge7,
                ROUND(ranking_medio_3ciclos, 2) AS ranking_medio_3ciclos,
                ROUND(indice_medio_3ciclos, 2) AS indice_medio_3ciclos,
                ROUND(custo_mobilizacao_relativo, 4) AS custo_mobilizacao_relativo,
                ROUND(emprego_formal, 4) AS emprego_formal,
                ROUND(urbanizacao_pct, 4) AS urbanizacao_pct,
                ROUND(acesso_internet_pct, 4) AS acesso_internet_pct,
                ROUND(estrutura_urbana_indice, 4) AS estrutura_urbana_indice,
                ROUND(ruralidade_pct, 4) AS ruralidade_pct
            FROM mart_custo_mobilizacao
            ORDER BY custo_mobilizacao_relativo ASC, ranking_medio_3ciclos ASC
            LIMIT 50
            """
        )
        c1, c2, c3 = st.columns(3)
        c1.metric("Municipios", len(df))
        c2.metric("Menor custo", f"{df['custo_mobilizacao_relativo'].min():.4f}" if not df.empty else "0.0000")
        c3.metric("Maior custo", f"{df['custo_mobilizacao_relativo'].max():.4f}" if not df.empty else "0.0000")
        st.dataframe(
            df.rename(
                columns={
                    "municipio_id_ibge7": "Municipio IBGE",
                    "ranking_medio_3ciclos": "Ranking 3 ciclos",
                    "indice_medio_3ciclos": "Indice 3 ciclos",
                    "custo_mobilizacao_relativo": "Custo Mobilizacao",
                    "emprego_formal": "Emprego Formal",
                    "urbanizacao_pct": "Urbanizacao",
                    "acesso_internet_pct": "Internet",
                    "estrutura_urbana_indice": "Estrutura Urbana",
                    "ruralidade_pct": "Ruralidade",
                }
            ),
            width="stretch",
            hide_index=True,
            column_config={
                "Ranking 3 ciclos": st.column_config.NumberColumn(format="%.2f"),
                "Indice 3 ciclos": st.column_config.NumberColumn(format="%.2f"),
                "Custo Mobilizacao": st.column_config.NumberColumn(format="%.4f"),
                "Emprego Formal": st.column_config.NumberColumn(format="%.4f"),
                "Urbanizacao": st.column_config.NumberColumn(format="%.4f"),
                "Internet": st.column_config.NumberColumn(format="%.4f"),
                "Estrutura Urbana": st.column_config.NumberColumn(format="%.4f"),
                "Ruralidade": st.column_config.NumberColumn(format="%.4f"),
            },
        )
    else:
        st.info("Publique o dataset gold `mart_custo_mobilizacao` para visualizar logistica de campo.")


def render_tab_ranking(df_mun):
    cl_opts = st.multiselect(
        "Cluster", ["Diamante", "Alavanca", "Consolidação", "Descarte"], default=["Diamante", "Alavanca"]
    )
    busca = st.text_input("Buscar município", "")

    df_rank = df_mun[df_mun["cluster"].isin(cl_opts)].copy() if cl_opts else df_mun.copy()
    if busca:
        df_rank = df_rank[df_rank["municipio"].str.upper().str.contains(busca.upper())]
    df_rank = df_rank.sort_values("ranking_final")

    c1, c2, c3 = st.columns(3)
    c1.metric("Municípios filtrados", len(df_rank))
    c2.metric("Índice médio", f"{df_rank['indice_final'].mean():.1f}")
    c3.metric("Pop. total", f"{df_rank['pop_censo2022'].sum():,.0f}" if "pop_censo2022" in df_rank.columns else "–")

    st.dataframe(
        df_rank[
            [
                "ranking_final",
                "municipio",
                "cluster",
                "indice_final",
                "score_territorial_qt",
                "VS_qt",
                "ise_qt",
                "PD_qt",
                "perfil_economico",
            ]
        ].rename(
            columns={
                "ranking_final": "#",
                "municipio": "Município",
                "cluster": "Cluster",
                "indice_final": "Índice",
                "score_territorial_qt": "Territorial",
                "VS_qt": "VS",
                "ise_qt": "ISE",
                "PD_qt": "PD",
                "perfil_economico": "Perfil",
            }
        ),
        width="stretch",
        hide_index=True,
        column_config={
            c: st.column_config.NumberColumn(format="%.1f") for c in ["Índice", "Territorial", "VS", "ISE", "PD"]
        },
    )


# Product decision workflow (Bloco 5)
from infrastructure.product_reports import (
    build_explainability_frame,
    build_product_exports,
    build_ranking_snapshot,
)


def _repo_table(repo, table_name: str, limit: int | None = None) -> pd.DataFrame:
    if not repo.table_exists(table_name):
        return pd.DataFrame()
    sql = f"SELECT * FROM {table_name}"
    if limit is not None:
        sql += f" LIMIT {int(limit)}"
    try:
        return repo.query_df(sql)
    except Exception as exc:
        logger.warning("Falha ao ler tabela %s: %s", table_name, exc)
        return pd.DataFrame()


def _display_columns(df: pd.DataFrame, preferred: list[str]) -> list[str]:
    cols = [col for col in preferred if col in df.columns]
    return cols or list(df.columns[:12])


def _show_missing(table_name: str) -> None:
    st.info(f"Not found in repo: {table_name}. Rode o pipeline medallion para publicar este mart gold.")


def _municipios_for_demo(repo, limit: int = 12) -> pd.DataFrame:
    if not repo.table_exists("municipios"):
        return pd.DataFrame()
    return repo.query_df(
        f"""
        SELECT ranking_final, municipio, cluster, indice_final, score_territorial_qt, VS_qt, ise_qt, PD_qt, pop_censo2022
        FROM municipios
        ORDER BY ranking_final
        LIMIT {int(limit)}
        """
    )


def _demo_scores(repo) -> pd.DataFrame:
    base = _municipios_for_demo(repo)
    if base.empty:
        return pd.DataFrame()
    score = pd.to_numeric(base.get("indice_final"), errors="coerce").fillna(0.0)
    pd_qt = pd.to_numeric(base.get("PD_qt"), errors="coerce").fillna(50.0)
    pop = pd.to_numeric(base.get("pop_censo2022"), errors="coerce").fillna(0.0)
    return pd.DataFrame(
        {
            "ranking": base["ranking_final"].astype(int),
            "municipio_id_ibge7": base["ranking_final"].astype(str).str.zfill(7),
            "municipio": base["municipio"],
            "cluster": base["cluster"],
            "score_alocacao": score,
            "score_potencial_eleitoral": (score / 100).round(3),
            "score_oportunidade": ((100 - pd_qt) / 100).round(3),
            "score_eficiencia_midia": (pd_qt / 100).round(3),
            "score_custo": (1 - (pop.rank(pct=True) * 0.55)).round(3),
            "score_risco": (0.08 + base["ranking_final"].rank(pct=True) * 0.18).round(3),
            "roi_politico_estimado": (score * 12.5).round(1),
            "desperdicio_midia": ((100 - score) * 38).round(0),
        }
    )


def _demo_recommendations(scores: pd.DataFrame) -> pd.DataFrame:
    if scores.empty:
        return pd.DataFrame()
    rec = scores[["ranking", "municipio_id_ibge7", "municipio", "cluster", "score_alocacao"]].copy()
    rec["verba_sugerida"] = (pd.to_numeric(rec["score_alocacao"], errors="coerce") * 1200).round(0)
    rec["canal_ideal"] = rec["cluster"].map({"Diamante": "Meta Ads", "Alavanca": "Google Ads"}).fillna("WhatsApp")
    rec["mensagem_ideal"] = (
        rec["cluster"].map({"Diamante": "protecao social", "Alavanca": "emprego local"}).fillna("presenca territorial")
    )
    rec["justificativa"] = "Amostra demo derivada do ranking municipal para teste da UI no HF."
    return rec


def _demo_media(scores: pd.DataFrame) -> pd.DataFrame:
    if scores.empty:
        return pd.DataFrame()
    media = scores[["municipio_id_ibge7", "municipio", "cluster", "score_alocacao"]].copy()
    media["plataforma"] = media["cluster"].map({"Diamante": "Meta Ads", "Alavanca": "Google Ads"}).fillna("WhatsApp")
    media["gasto"] = (pd.to_numeric(media["score_alocacao"], errors="coerce") * 850).round(0)
    media["impressoes"] = (media["gasto"] * 72).round(0)
    media["cliques"] = (media["impressoes"] * 0.026).round(0)
    media["ctr"] = 0.026
    media["cpc"] = (media["gasto"] / media["cliques"].replace(0, 1)).round(2)
    media["conversao"] = (media["cliques"] * 0.12).round(0)
    media["performance"] = (pd.to_numeric(media["score_alocacao"], errors="coerce") / 100).round(3)
    return media


def _demo_messages(scores: pd.DataFrame) -> pd.DataFrame:
    if scores.empty:
        return pd.DataFrame()
    msg = scores[["ranking", "municipio_id_ibge7", "municipio", "cluster", "score_alocacao"]].copy()
    msg["ranking_mensagem_cidade"] = msg["ranking"]
    msg["plataforma"] = msg["cluster"].map({"Diamante": "Meta Ads", "Alavanca": "Google Ads"}).fillna("WhatsApp")
    msg["mensagem"] = (
        msg["cluster"]
        .map({"Diamante": "entrega e confianca", "Alavanca": "oportunidade e emprego"})
        .fillna("escuta e presenca")
    )
    msg["tema"] = (
        msg["cluster"].map({"Diamante": "servicos publicos", "Alavanca": "economia local"}).fillna("territorio")
    )
    msg["emocao"] = msg["cluster"].map({"Diamante": "seguranca", "Alavanca": "esperanca"}).fillna("pertencimento")
    msg["narrativa"] = "Decisao baseada em prioridade territorial e eficiencia de canal."
    msg["publico_alvo"] = msg["cluster"].map({"Diamante": "eleitor urbano persuadivel"}).fillna("liderancas locais")
    msg["performance"] = (pd.to_numeric(msg["score_alocacao"], errors="coerce") / 100).round(3)
    return msg


def _demo_simulations(scores: pd.DataFrame) -> pd.DataFrame:
    if scores.empty:
        return pd.DataFrame()
    sim = scores[
        ["ranking", "municipio_id_ibge7", "municipio", "score_alocacao", "roi_politico_estimado", "desperdicio_midia"]
    ].copy()
    sim["verba_simulada"] = 50000
    sim["impacto_incremental_estimado"] = (pd.to_numeric(sim["score_alocacao"], errors="coerce") * 0.42).round(1)
    sim["pergunta_respondida"] = "Se eu investir R$50k aqui, qual territorio entrega maior resposta?"
    return sim


def _demo_notice() -> None:
    st.caption("Modo teste HF: visualizacao derivada da base municipal porque o mart gold ainda nao foi publicado.")


def render_tab_prioridade_territorial(repo):
    st.markdown("### Prioridade territorial")
    scores = _repo_table(repo, "mart_score_alocacao_modular")
    recommendations = _repo_table(repo, "mart_recomendacao_alocacao")
    if scores.empty:
        scores = _demo_scores(repo)
        recommendations = _demo_recommendations(scores)
        if scores.empty:
            _show_missing("mart_score_alocacao_modular")
            return
        _demo_notice()
    elif recommendations.empty:
        recommendations = _demo_recommendations(scores)

    explain = build_explainability_frame(scores, recommendations)
    top = scores.sort_values("score_alocacao", ascending=False) if "score_alocacao" in scores.columns else scores.copy()
    c1, c2, c3 = st.columns(3)
    c1.metric("Territorios", len(top))
    c2.metric("Score maximo", f"{top['score_alocacao'].max():.1f}" if "score_alocacao" in top.columns else "n/d")
    c3.metric("Confiabilidade media", f"{explain['confiabilidade'].mean() * 100:.0f}%" if not explain.empty else "0%")

    cols = _display_columns(
        top,
        [
            "ranking",
            "municipio_id_ibge7",
            "municipio",
            "score_alocacao",
            "score_potencial_eleitoral",
            "score_oportunidade",
            "score_eficiencia_midia",
            "score_custo",
            "score_risco",
            "roi_politico_estimado",
            "desperdicio_midia",
        ],
    )
    st.dataframe(top[cols].head(50), width="stretch", hide_index=True)

    st.markdown("#### Explicabilidade")
    if explain.empty:
        st.info("Not found in repo: variaveis de explicabilidade")
    else:
        selected = st.selectbox("Municipio", explain["municipio"].tolist(), key="expl_municipio")
        row = explain[explain["municipio"] == selected].iloc[0]
        st.write(row["por_que_municipio_esta_alto"])
        st.caption(f"Variaveis: {row['principais_variaveis']}")


def render_tab_zona_eleitoral(repo, df_mun: pd.DataFrame, budget_total: int | float) -> None:
    st.markdown("### Alocacao por zona eleitoral")
    fact = _repo_table(repo, "fact_zona_eleitoral")
    mart = _repo_table(repo, "mart_alocacao_zona_eleitoral")
    if mart.empty and not fact.empty:
        mart = score_zone_allocation(fact, df_mun, budget_total=budget_total)
        try:
            repo.register_table("mart_alocacao_zona_eleitoral", mart)
        except Exception as exc:
            logger.debug("Nao foi possivel registrar mart_alocacao_zona_eleitoral: %s", exc)
    if mart.empty:
        _show_missing("fact_zona_eleitoral / mart_alocacao_zona_eleitoral")
        return

    if fact["fonte"].astype(str).str.contains("demo", case=False, na=False).any() if "fonte" in fact.columns else False:
        _demo_notice()

    municipio_opts = ["Todos"] + sorted(mart["municipio"].dropna().astype(str).unique().tolist())
    selected_mun = st.selectbox("Municipio", municipio_opts, key="zona_municipio")
    filtered = mart.copy()
    if selected_mun != "Todos":
        filtered = filtered[filtered["municipio"].astype(str) == selected_mun]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Zonas", len(filtered))
    c2.metric("Eleitores", f"{pd.to_numeric(filtered['eleitores_aptos'], errors='coerce').sum():,.0f}")
    c3.metric("Verba", f"R$ {pd.to_numeric(filtered['verba_sugerida'], errors='coerce').sum():,.0f}")
    c4.metric("Qualidade media", f"{pd.to_numeric(filtered['data_quality_score'], errors='coerce').mean() * 100:.0f}%")

    cols = _display_columns(
        filtered,
        [
            "ranking_zona",
            "municipio",
            "zona_eleitoral",
            "cluster_municipal",
            "score_zona",
            "eleitores_aptos",
            "abstencao_pct",
            "competitividade",
            "verba_sugerida",
            "canal_ideal",
            "mensagem_ideal",
            "data_quality_score",
            "join_confidence",
            "justificativa",
        ],
    )
    st.dataframe(filtered[cols].head(100), width="stretch", hide_index=True)

    csv = filtered[cols].to_csv(index=False).encode("utf-8")
    st.download_button(
        "Baixar alocacao por zona",
        data=csv,
        file_name=f"alocacao_zona_eleitoral_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
        mime="text/csv",
        width="stretch",
    )


def render_tab_midia_performance(repo):
    st.markdown("### Midia e performance")
    media = _repo_table(repo, "mart_midia_paga_municipio")
    canais = _repo_table(repo, "mart_social_canal_regiao")
    if media.empty and canais.empty:
        scores = _demo_scores(repo)
        media = _demo_media(scores)
        canais = (
            pd.DataFrame(
                {
                    "regiao": ["Capital", "Interior", "Litoral"],
                    "plataforma": ["Meta Ads", "Google Ads", "WhatsApp"],
                    "performance": [0.82, 0.74, 0.69],
                    "ranking_canal_regiao": [1, 2, 3],
                    "gasto": [52000, 41000, 18000],
                    "ctr": [0.031, 0.024, 0.018],
                    "cpc": [1.78, 2.04, 1.21],
                }
            )
            if not media.empty
            else pd.DataFrame()
        )
        if media.empty and canais.empty:
            _show_missing("mart_midia_paga_municipio / mart_social_canal_regiao")
            return
        _demo_notice()

    if not media.empty:
        gasto_col = "gasto" if "gasto" in media.columns else None
        ctr_col = "ctr" if "ctr" in media.columns else None
        cpc_col = "cpc" if "cpc" in media.columns else None
        c1, c2, c3 = st.columns(3)
        c1.metric("Gasto", f"R$ {pd.to_numeric(media[gasto_col], errors='coerce').sum():,.0f}" if gasto_col else "n/d")
        c2.metric(
            "CTR medio", f"{pd.to_numeric(media[ctr_col], errors='coerce').mean() * 100:.2f}%" if ctr_col else "n/d"
        )
        c3.metric("CPC medio", f"R$ {pd.to_numeric(media[cpc_col], errors='coerce').mean():.2f}" if cpc_col else "n/d")
        cols = _display_columns(
            media,
            [
                "municipio_id_ibge7",
                "municipio",
                "plataforma",
                "gasto",
                "impressoes",
                "cliques",
                "ctr",
                "cpc",
                "conversao",
                "performance",
            ],
        )
        st.dataframe(media[cols].head(80), width="stretch", hide_index=True)

    if not canais.empty:
        st.markdown("#### Canal por regiao")
        cols = _display_columns(
            canais, ["regiao", "plataforma", "performance", "ranking_canal_regiao", "gasto", "ctr", "cpc"]
        )
        st.dataframe(canais[cols].head(50), width="stretch", hide_index=True)


def render_tab_mensagem(repo):
    st.markdown("### Mensagem")
    messages = _repo_table(repo, "mart_social_mensagem_territorial")
    if messages.empty:
        messages = _demo_messages(_demo_scores(repo))
        if messages.empty:
            _show_missing("mart_social_mensagem_territorial")
            return
        _demo_notice()
    cols = _display_columns(
        messages,
        [
            "ranking_mensagem_cidade",
            "municipio_id_ibge7",
            "municipio",
            "plataforma",
            "mensagem",
            "tema",
            "emocao",
            "narrativa",
            "publico_alvo",
            "performance",
        ],
    )
    st.dataframe(messages[cols].head(80), width="stretch", hide_index=True)


def render_tab_simulacao(repo):
    st.markdown("### Simulacao")
    scores = _repo_table(repo, "mart_score_alocacao_modular")
    recommendations = _repo_table(repo, "mart_recomendacao_alocacao")
    simulations = _repo_table(repo, "mart_simulacao_orcamento")
    media = _repo_table(repo, "mart_midia_paga_municipio")
    messages = _repo_table(repo, "mart_social_mensagem_territorial")
    if scores.empty:
        scores = _demo_scores(repo)
    if recommendations.empty:
        recommendations = _demo_recommendations(scores)
    if simulations.empty:
        simulations = _demo_simulations(scores)
    if media.empty:
        media = _demo_media(scores)
    if messages.empty:
        messages = _demo_messages(scores)
    if scores.empty and recommendations.empty and simulations.empty:
        _show_missing("mart_simulacao_orcamento / mart_recomendacao_alocacao")
        return
    if _repo_table(repo, "mart_score_alocacao_modular", limit=1).empty:
        _demo_notice()

    if not simulations.empty:
        cols = _display_columns(
            simulations,
            [
                "ranking",
                "municipio_id_ibge7",
                "verba_simulada",
                "impacto_incremental_estimado",
                "roi_politico_estimado",
                "desperdicio_midia",
                "pergunta_respondida",
            ],
        )
        st.dataframe(simulations[cols].head(50), width="stretch", hide_index=True)
    if not recommendations.empty:
        st.markdown("#### Recomendacao automatica")
        cols = _display_columns(
            recommendations,
            ["ranking", "municipio_id_ibge7", "verba_sugerida", "canal_ideal", "mensagem_ideal", "justificativa"],
        )
        st.dataframe(recommendations[cols].head(50), width="stretch", hide_index=True)

    exports = build_product_exports(
        scores=scores, recommendations=recommendations, simulations=simulations, media=media, messages=messages
    )
    c1, c2, c3 = st.columns(3)
    c1.download_button(
        "PDF executivo",
        data=exports["pdf_bytes"],
        file_name=f"relatorio_executivo_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf",
        mime="application/pdf",
        width="stretch",
    )
    c2.download_button(
        "Planilha operacional",
        data=exports["xlsx_bytes"],
        file_name=f"plano_operacional_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        width="stretch",
    )
    c3.download_button(
        "Ranking atualizado",
        data=exports["ranking_csv_bytes"],
        file_name=f"ranking_atualizado_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
        mime="text/csv",
        width="stretch",
    )


def _settings_thresholds(settings) -> AlertThresholds:
    return AlertThresholds(
        error_rate=float(getattr(settings, "ops_alert_error_rate_threshold", 0.10)),
        latency_p95_ms=float(getattr(settings, "ops_alert_latency_p95_ms", 30000.0)),
        daily_cost_usd=float(getattr(settings, "ops_alert_daily_cost_usd", 50.0)),
    )


def _render_operational_dashboard(paths) -> None:
    settings = get_settings()
    try:
        db = MetadataDb(paths.metadata_db_path)
    except Exception as exc:
        st.warning(f"Observabilidade indisponivel neste ambiente de teste: {exc}")
        return
    tenant_id = getattr(paths, "tenant_id", getattr(settings, "tenant_id", "default"))
    snapshot = build_observability_snapshot(
        db, tenant_id=tenant_id, thresholds=_settings_thresholds(settings), limit=500
    )
    summary = snapshot["summary"]

    st.markdown("#### Operacao")
    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Jobs", int(summary.get("jobs_total", 0)))
    m2.metric("Erros", int(summary.get("errors_total", 0)))
    m3.metric("Latencia p95", f"{float(summary.get('latency_p95_ms', 0.0)):.0f} ms")
    m4.metric("Custo", f"US$ {float(summary.get('cost_total_usd', 0.0)):.2f}")
    m5.metric("Uso", int(summary.get("usage_total", 0)))

    active_alerts = snapshot.get("alerts", [])
    persisted_alerts = db.list_alerts(tenant_id=tenant_id, limit=25)
    if active_alerts:
        st.error(f"{len(active_alerts)} alerta(s) operacional(is) ativo(s).")
    elif persisted_alerts:
        st.info("Sem alerta ativo pelos thresholds atuais. Historico recente abaixo.")
    else:
        st.success("Sem alertas operacionais recentes.")

    events = pd.DataFrame(db.list_operational_events(tenant_id=tenant_id, limit=100))
    jobs = pd.DataFrame(db.list_jobs(tenant_id=tenant_id, limit=50))
    alerts = pd.DataFrame(persisted_alerts)

    if not events.empty:
        failed_ingestion = events[
            (events["status"] == "failed")
            & (
                events["event_type"].astype(str).str.contains("ingest", case=False, na=False)
                | events["resource"].astype(str).str.contains("ingest", case=False, na=False)
            )
        ]
        if not failed_ingestion.empty:
            st.markdown("#### Falhas de ingestao")
            cols = _display_columns(
                failed_ingestion, ["created_at_utc", "event_type", "resource", "error", "latency_ms", "cost_usd"]
            )
            st.dataframe(failed_ingestion[cols].head(20), width="stretch", hide_index=True)

        st.markdown("#### Eventos operacionais")
        cols = _display_columns(
            events,
            ["created_at_utc", "event_type", "resource", "status", "latency_ms", "cost_usd", "usage_count", "error"],
        )
        st.dataframe(events[cols].head(50), width="stretch", hide_index=True)
    else:
        st.info("Not found in repo: eventos operacionais ainda nao registrados.")

    if not jobs.empty:
        st.markdown("#### Jobs")
        cols = _display_columns(jobs, ["updated_at_utc", "id", "job_type", "status", "error", "latency_ms", "cost_usd"])
        st.dataframe(jobs[cols].head(50), width="stretch", hide_index=True)

    if not alerts.empty:
        st.markdown("#### Alertas enviados/persistidos")
        cols = _display_columns(
            alerts,
            ["created_at_utc", "severity", "metric", "value", "threshold", "status", "channels", "message", "error"],
        )
        st.dataframe(alerts[cols].head(25), width="stretch", hide_index=True)


def render_tab_monitoramento(repo, df_mun, paths=None):
    st.markdown("### Monitoramento")
    if paths is None:
        paths = get_settings().build_paths()
    _render_operational_dashboard(paths)
    st.divider()
    tables = [
        "mart_score_alocacao_modular",
        "mart_recomendacao_alocacao",
        "mart_simulacao_orcamento",
        "mart_midia_paga_municipio",
        "mart_social_mensagem_territorial",
        "mart_social_canal_regiao",
        "dim_tempo",
    ]
    status = pd.DataFrame(
        [{"dataset": name, "status": "ok" if repo.table_exists(name) else "Not found in repo"} for name in tables]
    )
    c1, c2, c3 = st.columns(3)
    c1.metric("Datasets produto", int((status["status"] == "ok").sum()))
    c2.metric("Municipios base", len(df_mun))
    c3.metric("Pendencias", int((status["status"] != "ok").sum()))
    st.dataframe(status, width="stretch", hide_index=True)

    scores = _repo_table(repo, "mart_score_alocacao_modular")
    recommendations = _repo_table(repo, "mart_recomendacao_alocacao")
    ranking = build_ranking_snapshot(scores, recommendations) if not scores.empty else pd.DataFrame()
    if ranking.empty:
        st.info("Not found in repo: ranking atualizado")
    else:
        st.markdown("#### Ranking atualizado")
        st.dataframe(ranking.head(50), width="stretch", hide_index=True)

    tempo = _repo_table(repo, "dim_tempo")
    if not tempo.empty:
        st.markdown("#### Dimensao temporal")
        st.dataframe(tempo.head(20), width="stretch", hide_index=True)

def render_tab_plataforma_decisao(paths) -> None:
    from api.decision_contracts import AllocationScenarioRequest, CandidateProfileSchema
    from application.decision_platform_service import DecisionPlatformService, find_latest_zone_fact
    from data_catalog.sources import build_default_catalog

    st.subheader("Plataforma de decisao")
    st.caption("Cenario estrategico por zona eleitoral com score, alocacao e evidencias agregadas.")

    with st.expander("Onboarding do candidato", expanded=True):
        c1, c2, c3 = st.columns(3)
        candidate_id = c1.text_input("ID do candidato", value="cand_demo")
        nome = c2.text_input("Nome politico", value="Candidato Demo")
        partido = c3.text_input("Partido", value="PARTIDO")
        cargo = c1.text_input("Cargo", value="Prefeito")
        origem = c2.text_input("Origem territorial", value="SAO PAULO")
        incumbente = c3.checkbox("Incumbente", value=False)
        temas_prioritarios = st.text_input("Temas prioritarios", value="saude, educacao, seguranca")
        municipios_base = st.text_input("Municipios-base", value="SAO PAULO")
        zonas_base = st.text_input("Zonas-base", value="")

    c1, c2, c3, c4 = st.columns(4)
    budget = c1.number_input("Orcamento total", min_value=10000, max_value=5000000, value=200000, step=10000)
    top_n = c2.slider("Top zonas", 5, 50, 15)
    capacidade = c3.slider("Capacidade operacional", 0.0, 1.0, 0.7, 0.05)
    scenario = c4.selectbox("Cenario", ["hibrido", "conservador", "agressivo"], index=0)

    latest = find_latest_zone_fact(paths)
    st.caption(f"Gold usado: {latest.name if latest else 'dataset demo'}")

    if st.button("Gerar recomendacao por zona", type="primary", key="decision_platform_run"):
        req = AllocationScenarioRequest(
            candidate=CandidateProfileSchema(
                candidate_id=candidate_id,
                nome_politico=nome,
                cargo=cargo,
                partido=partido,
                origem_territorial=origem,
                incumbente=incumbente,
                temas_prioritarios=temas_prioritarios,
                municipios_base=municipios_base,
                zonas_base=zonas_base,
            ),
            budget_total=float(budget),
            capacidade_operacional=float(capacidade),
            top_n=int(top_n),
            scenario=scenario,
        )
        service = DecisionPlatformService(paths)
        st.session_state["decision_response"] = service.generate_allocation_scenario(req)

    response = st.session_state.get("decision_response")
    if response:
        r1, r2, r3 = st.columns(3)
        r1.metric("Recomendacoes", len(response.recommendations))
        r2.metric("Evidencias", response.evidence_count)
        r3.metric("Orcamento", f"R$ {response.budget_total:,.0f}")
        rows = [rec.model_dump(exclude={"evidencias"}) for rec in response.recommendations]
        st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)
        with st.expander("Evidencias e justificativas", expanded=False):
            for rec in response.recommendations[:5]:
                st.markdown(f"**{rec.territorio_id}** - {rec.justificativa}")
                for evidence in rec.evidencias:
                    st.caption(f"{evidence.dataset}: {evidence.descricao}")

    with st.expander("Catalogo de bases", expanded=False):
        catalog_rows = [source.model_dump(mode="json") for source in build_default_catalog().sources]
        st.dataframe(pd.DataFrame(catalog_rows), width="stretch", hide_index=True)

