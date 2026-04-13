from datetime import datetime

import pandas as pd
import streamlit as st

from config.settings import get_settings
from domain.constants import TETOS


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
        gerar = st.button("▶ Gerar Alocação", use_container_width=True, type="primary")

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
            if st.button(p, use_container_width=True, key=p):
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
        c4.metric("Média/município", f"R$ {total/len(df_show):,.0f}")
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
            use_container_width=True,
            hide_index=True,
            column_config={
                c: st.column_config.NumberColumn(format="R$ %.0f")
                for c in ["Budget R$", "Digital", "Offline", "Meta FB+IG", "YouTube", "TikTok", "WhatsApp", "Rádio", "Evento"]
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
                use_container_width=True,
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
                st.dataframe(repo.query_df(sql_s), use_container_width=True, hide_index=True)
            else:
                sql_s = "SELECT NM_MUNICIPIO as município, NR_ZONA as zona, NR_SECAO as seção, eleitores_aptos as votos, ROUND(engajamento*100,1) as engaj_pct, ROUND(score_secao,1) as score, prioridade_secao as prioridade FROM secoes WHERE NM_MUNICIPIO=? ORDER BY score_secao DESC"
                st.dataframe(repo.query_df(sql_s, [filtro]), use_container_width=True, hide_index=True)

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
        st.dataframe(df_dist, use_container_width=True, hide_index=True)
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
            use_container_width=True,
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
    cl_opts = st.multiselect("Cluster", ["Diamante", "Alavanca", "Consolidação", "Descarte"], default=["Diamante", "Alavanca"])
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
        use_container_width=True,
        hide_index=True,
        column_config={c: st.column_config.NumberColumn(format="%.1f") for c in ["Índice", "Territorial", "VS", "ISE", "PD"]},
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


def render_tab_prioridade_territorial(repo):
    st.markdown("### Prioridade territorial")
    scores = _repo_table(repo, "mart_score_alocacao_modular")
    recommendations = _repo_table(repo, "mart_recomendacao_alocacao")
    if scores.empty:
        _show_missing("mart_score_alocacao_modular")
        return

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
    st.dataframe(top[cols].head(50), use_container_width=True, hide_index=True)

    st.markdown("#### Explicabilidade")
    if explain.empty:
        st.info("Not found in repo: variaveis de explicabilidade")
    else:
        selected = st.selectbox("Municipio", explain["municipio"].tolist(), key="expl_municipio")
        row = explain[explain["municipio"] == selected].iloc[0]
        st.write(row["por_que_municipio_esta_alto"])
        st.caption(f"Variaveis: {row['principais_variaveis']}")


def render_tab_midia_performance(repo):
    st.markdown("### Midia e performance")
    media = _repo_table(repo, "mart_midia_paga_municipio")
    canais = _repo_table(repo, "mart_social_canal_regiao")
    if media.empty and canais.empty:
        _show_missing("mart_midia_paga_municipio / mart_social_canal_regiao")
        return

    if not media.empty:
        gasto_col = "gasto" if "gasto" in media.columns else None
        ctr_col = "ctr" if "ctr" in media.columns else None
        cpc_col = "cpc" if "cpc" in media.columns else None
        c1, c2, c3 = st.columns(3)
        c1.metric("Gasto", f"R$ {pd.to_numeric(media[gasto_col], errors='coerce').sum():,.0f}" if gasto_col else "n/d")
        c2.metric("CTR medio", f"{pd.to_numeric(media[ctr_col], errors='coerce').mean() * 100:.2f}%" if ctr_col else "n/d")
        c3.metric("CPC medio", f"R$ {pd.to_numeric(media[cpc_col], errors='coerce').mean():.2f}" if cpc_col else "n/d")
        cols = _display_columns(media, ["municipio_id_ibge7", "municipio", "plataforma", "gasto", "impressoes", "cliques", "ctr", "cpc", "conversao", "performance"])
        st.dataframe(media[cols].head(80), use_container_width=True, hide_index=True)

    if not canais.empty:
        st.markdown("#### Canal por regiao")
        cols = _display_columns(canais, ["regiao", "plataforma", "performance", "ranking_canal_regiao", "gasto", "ctr", "cpc"])
        st.dataframe(canais[cols].head(50), use_container_width=True, hide_index=True)


def render_tab_mensagem(repo):
    st.markdown("### Mensagem")
    messages = _repo_table(repo, "mart_social_mensagem_territorial")
    if messages.empty:
        _show_missing("mart_social_mensagem_territorial")
        return
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
    st.dataframe(messages[cols].head(80), use_container_width=True, hide_index=True)


def render_tab_simulacao(repo):
    st.markdown("### Simulacao")
    scores = _repo_table(repo, "mart_score_alocacao_modular")
    recommendations = _repo_table(repo, "mart_recomendacao_alocacao")
    simulations = _repo_table(repo, "mart_simulacao_orcamento")
    media = _repo_table(repo, "mart_midia_paga_municipio")
    messages = _repo_table(repo, "mart_social_mensagem_territorial")
    if scores.empty and recommendations.empty and simulations.empty:
        _show_missing("mart_simulacao_orcamento / mart_recomendacao_alocacao")
        return

    if not simulations.empty:
        cols = _display_columns(simulations, ["ranking", "municipio_id_ibge7", "verba_simulada", "impacto_incremental_estimado", "roi_politico_estimado", "desperdicio_midia", "pergunta_respondida"])
        st.dataframe(simulations[cols].head(50), use_container_width=True, hide_index=True)
    if not recommendations.empty:
        st.markdown("#### Recomendacao automatica")
        cols = _display_columns(recommendations, ["ranking", "municipio_id_ibge7", "verba_sugerida", "canal_ideal", "mensagem_ideal", "justificativa"])
        st.dataframe(recommendations[cols].head(50), use_container_width=True, hide_index=True)

    exports = build_product_exports(scores=scores, recommendations=recommendations, simulations=simulations, media=media, messages=messages)
    c1, c2, c3 = st.columns(3)
    c1.download_button("PDF executivo", data=exports["pdf_bytes"], file_name=f"relatorio_executivo_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf", mime="application/pdf", use_container_width=True)
    c2.download_button("Planilha operacional", data=exports["xlsx_bytes"], file_name=f"plano_operacional_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)
    c3.download_button("Ranking atualizado", data=exports["ranking_csv_bytes"], file_name=f"ranking_atualizado_{datetime.now().strftime('%Y%m%d_%H%M')}.csv", mime="text/csv", use_container_width=True)


def render_tab_monitoramento(repo, df_mun):
    st.markdown("### Monitoramento")
    tables = [
        "mart_score_alocacao_modular",
        "mart_recomendacao_alocacao",
        "mart_simulacao_orcamento",
        "mart_midia_paga_municipio",
        "mart_social_mensagem_territorial",
        "mart_social_canal_regiao",
        "dim_tempo",
    ]
    status = pd.DataFrame([{"dataset": name, "status": "ok" if repo.table_exists(name) else "Not found in repo"} for name in tables])
    c1, c2, c3 = st.columns(3)
    c1.metric("Datasets produto", int((status["status"] == "ok").sum()))
    c2.metric("Municipios base", len(df_mun))
    c3.metric("Pendencias", int((status["status"] != "ok").sum()))
    st.dataframe(status, use_container_width=True, hide_index=True)

    scores = _repo_table(repo, "mart_score_alocacao_modular")
    recommendations = _repo_table(repo, "mart_recomendacao_alocacao")
    ranking = build_ranking_snapshot(scores, recommendations) if not scores.empty else pd.DataFrame()
    if ranking.empty:
        st.info("Not found in repo: ranking atualizado")
    else:
        st.markdown("#### Ranking atualizado")
        st.dataframe(ranking.head(50), use_container_width=True, hide_index=True)

    tempo = _repo_table(repo, "dim_tempo")
    if not tempo.empty:
        st.markdown("#### Dimensao temporal")
        st.dataframe(tempo.head(20), use_container_width=True, hide_index=True)
