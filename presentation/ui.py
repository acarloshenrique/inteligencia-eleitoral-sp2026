import os
from datetime import datetime

import pandas as pd
import streamlit as st

from domain.constants import TETOS


def render_sidebar(bootstrap, chromadb_path, db):
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
        groq_ico = "✅ configurado" if os.environ.get("GROQ_API_KEY") else "⚠ simulado"
        chroma_ico = "✅ índice disponível" if chromadb_path.exists() else "⚠ sem índice"
        st.caption(f"LLM: {groq_ico}")
        st.caption(f"ChromaDB: {chroma_ico}")
        st.caption(f"DuckDB: ✅ {db.execute('SELECT COUNT(*) FROM municipios').fetchone()[0]} mun.")

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


def render_tab_alocacao(paths):
    df_show = st.session_state.get("aloc")
    if df_show is None:
        p = paths.pasta_rel / "ultima_alocacao.parquet"
        if not p.exists():
            p = paths.runtime_rel / "ultima_alocacao.parquet"
        if p.exists():
            df_show = pd.read_parquet(str(p))

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


def render_tab_secoes(db, table_exists_fn):
    if table_exists_fn(db, "secoes") and table_exists_fn(db, "mapa_tatico"):
        col_a, col_b = st.columns([1, 1])

        with col_a:
            st.markdown("#### Mapa Tático por Município")
            df_mt = db.execute(
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
                db.execute("SELECT DISTINCT NM_MUNICIPIO FROM secoes ORDER BY 1").df()["NM_MUNICIPIO"].tolist()
            )
            filtro = st.selectbox("Município", mun_opts, key="filtro_secao")
            if filtro == "Todos":
                sql_s = "SELECT NM_MUNICIPIO as município, NR_ZONA as zona, NR_SECAO as seção, eleitores_aptos as votos, ROUND(engajamento*100,1) as engaj_pct, ROUND(score_secao,1) as score, prioridade_secao as prioridade FROM secoes WHERE prioridade_secao='Alta' ORDER BY score_secao DESC LIMIT 30"
                st.dataframe(db.execute(sql_s).df(), use_container_width=True, hide_index=True)
            else:
                sql_s = "SELECT NM_MUNICIPIO as município, NR_ZONA as zona, NR_SECAO as seção, eleitores_aptos as votos, ROUND(engajamento*100,1) as engaj_pct, ROUND(score_secao,1) as score, prioridade_secao as prioridade FROM secoes WHERE NM_MUNICIPIO=? ORDER BY score_secao DESC"
                st.dataframe(db.execute(sql_s, [filtro]).df(), use_container_width=True, hide_index=True)

        st.divider()
        st.markdown("#### Distribuição de Prioridade por Município")
        df_dist = db.execute(
            """
            SELECT NM_MUNICIPIO,
                   SUM(CASE WHEN prioridade_secao='Alta'  THEN 1 ELSE 0 END) as Alta,
                   SUM(CASE WHEN prioridade_secao='Média' THEN 1 ELSE 0 END) as Média,
                   SUM(CASE WHEN prioridade_secao='Baixa' THEN 1 ELSE 0 END) as Baixa,
                   COUNT(*) as Total
            FROM secoes GROUP BY NM_MUNICIPIO ORDER BY Alta DESC
            """
        ).df()
        st.dataframe(df_dist, use_container_width=True, hide_index=True)
    else:
        st.info("Rode o Sprint 2 (sprint2_execucao_v2.py) para carregar os dados de seções.")


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
