
import streamlit as st
import duckdb, chromadb, pickle, os, time, warnings
import pandas as pd
import numpy as np
import tempfile
from pathlib import Path
from datetime import datetime
from sentence_transformers import SentenceTransformer
from sklearn.preprocessing import QuantileTransformer

warnings.filterwarnings("ignore")

st.set_page_config(
    page_title="Inteligência Eleitoral SP 2026",
    page_icon="🗳",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Detecta ambiente ──────────────────────────────────────────────────────────
def _resolve_data_root():
    env_root = os.environ.get("DATA_ROOT")
    candidatos = []
    if env_root:
        candidatos.append(Path(env_root))
    candidatos.extend([
        Path("./data"),
        Path("/app/data"),
        Path("/content/drive/MyDrive/inteligencia_eleitoral"),
    ])
    for p in candidatos:
        if p.exists():
            return p.resolve()
    return candidatos[0].resolve()


def _df_municipios_vazio():
    cols = {
        "ranking_final": pd.Series(dtype="int64"),
        "municipio": pd.Series(dtype="string"),
        "cluster": pd.Series(dtype="string"),
        "indice_final": pd.Series(dtype="float64"),
        "score_territorial_qt": pd.Series(dtype="float64"),
        "VS_qt": pd.Series(dtype="float64"),
        "ise_qt": pd.Series(dtype="float64"),
        "PD_qt": pd.Series(dtype="float64"),
        "pop_censo2022": pd.Series(dtype="float64"),
        "perfil_economico": pd.Series(dtype="string"),
    }
    return pd.DataFrame(cols)


DATA_ROOT = _resolve_data_root()
PASTA_EST = DATA_ROOT / "outputs" / "estado_sessao"
PASTA_REL = DATA_ROOT / "outputs" / "relatorios"
CHROMADB_PATH = DATA_ROOT / "chromadb"
RUNTIME_REL = Path(tempfile.gettempdir()) / "inteligencia_eleitoral" / "relatorios"
TS = os.environ.get("DF_MUN_TS", "20260316_1855")


def _env_bool(name, default=False):
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _get_app_env():
    return os.environ.get("APP_ENV", "development").strip().lower()


def _resolve_df_mun_path():
    if not PASTA_EST.exists():
        return None
    fixo = PASTA_EST / f"df_mun_{TS}.parquet"
    if fixo.exists():
        return fixo
    candidatos = sorted(PASTA_EST.glob("df_mun_*.parquet"), reverse=True)
    return candidatos[0] if candidatos else None


def _resolve_relatorio_path(nome_arquivo):
    primario = PASTA_REL / nome_arquivo
    if primario.exists():
        return primario
    fallback = RUNTIME_REL / nome_arquivo
    return fallback if fallback.exists() else primario


def _persistir_relatorio(df, nome_arquivo):
    alvos = [PASTA_REL, RUNTIME_REL]
    ultimo_erro = None
    for pasta in alvos:
        try:
            pasta.mkdir(parents=True, exist_ok=True)
            destino = pasta / nome_arquivo
            df.to_parquet(destino, index=False)
            return destino
        except Exception as e:
            ultimo_erro = e
    raise ultimo_erro


def _bootstrap_ambiente():
    erros = []
    avisos = []

    app_env = _get_app_env()
    if app_env not in {"development", "staging", "production"}:
        erros.append("APP_ENV inválido. Use: development, staging ou production.")

    require_data = _env_bool("REQUIRE_DATA", default=False)
    require_groq = _env_bool("REQUIRE_GROQ_API_KEY", default=False)

    if require_groq and not os.environ.get("GROQ_API_KEY"):
        erros.append("REQUIRE_GROQ_API_KEY=true, mas GROQ_API_KEY não foi definida.")
    elif not os.environ.get("GROQ_API_KEY"):
        avisos.append("GROQ_API_KEY ausente: o app usará LLM simulado.")

    df_mun_path = _resolve_df_mun_path()
    if require_data and df_mun_path is None:
        erros.append("REQUIRE_DATA=true, mas nenhum df_mun_*.parquet foi encontrado.")
    elif df_mun_path is None:
        avisos.append("Sem base de municípios em data/outputs/estado_sessao.")

    for pasta in [PASTA_REL, RUNTIME_REL]:
        try:
            pasta.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            avisos.append(f"Não foi possível preparar pasta de saída {pasta}: {e}")

    return {
        "app_env": app_env,
        "require_data": require_data,
        "require_groq": require_groq,
        "erros": erros,
        "avisos": avisos,
    }


BOOTSTRAP = _bootstrap_ambiente()
if BOOTSTRAP["erros"]:
    st.error("Falha de bootstrap de ambiente:\n- " + "\n- ".join(BOOTSTRAP["erros"]))
    st.stop()

TETOS = {
    "deputado_federal":2_500_000,"deputado_estadual":1_350_000,
    "governador":70_680_000,"senador":10_500_000,
    "vereador_grande":340_000,"vereador_medio":180_000,"vereador_pequeno":60_000,
    "prefeito_grande":4_200_000,"prefeito_medio":1_600_000,"prefeito_pequeno":420_000,
}
CARGOS_EST    = {"deputado_federal","deputado_estadual","governador","senador"}
ALOC_COLS = [
    "municipio", "cluster", "ranking", "indice", "PD_qt", "pop",
    "budget", "digital", "offline", "meta_fb_ig", "youtube", "tiktok",
    "whatsapp", "google_ads", "evento_presencial", "radio_local", "impresso",
]
PESOS_CLUSTER = {"Diamante":1.0,"Alavanca":0.70,"Consolidação":0.45,"Descarte":0.10}

# ── Carrega recursos ──────────────────────────────────────────────────────────
@st.cache_resource(show_spinner="Carregando dados...")
def carrega_dados():
    caminho = _resolve_df_mun_path()
    if caminho is None:
        st.warning("Base de municípios não encontrada. Coloque os parquets em data/outputs/estado_sessao.")
        return _df_municipios_vazio()
    df = pd.read_parquet(caminho)
    if "cluster" not in df.columns:
        at, av = df["score_territorial_qt"]>70, df["VS_qt"]>70
        df["cluster"] = np.select([at&av,~at&av,at&~av,~at&~av],
                                   ["Diamante","Alavanca","Consolidação","Descarte"],"Descarte")
    df_base = _df_municipios_vazio()
    for col in df_base.columns:
        if col not in df.columns:
            df[col] = df_base[col]
    return df

@st.cache_resource(show_spinner="Conectando banco de dados...")
def carrega_db(df_mun):
    db = duckdb.connect()
    db.register("municipios", df_mun)
    for nome, glob in [
        ("alocacao",    "ultima_alocacao.parquet"),
        ("secoes",      "secoes_score_top20_*.parquet"),
        ("mapa_tatico", "mapa_tatico_*.parquet"),
    ]:
        p = _resolve_relatorio_path(glob)
        if p.exists():
            db.register(nome, pd.read_parquet(str(p)))
            continue
        found = sorted(PASTA_REL.glob(glob), reverse=True)
        if not found:
            found = sorted(RUNTIME_REL.glob(glob), reverse=True)
        if found:
            db.register(nome, pd.read_parquet(str(found[0])))
    return db

@st.cache_resource(show_spinner="Carregando embedder...")
def carrega_embedder():
    return SentenceTransformer("all-MiniLM-L6-v2")

@st.cache_resource(show_spinner="Conectando ChromaDB...")
def carrega_chroma():
    if not CHROMADB_PATH.exists():
        return None
    c = chromadb.PersistentClient(path=str(CHROMADB_PATH))
    for nome in ("municipios_v2", "municipios"):
        try:
            col = c.get_collection(nome)
            if col.count() >= 600:
                return col
        except Exception:
            pass
    return None

@st.cache_resource(show_spinner="Conectando LLM...")
def carrega_llm():
    key = os.environ.get("GROQ_API_KEY","")
    if key:
        try:
            from groq import Groq
            c = Groq(api_key=key)
            c.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[{"role":"user","content":"OK"}], max_tokens=3
            )
            return c, True
        except Exception:
            pass
    from unittest.mock import MagicMock
    class _F:
        class _C:
            class _Cc:
                def create(self,model,messages,max_tokens=512,temperature=0.3):
                    p = messages[-1]["content"].lower()
                    if "budget" in p or "alocar" in p:
                        t="Recomendo concentrar o investimento nos municípios Diamante, com foco em eventos presenciais e Meta FB+IG para maximizar o retorno eleitoral."
                    elif "diamante" in p or "cluster" in p:
                        t="Municípios Diamante combinam score territorial >70 e VS >70 — histórico favorável e alta receptividade social. São a prioridade máxima de investimento."
                    elif "seção" in p or "campo" in p:
                        t="As seções de Alta prioridade têm maior volume de votos e alto engajamento nominal. Concentre eventos e distribuição de material nessas seções."
                    elif "ranking" in p or "priorit" in p:
                        t="Cássia dos Coqueiros lidera (índice 94.5), seguida por Dirce Reis (88.8) e Cândido Rodrigues (85.8). Todos no cluster Diamante."
                    else:
                        t="Posso analisar ranking de municípios, clusters táticos, alocação de budget, estratégia de mídia e priorização de seções eleitorais para SP 2026."
                    r=MagicMock(); r.choices[0].message.content=t
                    r.usage.total_tokens=len(t.split())*2; return r
            def __init__(self): self.completions=self._Cc()
        def __init__(self): self.chat=self._C()
    return _F(), False

df_mun   = carrega_dados()
db       = carrega_db(df_mun)
embedder = carrega_embedder()
col      = carrega_chroma()
llm, groq_real = carrega_llm()

def _tem(tab):
    try: db.execute(f"SELECT 1 FROM {tab} LIMIT 1"); return True
    except: return False

# ── Funções de alocação ───────────────────────────────────────────────────────
def aloca(budget, cargo, n, split_d):
    top = df_mun.nsmallest(n, "ranking_final").copy()
    top = top[top["cluster"] != "Descarte"]
    if top.empty:
        df_vazio = pd.DataFrame(columns=ALOC_COLS)
        db.register("alocacao", df_vazio)
        return df_vazio
    top["pw"] = top["indice_final"] * top["cluster"].map(PESOS_CLUSTER).fillna(0.1)
    top["pn"] = top["pw"] / top["pw"].sum()
    top["budget"] = (top["pn"] * budget).round(0)
    if cargo not in CARGOS_EST:
        cap = float(TETOS.get(cargo, budget))
        top["budget"] = top["budget"].clip(upper=cap)
    rows = []
    for _, r in top.iterrows():
        pq = float(r.get("PD_qt", 50) or 50)
        po = float(r.get("pop_censo2022", 50000) or 50000)
        bd = round(r["budget"] * split_d, 0)
        bo = round(r["budget"] * (1-split_d), 0)
        j, s = min(pq/100,1.0), 1.0-min(pq/100,1.0)
        bw = 0.05 if po < 20_000 else 0.0
        dp = {"meta_fb_ig":0.40+s*0.15,"youtube":0.25,"tiktok":0.10+j*0.15,
              "whatsapp":0.10+s*0.05+bw,"google_ads":0.10}
        if bw: dp["meta_fb_ig"] = max(dp["meta_fb_ig"]-bw, 0.30)
        dt = sum(dp.values())
        op = {"evento_presencial":0.55 if pq>60 else 0.40 if pq>=40 else 0.25,
              "radio_local":0.30 if pq>60 else 0.35 if pq>=40 else 0.45,
              "impresso":0.15 if pq>60 else 0.25 if pq>=40 else 0.30}
        rows.append({
            "municipio":r["municipio"],"cluster":r["cluster"],
            "ranking":int(r["ranking_final"]),"indice":round(r["indice_final"],1),
            "PD_qt":round(pq,1),"pop":int(po),
            "budget":r["budget"],"digital":bd,"offline":bo,
            **{k:round(bd*(v/dt),0) for k,v in dp.items()},
            **{k:round(bo*v,0) for k,v in op.items()},
        })
    df_r = pd.DataFrame(rows, columns=ALOC_COLS)
    try:
        _persistir_relatorio(df_r, "ultima_alocacao.parquet")
    except Exception:
        pass
    db.register("alocacao", df_r)
    return df_r

# ── RAG ───────────────────────────────────────────────────────────────────────
SYSTEM = """Você é analista sênior de inteligência eleitoral SP 2026.
644 municípios paulistas ranqueados por: Territorial 35% + VS 25% + ISE 20% + PD 20%.
Clusters: Diamante (territorial>70 e VS>70) → máximo investimento | Alavanca → potencial latente | Consolidação → manutenção | Descarte → mínimo.
Responda em português, seja preciso, cite dados do contexto. Não invente valores."""

def responde(pergunta, historico):
    # Busca semântica
    sem_txt, est = "", ""
    if col:
        vec = embedder.encode([pergunta])[0].tolist()
        res = col.query(query_embeddings=[vec], n_results=5)
        sem_txt = ", ".join(m["municipio"] for m in res["metadatas"][0])
    # SQL por intenção
    q = pergunta.lower()
    if ("budget" in q or "alocar" in q) and _tem("alocacao"):
        sql = "SELECT municipio,cluster,ROUND(budget,0) as budget,ROUND(digital,0) as digital,ROUND(offline,0) as offline FROM alocacao ORDER BY ranking LIMIT 15"
    elif ("seção" in q or "campo" in q or "seções" in q) and _tem("secoes"):
        sql = "SELECT NM_MUNICIPIO,NR_ZONA,NR_SECAO,eleitores_aptos,votos_nominais,score_secao,prioridade_secao FROM secoes ORDER BY score_secao DESC LIMIT 15"
    elif ("mapa" in q or "custo" in q) and _tem("mapa_tatico"):
        sql = "SELECT NM_MUNICIPIO,cluster,total_secoes,secoes_alta,ROUND(budget_total_mun,0) as budget,ROUND(custo_por_secao_alta,0) as custo_secao FROM mapa_tatico ORDER BY ranking_final LIMIT 15"
    elif "cluster" in q or "diamante" in q or "alavanca" in q:
        sql = "SELECT cluster,COUNT(*) as n,ROUND(AVG(indice_final),1) as indice_medio FROM municipios GROUP BY cluster ORDER BY indice_medio DESC"
    elif "média" in q or "total" in q or "estatística" in q:
        sql = "SELECT ROUND(AVG(indice_final),1) as media,ROUND(MAX(indice_final),1) as maximo,COUNT(*) as total,SUM(CASE WHEN cluster='Diamante' THEN 1 ELSE 0 END) as diamante FROM municipios"
    else:
        sql = "SELECT municipio,cluster,ROUND(indice_final,1) as indice,ranking_final FROM municipios ORDER BY ranking_final LIMIT 15"
    try:    est = db.execute(sql).df().to_string(index=False)
    except: est = ""
    ctx  = f"Municípios relevantes: {sem_txt}\n\nDados:\n{est}"
    msgs = [{"role":"system","content":SYSTEM}]
    for h in historico[-6:]: msgs.append(h)
    msgs.append({"role":"user","content":f"CONTEXTO:\n{ctx}\n\nPERGUNTA: {pergunta}"})
    r = llm.chat.completions.create(
        model="llama-3.3-70b-versatile", messages=msgs, max_tokens=1024, temperature=0.3
    )
    return r.choices[0].message.content, sem_txt, r.usage.total_tokens

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🗳 Inteligência Eleitoral SP")
    st.caption(f"644 municípios · {datetime.now().strftime('%d/%m/%Y')}")
    st.caption(f"Ambiente: {BOOTSTRAP['app_env']}")
    if BOOTSTRAP["avisos"]:
        with st.expander("Bootstrap de ambiente", expanded=False):
            for aviso in BOOTSTRAP["avisos"]:
                st.warning(aviso)
    st.divider()

    st.markdown("### ⚙ Simulador de Alocação")
    budget  = st.number_input("Budget total (R$)", 10_000, 2_500_000, 200_000, 10_000, format="%d")
    cargo   = st.selectbox("Cargo", list(TETOS.keys()), index=0)
    n_mun   = st.slider("Top N municípios", 5, 50, 20)
    split_d = st.slider("% digital", 20, 80, 45) / 100

    if st.button("▶ Gerar Alocação", use_container_width=True, type="primary"):
        with st.spinner("Calculando..."):
            df_aloc = aloca(budget, cargo, n_mun, split_d)
            st.session_state["aloc"] = df_aloc
            st.session_state["budget"] = budget
        total = df_aloc["budget"].sum()
        st.success(f"✓ R$ {total:,.0f} alocados em {len(df_aloc)} municípios")

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
    groq_ico = "✅ Groq real" if groq_real else "⚠ simulado"
    chroma_ico = f"✅ {col.count()} docs" if col else "⚠ sem índice"
    st.caption(f"LLM: {groq_ico}")
    st.caption(f"ChromaDB: {chroma_ico}")
    st.caption(f"DuckDB: ✅ {db.execute('SELECT COUNT(*) FROM municipios').fetchone()[0]} mun.")

# ── Tabs ──────────────────────────────────────────────────────────────────────
t1, t2, t3, t4 = st.tabs(["💬 Analista", "📊 Alocação", "📍 Seções de Campo", "🏆 Ranking"])

# ── TAB 1: Chat ───────────────────────────────────────────────────────────────
with t1:
    if "msgs" not in st.session_state:
        st.session_state.msgs = [{"role":"assistant","content":
            "Olá! Sou seu analista de inteligência eleitoral para SP 2026. "
            "Tenho acesso ao ranking de 644 municípios, alocação de budget, "
            "scores por seção eleitoral e estratégia de mídia. Como posso ajudar?"}]
        st.session_state.hist = []

    for m in st.session_state.msgs:
        with st.chat_message(m["role"]):
            st.write(m["content"])

    prompt = st.session_state.pop("pq", None) or              st.chat_input("Pergunte sobre municípios, clusters, seções ou estratégia...")

    if prompt:
        st.session_state.msgs.append({"role":"user","content":prompt})
        with st.chat_message("user"): st.write(prompt)
        with st.chat_message("assistant"):
            with st.spinner("Analisando dados eleitorais..."):
                resp, muns, tokens = responde(prompt, st.session_state.hist)
            st.write(resp)
            with st.expander("🔍 Contexto", expanded=False):
                st.caption(f"Municípios: {muns}")
                st.caption(f"Tokens: {tokens}")
        st.session_state.hist += [{"role":"user","content":prompt},
                                   {"role":"assistant","content":resp}]
        st.session_state.msgs.append({"role":"assistant","content":resp})

    if st.button("🗑 Limpar conversa", key="clear"):
        st.session_state.msgs = [{"role":"assistant","content":"Conversa reiniciada. Como posso ajudar?"}]
        st.session_state.hist = []
        st.rerun()

# ── TAB 2: Alocação ───────────────────────────────────────────────────────────
with t2:
    df_show = st.session_state.get("aloc")
    if df_show is None:
        _p = _resolve_relatorio_path("ultima_alocacao.parquet")
        if _p.exists():
            df_show = pd.read_parquet(str(_p))

    if df_show is not None and not df_show.empty:
        bud_col = "budget" if "budget" in df_show.columns else "budget_total_mun"
        total   = df_show[bud_col].sum()

        c1,c2,c3,c4 = st.columns(4)
        c1.metric("Total Alocado",   f"R$ {total:,.0f}")
        c2.metric("Municípios",      len(df_show))
        c3.metric("💎 Diamante",     int((df_show["cluster"]=="Diamante").sum()))
        c4.metric("Média/município", f"R$ {total/len(df_show):,.0f}")
        st.divider()

        # Colunas a exibir — compatível com ambos os formatos
        cols_disp = ["municipio","cluster"]
        for c in [bud_col,"digital","offline","budget_digital","budget_offline",
                  "meta_fb_ig","youtube","tiktok","whatsapp","radio_local","evento_presencial"]:
            if c in df_show.columns and c not in cols_disp:
                cols_disp.append(c)

        st.dataframe(
            df_show[cols_disp].rename(columns={
                bud_col:"Budget R$","budget_digital":"Digital","budget_offline":"Offline",
                "digital":"Digital","offline":"Offline",
                "meta_fb_ig":"Meta FB+IG","youtube":"YouTube","tiktok":"TikTok",
                "whatsapp":"WhatsApp","radio_local":"Rádio","evento_presencial":"Evento",
            }),
            use_container_width=True, hide_index=True,
            column_config={c: st.column_config.NumberColumn(format="R$ %.0f")
                           for c in ["Budget R$","Digital","Offline","Meta FB+IG",
                                     "YouTube","TikTok","WhatsApp","Rádio","Evento"]},
        )

        import io
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as w:
            df_show.to_excel(w, index=False)
        st.download_button("⬇ Baixar Excel",
            data=buf.getvalue(),
            file_name=f"alocacao_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    elif df_show is not None and df_show.empty:
        st.warning("Sem municípios elegíveis para alocação com os dados atuais.")
    else:
        st.info("Use o simulador no menu lateral para gerar uma alocação.")

# ── TAB 3: Seções de Campo ────────────────────────────────────────────────────
with t3:
    if _tem("secoes") and _tem("mapa_tatico"):
        col_a, col_b = st.columns([1, 1])

        with col_a:
            st.markdown("#### Mapa Tático por Município")
            df_mt = db.execute("""
                SELECT NM_MUNICIPIO as município, cluster,
                       total_secoes as seções,
                       secoes_alta as alta,
                       secoes_media as média,
                       ROUND(budget_total_mun,0) as budget,
                       ROUND(custo_por_secao_alta,0) as R_secao_alta,
                       ROUND(eleitores_por_real,2) as eleit_R
                FROM mapa_tatico ORDER BY ranking_final
            """).df()
            st.dataframe(df_mt, use_container_width=True, hide_index=True,
                column_config={
                    "budget":       st.column_config.NumberColumn(format="R$ %.0f"),
                    "R_secao_alta": st.column_config.NumberColumn(format="R$ %.0f"),
                })

        with col_b:
            st.markdown("#### Top Seções Alta Prioridade")
            mun_opts = ["Todos"] + sorted(
                db.execute("SELECT DISTINCT NM_MUNICIPIO FROM secoes ORDER BY 1").df()["NM_MUNICIPIO"].tolist()
            )
            filtro = st.selectbox("Município", mun_opts, key="filtro_secao")
            if filtro == "Todos":
                sql_s = "SELECT NM_MUNICIPIO as município, NR_ZONA as zona, NR_SECAO as seção, eleitores_aptos as votos, ROUND(engajamento*100,1) as engaj_pct, ROUND(score_secao,1) as score, prioridade_secao as prioridade FROM secoes WHERE prioridade_secao='Alta' ORDER BY score_secao DESC LIMIT 30"
            else:
                sql_s = f"SELECT NM_MUNICIPIO as município, NR_ZONA as zona, NR_SECAO as seção, eleitores_aptos as votos, ROUND(engajamento*100,1) as engaj_pct, ROUND(score_secao,1) as score, prioridade_secao as prioridade FROM secoes WHERE NM_MUNICIPIO='{filtro}' ORDER BY score_secao DESC"
            st.dataframe(db.execute(sql_s).df(), use_container_width=True, hide_index=True)

        st.divider()
        st.markdown("#### Distribuição de Prioridade por Município")
        df_dist = db.execute("""
            SELECT NM_MUNICIPIO,
                   SUM(CASE WHEN prioridade_secao='Alta'  THEN 1 ELSE 0 END) as Alta,
                   SUM(CASE WHEN prioridade_secao='Média' THEN 1 ELSE 0 END) as Média,
                   SUM(CASE WHEN prioridade_secao='Baixa' THEN 1 ELSE 0 END) as Baixa,
                   COUNT(*) as Total
            FROM secoes GROUP BY NM_MUNICIPIO ORDER BY Alta DESC
        """).df()
        st.dataframe(df_dist, use_container_width=True, hide_index=True)
    else:
        st.info("Rode o Sprint 2 (sprint2_execucao_v2.py) para carregar os dados de seções.")

# ── TAB 4: Ranking ────────────────────────────────────────────────────────────
with t4:
    cl_opts = st.multiselect("Cluster", ["Diamante","Alavanca","Consolidação","Descarte"],
                              default=["Diamante","Alavanca"])
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
        df_rank[[
            "ranking_final","municipio","cluster","indice_final",
            "score_territorial_qt","VS_qt","ise_qt","PD_qt","perfil_economico",
        ]].rename(columns={
            "ranking_final":"#","municipio":"Município","cluster":"Cluster",
            "indice_final":"Índice","score_territorial_qt":"Territorial",
            "VS_qt":"VS","ise_qt":"ISE","PD_qt":"PD","perfil_economico":"Perfil",
        }),
        use_container_width=True, hide_index=True,
        column_config={c: st.column_config.NumberColumn(format="%.1f")
                       for c in ["Índice","Territorial","VS","ISE","PD"]},
    )
