import logging

import duckdb
import numpy as np
import pandas as pd
import streamlit as st

from infrastructure.env import df_municipios_vazio, resolve_df_mun_path, resolve_relatorio_path

logger = logging.getLogger(__name__)


@st.cache_resource(show_spinner="Carregando dados...")
def carrega_dados(paths):
    caminho = resolve_df_mun_path(paths)
    if caminho is None:
        st.warning("Base de municípios não encontrada. Coloque os parquets em data/outputs/estado_sessao.")
        return df_municipios_vazio()

    df = pd.read_parquet(caminho)
    if "cluster" not in df.columns:
        at, av = df["score_territorial_qt"] > 70, df["VS_qt"] > 70
        df["cluster"] = np.select(
            [at & av, ~at & av, at & ~av, ~at & ~av],
            ["Diamante", "Alavanca", "Consolidação", "Descarte"],
            "Descarte",
        )

    df_base = df_municipios_vazio()
    for col in df_base.columns:
        if col not in df.columns:
            df[col] = df_base[col]
    return df


@st.cache_resource(show_spinner="Conectando banco de dados...")
def carrega_db(paths, df_mun):
    db = duckdb.connect()
    db.register("municipios", df_mun)

    for nome, glob in [
        ("alocacao", "ultima_alocacao.parquet"),
        ("secoes", "secoes_score_top20_*.parquet"),
        ("mapa_tatico", "mapa_tatico_*.parquet"),
    ]:
        p = resolve_relatorio_path(paths, glob)
        if p.exists():
            db.register(nome, pd.read_parquet(str(p)))
            continue
        found = sorted(paths.pasta_rel.glob(glob), reverse=True)
        if not found:
            found = sorted(paths.runtime_rel.glob(glob), reverse=True)
        if found:
            db.register(nome, pd.read_parquet(str(found[0])))
    return db


def tem_tabela(db, tab):
    try:
        db.execute(f"SELECT 1 FROM {tab} LIMIT 1")
        return True
    except Exception as e:
        logger.debug("Tabela/visão indisponível (%s): %s", tab, e)
        return False
