import logging
from functools import lru_cache

import numpy as np
import pandas as pd

from infrastructure.env import df_municipios_vazio, resolve_df_mun_path, resolve_relatorio_path
from infrastructure.sql_safety import is_allowed_table_name

logger = logging.getLogger(__name__)


def _paths_cache_key(paths):
    return (
        paths.tenant_id,
        paths.ts,
        str(paths.gold_root.resolve()),
        str(paths.gold_reports_root.resolve()),
        str(paths.runtime_reports_root.resolve()),
    )


def _parquet_cache_key(path):
    stat = path.stat()
    return (str(path.resolve()), stat.st_mtime_ns, stat.st_size)


def _dataframe_cache_key(df: pd.DataFrame):
    return (id(df), len(df), tuple(str(col) for col in df.columns))


@lru_cache(maxsize=16)
def _load_municipios_parquet(path_key):
    path, _, _ = path_key
    df = pd.read_parquet(path)
    if "cluster" not in df.columns:
        at, av = df["score_territorial_qt"] > 70, df["VS_qt"] > 70
        df["cluster"] = np.select(
            [at & av, ~at & av, at & ~av, ~at & ~av],
            ["Diamante", "Alavanca", "Consolidacao", "Descarte"],
            "Descarte",
        )

    df_base = df_municipios_vazio()
    for col in df_base.columns:
        if col not in df.columns:
            df[col] = df_base[col]
    return df


@lru_cache(maxsize=16)
def _load_report_parquet(path_key):
    path, _, _ = path_key
    return pd.read_parquet(path)


def carrega_dados(paths):
    caminho = resolve_df_mun_path(paths)
    if caminho is None:
        logger.warning("Base gold de municipios nao encontrada. Publique um parquet em lake/gold.")
        return df_municipios_vazio()
    return _load_municipios_parquet(_parquet_cache_key(caminho))


@lru_cache(maxsize=16)
def _build_db(paths_key, df_key):
    import duckdb

    paths, df_mun = _DB_INPUTS[(paths_key, df_key)]
    db = duckdb.connect()
    db.register("municipios", df_mun)

    for nome, glob in [
        ("alocacao", "ultima_alocacao.parquet"),
        ("secoes", "secoes_score_top20_*.parquet"),
        ("mapa_tatico", "mapa_tatico_*.parquet"),
    ]:
        p = resolve_relatorio_path(paths, glob)
        if p.exists():
            db.register(nome, _load_report_parquet(_parquet_cache_key(p)))
            continue
        found = sorted(paths.gold_reports_root.glob(glob), reverse=True)
        if not found:
            found = sorted(paths.runtime_reports_root.glob(glob), reverse=True)
        if found:
            db.register(nome, _load_report_parquet(_parquet_cache_key(found[0])))
    for nome, glob in [
        ("mart_custo_mobilizacao", "mart_custo_mobilizacao_*.parquet"),
        ("mart_priorizacao_territorial_sp", "mart_priorizacao_territorial_sp_*.parquet"),
        ("dim_tempo", "dim_tempo_*.parquet"),
        ("mart_municipio_eleitoral", "mart_municipio_eleitoral_*.parquet"),
        ("mart_score_alocacao_modular", "mart_score_alocacao_modular_*.parquet"),
        ("mart_simulacao_orcamento", "mart_simulacao_orcamento_*.parquet"),
        ("mart_recomendacao_alocacao", "mart_recomendacao_alocacao_*.parquet"),
        ("mart_midia_paga_municipio", "mart_midia_paga_municipio_*.parquet"),
        ("mart_social_mensagem_territorial", "mart_social_mensagem_territorial_*.parquet"),
        ("mart_social_canal_regiao", "mart_social_canal_regiao_*.parquet"),
    ]:
        found = sorted(paths.gold_root.glob(glob), reverse=True)
        if found:
            db.register(nome, _load_report_parquet(_parquet_cache_key(found[0])))
    return db


_DB_INPUTS = {}


def carrega_db(paths, df_mun):
    paths_key = _paths_cache_key(paths)
    df_key = _dataframe_cache_key(df_mun)
    _DB_INPUTS[(paths_key, df_key)] = (paths, df_mun)
    return _build_db(paths_key, df_key)


def clear_storage_cache():
    _load_municipios_parquet.cache_clear()
    _load_report_parquet.cache_clear()
    _build_db.cache_clear()
    _DB_INPUTS.clear()


def tem_tabela(db, tab):
    if not is_allowed_table_name(tab):
        logger.warning("Nome de tabela rejeitado por politica de seguranca: %s", tab)
        return False
    try:
        tabelas = db.execute("SHOW TABLES").df()
        if "name" in tabelas.columns:
            nomes = {str(n).lower() for n in tabelas["name"].tolist()}
        else:
            nomes = {str(n).lower() for n in tabelas.iloc[:, 0].tolist()}
        return tab.lower() in nomes
    except Exception as e:
        logger.debug("Tabela/visao indisponivel (%s): %s", tab, e)
        return False
