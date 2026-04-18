import logging
from functools import lru_cache

import numpy as np
import pandas as pd

from config.settings import get_settings
from infrastructure.env import df_municipios_vazio, resolve_df_mun_path, resolve_relatorio_path
from infrastructure.sql_safety import is_allowed_table_name

logger = logging.getLogger(__name__)


def _demo_municipios() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ranking_final": 1,
                "municipio": "Sao Paulo",
                "cluster": "Diamante",
                "indice_final": 94.5,
                "score_territorial_qt": 91.0,
                "VS_qt": 88.0,
                "ise_qt": 82.0,
                "PD_qt": 68.0,
                "pop_censo2022": 11451245,
                "perfil_economico": "metropole",
            },
            {
                "ranking_final": 2,
                "municipio": "Campinas",
                "cluster": "Diamante",
                "indice_final": 88.8,
                "score_territorial_qt": 84.0,
                "VS_qt": 81.0,
                "ise_qt": 78.0,
                "PD_qt": 54.0,
                "pop_censo2022": 1139047,
                "perfil_economico": "urbano diversificado",
            },
            {
                "ranking_final": 3,
                "municipio": "Santos",
                "cluster": "Alavanca",
                "indice_final": 82.4,
                "score_territorial_qt": 76.0,
                "VS_qt": 73.0,
                "ise_qt": 74.0,
                "PD_qt": 47.0,
                "pop_censo2022": 418608,
                "perfil_economico": "servicos",
            },
            {
                "ranking_final": 4,
                "municipio": "Ribeirao Preto",
                "cluster": "Alavanca",
                "indice_final": 79.6,
                "score_territorial_qt": 71.0,
                "VS_qt": 69.0,
                "ise_qt": 72.0,
                "PD_qt": 43.0,
                "pop_censo2022": 698642,
                "perfil_economico": "agronegocio e servicos",
            },
            {
                "ranking_final": 5,
                "municipio": "Bauru",
                "cluster": "Consolidacao",
                "indice_final": 72.1,
                "score_territorial_qt": 66.0,
                "VS_qt": 61.0,
                "ise_qt": 64.0,
                "PD_qt": 38.0,
                "pop_censo2022": 379146,
                "perfil_economico": "polo regional",
            },
        ]
    )


def build_demo_zone_tables(df_mun: pd.DataFrame) -> dict[str, pd.DataFrame]:
    if df_mun.empty:
        return {}

    rows = []
    for _, row in df_mun.head(12).iterrows():
        municipio = str(row.get("municipio", "Municipio"))
        ranking = int(row.get("ranking_final", len(rows) + 1) or len(rows) + 1)
        municipio_id = str(row.get("municipio_id_ibge7", str(ranking).zfill(7))).zfill(7)
        pop = float(row.get("pop_censo2022", 120000) or 120000)
        base_eleitores = max(9000, int(pop * 0.72))
        for offset, share in enumerate([0.44, 0.33, 0.23], start=1):
            zona = ranking * 10 + offset
            eleitores = int(base_eleitores * share)
            abstencao = min(0.34, 0.14 + offset * 0.035 + ranking * 0.006)
            competitividade = max(0.35, min(0.92, 0.82 - offset * 0.06 + (12 - ranking) * 0.008))
            territorio_id = f"{municipio_id}-ZE{zona}"
            rows.append(
                {
                    "territorio_id": territorio_id,
                    "uf": "SP",
                    "municipio": municipio,
                    "municipio_id_ibge7": municipio_id,
                    "cod_tse_municipio": str(60000 + ranking),
                    "zona_eleitoral": zona,
                    "zona_id": f"SP-{zona}",
                    "latitude": -23.55 + ranking * 0.04 + offset * 0.01,
                    "longitude": -46.63 + ranking * 0.04 - offset * 0.01,
                    "fonte": "demo_derivado_municipios",
                    "data_referencia": "2026-04-15",
                    "match_method": "demo_exact_municipio_rank",
                    "data_quality_score": round(0.91 - offset * 0.025, 3),
                    "join_confidence": round(0.94 - offset * 0.02, 3),
                    "ano_eleicao": 2026,
                    "eleitores_aptos": eleitores,
                    "votos_validos": int(eleitores * (1 - abstencao) * 0.93),
                    "abstencoes": int(eleitores * abstencao),
                    "comparecimento": round(1 - abstencao, 3),
                    "abstencao_pct": round(abstencao, 3),
                    "competitividade": round(competitividade, 3),
                    "volatilidade_historica": round(0.18 + offset * 0.04 + ranking * 0.004, 3),
                    "secoes_total": max(8, int(eleitores / 4200)),
                }
            )

    fact = pd.DataFrame(rows)
    dim_cols = [
        "territorio_id",
        "uf",
        "municipio",
        "municipio_id_ibge7",
        "cod_tse_municipio",
        "zona_eleitoral",
        "zona_id",
        "latitude",
        "longitude",
        "fonte",
        "data_referencia",
        "match_method",
        "data_quality_score",
        "join_confidence",
    ]
    feature_cols = [
        "territorio_id",
        "municipio",
        "municipio_id_ibge7",
        "zona_eleitoral",
        "eleitores_aptos",
        "abstencao_pct",
        "competitividade",
        "volatilidade_historica",
        "data_quality_score",
        "join_confidence",
    ]
    return {
        "dim_territorio_eleitoral": fact[dim_cols].copy(),
        "fact_zona_eleitoral": fact.copy(),
        "features_zona_eleitoral": fact[feature_cols].copy(),
    }


def _latest_parquet(root, pattern: str):
    found = sorted(root.glob(pattern), reverse=True)
    return found[0] if found else None


def build_pandas_repository_tables(paths, df_mun: pd.DataFrame) -> dict[str, pd.DataFrame]:
    tables: dict[str, pd.DataFrame] = {"municipios": df_mun}
    for nome, root, pattern in [
        ("dim_territorio_eleitoral", paths.silver_root, "dim_territorio_eleitoral_*.parquet"),
        ("fact_secao_eleitoral", paths.silver_root, "fact_secao_eleitoral_*.parquet"),
        ("fact_zona_eleitoral", paths.gold_root, "fact_zona_eleitoral_*.parquet"),
        ("features_zona_eleitoral", paths.gold_root, "features_zona_eleitoral_*.parquet"),
        ("mart_alocacao_zona_eleitoral", paths.gold_root, "mart_alocacao_zona_eleitoral_*.parquet"),
        ("mart_recomendacao_zona_eleitoral", paths.gold_root, "mart_recomendacao_zona_eleitoral_*.parquet"),
    ]:
        latest = _latest_parquet(root, pattern)
        if latest is not None:
            tables[nome] = _load_report_parquet(_parquet_cache_key(latest))
    if "fact_zona_eleitoral" not in tables:
        tables.update(build_demo_zone_tables(df_mun))
    return tables


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
        settings = get_settings()
        if settings.app_env != "prod" and not settings.require_data:
            logger.warning("Base gold de municipios nao encontrada; usando dataset demo para testes da UI.")
            return _demo_municipios()
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
    registered_gold = set()
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
        ("dim_territorio_eleitoral", "dim_territorio_eleitoral_*.parquet"),
        ("fact_zona_eleitoral", "fact_zona_eleitoral_*.parquet"),
        ("features_zona_eleitoral", "features_zona_eleitoral_*.parquet"),
        ("mart_alocacao_zona_eleitoral", "mart_alocacao_zona_eleitoral_*.parquet"),
        ("mart_recomendacao_zona_eleitoral", "mart_recomendacao_zona_eleitoral_*.parquet"),
    ]:
        found = sorted(paths.gold_root.glob(glob), reverse=True)
        if found:
            db.register(nome, _load_report_parquet(_parquet_cache_key(found[0])))
            registered_gold.add(nome)

    if not {"dim_territorio_eleitoral", "fact_zona_eleitoral", "features_zona_eleitoral"}.issubset(registered_gold):
        for nome, frame in build_demo_zone_tables(df_mun).items():
            if nome not in registered_gold:
                db.register(nome, frame)
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
