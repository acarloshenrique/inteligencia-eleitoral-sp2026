import pandas as pd

from config.settings import AppPaths, get_settings


def build_paths():
    return get_settings().build_paths()


def df_municipios_vazio():
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


def resolve_df_mun_path(paths: AppPaths):
    preferred_patterns = (
        f"mart_municipio_eleitoral_{paths.ts}.parquet",
        f"df_mun_enriched_{paths.ts}.parquet",
        f"df_mun_{paths.ts}.parquet",
        "mart_municipio_eleitoral_*.parquet",
        "df_mun_enriched_*.parquet",
        "df_mun_*.parquet",
    )
    for pattern in preferred_patterns:
        candidatos = sorted(paths.gold_root.glob(pattern), reverse=True)
        if candidatos:
            return candidatos[0]
    return None


def resolve_relatorio_path(paths: AppPaths, nome_arquivo):
    primario = paths.gold_reports_root / nome_arquivo
    if primario.exists():
        return primario
    fallback = paths.runtime_reports_root / nome_arquivo
    return fallback if fallback.exists() else primario


def is_within_gold_layer(paths: AppPaths, path) -> bool:
    candidate = paths.gold_root.__class__(path).resolve()
    allowed_roots = [paths.gold_root.resolve(), paths.gold_reports_root.resolve(), paths.gold_serving_root.resolve(), paths.catalog_root.resolve()]
    return any(candidate == root or root in candidate.parents for root in allowed_roots)


def persistir_relatorio(paths: AppPaths, df, nome_arquivo):
    alvos = [paths.gold_reports_root, paths.runtime_reports_root]
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


def bootstrap_ambiente(paths: AppPaths):
    settings = get_settings()
    erros = []
    avisos = []

    app_env = settings.app_env
    require_data = settings.require_data
    require_groq = settings.require_groq_api_key

    if require_groq and not settings.groq_api_key:
        erros.append("REQUIRE_GROQ_API_KEY=true, mas GROQ_API_KEY nao foi definida.")
    elif not settings.groq_api_key:
        avisos.append("GROQ_API_KEY ausente: o app usara LLM simulado.")

    df_mun_path = resolve_df_mun_path(paths)
    if require_data and df_mun_path is None:
        erros.append("REQUIRE_DATA=true, mas nenhum dataset gold de municipios foi encontrado.")
    elif df_mun_path is None:
        avisos.append("Sem base gold de municipios em data_lake/gold.")

    for pasta in [
        paths.ingestion_root,
        paths.bronze_root,
        paths.silver_root,
        paths.gold_root,
        paths.catalog_root,
        paths.gold_reports_root,
        paths.runtime_reports_root,
    ]:
        try:
            pasta.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            avisos.append(f"Nao foi possivel preparar pasta de saida {pasta}: {e}")

    return {
        "app_env": app_env,
        "require_data": require_data,
        "require_groq": require_groq,
        "erros": erros,
        "avisos": avisos,
    }
