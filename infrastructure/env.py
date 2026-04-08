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
    if not paths.pasta_est.exists():
        return None
    fixo = paths.pasta_est / f"df_mun_{paths.ts}.parquet"
    if fixo.exists():
        return fixo
    candidatos = sorted(paths.pasta_est.glob("df_mun_*.parquet"), reverse=True)
    return candidatos[0] if candidatos else None


def resolve_relatorio_path(paths: AppPaths, nome_arquivo):
    primario = paths.pasta_rel / nome_arquivo
    if primario.exists():
        return primario
    fallback = paths.runtime_rel / nome_arquivo
    return fallback if fallback.exists() else primario


def persistir_relatorio(paths: AppPaths, df, nome_arquivo):
    alvos = [paths.pasta_rel, paths.runtime_rel]
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
        erros.append("REQUIRE_GROQ_API_KEY=true, mas GROQ_API_KEY não foi definida.")
    elif not settings.groq_api_key:
        avisos.append("GROQ_API_KEY ausente: o app usará LLM simulado.")

    df_mun_path = resolve_df_mun_path(paths)
    if require_data and df_mun_path is None:
        erros.append("REQUIRE_DATA=true, mas nenhum df_mun_*.parquet foi encontrado.")
    elif df_mun_path is None:
        avisos.append("Sem base de municípios em data/outputs/estado_sessao.")

    for pasta in [paths.pasta_rel, paths.runtime_rel]:
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
