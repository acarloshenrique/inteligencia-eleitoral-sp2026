import os
import tempfile
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class AppPaths:
    data_root: Path
    pasta_est: Path
    pasta_rel: Path
    chromadb_path: Path
    runtime_rel: Path
    ts: str


def env_bool(name, default=False):
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def get_app_env():
    return os.environ.get("APP_ENV", "development").strip().lower()


def resolve_data_root():
    env_root = os.environ.get("DATA_ROOT")
    candidatos = []
    if env_root:
        candidatos.append(Path(env_root))
    candidatos.extend([Path("./data"), Path("/app/data"), Path("/content/drive/MyDrive/inteligencia_eleitoral")])
    for p in candidatos:
        if p.exists():
            return p.resolve()
    return candidatos[0].resolve()


def build_paths():
    data_root = resolve_data_root()
    ts = os.environ.get("DF_MUN_TS", "20260316_1855")
    return AppPaths(
        data_root=data_root,
        pasta_est=data_root / "outputs" / "estado_sessao",
        pasta_rel=data_root / "outputs" / "relatorios",
        chromadb_path=data_root / "chromadb",
        runtime_rel=Path(tempfile.gettempdir()) / "inteligencia_eleitoral" / "relatorios",
        ts=ts,
    )


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
    erros = []
    avisos = []

    app_env = get_app_env()
    if app_env not in {"development", "staging", "production"}:
        erros.append("APP_ENV inválido. Use: development, staging ou production.")

    require_data = env_bool("REQUIRE_DATA", default=False)
    require_groq = env_bool("REQUIRE_GROQ_API_KEY", default=False)

    if require_groq and not os.environ.get("GROQ_API_KEY"):
        erros.append("REQUIRE_GROQ_API_KEY=true, mas GROQ_API_KEY não foi definida.")
    elif not os.environ.get("GROQ_API_KEY"):
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
