from urllib.parse import urlparse

import pandas as pd

from config.settings import AppPaths, get_settings

WEAK_SECRET_PLACEHOLDERS = {
    "",
    "admin",
    "adminadmin",
    "change-me",
    "changeme",
    "default",
    "dev-admin-token",
    "minioadmin",
    "password",
    "secret",
    "test",
    "token",
}


def _is_weak_secret(value) -> bool:
    normalized = str(value or "").strip().lower()
    return normalized in WEAK_SECRET_PLACEHOLDERS


def _require_strong_secret(errors: list[str], *, name: str, value, reason: str) -> None:
    if _is_weak_secret(value):
        errors.append(f"APP_ENV=prod exige {name} forte: {reason}.")


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
    allowed_roots = [
        paths.gold_root.resolve(),
        paths.gold_reports_root.resolve(),
        paths.gold_serving_root.resolve(),
        paths.catalog_root.resolve(),
    ]
    return any(candidate == root or root in candidate.parents for root in allowed_roots)


def validate_prod_runtime_hardening(settings, paths: AppPaths) -> list[str]:
    if settings.app_env != "prod":
        return []

    erros: list[str] = []
    parsed_redis = urlparse(settings.redis_url)
    redis_has_tls = parsed_redis.scheme == "rediss"
    redis_has_auth = bool(parsed_redis.password)
    if settings.require_redis_tls_in_prod and not redis_has_tls:
        erros.append("APP_ENV=prod exige Redis com TLS: use REDIS_URL rediss://...")
    if settings.require_redis_auth_in_prod and not redis_has_auth:
        erros.append("APP_ENV=prod exige Redis com senha em REDIS_URL.")

    if bool(getattr(settings, "api_rate_limit_enabled", True)) and settings.api_rate_limit_backend != "redis":
        erros.append("APP_ENV=prod exige API_RATE_LIMIT_BACKEND=redis para rate limiting distribuido.")

    backend = str(settings.chroma_vector_backend).lower()
    if backend == "local" and not settings.chroma_allow_shared_volume:
        if paths.tenant_id == "default":
            erros.append("APP_ENV=prod exige TENANT_ID dedicado ou CHROMA_VECTOR_BACKEND=external para ChromaDB.")
        tenant_root = paths.tenant_root.resolve() if paths.tenant_root else None
        chroma_path = paths.chromadb_path.resolve()
        if tenant_root is None or not (chroma_path == tenant_root or tenant_root in chroma_path.parents):
            erros.append("APP_ENV=prod exige volume ChromaDB isolado dentro do tenant.")

    _require_strong_secret(
        erros,
        name="LGPD_ANONYMIZATION_SALT",
        value=settings.lgpd_anonymization_salt,
        reason="nao use vazio, change-me ou outro placeholder para anonimizar dados",
    )

    if str(settings.artifact_backend).lower() == "s3":
        _require_strong_secret(
            erros,
            name="S3_ACCESS_KEY",
            value=settings.s3_access_key,
            reason="configure credencial real do storage de artefatos",
        )
        _require_strong_secret(
            erros,
            name="S3_SECRET_KEY",
            value=settings.s3_secret_key,
            reason="minioadmin e placeholders sao bloqueados",
        )

    if str(settings.secret_backend).lower() == "vault":
        _require_strong_secret(
            erros,
            name="VAULT_TOKEN",
            value=settings.vault_token,
            reason="configure token real via secret manager ou env seguro",
        )

    if bool(settings.ops_alert_email_enabled):
        _require_strong_secret(
            erros,
            name="OPS_ALERT_SMTP_PASSWORD",
            value=settings.ops_alert_smtp_password,
            reason="alertas por email em producao exigem senha real",
        )
    return erros


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
    if ultimo_erro is not None:
        raise ultimo_erro
    raise RuntimeError("Nao foi possivel persistir relatorio: nenhum destino configurado.")


def bootstrap_ambiente(paths: AppPaths):
    settings = get_settings()
    erros = []
    avisos = []

    app_env = settings.app_env
    require_data = settings.require_data
    require_groq = settings.require_groq_api_key

    erros.extend(validate_prod_runtime_hardening(settings, paths))

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
