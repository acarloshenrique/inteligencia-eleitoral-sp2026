import tempfile
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from infrastructure.tenancy import build_tenant_context, normalize_tenant_id

AppEnv = Literal["dev", "staging", "prod"]


@dataclass(frozen=True)
class AppPaths:
    data_root: Path
    ingestion_root: Path
    lake_root: Path
    bronze_root: Path
    silver_root: Path
    gold_root: Path
    gold_reports_root: Path
    gold_serving_root: Path
    catalog_root: Path
    chromadb_path: Path
    runtime_reports_root: Path
    ts: str
    metadata_db_path: Path
    artifact_root: Path
    tenant_id: str = "default"
    tenant_root: Path | None = None

    @property
    def pasta_est(self) -> Path:
        return self.gold_root

    @property
    def pasta_rel(self) -> Path:
        return self.gold_reports_root

    @property
    def runtime_rel(self) -> Path:
        return self.runtime_reports_root

    @property
    def features_root(self) -> Path:
        return self.lake_root / "features"

    @property
    def lakehouse_root(self) -> Path:
        base = self.tenant_root if self.tenant_root is not None else self.data_root
        return base / "lake"

    @property
    def semantic_root(self) -> Path:
        return self.lakehouse_root / "semantic"

    @property
    def serving_root(self) -> Path:
        return self.lakehouse_root / "serving"

    @property
    def lakehouse_catalog_root(self) -> Path:
        return self.lakehouse_root / "catalog"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_env: AppEnv = Field(default="dev", alias="APP_ENV")
    data_root: str | None = Field(default=None, alias="DATA_ROOT")
    data_lake_root: str | None = Field(default=None, alias="DATA_LAKE_ROOT")
    tenant_id: str = Field(default="default", alias="TENANT_ID")
    groq_api_key: str = Field(default="", alias="GROQ_API_KEY")
    require_data: bool = Field(default=False, alias="REQUIRE_DATA")
    require_groq_api_key: bool = Field(default=False, alias="REQUIRE_GROQ_API_KEY")
    df_mun_ts: str = Field(default="20260316_1855", alias="DF_MUN_TS")
    port: int = Field(default=7860, alias="PORT")
    rag_cost_per_1k_tokens_usd: float = Field(default=0.00059, alias="RAG_COST_PER_1K_TOKENS_USD")
    redis_url: str = Field(default="redis://redis:6379/0", alias="REDIS_URL")
    rq_queue_name: str = Field(default="jobs", alias="RQ_QUEUE_NAME")
    api_rate_limit_enabled: bool = Field(default=True, alias="API_RATE_LIMIT_ENABLED")
    api_rate_limit_backend: str = Field(default="redis", alias="API_RATE_LIMIT_BACKEND")
    api_rate_limit_requests: int = Field(default=120, alias="API_RATE_LIMIT_REQUESTS")
    api_rate_limit_window_seconds: int = Field(default=60, alias="API_RATE_LIMIT_WINDOW_SECONDS")
    api_rate_limit_exempt_paths: str = Field(default="/health", alias="API_RATE_LIMIT_EXEMPT_PATHS")
    metadata_db_path: str | None = Field(default=None, alias="METADATA_DB_PATH")
    artifact_backend: str = Field(default="local", alias="ARTIFACT_BACKEND")
    artifact_local_root: str | None = Field(default=None, alias="ARTIFACT_LOCAL_ROOT")
    ingestion_source_catalog_path: str | None = Field(default=None, alias="INGESTION_SOURCE_CATALOG_PATH")
    s3_endpoint_url: str = Field(default="", alias="S3_ENDPOINT_URL")
    s3_bucket: str = Field(default="", alias="S3_BUCKET")
    s3_access_key: str = Field(default="", alias="S3_ACCESS_KEY")
    s3_secret_key: str = Field(default="", alias="S3_SECRET_KEY")
    s3_region: str = Field(default="us-east-1", alias="S3_REGION")
    secret_backend: str = Field(default="env", alias="SECRET_BACKEND")
    vault_addr: str = Field(default="", alias="VAULT_ADDR")
    vault_token: str = Field(default="", alias="VAULT_TOKEN")
    vault_kv_path: str = Field(default="", alias="VAULT_KV_PATH")
    retention_days: int = Field(default=180, alias="RETENTION_DAYS")
    ops_daily_ingestion_hour: int = Field(default=5, alias="OPS_DAILY_INGESTION_HOUR")
    ops_weekly_update_day: str = Field(default="MON", alias="OPS_WEEKLY_UPDATE_DAY")
    ops_weekly_update_hour: int = Field(default=6, alias="OPS_WEEKLY_UPDATE_HOUR")
    ops_alert_error_rate_threshold: float = Field(default=0.10, alias="OPS_ALERT_ERROR_RATE_THRESHOLD")
    ops_alert_latency_p95_ms: float = Field(default=30000.0, alias="OPS_ALERT_LATENCY_P95_MS")
    ops_alert_daily_cost_usd: float = Field(default=50.0, alias="OPS_ALERT_DAILY_COST_USD")
    ops_alert_webhook_url: str = Field(default="", alias="OPS_ALERT_WEBHOOK_URL")
    ops_alert_slack_webhook_url: str = Field(default="", alias="OPS_ALERT_SLACK_WEBHOOK_URL")
    ops_alert_teams_webhook_url: str = Field(default="", alias="OPS_ALERT_TEAMS_WEBHOOK_URL")
    ops_alert_email_enabled: bool = Field(default=False, alias="OPS_ALERT_EMAIL_ENABLED")
    ops_alert_email_from: str = Field(default="", alias="OPS_ALERT_EMAIL_FROM")
    ops_alert_email_to: str = Field(default="", alias="OPS_ALERT_EMAIL_TO")
    ops_alert_smtp_host: str = Field(default="", alias="OPS_ALERT_SMTP_HOST")
    ops_alert_smtp_port: int = Field(default=587, alias="OPS_ALERT_SMTP_PORT")
    ops_alert_smtp_username: str = Field(default="", alias="OPS_ALERT_SMTP_USERNAME")
    ops_alert_smtp_password: str = Field(default="", alias="OPS_ALERT_SMTP_PASSWORD")
    ops_alert_smtp_tls: bool = Field(default=True, alias="OPS_ALERT_SMTP_TLS")
    require_redis_tls_in_prod: bool = Field(default=True, alias="REQUIRE_REDIS_TLS_IN_PROD")
    require_redis_auth_in_prod: bool = Field(default=True, alias="REQUIRE_REDIS_AUTH_IN_PROD")
    chroma_vector_backend: str = Field(default="local", alias="CHROMA_VECTOR_BACKEND")
    chroma_allow_shared_volume: bool = Field(default=False, alias="CHROMA_ALLOW_SHARED_VOLUME")
    lgpd_anonymization_salt: str = Field(default="", alias="LGPD_ANONYMIZATION_SALT")
    embedding_model_id: str = Field(default="Xenova/all-MiniLM-L6-v2", alias="EMBEDDING_MODEL_ID")
    embedding_onnx_model_file: str = Field(default="onnx/model.onnx", alias="EMBEDDING_ONNX_MODEL_FILE")
    embedding_cache_dir: str = Field(default="", alias="EMBEDDING_CACHE_DIR")
    embedding_max_length: int = Field(default=256, alias="EMBEDDING_MAX_LENGTH")

    @field_validator("app_env", mode="before")
    @classmethod
    def normalize_app_env(cls, value):
        if value is None:
            return "dev"
        env = str(value).strip().lower()
        aliases = {"development": "dev", "dev": "dev", "staging": "staging", "prod": "prod", "production": "prod"}
        if env not in aliases:
            raise ValueError("APP_ENV invalido. Use: dev, staging ou prod.")
        return aliases[env]

    @field_validator("tenant_id", mode="before")
    @classmethod
    def validate_tenant_id(cls, value):
        return normalize_tenant_id(value)

    @field_validator("ops_daily_ingestion_hour", "ops_weekly_update_hour", mode="before")
    @classmethod
    def validate_ops_hour(cls, value):
        hour = int(value)
        if hour < 0 or hour > 23:
            raise ValueError("hora operacional deve estar entre 0 e 23")
        return hour

    @field_validator("ops_weekly_update_day", mode="before")
    @classmethod
    def validate_ops_weekday(cls, value):
        day = str(value or "MON").strip().upper()
        allowed = {"MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"}
        if day not in allowed:
            raise ValueError("OPS_WEEKLY_UPDATE_DAY invalido")
        return day

    @field_validator("api_rate_limit_backend", mode="before")
    @classmethod
    def validate_rate_limit_backend(cls, value):
        backend = str(value or "redis").strip().lower()
        allowed = {"redis", "memory"}
        if backend not in allowed:
            raise ValueError("API_RATE_LIMIT_BACKEND invalido. Use: redis ou memory.")
        return backend

    @field_validator("api_rate_limit_requests", "api_rate_limit_window_seconds", mode="before")
    @classmethod
    def validate_rate_limit_positive_int(cls, value):
        number = int(value)
        if number < 1:
            raise ValueError("rate limit deve ser maior que zero")
        return number

    @field_validator("chroma_vector_backend", mode="before")
    @classmethod
    def validate_chroma_backend(cls, value):
        backend = str(value or "local").strip().lower()
        allowed = {"local", "external"}
        if backend not in allowed:
            raise ValueError("CHROMA_VECTOR_BACKEND invalido. Use: local ou external.")
        return backend

    def resolved_data_root(self):
        if self.data_root:
            return Path(self.data_root).resolve()
        candidatos = [
            Path("."),
            Path("./data"),
            Path("/app/data"),
            Path("/content/drive/MyDrive/inteligencia_eleitoral"),
        ]
        for p in candidatos:
            if p.exists():
                return p.resolve()
        return candidatos[0].resolve()

    def build_paths(self):
        data_root = self.resolved_data_root()
        tenant = build_tenant_context(data_root, self.tenant_id)
        effective_root = tenant.tenant_root
        if self.data_lake_root and tenant.tenant_id == "default":
            lake_root = Path(self.data_lake_root).resolve()
        elif self.data_lake_root:
            lake_root = effective_root / "data_lake"
        else:
            lake_root = effective_root / "data_lake"
        gold_root = lake_root / "gold"
        catalog_root = lake_root / "catalog"
        for folder in [
            data_root,
            effective_root,
            effective_root / "ingestion",
            lake_root,
            lake_root / "bronze",
            lake_root / "silver",
            lake_root / "features",
            gold_root,
            catalog_root,
            gold_root / "reports",
            gold_root / "serving",
            effective_root / "lake",
            effective_root / "lake" / "bronze",
            effective_root / "lake" / "silver",
            effective_root / "lake" / "gold",
            effective_root / "lake" / "semantic",
            effective_root / "lake" / "serving",
            effective_root / "lake" / "catalog",
            effective_root / "lake" / "manifests",
            effective_root / "lake" / "lineage",
            effective_root / "lake" / "duckdb",
            effective_root / "chromadb",
        ]:
            folder.mkdir(parents=True, exist_ok=True)
        return AppPaths(
            data_root=data_root,
            ingestion_root=effective_root / "ingestion",
            lake_root=lake_root,
            bronze_root=lake_root / "bronze",
            silver_root=lake_root / "silver",
            gold_root=gold_root,
            gold_reports_root=gold_root / "reports",
            gold_serving_root=gold_root / "serving",
            catalog_root=catalog_root,
            chromadb_path=effective_root / "chromadb",
            runtime_reports_root=Path(tempfile.gettempdir())
            / "inteligencia_eleitoral"
            / tenant.tenant_id
            / "gold_reports",
            ts=self.df_mun_ts,
            metadata_db_path=(
                Path(self.metadata_db_path).resolve()
                if self.metadata_db_path
                else (effective_root / "metadata" / "jobs.sqlite3").resolve()
            ),
            artifact_root=(
                Path(self.artifact_local_root).resolve()
                if self.artifact_local_root
                else (effective_root / "artifacts").resolve()
            ),
            tenant_id=tenant.tenant_id,
            tenant_root=tenant.tenant_root,
        )


@lru_cache(maxsize=1)
def get_settings():
    return Settings()
