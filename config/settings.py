from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Literal
import tempfile

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


AppEnv = Literal["dev", "staging", "prod"]


@dataclass(frozen=True)
class AppPaths:
    data_root: Path
    pasta_est: Path
    pasta_rel: Path
    chromadb_path: Path
    runtime_rel: Path
    ts: str


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_env: AppEnv = Field(default="dev", alias="APP_ENV")
    data_root: str | None = Field(default=None, alias="DATA_ROOT")
    groq_api_key: str = Field(default="", alias="GROQ_API_KEY")
    require_data: bool = Field(default=False, alias="REQUIRE_DATA")
    require_groq_api_key: bool = Field(default=False, alias="REQUIRE_GROQ_API_KEY")
    df_mun_ts: str = Field(default="20260316_1855", alias="DF_MUN_TS")
    port: int = Field(default=7860, alias="PORT")
    rag_cost_per_1k_tokens_usd: float = Field(default=0.00059, alias="RAG_COST_PER_1K_TOKENS_USD")

    @field_validator("app_env", mode="before")
    @classmethod
    def normalize_app_env(cls, value):
        if value is None:
            return "dev"
        env = str(value).strip().lower()
        aliases = {"development": "dev", "dev": "dev", "staging": "staging", "prod": "prod", "production": "prod"}
        if env not in aliases:
            raise ValueError("APP_ENV inválido. Use: dev, staging ou prod.")
        return aliases[env]

    def resolved_data_root(self):
        if self.data_root:
            return Path(self.data_root).resolve()
        candidatos = [Path("./data"), Path("/app/data"), Path("/content/drive/MyDrive/inteligencia_eleitoral")]
        for p in candidatos:
            if p.exists():
                return p.resolve()
        return candidatos[0].resolve()

    def build_paths(self):
        data_root = self.resolved_data_root()
        return AppPaths(
            data_root=data_root,
            pasta_est=data_root / "outputs" / "estado_sessao",
            pasta_rel=data_root / "outputs" / "relatorios",
            chromadb_path=data_root / "chromadb",
            runtime_rel=Path(tempfile.gettempdir()) / "inteligencia_eleitoral" / "relatorios",
            ts=self.df_mun_ts,
        )


@lru_cache(maxsize=1)
def get_settings():
    return Settings()
