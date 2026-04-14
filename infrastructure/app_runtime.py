from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from config.settings import AppPaths, Settings, get_settings
from infrastructure.env import bootstrap_ambiente, build_paths
from infrastructure.repositories import ChromaGroqAIService, DuckDBAnalyticsRepository, ParquetReportStore
from infrastructure.storage import carrega_dados, carrega_db


@dataclass(frozen=True)
class AppEnvironment:
    settings: Settings
    paths: AppPaths
    bootstrap: dict[str, Any]


@dataclass(frozen=True)
class AppRuntime:
    environment: AppEnvironment
    df_mun: pd.DataFrame
    repository: DuckDBAnalyticsRepository
    report_store: ParquetReportStore
    ai_service: ChromaGroqAIService

    @property
    def paths(self) -> AppPaths:
        return self.environment.paths

    @property
    def bootstrap(self) -> dict[str, Any]:
        return self.environment.bootstrap


def initialize_app_environment() -> AppEnvironment:
    settings = get_settings()
    paths = build_paths()
    bootstrap = bootstrap_ambiente(paths)
    return AppEnvironment(settings=settings, paths=paths, bootstrap=bootstrap)


def build_app_runtime(environment: AppEnvironment | None = None) -> AppRuntime:
    env = environment or initialize_app_environment()
    paths = env.paths
    df_mun = carrega_dados(paths)
    db = carrega_db(paths, df_mun)
    repository = DuckDBAnalyticsRepository(db)
    report_store = ParquetReportStore(paths)
    ai_service = ChromaGroqAIService(paths.chromadb_path, app_paths=paths)
    return AppRuntime(
        environment=env,
        df_mun=df_mun,
        repository=repository,
        report_store=report_store,
        ai_service=ai_service,
    )
