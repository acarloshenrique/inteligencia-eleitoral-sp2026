from application.interfaces import AIService, AnalyticsRepository, ReportStore
from application.use_cases_layered import executar_alocacao as _executar_alocacao
from application.use_cases_layered import responder_pergunta as _responder_pergunta


def executar_alocacao(
    repo: AnalyticsRepository,
    report_store: ReportStore,
    df_mun,
    budget: int,
    cargo: str,
    n: int,
    split_d: float,
):
    return _executar_alocacao(repo, report_store, df_mun, budget, cargo, n, split_d)


def responder_pergunta(
    repo: AnalyticsRepository,
    ai_service: AIService,
    pergunta: str,
    historico: list[dict],
):
    return _responder_pergunta(repo, ai_service, pergunta, historico)
