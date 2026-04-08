import logging

from application.interfaces import AIService, AnalyticsRepository, ReportStore
from domain.allocation import calcular_alocacao
from domain.constants import CARGOS_EST, PESOS_CLUSTER, SYSTEM_PROMPT, TETOS
from domain.contracts import validate_alocacao_output, validate_municipios_input

logger = logging.getLogger(__name__)


def executar_alocacao(
    repo: AnalyticsRepository,
    report_store: ReportStore,
    df_mun,
    budget: int,
    cargo: str,
    n: int,
    split_d: float,
):
    df_mun = validate_municipios_input(df_mun)
    df_r = calcular_alocacao(
        df_mun=df_mun,
        budget=budget,
        cargo=cargo,
        n=n,
        split_d=split_d,
        pesos_cluster=PESOS_CLUSTER,
        tetos=TETOS,
        cargos_est=CARGOS_EST,
    )
    df_r = validate_alocacao_output(df_r)
    try:
        report_store.save_report(df_r, "ultima_alocacao.parquet")
    except Exception as e:
        logger.warning("Falha ao persistir relatorio de alocacao: %s", e)
    repo.register_table("alocacao", df_r)
    return df_r


def _sql_contextual(repo: AnalyticsRepository, pergunta: str) -> str:
    q = pergunta.lower()
    if ("budget" in q or "alocar" in q) and repo.table_exists("alocacao"):
        return "SELECT municipio,cluster,ROUND(budget,0) as budget,ROUND(digital,0) as digital,ROUND(offline,0) as offline FROM alocacao ORDER BY ranking LIMIT 15"
    if ("seção" in q or "campo" in q or "seções" in q) and repo.table_exists("secoes"):
        return "SELECT NM_MUNICIPIO,NR_ZONA,NR_SECAO,eleitores_aptos,votos_nominais,score_secao,prioridade_secao FROM secoes ORDER BY score_secao DESC LIMIT 15"
    if ("mapa" in q or "custo" in q) and repo.table_exists("mapa_tatico"):
        return "SELECT NM_MUNICIPIO,cluster,total_secoes,secoes_alta,ROUND(budget_total_mun,0) as budget,ROUND(custo_por_secao_alta,0) as custo_secao FROM mapa_tatico ORDER BY ranking_final LIMIT 15"
    if "cluster" in q or "diamante" in q or "alavanca" in q:
        return "SELECT cluster,COUNT(*) as n,ROUND(AVG(indice_final),1) as indice_medio FROM municipios GROUP BY cluster ORDER BY indice_medio DESC"
    if "média" in q or "total" in q or "estatística" in q:
        return "SELECT ROUND(AVG(indice_final),1) as media,ROUND(MAX(indice_final),1) as maximo,COUNT(*) as total,SUM(CASE WHEN cluster='Diamante' THEN 1 ELSE 0 END) as diamante FROM municipios"
    return "SELECT municipio,cluster,ROUND(indice_final,1) as indice,ranking_final FROM municipios ORDER BY ranking_final LIMIT 15"


def responder_pergunta(
    repo: AnalyticsRepository,
    ai_service: AIService,
    pergunta: str,
    historico: list[dict],
):
    sem_txt = ""
    try:
        sem_txt = ai_service.search_relevant(pergunta, n_results=5)
    except Exception as e:
        logger.warning("Busca semantica indisponivel nesta consulta: %s", e)

    est = ""
    sql = _sql_contextual(repo, pergunta)
    try:
        est_df = repo.query_df(sql)
        if not est_df.empty:
            est = est_df.to_string(index=False)
    except Exception as e:
        logger.warning("Falha ao executar SQL contextual do chat: %s", e)

    ctx = f"Municipios relevantes: {sem_txt}\n\nDados:\n{est}"
    try:
        resposta, total_tokens = ai_service.complete(SYSTEM_PROMPT, historico, pergunta, ctx)
        return resposta, sem_txt, total_tokens
    except Exception as e:
        logger.error("Falha na geracao da resposta do chat: %s", e)
        return "Nao foi possivel gerar resposta agora. Tente novamente em instantes.", sem_txt, 0
