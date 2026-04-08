import logging

from application.interfaces import AIService, AnalyticsRepository, ReportStore
from application.use_cases_layered import executar_alocacao as _executar_alocacao
from application.use_cases_layered import responder_pergunta as _responder_pergunta
from domain.allocation import calcular_alocacao
from domain.constants import CARGOS_EST, PESOS_CLUSTER, SYSTEM_PROMPT, TETOS

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
    return _executar_alocacao(repo, report_store, df_mun, budget, cargo, n, split_d)
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
    try:
        report_store.save_report(df_r, "ultima_alocacao.parquet")
    except Exception as e:
        logger.warning("Falha ao persistir relatório de alocação: %s", e)
    repo.register_table("alocacao", df_r)
    return df_r


def responder_pergunta(
    repo: AnalyticsRepository,
    ai_service: AIService,
    pergunta: str,
    historico: list[dict],
):
    return _responder_pergunta(repo, ai_service, pergunta, historico)

    sem_txt, est = "", ""
    if col and embedder is not None:
        try:
            vec = embedder.encode([pergunta])[0].tolist()
            res = col.query(query_embeddings=[vec], n_results=5)
            sem_txt = ", ".join(m["municipio"] for m in res["metadatas"][0])
        except Exception as e:
            logger.warning("Busca semântica indisponível nesta consulta: %s", e)

    q = pergunta.lower()
    if ("budget" in q or "alocar" in q) and tem_tabela(db, "alocacao"):
        sql = "SELECT municipio,cluster,ROUND(budget,0) as budget,ROUND(digital,0) as digital,ROUND(offline,0) as offline FROM alocacao ORDER BY ranking LIMIT 15"
    elif ("seção" in q or "campo" in q or "seções" in q) and tem_tabela(db, "secoes"):
        sql = "SELECT NM_MUNICIPIO,NR_ZONA,NR_SECAO,eleitores_aptos,votos_nominais,score_secao,prioridade_secao FROM secoes ORDER BY score_secao DESC LIMIT 15"
    elif ("mapa" in q or "custo" in q) and tem_tabela(db, "mapa_tatico"):
        sql = "SELECT NM_MUNICIPIO,cluster,total_secoes,secoes_alta,ROUND(budget_total_mun,0) as budget,ROUND(custo_por_secao_alta,0) as custo_secao FROM mapa_tatico ORDER BY ranking_final LIMIT 15"
    elif "cluster" in q or "diamante" in q or "alavanca" in q:
        sql = "SELECT cluster,COUNT(*) as n,ROUND(AVG(indice_final),1) as indice_medio FROM municipios GROUP BY cluster ORDER BY indice_medio DESC"
    elif "média" in q or "total" in q or "estatística" in q:
        sql = "SELECT ROUND(AVG(indice_final),1) as media,ROUND(MAX(indice_final),1) as maximo,COUNT(*) as total,SUM(CASE WHEN cluster='Diamante' THEN 1 ELSE 0 END) as diamante FROM municipios"
    else:
        sql = "SELECT municipio,cluster,ROUND(indice_final,1) as indice,ranking_final FROM municipios ORDER BY ranking_final LIMIT 15"

    try:
        est = db.execute(sql).df().to_string(index=False)
    except Exception as e:
        logger.warning("Falha ao executar SQL contextual do chat: %s", e)
        est = ""

    ctx = f"Municípios relevantes: {sem_txt}\n\nDados:\n{est}"
    msgs = [{"role": "system", "content": SYSTEM_PROMPT}]
    for h in historico[-6:]:
        msgs.append(h)
    msgs.append({"role": "user", "content": f"CONTEXTO:\n{ctx}\n\nPERGUNTA: {pergunta}"})

    try:
        r = llm.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=msgs,
            max_tokens=1024,
            temperature=0.3,
        )
        return r.choices[0].message.content, sem_txt, r.usage.total_tokens
    except Exception as e:
        logger.error("Falha na geração da resposta do chat: %s", e)
        return "Não foi possível gerar resposta agora. Tente novamente em instantes.", sem_txt, 0
