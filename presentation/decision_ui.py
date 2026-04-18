from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd
import streamlit as st
from pydantic import ValidationError

from api.decision_contracts import AllocationScenarioResponse, CandidateProfileSchema
from application.candidate_registry_service import CandidateRegistryService
from application.decision_platform_service import DecisionPlatformService, find_latest_zone_fact, load_zone_territories
from application.municipal_strategy_service import MunicipalStrategyService
from application.serving_service import ServingDataNotFoundError, ServingOutputService
from data_catalog.sources import build_default_catalog


def _service(paths) -> DecisionPlatformService:
    return DecisionPlatformService(paths)


def _registry(paths) -> CandidateRegistryService:
    return CandidateRegistryService(paths)


def _serving(paths) -> ServingOutputService:
    return ServingOutputService(paths)


def _municipal_strategy(paths) -> MunicipalStrategyService:
    return MunicipalStrategyService(paths)


def _serving_frame(result) -> pd.DataFrame:
    return pd.DataFrame(result.records)


def _serving_readiness(paths) -> pd.DataFrame:
    try:
        return _serving_frame(_serving(paths).data_readiness(tenant_id=paths.tenant_id))
    except ServingDataNotFoundError:
        return pd.DataFrame()


def _candidate_to_schema(candidate) -> CandidateProfileSchema:
    return CandidateProfileSchema(
        candidate_id=candidate.candidate_id,
        nome_politico=candidate.nome_politico,
        cargo=candidate.cargo,
        partido=candidate.partido,
        idade=candidate.idade,
        faixa_etaria=candidate.faixa_etaria,
        origem_territorial=candidate.origem_territorial,
        incumbente=candidate.incumbente,
        biografia_resumida=candidate.biografia_resumida,
        temas_prioritarios=list(candidate.temas_prioritarios),
        temas_secundarios=list(candidate.temas_secundarios),
        historico_eleitoral=list(candidate.historico_eleitoral),
        municipios_base=list(candidate.municipios_base),
        zonas_base=list(candidate.zonas_base),
        observacoes_estrategicas=candidate.observacoes_estrategicas,
    )


def _candidate_options(paths) -> list[CandidateProfileSchema]:
    return [_candidate_to_schema(candidate) for candidate in _registry(paths).list()]


def _selected_candidate_id(paths) -> str | None:
    candidates = _candidate_options(paths)
    if not candidates:
        st.info("Cadastre um candidato na tela de onboarding para liberar rankings, simulacoes e explicacoes.")
        return None
    labels = {f"{item.nome_politico} ({item.candidate_id})": item.candidate_id for item in candidates}
    current = st.session_state.get("decision_candidate_id")
    index = list(labels.values()).index(current) if current in labels.values() else 0
    selected_label = st.selectbox("Candidato", list(labels), index=index, key="decision_candidate_selector")
    candidate_id = labels[selected_label]
    st.session_state["decision_candidate_id"] = candidate_id
    return candidate_id


def _scenario_controls(prefix: str, *, default_top_n: int = 20) -> dict[str, Any]:
    c1, c2, c3, c4 = st.columns(4)
    return {
        "budget_total": float(
            c1.number_input("Orcamento total (R$)", 10000, 5000000, 200000, 10000, key=f"{prefix}_budget")
        ),
        "top_n": int(c2.slider("Top territorios", 5, 100, default_top_n, key=f"{prefix}_top_n")),
        "capacidade_operacional": float(c3.slider("Capacidade operacional", 0.0, 1.0, 0.7, 0.05, key=f"{prefix}_cap")),
        "scenario": c4.selectbox("Cenario", ["hibrido", "conservador", "agressivo"], key=f"{prefix}_scenario"),
    }


def _recommendations_frame(response: AllocationScenarioResponse) -> pd.DataFrame:
    rows = []
    for rec in response.recommendations:
        rows.append(
            {
                "territorio_id": rec.territorio_id,
                "municipio": rec.provenance.get("municipio", ""),
                "zona": rec.provenance.get("zona", ""),
                "cluster": rec.provenance.get("cluster_territorial", ""),
                "tipo": rec.tipo_recomendacao,
                "score": rec.score_prioridade,
                "aderencia_tematica": rec.score_aderencia_tematica,
                "expansao": rec.score_expansao,
                "competicao": rec.score_competicao,
                "eficiencia": rec.score_eficiencia,
                "recurso_sugerido": rec.recurso_sugerido,
                "% orcamento": rec.percentual_orcamento_sugerido,
                "confianca": rec.confidence_score,
                "justificativa": rec.justificativa,
            }
        )
    return pd.DataFrame(rows)


def _score_frame(response: AllocationScenarioResponse) -> pd.DataFrame:
    return pd.DataFrame([score.model_dump() for score in response.scores])


def _territory_ids_from_response(response: AllocationScenarioResponse | None) -> list[str]:
    if response is None:
        return []
    return [rec.territorio_id for rec in response.recommendations]


def render_candidate_onboarding(paths) -> None:
    st.subheader("Onboarding do candidato")
    st.caption("Cadastro estrategico agregado. Nao use dados pessoais de eleitores ou inferencias individuais.")

    candidates = _candidate_options(paths)
    if candidates:
        with st.expander("Candidatos cadastrados", expanded=False):
            st.dataframe(pd.DataFrame([item.model_dump() for item in candidates]), width="stretch", hide_index=True)

    with st.form("candidate_onboarding_form"):
        c1, c2, c3 = st.columns(3)
        candidate_id = c1.text_input(
            "ID do candidato", value=st.session_state.get("decision_candidate_id", "cand_demo")
        )
        nome_politico = c2.text_input("Nome politico", value="Candidato Demo")
        partido = c3.text_input("Partido", value="PARTIDO")
        cargo = c1.text_input("Cargo", value="Prefeito")
        idade = c2.number_input("Idade", min_value=16, max_value=120, value=45)
        faixa_etaria = c3.selectbox("Faixa etaria", ["16-29", "30-44", "45-59", "60+", "nao_informada"], index=2)
        origem_territorial = c1.text_input("Origem territorial", value="SAO PAULO")
        incumbente = c2.checkbox("Incumbente", value=False)
        municipios_base = c3.text_input("Municipios-base", value="SAO PAULO")
        zonas_base = st.text_input("Zonas-base", value="")
        temas_prioritarios = st.text_input("Temas prioritarios", value="saude, educacao, seguranca")
        temas_secundarios = st.text_input("Temas secundarios", value="mobilidade, emprego")
        biografia_resumida = st.text_area("Biografia resumida", value="", height=90)
        observacoes = st.text_area("Observacoes estrategicas", value="", height=90)
        submitted = st.form_submit_button("Salvar candidato", type="primary", width="stretch")

    if submitted:
        try:
            payload = CandidateProfileSchema(
                candidate_id=candidate_id,
                nome_politico=nome_politico,
                cargo=cargo,
                partido=partido,
                idade=int(idade),
                faixa_etaria=faixa_etaria,
                origem_territorial=origem_territorial,
                incumbente=incumbente,
                biografia_resumida=biografia_resumida,
                temas_prioritarios=temas_prioritarios,
                temas_secundarios=temas_secundarios,
                municipios_base=municipios_base,
                zonas_base=zonas_base,
                observacoes_estrategicas=observacoes,
            )
            _registry(paths).upsert(payload)
            st.session_state["decision_candidate_id"] = payload.candidate_id
            st.success("Candidato salvo no registry do tenant.")
        except ValidationError as exc:
            st.error(f"Cadastro invalido: {exc}")


def render_electoral_base_map(paths) -> None:
    st.subheader("Mapa de base eleitoral")
    latest = find_latest_zone_fact(paths)
    st.caption(f"Fonte gold: {latest.name if latest else 'dataset demo'}")
    territories = load_zone_territories(paths)
    if territories.empty:
        st.info("Not found in repo: territorios eleitorais gold")
        return

    municipio_col = "municipio" if "municipio" in territories.columns else None
    filtered = territories.copy()
    if municipio_col:
        options = ["Todos"] + sorted(filtered[municipio_col].dropna().astype(str).unique().tolist())
        selected = st.selectbox("Municipio", options, key="base_map_municipio")
        if selected != "Todos":
            filtered = filtered[filtered[municipio_col].astype(str) == selected]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Territorios", len(filtered))
    c2.metric(
        "Eleitores",
        f"{pd.to_numeric(filtered.get('eleitores_aptos'), errors='coerce').sum():,.0f}"
        if "eleitores_aptos" in filtered
        else "n/d",
    )
    c3.metric(
        "Abstencao media",
        f"{pd.to_numeric(filtered.get('abstencao_pct'), errors='coerce').mean() * 100:.1f}%"
        if "abstencao_pct" in filtered
        else "n/d",
    )
    c4.metric(
        "Qualidade",
        f"{pd.to_numeric(filtered.get('data_quality_score'), errors='coerce').mean() * 100:.0f}%"
        if "data_quality_score" in filtered
        else "n/d",
    )

    if {"lat", "lon"}.issubset(filtered.columns):
        st.map(filtered.rename(columns={"lat": "latitude", "lon": "longitude"}))
    elif {"latitude", "longitude"}.issubset(filtered.columns):
        st.map(filtered)
    else:
        st.info("Not found in repo: coordenadas para mapa interativo. Exibindo tabela territorial.")

    preferred = [
        "territorio_id",
        "municipio",
        "cod_tse_municipio",
        "cod_municipio_ibge",
        "zona_eleitoral",
        "secao",
        "local_votacao",
        "eleitores_aptos",
        "votos_validos",
        "abstencao_pct",
        "competitividade",
        "data_quality_score",
        "join_confidence",
        "source_name",
        "ingestion_run_id",
    ]
    cols = [col for col in preferred if col in filtered.columns]
    st.dataframe(filtered[cols].head(200) if cols else filtered.head(200), width="stretch", hide_index=True)


def render_territorial_ranking(paths) -> None:
    st.subheader("Ranking territorial")
    candidate_id = _selected_candidate_id(paths)
    if candidate_id is None:
        return
    controls = _scenario_controls("ranking", default_top_n=20)
    try:
        serving_result = _serving(paths).territory_ranking(
            tenant_id=paths.tenant_id,
            candidate_id=candidate_id,
            limit=int(controls["top_n"]),
        )
        serving_df = _serving_frame(serving_result)
    except ServingDataNotFoundError:
        serving_df = pd.DataFrame()
    if not serving_df.empty:
        st.caption(f"Fonte: serving_territory_ranking | snapshot={serving_result.snapshot_id or 'latest'}")
        c1, c2, c3 = st.columns(3)
        c1.metric("Territorios", len(serving_df))
        c2.metric(
            "Score maximo",
            f"{pd.to_numeric(serving_df.get('score_prioridade_final'), errors='coerce').max():.2f}"
            if "score_prioridade_final" in serving_df
            else "n/d",
        )
        c3.metric(
            "Confianca media",
            f"{pd.to_numeric(serving_df.get('confidence_score'), errors='coerce').mean() * 100:.0f}%"
            if "confidence_score" in serving_df
            else "n/d",
        )
        st.dataframe(serving_df, width="stretch", hide_index=True)
        if serving_result.warnings:
            with st.expander("Avisos de serving", expanded=False):
                for warning in serving_result.warnings:
                    st.warning(warning)
        return
    if st.button("Atualizar ranking", type="primary", width="stretch", key="ranking_run"):
        with st.spinner("Gerando ranking territorial..."):
            st.session_state["decision_ranking_response"] = _service(paths).list_prioritized_territories(
                candidate_id=candidate_id,
                tenant_id=paths.tenant_id,
                **controls,
            )
    response = st.session_state.get("decision_ranking_response")
    if response is None:
        st.info("Clique em Atualizar ranking para consultar os territorios priorizados.")
        return
    st.metric("Orcamento analisado", f"R$ {response.total_budget:,.0f}")
    st.dataframe(pd.DataFrame([item.model_dump() for item in response.items]), width="stretch", hide_index=True)


def render_municipal_strategy(paths) -> None:
    st.subheader("Analise estrategica por municipio")
    st.caption("Leitura municipal baseada em gold/serving. Campos ausentes ficam marcados como Not found in repo.")
    service = _municipal_strategy(paths)
    municipios = service.municipalities()
    if not municipios:
        st.info("Not found in repo: municipios na camada gold/serving.")
        return
    selected = st.selectbox("Municipio", municipios, key="municipal_strategy_municipio")
    top_n = st.slider("Territorios prioritarios", 5, 50, 15, key="municipal_strategy_top_n")
    view = service.build(selected, top_n=int(top_n))

    metrics = view.metrics
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Territorios", int(metrics.get("territorios") or 0))
    c2.metric(
        "Prioridade max",
        f"{float(metrics['score_prioridade_max']):.2f}" if metrics.get("score_prioridade_max") is not None else "n/d",
    )
    c3.metric(
        "Disputabilidade",
        f"{float(metrics['score_disputabilidade']):.2f}"
        if metrics.get("score_disputabilidade") is not None
        else "Not found",
    )
    c4.metric(
        "Confianca",
        f"{float(metrics['confianca_media']) * 100:.0f}%" if metrics.get("confianca_media") is not None else "n/d",
    )

    d1, d2, d3, d4 = st.columns(4)
    d1.metric(
        "Competitividade",
        f"{float(metrics['competitividade']):.2f}" if metrics.get("competitividade") is not None else "n/d",
    )
    d2.metric("Margem", f"{float(metrics['margem']):.2f}" if metrics.get("margem") is not None else "Not found")
    d3.metric(
        "Volatilidade",
        f"{float(metrics['volatilidade']):.2f}" if metrics.get("volatilidade") is not None else "Not found",
    )
    d4.metric("Partido dominante", str(metrics.get("partido_dominante") or "Not found"))

    h1, h2 = st.columns(2)
    h1.metric(
        "Votos hist. PT",
        f"{float(metrics['votos_hist_pt']):,.0f}" if metrics.get("votos_hist_pt") is not None else "Not found",
    )
    h2.metric(
        "Votos hist. PL",
        f"{float(metrics['votos_hist_pl']):,.0f}" if metrics.get("votos_hist_pl") is not None else "Not found",
    )

    st.markdown("#### Recomendações curtas")
    for item in view.recommendations:
        st.write(f"- {item}")

    st.markdown("#### Ranking de prioridade")
    if view.priority_ranking.empty:
        st.info("Not found in repo: ranking de prioridade para o municipio selecionado.")
    else:
        st.dataframe(view.priority_ranking, width="stretch", hide_index=True)

    with st.expander("Contexto RAG e proveniencia disponivel", expanded=False):
        if view.rag_context:
            st.json(view.rag_context)
        else:
            st.info("Not found in repo: campos de contexto RAG para este municipio.")
        if view.source_paths:
            st.caption("Fontes carregadas")
            for path in view.source_paths:
                st.code(path)
    if view.missing_fields:
        with st.expander("Campos não encontrados na gold", expanded=False):
            st.write(", ".join(view.missing_fields))


def render_budget_simulator(paths) -> None:
    st.subheader("Simulador de orcamento")
    candidate_id = _selected_candidate_id(paths)
    if candidate_id is None:
        return
    controls = _scenario_controls("simulator", default_top_n=15)
    janela = st.slider("Janela temporal (dias)", 7, 180, 45, key="simulator_window")
    try:
        serving_recs = _serving_frame(
            _serving(paths).allocation_recommendations(
                tenant_id=paths.tenant_id,
                candidate_id=candidate_id,
                scenario_id=str(controls["scenario"]),
                limit=int(controls["top_n"]),
            )
        )
    except ServingDataNotFoundError:
        serving_recs = pd.DataFrame()
    if not serving_recs.empty:
        st.caption("Fonte: serving_allocation_recommendations")
        st.dataframe(serving_recs, width="stretch", hide_index=True)
        csv = serving_recs.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Baixar recomendacoes serving",
            data=csv,
            file_name=f"serving_recommendations_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
            mime="text/csv",
            width="stretch",
        )
        return
    if st.button("Gerar cenario de alocacao", type="primary", width="stretch", key="simulator_run"):
        with st.spinner("Calculando cenario..."):
            st.session_state["decision_scenario_response"] = _service(paths).generate_allocation_for_candidate_id(
                candidate_id=candidate_id,
                janela_temporal_dias=int(janela),
                **controls,
            )
    response = st.session_state.get("decision_scenario_response")
    if response is None:
        st.info("Defina os parametros e gere um cenario para comparar alocacao, ROI politico e confianca.")
        return
    c1, c2, c3 = st.columns(3)
    c1.metric("Recomendacoes", len(response.recommendations))
    c2.metric("Evidencias", response.evidence_count)
    c3.metric("Orcamento", f"R$ {response.budget_total:,.0f}")
    st.dataframe(_recommendations_frame(response), width="stretch", hide_index=True)


def render_recommendation_detail(paths) -> None:
    st.subheader("Detalhamento de recomendacao")
    response = st.session_state.get("decision_scenario_response")
    if response is None:
        st.info("Gere um cenario no simulador para abrir o detalhamento de recomendacao.")
        return
    territory_ids = _territory_ids_from_response(response)
    selected = st.selectbox("Territorio", territory_ids, key="recommendation_detail_territory")
    rec = next(item for item in response.recommendations if item.territorio_id == selected)
    score_df = _score_frame(response)
    score_row = score_df[score_df["territorio_id"] == selected]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Score", f"{rec.score_prioridade:.2f}")
    c2.metric("Recurso", f"R$ {rec.recurso_sugerido:,.0f}")
    c3.metric("Confianca", f"{rec.confidence_score * 100:.0f}%")
    c4.metric("Tipo", rec.tipo_recomendacao)
    st.write(rec.justificativa)
    if not score_row.empty:
        st.dataframe(score_row, width="stretch", hide_index=True)
    with st.expander("Fatores positivos", expanded=True):
        for item in rec.fatores_positivos:
            st.write(f"- {item}")
    with st.expander("Fatores contra", expanded=False):
        for item in rec.fatores_contra:
            st.write(f"- {item}")


def render_evidence_and_explanations(paths) -> None:
    st.subheader("Evidencias e justificativas")
    candidate_id = _selected_candidate_id(paths)
    if candidate_id is None:
        return
    response = st.session_state.get("decision_scenario_response")
    territory_ids = _territory_ids_from_response(response)
    if not territory_ids:
        st.info("Gere um cenario no simulador para consultar explicacoes auditaveis.")
        return
    territorio_id = st.selectbox("Territorio", territory_ids, key="evidence_territory")
    scenario = st.selectbox("Cenario da explicacao", ["hibrido", "conservador", "agressivo"], key="evidence_scenario")
    if st.button("Obter explicacao", type="primary", width="stretch", key="evidence_run"):
        with st.spinner("Buscando evidencias e proveniencia..."):
            st.session_state["decision_explanation_response"] = _service(paths).get_recommendation_explanation(
                candidate_id=candidate_id,
                territorio_id=territorio_id,
                tenant_id=paths.tenant_id,
                scenario=scenario,
            )
    explanation = st.session_state.get("decision_explanation_response")
    if explanation is None:
        return
    st.markdown("#### Por que priorizar")
    st.write(explanation.why_prioritized)
    c1, c2 = st.columns(2)
    c1.metric("Confianca", f"{explanation.confidence_score * 100:.0f}%")
    c2.metric("Bases de apoio", len(explanation.supporting_bases))
    st.write(explanation.detailed_justification)
    if explanation.supporting_bases:
        st.markdown("#### Bases sustentadoras")
        st.write("; ".join(explanation.supporting_bases))
    if explanation.evidencias:
        st.markdown("#### Evidencias")
        st.dataframe(
            pd.DataFrame([item.model_dump(mode="json") for item in explanation.evidencias]),
            width="stretch",
            hide_index=True,
        )
    with st.expander("Proveniencia", expanded=False):
        st.json(explanation.provenance)


def render_data_catalog(paths) -> None:
    st.subheader("Catalogo de bases")
    catalog = build_default_catalog()
    rows = [source.model_dump(mode="json") for source in catalog.sources]
    c1, c2, c3 = st.columns(3)
    c1.metric("Versao", catalog.version)
    c2.metric("Fontes", len(rows))
    c3.metric("Tenant", paths.tenant_id)
    readiness = _serving_readiness(paths)
    if not readiness.empty:
        st.markdown("#### Readiness serving")
        r1, r2, r3 = st.columns(3)
        r1.metric("Readiness", f"{float(readiness.iloc[0].get('readiness_score', 0.0)) * 100:.0f}%")
        r2.metric("Territorios ranqueados", int(readiness.iloc[0].get("territories_ranked", 0)))
        r3.metric("Candidatos suportados", int(readiness.iloc[0].get("candidates_supported", 0)))
        st.dataframe(readiness, width="stretch", hide_index=True)
    st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)
    st.caption(f"Atualizado em UI: {datetime.now().strftime('%d/%m/%Y %H:%M')}")


def render_decision_workspace(paths) -> None:
    tabs = st.tabs(
        [
            "Onboarding",
            "Mapa de base",
            "Ranking territorial",
            "Analise municipal",
            "Simulador",
            "Recomendacao",
            "Evidencias",
            "Catalogo",
        ]
    )
    with tabs[0]:
        render_candidate_onboarding(paths)
    with tabs[1]:
        render_electoral_base_map(paths)
    with tabs[2]:
        render_territorial_ranking(paths)
    with tabs[3]:
        render_municipal_strategy(paths)
    with tabs[4]:
        render_budget_simulator(paths)
    with tabs[5]:
        render_recommendation_detail(paths)
    with tabs[6]:
        render_evidence_and_explanations(paths)
    with tabs[7]:
        render_data_catalog(paths)
