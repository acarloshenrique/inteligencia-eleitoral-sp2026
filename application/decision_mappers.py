from __future__ import annotations

from api.decision_contracts import (
    AllocationRecommendationSchema,
    CampaignFinanceSummarySchema,
    CandidateProfileSchema,
    CandidateThemeSchema,
    ElectoralBaseStrengthSchema,
    ElectoralResultSchema,
    EvidenceRecordSchema,
    TerritoryProfileSchema,
)
from domain.decision_models import (
    AllocationRecommendation,
    CampaignFinanceSummary,
    CandidateProfile,
    CandidateTheme,
    ElectoralBaseStrength,
    ElectoralResult,
    EvidenceRecord,
    TerritoryProfile,
)


def candidate_profile_to_domain(schema: CandidateProfileSchema) -> CandidateProfile:
    return CandidateProfile(
        candidate_id=schema.candidate_id,
        nome_politico=schema.nome_politico,
        cargo=schema.cargo,
        partido=schema.partido,
        idade=schema.idade,
        faixa_etaria=schema.faixa_etaria,
        origem_territorial=schema.origem_territorial,
        incumbente=schema.incumbente,
        biografia_resumida=schema.biografia_resumida,
        temas_prioritarios=tuple(schema.temas_prioritarios),
        temas_secundarios=tuple(schema.temas_secundarios),
        historico_eleitoral=tuple(schema.historico_eleitoral),
        municipios_base=tuple(schema.municipios_base),
        zonas_base=tuple(schema.zonas_base),
        observacoes_estrategicas=schema.observacoes_estrategicas,
    )


def candidate_theme_to_domain(schema: CandidateThemeSchema) -> CandidateTheme:
    return CandidateTheme(
        candidate_id=schema.candidate_id,
        tema=schema.tema,
        prioridade=schema.prioridade,
        evidencias_publicas=tuple(schema.evidencias_publicas),
        legitimidade_percebida=schema.legitimidade_percebida,
        notas=schema.notas,
    )


def territory_profile_to_domain(schema: TerritoryProfileSchema) -> TerritoryProfile:
    return TerritoryProfile(**schema.model_dump())


def electoral_result_to_domain(schema: ElectoralResultSchema) -> ElectoralResult:
    return ElectoralResult(**schema.model_dump())


def electoral_base_strength_to_domain(schema: ElectoralBaseStrengthSchema) -> ElectoralBaseStrength:
    return ElectoralBaseStrength(**schema.model_dump())


def campaign_finance_summary_to_domain(schema: CampaignFinanceSummarySchema) -> CampaignFinanceSummary:
    return CampaignFinanceSummary(**schema.model_dump())


def evidence_to_schema(evidence: EvidenceRecord) -> EvidenceRecordSchema:
    return EvidenceRecordSchema(
        evidence_id=evidence.evidence_id,
        entidade_origem=evidence.entidade_origem,
        fonte=evidence.fonte,
        dataset=evidence.dataset,
        chave_registro=evidence.chave_registro,
        descricao=evidence.descricao,
        timestamp_ingestao=evidence.timestamp_ingestao,
    )


def allocation_recommendation_to_schema(rec: AllocationRecommendation) -> AllocationRecommendationSchema:
    return AllocationRecommendationSchema(
        candidate_id=rec.candidate_id,
        territorio_id=rec.territorio_id,
        tipo_recomendacao=rec.tipo_recomendacao,
        score_prioridade=rec.score_prioridade,
        score_aderencia_tematica=rec.score_aderencia_tematica,
        score_expansao=rec.score_expansao,
        score_competicao=rec.score_competicao,
        score_eficiencia=rec.score_eficiencia,
        recurso_sugerido=rec.recurso_sugerido,
        percentual_orcamento_sugerido=rec.percentual_orcamento_sugerido,
        justificativa=rec.justificativa,
        confidence_score=rec.confidence_score,
        evidencias=[evidence_to_schema(evidence) for evidence in rec.evidencias],
        fatores_positivos=list(rec.fatores_positivos),
        fatores_contra=list(rec.fatores_contra),
        provenance=rec.provenance,
    )
