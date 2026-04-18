from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

PriorityLevel = Literal["alta", "media", "baixa"]
RecommendationType = Literal[
    "retencao_base",
    "expansao_territorial",
    "presenca_fisica",
    "ativacao_lideranca_local",
    "comunicacao_programatica",
    "reforco_area_competitiva",
]


class CandidateProfileSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    candidate_id: str = Field(min_length=1)
    nome_politico: str = Field(min_length=1)
    cargo: str = Field(min_length=1)
    partido: str = Field(min_length=1)
    idade: int | None = Field(default=None, ge=16, le=120)
    faixa_etaria: str = "nao_informada"
    origem_territorial: str = ""
    incumbente: bool = False
    biografia_resumida: str = ""
    temas_prioritarios: list[str] = Field(default_factory=list)
    temas_secundarios: list[str] = Field(default_factory=list)
    historico_eleitoral: list[dict[str, Any]] = Field(default_factory=list)
    municipios_base: list[str] = Field(default_factory=list)
    zonas_base: list[str] = Field(default_factory=list)
    observacoes_estrategicas: str = ""

    @field_validator("temas_prioritarios", "temas_secundarios", "municipios_base", "zonas_base", mode="before")
    @classmethod
    def normalize_list(cls, value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        return [str(item).strip() for item in value if str(item).strip()]


class CandidateThemeSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    candidate_id: str
    tema: str
    prioridade: PriorityLevel = "media"
    evidencias_publicas: list[str] = Field(default_factory=list)
    legitimidade_percebida: float = Field(default=0.5, ge=0, le=1)
    notas: str = ""


class TerritoryProfileSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ano: int
    uf: str
    cod_municipio_tse: str
    cod_municipio_ibge: str = ""
    municipio: str = ""
    zona: int | None = None
    secao: int | None = None
    local_votacao: str = ""
    setor_censitario: str = ""
    populacao: int | None = None
    domicilios: int | None = None
    densidade: float | None = None
    renda_proxy: float | None = None
    escolaridade_proxy: float | None = None
    faixas_etarias_agregadas: dict[str, float] = Field(default_factory=dict)
    indicadores_tematicos: dict[str, float] = Field(default_factory=dict)
    concorrencia_local: float = Field(default=0.5, ge=0, le=1)
    custo_operacional_estimado: float = Field(default=1.0, ge=0)


class ElectoralResultSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ano: int
    turno: int
    uf: str
    municipio: str
    zona: int | None = None
    secao: int | None = None
    candidate_id: str
    votos_nominais: int = 0
    votos_legenda: int = 0
    total_aptos: int = 0
    comparecimento: float = Field(default=0.0, ge=0, le=1)
    abstencoes: int = 0
    percentual_votos: float = Field(default=0.0, ge=0, le=1)
    ranking_na_secao: int | None = None


class ElectoralBaseStrengthSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    candidate_id: str
    municipio: str
    zona: int | None = None
    secao: int | None = None
    base_strength_score: float = Field(ge=0, le=1)
    retention_score: float = Field(ge=0, le=1)
    expansion_score: float = Field(ge=0, le=1)
    competition_score: float = Field(ge=0, le=1)


class CampaignFinanceSummarySchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    candidate_id: str
    municipio: str
    receita_total: float = Field(default=0.0, ge=0)
    despesa_total: float = Field(default=0.0, ge=0)
    custo_por_voto_estimado: float = Field(default=0.0, ge=0)
    intensidade_financeira: float = Field(default=0.0, ge=0)


class EvidenceRecordSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    evidence_id: str
    entidade_origem: str
    fonte: str
    dataset: str
    chave_registro: str
    descricao: str
    timestamp_ingestao: datetime


class ScoreBreakdownSchema(BaseModel):
    territorio_id: str
    base_eleitoral_score: float = Field(ge=0, le=1)
    afinidade_tematica_score: float = Field(ge=0, le=1)
    potencial_expansao_score: float = Field(ge=0, le=1)
    custo_eficiencia_score: float = Field(ge=0, le=1)
    concorrencia_score: float = Field(ge=0, le=1)
    capacidade_operacional_score: float = Field(ge=0, le=1)
    score_prioridade_final: float = Field(ge=0, le=1)
    explicacoes: dict[str, Any] = Field(default_factory=dict)


class AllocationRecommendationSchema(BaseModel):
    candidate_id: str
    territorio_id: str
    tipo_recomendacao: RecommendationType
    score_prioridade: float = Field(ge=0, le=1)
    score_aderencia_tematica: float = Field(ge=0, le=1)
    score_expansao: float = Field(ge=0, le=1)
    score_competicao: float = Field(ge=0, le=1)
    score_eficiencia: float = Field(ge=0, le=1)
    recurso_sugerido: float = Field(ge=0)
    percentual_orcamento_sugerido: float = Field(ge=0, le=1)
    justificativa: str
    confidence_score: float = Field(ge=0, le=1)
    evidencias: list[EvidenceRecordSchema] = Field(default_factory=list)
    fatores_positivos: list[str] = Field(default_factory=list)
    fatores_contra: list[str] = Field(default_factory=list)
    provenance: dict[str, Any] = Field(default_factory=dict)


class AllocationScenarioRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    candidate: CandidateProfileSchema
    budget_total: float = Field(default=200000.0, gt=0)
    capacidade_operacional: float = Field(default=0.7, ge=0, le=1)
    janela_temporal_dias: int = Field(default=45, ge=1)
    top_n: int = Field(default=20, ge=1, le=200)
    scenario: Literal["conservador", "hibrido", "agressivo"] = "hibrido"


class AllocationScenarioResponse(BaseModel):
    candidate_id: str
    scenario: str
    budget_total: float
    recommendations: list[AllocationRecommendationSchema]
    scores: list[ScoreBreakdownSchema]
    evidence_count: int


class CandidateUpsertResponse(BaseModel):
    candidate: CandidateProfileSchema
    tenant_id: str
    status: Literal["saved"] = "saved"


class CandidateListResponse(BaseModel):
    items: list[CandidateProfileSchema]
    tenant_id: str


class PrioritizedTerritoryItem(BaseModel):
    territorio_id: str
    municipio: str = ""
    zona: int | None = None
    cluster_territorial: str = ""
    tipo_recomendacao: RecommendationType
    score_prioridade: float = Field(ge=0, le=1)
    recurso_sugerido: float = Field(ge=0)
    percentual_orcamento_sugerido: float = Field(ge=0, le=1)
    confidence_score: float = Field(ge=0, le=1)
    justificativa: str


class PrioritizedTerritoriesResponse(BaseModel):
    candidate_id: str
    scenario: str
    items: list[PrioritizedTerritoryItem]
    total_budget: float
    tenant_id: str


class TerritoryScoreResponse(BaseModel):
    candidate_id: str
    territorio_id: str
    score: ScoreBreakdownSchema
    tenant_id: str


class RecommendationExplanationResponse(BaseModel):
    candidate_id: str
    territorio_id: str
    tipo_recomendacao: RecommendationType
    why_prioritized: str
    supporting_bases: list[str]
    confidence_score: float = Field(ge=0, le=1)
    positive_factors: list[str] = Field(default_factory=list)
    counter_factors: list[str] = Field(default_factory=list)
    provenance: dict[str, Any] = Field(default_factory=dict)
    evidencias: list[EvidenceRecordSchema] = Field(default_factory=list)
    detailed_justification: str
    tenant_id: str


class PipelineStatusResponse(BaseModel):
    job: Any
    tenant_id: str


class ServingOutputResponse(BaseModel):
    tenant_id: str
    campaign_id: str = ""
    snapshot_id: str = ""
    output_id: str
    row_count: int
    records: list[dict[str, Any]]
    warnings: list[str] = Field(default_factory=list)
    source_path: str = ""


class ServingManifestResponse(BaseModel):
    tenant_id: str
    manifest: dict[str, Any]
