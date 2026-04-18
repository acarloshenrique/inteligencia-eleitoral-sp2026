from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal

PriorityLevel = Literal["alta", "media", "baixa"]
RecommendationType = Literal[
    "retencao_base",
    "expansao_territorial",
    "presenca_fisica",
    "ativacao_lideranca_local",
    "comunicacao_programatica",
    "reforco_area_competitiva",
]


@dataclass(frozen=True)
class CandidateProfile:
    candidate_id: str
    nome_politico: str
    cargo: str
    partido: str
    idade: int | None = None
    faixa_etaria: str = "nao_informada"
    origem_territorial: str = ""
    incumbente: bool = False
    biografia_resumida: str = ""
    temas_prioritarios: tuple[str, ...] = field(default_factory=tuple)
    temas_secundarios: tuple[str, ...] = field(default_factory=tuple)
    historico_eleitoral: tuple[dict[str, Any], ...] = field(default_factory=tuple)
    municipios_base: tuple[str, ...] = field(default_factory=tuple)
    zonas_base: tuple[str, ...] = field(default_factory=tuple)
    observacoes_estrategicas: str = ""


@dataclass(frozen=True)
class CandidateTheme:
    candidate_id: str
    tema: str
    prioridade: PriorityLevel
    evidencias_publicas: tuple[str, ...] = field(default_factory=tuple)
    legitimidade_percebida: float = 0.5
    notas: str = ""


@dataclass(frozen=True)
class TerritoryProfile:
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
    faixas_etarias_agregadas: dict[str, float] = field(default_factory=dict)
    indicadores_tematicos: dict[str, float] = field(default_factory=dict)
    concorrencia_local: float = 0.5
    custo_operacional_estimado: float = 1.0

    @property
    def territorio_id(self) -> str:
        zona = self.zona if self.zona is not None else "MUN"
        secao = self.secao if self.secao is not None else "ALL"
        return f"{self.ano}:{self.uf}:{self.cod_municipio_tse}:ZE{zona}:S{secao}"


@dataclass(frozen=True)
class ElectoralResult:
    ano: int
    turno: int
    uf: str
    municipio: str
    zona: int | None
    secao: int | None
    candidate_id: str
    votos_nominais: int = 0
    votos_legenda: int = 0
    total_aptos: int = 0
    comparecimento: float = 0.0
    abstencoes: int = 0
    percentual_votos: float = 0.0
    ranking_na_secao: int | None = None


@dataclass(frozen=True)
class ElectoralBaseStrength:
    candidate_id: str
    municipio: str
    zona: int | None
    secao: int | None
    base_strength_score: float
    retention_score: float
    expansion_score: float
    competition_score: float


@dataclass(frozen=True)
class CampaignFinanceSummary:
    candidate_id: str
    municipio: str
    receita_total: float = 0.0
    despesa_total: float = 0.0
    custo_por_voto_estimado: float = 0.0
    intensidade_financeira: float = 0.0


@dataclass(frozen=True)
class EvidenceRecord:
    evidence_id: str
    entidade_origem: str
    fonte: str
    dataset: str
    chave_registro: str
    descricao: str
    timestamp_ingestao: datetime


@dataclass(frozen=True)
class ScoreBreakdown:
    territorio_id: str
    base_eleitoral_score: float
    afinidade_tematica_score: float
    potencial_expansao_score: float
    custo_eficiencia_score: float
    concorrencia_score: float
    capacidade_operacional_score: float
    score_prioridade_final: float
    explicacoes: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class AllocationRecommendation:
    candidate_id: str
    territorio_id: str
    tipo_recomendacao: RecommendationType
    score_prioridade: float
    score_aderencia_tematica: float
    score_expansao: float
    score_competicao: float
    score_eficiencia: float
    recurso_sugerido: float
    percentual_orcamento_sugerido: float
    justificativa: str
    confidence_score: float
    evidencias: tuple[EvidenceRecord, ...] = field(default_factory=tuple)
    fatores_positivos: tuple[str, ...] = field(default_factory=tuple)
    fatores_contra: tuple[str, ...] = field(default_factory=tuple)
    provenance: dict[str, Any] = field(default_factory=dict)
