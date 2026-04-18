from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator, model_validator

SourceTier = Literal[1, 2, 3]
IngestionStatus = Literal["not_started", "available", "ingested", "failed", "manual_review"]
Granularity = Literal["secao", "zona", "municipio", "setor_censitario", "candidato", "campanha", "estado"]
CatalogLayer = Literal["bronze", "silver", "gold"]
Sensitivity = Literal[
    "public_open_data_aggregated",
    "public_open_data_personal",
    "derived_aggregate",
    "campaign_operational_confidential",
    "manual_review_required",
]
ProductCapability = Literal[
    "contexto_candidato",
    "forca_eleitoral",
    "competicao",
    "eficiencia_gasto",
    "tematica_territorial",
    "capacidade_operacional",
]


class DataSourceSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    tier: SourceTier
    fonte: str
    url: HttpUrl | str
    granularidade: Granularity
    periodicidade: str
    formato: str
    chaves_principais: list[str] = Field(default_factory=list)
    prioridade: int = Field(ge=1, le=100)
    status_ingestao: IngestionStatus = "not_started"
    estrategia_normalizacao: str
    lgpd_classification: str = "public_open_data_aggregated"
    notes: str = ""


class DataCatalog(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: str = "decision_catalog_v1"
    sources: list[DataSourceSpec]


class Coverage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    start: str
    end: str
    notes: str = ""


class TechnicalMetadata(BaseModel):
    model_config = ConfigDict(extra="forbid")

    owner_logico: str
    schema_uri: str
    storage_path: str
    formato_canonico: str = "parquet"
    particionamento: list[str] = Field(default_factory=list)
    incremental_key: str | None = None
    lineage_upstream: list[str] = Field(default_factory=list)
    lineage_downstream: list[str] = Field(default_factory=list)


class BusinessMetadata(BaseModel):
    model_config = ConfigDict(extra="forbid")

    finalidade_produto: str
    funcionalidades_suportadas: list[ProductCapability]
    perguntas_respondidas: list[str]
    lacunas_conhecidas: list[str] = Field(default_factory=list)
    documentacao_negocio: str = ""

    @field_validator("funcionalidades_suportadas", "perguntas_respondidas")
    @classmethod
    def _non_empty_business_lists(cls, value: list[str]) -> list[str]:
        if not value:
            raise ValueError("business metadata lists cannot be empty")
        return value


class QualityRule(BaseModel):
    model_config = ConfigDict(extra="forbid")

    rule_id: str
    description: str
    severity: Literal["low", "medium", "high", "critical"]
    check_type: Literal["schema", "completeness", "uniqueness", "referential_integrity", "range", "freshness"]
    target_columns: list[str] = Field(default_factory=list)
    threshold: float | None = Field(default=None, ge=0, le=1)


class DatasetDependency(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dataset_id: str
    dependency_type: Literal["source", "crosswalk", "enrichment", "gold_input", "serving_input"]
    required: bool = True
    join_keys: list[str] = Field(default_factory=list)
    join_confidence: float = Field(ge=0, le=1)
    notes: str = ""


class EnterpriseDataset(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dataset_id: str
    nome: str
    descricao: str
    fonte: str
    url: HttpUrl | str
    formato: str
    camada_alvo: CatalogLayer
    granularidade: Granularity
    periodicidade: str
    chave_primaria: list[str]
    chaves_estrangeiras_possiveis: list[str] = Field(default_factory=list)
    cobertura_temporal: Coverage
    cobertura_geografica: Coverage
    sensibilidade: Sensitivity
    prioridade_produto: int = Field(ge=1, le=100)
    status_ingestao: IngestionStatus
    estrategia_atualizacao: str
    observacoes_join: str
    score_confiabilidade: float = Field(ge=0, le=1)
    tier: SourceTier
    technical_metadata: TechnicalMetadata
    business_metadata: BusinessMetadata
    quality_rules: list[QualityRule]
    dependencies: list[DatasetDependency] = Field(default_factory=list)
    product_capabilities: list[ProductCapability]
    lacunas: list[str] = Field(default_factory=list)
    owner: str = "data-platform"
    lineage_notes: str = ""

    @field_validator("chave_primaria", "quality_rules", "product_capabilities")
    @classmethod
    def _non_empty_required_lists(cls, value: list[object]) -> list[object]:
        if not value:
            raise ValueError("required list cannot be empty")
        return value

    @model_validator(mode="after")
    def _validate_gold_dependencies_and_capabilities(self) -> EnterpriseDataset:
        business_caps = set(self.business_metadata.funcionalidades_suportadas)
        product_caps = set(self.product_capabilities)
        if not product_caps.issubset(business_caps):
            raise ValueError("product_capabilities must be covered by business_metadata.funcionalidades_suportadas")
        if self.camada_alvo == "gold" and not self.dependencies:
            raise ValueError("gold datasets must declare dependencies")
        if self.camada_alvo == "gold" and not self.business_metadata.documentacao_negocio:
            raise ValueError("gold datasets must declare business documentation")
        return self


class PrioritizationRow(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dataset_id: str
    tier: SourceTier
    prioridade_produto: int = Field(ge=1, le=100)
    score_confiabilidade: float = Field(ge=0, le=1)
    status_ingestao: IngestionStatus
    capacidades_produto: list[ProductCapability]
    impacto_comercial: Literal["critical", "high", "medium", "low"]
    lacunas_bloqueantes: list[str] = Field(default_factory=list)


class EnterpriseDataCatalog(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: str = "enterprise_data_catalog_v1"
    generated_by: str = "data_catalog.enterprise_registry"
    datasets: list[EnterpriseDataset]

    @field_validator("datasets")
    @classmethod
    def _unique_dataset_ids(cls, value: list[EnterpriseDataset]) -> list[EnterpriseDataset]:
        dataset_ids = [dataset.dataset_id for dataset in value]
        if len(dataset_ids) != len(set(dataset_ids)):
            raise ValueError("dataset_id values must be unique")
        return value
