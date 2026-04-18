from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

SilverDatasetKind = Literal[
    "tse_resultados_secao",
    "tse_eleitorado_secao",
    "tse_eleitorado_local_votacao",
    "tse_candidatos",
    "tse_prestacao_contas",
    "ibge_malha_setores",
    "ibge_agregados_censo",
    "generic",
]


class SilverSchemaContract(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dataset_id: str
    kind: SilverDatasetKind = "generic"
    required_columns: list[str]
    primary_key: list[str]
    nullable_columns: list[str] = Field(default_factory=list)
    strong_types: dict[str, str] = Field(default_factory=dict)
    description: str = ""

    @field_validator("required_columns", "primary_key")
    @classmethod
    def non_empty(cls, value: list[str]) -> list[str]:
        if not value:
            raise ValueError("contract lists cannot be empty")
        return value


SILVER_CONTRACTS: dict[str, SilverSchemaContract] = {
    "tse_resultados_secao": SilverSchemaContract(
        dataset_id="tse_resultados_secao",
        kind="tse_resultados_secao",
        required_columns=[
            "ano_eleicao",
            "uf",
            "cod_municipio_tse",
            "municipio_nome",
            "zona",
            "secao",
            "candidate_id",
            "numero_candidato",
            "partido",
        ],
        primary_key=["ano_eleicao", "uf", "cod_municipio_tse", "zona", "secao", "candidate_id"],
        strong_types={"ano_eleicao": "Int64", "zona": "string", "secao": "string", "candidate_id": "string"},
        description="Resultados eleitorais harmonizados por secao e candidato.",
    ),
    "tse_eleitorado_secao": SilverSchemaContract(
        dataset_id="tse_eleitorado_secao",
        kind="tse_eleitorado_secao",
        required_columns=["ano_eleicao", "uf", "cod_municipio_tse", "municipio_nome", "zona", "secao"],
        primary_key=["ano_eleicao", "uf", "cod_municipio_tse", "zona", "secao"],
        strong_types={"ano_eleicao": "Int64", "zona": "string", "secao": "string"},
        description="Perfil agregado do eleitorado por secao.",
    ),
    "tse_eleitorado_local_votacao": SilverSchemaContract(
        dataset_id="tse_eleitorado_local_votacao",
        kind="tse_eleitorado_local_votacao",
        required_columns=[
            "ano_eleicao",
            "uf",
            "cod_municipio_tse",
            "municipio_nome",
            "zona",
            "secao",
            "local_votacao",
        ],
        primary_key=["ano_eleicao", "uf", "cod_municipio_tse", "zona", "secao", "local_votacao"],
        strong_types={"ano_eleicao": "Int64", "zona": "string", "secao": "string", "local_votacao": "string"},
        description="Locais de votacao e secoes associadas.",
    ),
    "tse_candidatos": SilverSchemaContract(
        dataset_id="tse_candidatos",
        kind="tse_candidatos",
        required_columns=["ano_eleicao", "uf", "candidate_id", "numero_candidato", "partido", "cargo"],
        primary_key=["ano_eleicao", "uf", "candidate_id"],
        nullable_columns=["cpf_candidato"],
        strong_types={"ano_eleicao": "Int64", "candidate_id": "string", "numero_candidato": "string"},
        description="Cadastro de candidatos harmonizado para Candidate Context Engine.",
    ),
    "tse_prestacao_contas": SilverSchemaContract(
        dataset_id="tse_prestacao_contas",
        kind="tse_prestacao_contas",
        required_columns=["ano_eleicao", "uf", "candidate_id", "partido"],
        primary_key=["ano_eleicao", "uf", "candidate_id", "partido"],
        nullable_columns=["valor_receita", "valor_despesa", "data_receita", "data_despesa"],
        strong_types={"ano_eleicao": "Int64", "candidate_id": "string"},
        description="Prestacao de contas com valores monetarios normalizados.",
    ),
    "ibge_malha_setores": SilverSchemaContract(
        dataset_id="ibge_malha_setores",
        kind="ibge_malha_setores",
        required_columns=["cod_municipio_ibge", "municipio_nome", "cd_setor"],
        primary_key=["cd_setor"],
        strong_types={"cod_municipio_ibge": "string", "cd_setor": "string"},
        description="Malha de setores censitarios com codigos IBGE harmonizados.",
    ),
    "ibge_agregados_censo": SilverSchemaContract(
        dataset_id="ibge_agregados_censo",
        kind="ibge_agregados_censo",
        required_columns=["cod_municipio_ibge", "municipio_nome", "cd_setor"],
        primary_key=["cd_setor"],
        strong_types={"cod_municipio_ibge": "string", "cd_setor": "string"},
        description="Agregados censitarios por setor.",
    ),
}


def contract_for(dataset_id: str) -> SilverSchemaContract:
    return SILVER_CONTRACTS.get(
        dataset_id,
        SilverSchemaContract(
            dataset_id=dataset_id,
            required_columns=["source_dataset"],
            primary_key=["source_dataset"],
            description="Generic silver contract.",
        ),
    )
