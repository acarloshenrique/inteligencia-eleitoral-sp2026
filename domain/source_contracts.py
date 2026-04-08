from __future__ import annotations

from pydantic import BaseModel, Field


class BaseEleitoralRow(BaseModel):
    municipio: str = Field(min_length=1)
    ranking_final: float = Field(ge=0)
    ano: int | None = None
    mes: int | None = None
    turno: int | None = None


class MappingTseIbgeRow(BaseModel):
    codigo_tse: str | int = Field()
    codigo_ibge: str | int = Field()
    nome_municipio: str = Field(min_length=1)


class SecaoResultadoRow(BaseModel):
    municipio: str = Field(min_length=1)
    zona: int | None = None
    secao: int | None = None
    votos_validos: float | None = Field(default=None, ge=0)
    ano: int | None = None
    turno: int | None = None


class IbgeSocioRow(BaseModel):
    codigo_ibge: str | int = Field()
    pop_total: float | None = Field(default=None, ge=0)
    renda_media: float | None = Field(default=None, ge=0)
    educacao_indice: float | None = Field(default=None, ge=0)


class SeadeRow(BaseModel):
    codigo_ibge: str | int = Field()
    ipvs: float | None = None
    emprego_formal: float | None = None
    indice_saude: float | None = None


class FiscalRow(BaseModel):
    codigo_ibge: str | int = Field()
    ano: int | None = None
    transferencias: float | None = Field(default=None, ge=0)
    emendas: float | None = Field(default=None, ge=0)
