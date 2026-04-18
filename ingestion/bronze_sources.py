from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

from ingestion.bronze import BaseIngestionJob, BronzeIngestionRequest, IngestionReport


class BronzeDatasetDefinition(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dataset_id: str
    source: str
    dataset_name: str
    formato: str
    url_template: str
    reference_period_template: str = "{ano}"
    requires_uf: bool = True
    supports_municipio: bool = False
    metadata: dict[str, str] = Field(default_factory=dict)

    def build_request(
        self,
        *,
        ano: int,
        uf: str | None = None,
        municipio: str | None = None,
        local_path: Path | None = None,
        expected_sha256: str | None = None,
        extra_metadata: dict[str, str] | None = None,
    ) -> BronzeIngestionRequest:
        if self.requires_uf and not uf:
            raise ValueError(f"dataset {self.dataset_id} requires UF")
        metadata = dict(self.metadata)
        metadata.update(extra_metadata or {})
        return BronzeIngestionRequest(
            dataset_id=self.dataset_id,
            source=self.source,
            source_url=self.url_template.format(ano=ano, uf=(uf or "BR").upper(), municipio=municipio or ""),
            formato=self.formato,
            reference_period=self.reference_period_template.format(ano=ano, uf=(uf or "BR").upper()),
            ano=ano,
            uf=(uf or "BR").upper(),
            municipio=municipio,
            local_path=local_path,
            expected_sha256=expected_sha256,
            metadata=metadata,
        )


TSE_BRONZE_DATASETS: dict[str, BronzeDatasetDefinition] = {
    "boletim_urna": BronzeDatasetDefinition(
        dataset_id="boletim_urna",
        source="tse",
        dataset_name="Boletim de urna",
        formato="zip",
        url_template="https://cdn.tse.jus.br/estatistica/sead/odsele/buweb/bweb_{ano}_{uf}.zip",
        metadata={"domain": "electoral_official", "granularity": "secao"},
    ),
    "eleitorado_secao": BronzeDatasetDefinition(
        dataset_id="eleitorado_secao",
        source="tse",
        dataset_name="Perfil do eleitorado por secao",
        formato="zip",
        url_template="https://cdn.tse.jus.br/estatistica/sead/odsele/perfil_eleitorado/perfil_eleitorado_{ano}.zip",
        requires_uf=False,
        metadata={"domain": "electoral_official", "granularity": "secao"},
    ),
    "eleitorado_local_votacao": BronzeDatasetDefinition(
        dataset_id="eleitorado_local_votacao",
        source="tse",
        dataset_name="Eleitorado por local de votacao",
        formato="zip",
        url_template="https://cdn.tse.jus.br/estatistica/sead/odsele/eleitorado_locais_votacao/eleitorado_local_votacao_{ano}_{uf}.zip",
        metadata={"domain": "electoral_official", "granularity": "local_votacao"},
    ),
    "candidatos": BronzeDatasetDefinition(
        dataset_id="candidatos",
        source="tse",
        dataset_name="Cadastro de candidatos",
        formato="zip",
        url_template="https://cdn.tse.jus.br/estatistica/sead/odsele/consulta_cand/consulta_cand_{ano}.zip",
        requires_uf=False,
        metadata={"domain": "electoral_official", "granularity": "candidato"},
    ),
    "prestacao_contas": BronzeDatasetDefinition(
        dataset_id="prestacao_contas",
        source="tse",
        dataset_name="Prestacao de contas",
        formato="zip",
        url_template="https://cdn.tse.jus.br/estatistica/sead/odsele/prestacao_contas/prestacao_contas_{ano}.zip",
        requires_uf=False,
        metadata={"domain": "campaign_finance", "granularity": "candidato"},
    ),
}


IBGE_BRONZE_DATASETS: dict[str, BronzeDatasetDefinition] = {
    "malha_setores": BronzeDatasetDefinition(
        dataset_id="malha_setores",
        source="ibge",
        dataset_name="Malha de setores censitarios",
        formato="zip",
        url_template="https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/Agregados_por_Setores_Censitarios/malha_com_atributos/setores/shp/UF/{uf}/{uf}_setores_CD2022.zip",
        metadata={"domain": "territorial", "granularity": "setor_censitario"},
    ),
    "agregados_censo": BronzeDatasetDefinition(
        dataset_id="agregados_censo",
        source="ibge",
        dataset_name="Agregados censitarios",
        formato="zip",
        url_template="https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/Agregados_por_Setores_Censitarios/Agregados_por_Setores_Censitarios_{uf}.zip",
        metadata={"domain": "socioeconomic", "granularity": "setor_censitario"},
    ),
}


STATE_THEME_BRONZE_DATASETS: dict[str, BronzeDatasetDefinition] = {
    "ssp_sp_criminalidade": BronzeDatasetDefinition(
        dataset_id="ssp_sp_criminalidade",
        source="ssp_sp",
        dataset_name="Estatisticas criminais SSP-SP",
        formato="xlsx",
        url_template="https://www.ssp.sp.gov.br/estatistica/dados.aspx",
        requires_uf=False,
        metadata={"domain": "public_security", "granularity": "municipio", "connector_status": "prepared"},
    ),
    "cnes_municipio": BronzeDatasetDefinition(
        dataset_id="cnes_municipio",
        source="datasus",
        dataset_name="CNES por municipio",
        formato="dbc",
        url_template="https://datasus.saude.gov.br/transferencia-de-arquivos/",
        requires_uf=False,
        metadata={"domain": "health", "granularity": "municipio", "connector_status": "prepared"},
    ),
}


ALL_BRONZE_DATASETS: dict[str, BronzeDatasetDefinition] = {
    **{f"tse.{key}": value for key, value in TSE_BRONZE_DATASETS.items()},
    **{f"ibge.{key}": value for key, value in IBGE_BRONZE_DATASETS.items()},
    **{f"theme.{key}": value for key, value in STATE_THEME_BRONZE_DATASETS.items()},
}


class TSEBronzeIngestionJob(BaseIngestionJob):
    def run_dataset(
        self,
        dataset: str,
        *,
        ano: int,
        uf: str | None = None,
        local_path: Path | None = None,
        expected_sha256: str | None = None,
    ) -> IngestionReport:
        definition = TSE_BRONZE_DATASETS[dataset]
        request = definition.build_request(ano=ano, uf=uf, local_path=local_path, expected_sha256=expected_sha256)
        return self.run(request)


class IBGEBronzeIngestionJob(BaseIngestionJob):
    def run_dataset(
        self,
        dataset: str,
        *,
        ano: int,
        uf: str,
        local_path: Path | None = None,
        expected_sha256: str | None = None,
    ) -> IngestionReport:
        definition = IBGE_BRONZE_DATASETS[dataset]
        request = definition.build_request(ano=ano, uf=uf, local_path=local_path, expected_sha256=expected_sha256)
        return self.run(request)


class ExtensibleBronzeIngestionJob(BaseIngestionJob):
    def run_definition(
        self,
        definition: BronzeDatasetDefinition,
        *,
        ano: int,
        uf: str | None = None,
        municipio: str | None = None,
        local_path: Path | None = None,
        expected_sha256: str | None = None,
    ) -> IngestionReport:
        request = definition.build_request(
            ano=ano,
            uf=uf,
            municipio=municipio,
            local_path=local_path,
            expected_sha256=expected_sha256,
        )
        return self.run(request)
