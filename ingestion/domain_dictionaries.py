from __future__ import annotations

COLUMN_ALIASES: dict[str, list[str]] = {
    "ano_eleicao": ["ano_eleicao", "ano", "aa_eleicao"],
    "turno": ["turno", "nr_turno"],
    "uf": ["uf", "sg_uf", "sigla_uf"],
    "cod_municipio_tse": ["cod_municipio_tse", "cod_mun_tse", "cd_municipio", "codigo_municipio"],
    "cod_municipio_ibge": ["cod_municipio_ibge", "cod_mun_ibge", "cd_mun", "codmun", "municipio_id_ibge7"],
    "municipio_nome": ["municipio_nome", "municipio", "nm_municipio", "nome_municipio"],
    "zona": ["zona", "nr_zona", "zona_eleitoral"],
    "secao": ["secao", "nr_secao", "secao_eleitoral"],
    "local_votacao": ["local_votacao", "nm_local_votacao", "nome_local_votacao"],
    "candidate_id": ["candidate_id", "id_candidato", "sq_candidato", "sequencial_candidato"],
    "cpf_candidato": ["cpf_candidato", "nr_cpf_candidato"],
    "numero_candidato": ["numero_candidato", "nr_candidato"],
    "partido": ["partido", "sg_partido", "sigla_partido"],
    "cd_setor": ["cd_setor", "setor_censitario", "cod_setor"],
    "data_receita": ["data_receita", "dt_receita", "data_doacao"],
    "data_despesa": ["data_despesa", "dt_despesa", "data_pagamento"],
    "valor_receita": ["valor_receita", "vr_receita", "valor_doacao"],
    "valor_despesa": ["valor_despesa", "vr_despesa", "valor_pago"],
    "cargo": ["cargo", "ds_cargo", "descricao_cargo"],
    "situacao_candidatura": ["situacao_candidatura", "ds_situacao_candidatura", "situacao"],
    "genero": ["genero", "ds_genero"],
    "grau_instrucao": ["grau_instrucao", "ds_grau_instrucao", "escolaridade"],
}

ELECTORAL_CATEGORY_COLUMNS: tuple[str, ...] = (
    "partido",
    "cargo",
    "situacao_candidatura",
    "genero",
    "grau_instrucao",
)

MONEY_COLUMNS: tuple[str, ...] = ("valor_receita", "valor_despesa")

DATE_COLUMNS: tuple[str, ...] = ("data_receita", "data_despesa")

MASTER_KEY_COLUMNS: tuple[str, ...] = (
    "ano_eleicao",
    "uf",
    "cod_municipio_tse",
    "cod_municipio_ibge",
    "municipio_nome",
    "zona",
    "secao",
    "local_votacao",
    "candidate_id",
    "cpf_candidato",
    "numero_candidato",
    "partido",
    "cd_setor",
)

CATEGORY_NORMALIZATION: dict[str, dict[str, str]] = {
    "genero": {
        "MASCULINO": "MASCULINO",
        "FEMININO": "FEMININO",
        "NAO DIVULGAVEL": "NAO_DIVULGAVEL",
        "NAO INFORMADO": "NAO_INFORMADO",
    },
    "situacao_candidatura": {
        "APTO": "APTO",
        "INAPTO": "INAPTO",
        "DEFERIDO": "DEFERIDO",
        "INDEFERIDO": "INDEFERIDO",
        "RENUNCIA": "RENUNCIA",
    },
}
