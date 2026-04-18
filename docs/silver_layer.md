# Silver Layer

## Objetivo

A camada silver transforma bronze/raw heterogeneo em datasets tipados, deduplicados e harmonizados para cruzamento eleitoral-territorial. Ela nao calcula score final; prepara chaves, tipos, contratos e qualidade para gold.

## Componentes

- `SilverNormalizer`: normaliza colunas, corrige mojibake, limpa nomes territoriais, codigos, datas, dinheiro e categorias.
- `MunicipalCrosswalk`: enriquece TSE com IBGE por codigo exato ou nome normalizado, gerando `join_confidence`.
- `SilverSchemaContract`: contrato Pydantic por dataset silver.
- `SilverSchemaValidator`: valida colunas obrigatorias, ausentes e dataset vazio.
- `SilverDatasetTransformer`: transformer por dataset.
- `SilverDatasetWriter`: grava parquet harmonizado e relatorio de qualidade.

## Chaves Mestres

Padrao silver:

- `ano_eleicao`
- `uf`
- `cod_municipio_tse`
- `cod_municipio_ibge`
- `municipio_nome`
- `zona`
- `secao`
- `local_votacao`
- `candidate_id`
- `cpf_candidato`
- `numero_candidato`
- `partido`
- `cd_setor`

## Regras de Normalizacao

- colunas viram snake_case ASCII;
- strings com mojibake comum (`Ã`, `Â`, `â`, `ð`) passam por tentativa segura de recodificacao;
- municipio ganha `municipio_nome_normalizado` sem acento, uppercase e sem pontuacao;
- `zona` e `secao` recebem padding de 4 digitos;
- `cod_municipio_tse` recebe padding de 5 digitos;
- `cod_municipio_ibge` recebe padding de 7 digitos;
- CPF, quando permitido e pertinente, recebe somente digitos e padding de 11;
- datas mistas sao normalizadas para ISO `YYYY-MM-DD`;
- valores monetarios aceitam formatos brasileiros e viram `Float64`;
- categorias eleitorais sao normalizadas em uppercase ASCII.

## Crosswalk TSE-IBGE

Ordem de matching:

1. `uf + cod_municipio_ibge`: confianca `1.00`
2. `uf + cod_municipio_tse`: confianca `0.98`
3. `uf + municipio_nome_normalizado`: confianca `0.86`, somente se houver match unico
4. sem match: `join_confidence = 0.0`

Todo join aproximado deve permanecer auditavel na gold.

## Auditoria

Toda transformacao gera:

- `source_dataset`
- `source_file`
- `ingestion_timestamp`
- `transform_timestamp`
- `join_confidence`

## Execucao

```powershell
python scripts/run_silver_transform.py `
  --dataset-id tse_resultados_secao `
  --input-path lake/bronze/tse/boletim_urna/2024/SP/raw.csv `
  --crosswalk-path data/crosswalk_tse_ibge.csv
```

O output padrao vai para:

```text
lake/silver/{dataset_id}/{dataset_id}.parquet
lake/silver/{dataset_id}/quality_report.json
```

## Qualidade

O relatorio inclui:

- linhas de entrada e saida;
- duplicatas removidas;
- colunas obrigatorias ausentes;
- contagem de nulos/vazios por coluna obrigatoria;
- erros de schema;
- media de `join_confidence`;
- status final.

## Riscos

- Municipios homonimos exigem UF e, quando possivel, codigo.
- Candidatos e prestacao de contas podem conter dados pessoais publicos; silver deve manter minimizacao e evitar logs com CPF.
- Atribuicao territorial de gastos eleitorais e parcial e nao deve ser tratada como secao/zona sem evidencia.
