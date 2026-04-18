# Lakehouse eleitoral production-ready

## Diagnostico da estrutura atual

O projeto ja possuia pipelines medallion em `ingestion/` e contratos em `data_catalog/`, com escrita em `data_lake/bronze`, `data_lake/silver`, `data_lake/gold`, `catalog` e `features`. Essa base era suficiente para ingestao inicial, mas ainda misturava catalogo operacional, outputs de produto e artefatos de execucao. A nova camada `lakehouse/` formaliza o Data Lake como ativo central do produto e cria o caminho canonico `/lake`.

## Arquitetura proposta

```text
/lake
  /bronze    dados brutos preservados, hash, evidencia e origem
  /silver    dados limpos, tipados, deduplicados e harmonizados
  /gold      fatos e dimensoes analiticas consolidadas
  /semantic  metricas canonicas, scores, entidades e views de negocio
  /serving   outputs prontos para API, UI e recommendation engine
  /catalog   contratos centrais de datasets
  /manifests manifestos por execucao/camada/dataset/versao
  /lineage   lineage de transformacoes, inputs, outputs e regra de negocio
  /duckdb    banco DuckDB local para exploracao e consolidacao analitica
```

A compatibilidade com `data_lake/` foi preservada para os pipelines existentes. Novos componentes devem usar `LakehouseConfig.from_paths(paths)`, que aponta para `paths.lakehouse_root` (`/lake`).

## Convencao de nomes

Formato canonico de escrita:

```text
/lake/{layer}/{entity}/{dataset_id}/dataset_version={version}/{partition=value}/{dataset_id}.parquet
```

Exemplo:

```text
/lake/gold/territory/gold_fact_territorio_eleitoral/dataset_version=v2026/ano_eleicao=2024/sigla_uf=sp/gold_fact_territorio_eleitoral.parquet
```

Manifestos:

```text
/lake/manifests/{layer}/{dataset_id}/{dataset_version}/{run_id}.json
```

Lineage:

```text
/lake/lineage/{layer}/{dataset_id}/{dataset_version}/{run_id}.json
```

## Taxonomia de entidades e fatos

Entidades principais:

- `territory`: municipio, zona, secao, local de votacao e setor censitario.
- `candidate`: candidato, partido, cargo, historico e contexto estrategico.
- `electoral_result`: votos, comparecimento, abstencao e ranking por secao/zona.
- `campaign_finance`: receita, despesa, custo por voto e intensidade financeira.
- `theme`: tema, legitimidade, aderencia territorial e evidencias publicas.
- `score`: scores modulares e score final de prioridade.
- `allocation_recommendation`: ranking, verba sugerida, acao, justificativa e confidence.
- `evidence`: fonte, dataset, chave, timestamp, qualidade e proveniencia.

Fatos e dimensoes canonicas:

- `fact_electoral_result_section`
- `fact_campaign_finance_candidate_territory`
- `fact_social_paid_media_territory`
- `fact_allocation_recommendation`
- `dim_territory_master`
- `dim_candidate_profile`
- `dim_theme`
- `dim_time_electoral_phase`
- `metric_priority_score_components`

## Metadados obrigatorios por dataset

Todo contrato deve declarar:

- `dataset_id`, `layer`, `owner`, `entity`, `fact_or_dimension`
- `business_description` e `business_documentation`
- `source_name`, `source_url`, `schema_version`, `dataset_version`
- `granularity`, `primary_key`, `schema`, `required_columns`
- `partition_policy`, `incremental_strategy`
- `coverage`, `quality_rules`, `lineage_inputs`
- `lgpd_classification`

## Politica de particionamento

- Bronze: particionar por `ANO_ELEICAO` e `SIGLA_UF` quando a fonte permitir; preservar arquivo original.
- Silver: particionar por ciclo eleitoral, UF e dominio quando houver volume.
- Gold: particionar por `ANO_ELEICAO` e `SIGLA_UF`; usar merge por particao.
- Semantic: particionar por `scenario`, `score_version` ou `metric_version`.
- Serving: particionar por `scenario`, `tenant_id` e janela de geracao quando aplicavel.

## Incremental load

Estrategias aceitas:

- `snapshot`: substitui versao completa, adequada para cadastros pequenos.
- `append`: adiciona novos arquivos mantendo historico imutavel.
- `upsert_by_key`: atualiza por chave primaria declarada.
- `merge_by_partition`: reprocessa particoes completas, ideal para ano/UF.

Toda carga incremental deve registrar `run_id`, hash, origem, cobertura e quality report.

## Data contracts por camada

- Bronze: contrato minimo para verificar chaves esperadas, fonte e hash; nao exige typing completo.
- Silver: contrato estrito de colunas, tipos, chave primaria e deduplicacao.
- Gold: contrato de negocio com granularidade, joins documentados e coverage.
- Semantic: contrato de metricas canonicas com definicao de score e versionamento de pesos.
- Serving: contrato de consumo para API/UI com confidencialidade operacional e evidencias obrigatorias.

## LGPD e seguranca

A plataforma deve operar em nivel agregado territorial. Dados pessoais publicos de candidatos podem existir em contratos especificos, mas dados de eleitores nao devem ser armazenados em nivel individual. Logs, manifestos e lineage devem evitar CPF, titulo eleitoral, telefone, email ou identificadores pessoais de eleitores.

## Arquivos implementados

- `lakehouse/contracts.py`: contratos Pydantic do lakehouse.
- `lakehouse/config.py`: layout, naming e configuracao fisica.
- `lakehouse/manifest.py`: manifestos, hashes e lineage.
- `lakehouse/base.py`: classes base de ingestao e transformacao.
- `lakehouse/registry.py`: catalogo central de datasets eleitorais.
- `lake/catalog/datasets.json`: manifesto declarativo do catalogo central.
