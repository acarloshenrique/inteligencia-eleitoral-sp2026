# Pipeline Bronze / Silver / Gold

Implementacao de arquitetura medallion para dados eleitorais e contexto.

## Bronze

Dados brutos por fonte com versionamento em run:

- `source`
- `dt_coleta_utc`
- `hash_sha256`
- `path`
- `incremental_status` (`loaded_changed` ou `skipped_unchanged`)

Fontes gravadas no run:

- base eleitoral (`parquet`)
- mapeamento TSE/IBGE (`csv`)
- socioeconomico (`csv`, opcional)
- resultados por secao (`csv`, opcional)

Politica:

- carga incremental por hash da fonte (sem full reload quando nao houver alteracao)
- espelhamento particionado em lake parquet por `fonte/ano/uf`

## Silver

Dados padronizados por contrato:

- tipagem e coercao de campos temporais (`ano`, `mes`, `turno`)
- normalizacao de nomes/codigos (`municipio_norm`, `municipio_id_ibge7`)
- deduplicacao por chave de negocio
- datasets:
  - `dim_municipio`
  - `dim_municipio_alias`
  - `fato_eleitoral_municipio`
  - `fato_eleitoral_secao`
  - `dim_contexto_socioeconomico`
  - `dim_ibge_indicadores`
  - `dim_seade_indicadores`
  - `fato_fiscal_municipio`

Armazenamento:

- parquet particionado por `fonte/ano/uf`
- joins pesados e agregacoes executados via DuckDB

## Gold

Marts analiticos para app/RAG:

- `mart_municipio_eleitoral`
- `mart_tendencia_turno`
- `mart_contexto_socioeconomico`
- `mart_potencial_eleitoral_social` (TSE x IBGE pop/renda/educacao)
- `mart_priorizacao_territorial_sp` (TSE x SEADE IPVS/emprego/saude)
- `mart_sensibilidade_investimento_publico` (TSE x transferencias/emendas)

Cada mart e publicado em `data/outputs/estado_sessao/` e registrado no catalogo.
Todas as agregacoes sao em nivel municipio com janela temporal fixa (padrao: ultimos 3 ciclos eleitorais).

Serving/cache:

- materializacao em `outputs/serving/serving.duckdb`
- tabela `query_cache` para consultas recorrentes
- indices em chaves de consulta (`municipio_id_ibge7`, `canonical_key` quando aplicavel)
- `ANALYZE` para estatisticas de otimizacao

## Contratos e Qualidade

- contratos de entrada por fonte com Pydantic (`infrastructure/source_contracts.py`)
- testes de contrato em CI (`pytest tests/contract -m contract`)
- metricas obrigatorias no manifest:
  - `%join_success`
  - `%null_critico`
  - atraso de atualizacao por fonte (dias)
  - drift score (comparando com versao anterior)

## LGPD

- minimizacao por finalidade nos marts materializados
- anonimização automatica quando colunas pessoais forem detectadas
- politica de retencao aplicada por `RETENTION_DAYS` sobre pipeline/lake/serving

## Execucao

```bash
python scripts/run_medallion_pipeline.py \
  --mapping-csv data/open_data/raw/municipios_tse_ibge.csv \
  --socio-csv data/open_data/raw/indicadores_municipais.csv \
  --secao-csv data/open_data/raw/resultados_secao.csv \
  --ibge-csv data/open_data/raw/ibge_pop_renda_educacao.csv \
  --seade-csv data/open_data/raw/seade_ipvs_emprego_saude.csv \
  --fiscal-csv data/open_data/raw/transparencia_transferencias_emendas.csv \
  --window-cycles 3
```
