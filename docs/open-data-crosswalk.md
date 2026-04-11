# Open Data Crosswalk

Este fluxo adiciona camada de enriquecimento de dados abertos para o ranking municipal.

## Objetivo

- Padronizar chave municipal para cruzamentos: `municipio_id_ibge7 + ano + mes + turno`.
- Enriquecer `df_mun` com dimensao canonica TSE/IBGE e indicadores socioeconomicos.
- Materializar `dim_territorio` como fonte unica de verdade para municipio, zona e secao.
- Publicar dataset enriquecido com versionamento e metricas de match.

## Entradas

- Base eleitoral: `df_mun_*.parquet`
- Mapping TSE/IBGE: CSV com colunas equivalentes a:
  - `codigo_tse`
  - `codigo_ibge`
  - `nome_municipio`
- Socioeconomico (opcional): CSV com `codigo_ibge` e indicadores.

## Execucao

Sincronizar dados abertos com cache incremental (ETag/Last-Modified):

```bash
python scripts/sync_open_data_assets.py \
  --asset "mapping_tse_ibge|https://exemplo/mapping.csv|municipios_tse_ibge.csv" \
  --asset "ibge_socio|https://exemplo/indicadores.csv|indicadores_municipais.csv"
```

Executar o cruzamento:

```bash
python scripts/run_open_data_crosswalk.py \
  --mapping-csv data/open_data/raw/municipios_tse_ibge.csv \
  --socio-csv data/open_data/raw/indicadores_municipais.csv
```

## Saidas

- `data/outputs/estado_sessao/df_mun_enriched_<run_id>.parquet`
- `data/outputs/estado_sessao/dim_municipio_<run_id>.parquet`
- `data/outputs/estado_sessao/dim_municipio_aliases_<run_id>.parquet`
- `lake/silver/dim_territorio`
- `lake/gold/dim_territorio`
- `data/outputs/pipeline_runs/open_data_v1/<run_id>/manifest.json`
- Catalogo atualizado em `data/outputs/catalog/`

## Dimensao canonica territorial

Campos minimos de `dim_territorio`:

- `territorio_id`
- `cod_tse_municipio`
- `cod_ibge_municipio`
- `uf`
- `nome_padronizado`
- `zona_eleitoral`
- `secao_eleitoral`
- `latitude`
- `longitude`
- `geohash`
- `vigencia_inicio`
- `vigencia_fim`

## Qualidade minima sugerida

- `join_rate` >= 0.97
- `% codigo_ibge nulo` <= 0.5%
- rejeitar carga quando colunas canonicas estiverem ausentes
