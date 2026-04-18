# Territorial-Electoral Master Index

## Objetivo

O Master Index e a tabela gold que conecta bases eleitorais, territoriais, censitarias, tematicas e de candidato. Ele nao esconde incerteza: cada linha carrega a estrategia de join, a confianca estimada e a cobertura de fontes.

## Schema Canonico

- `master_record_id`
- `ano_eleicao`
- `uf`
- `cod_municipio_tse`
- `cod_municipio_ibge`
- `municipio_nome`
- `zona`
- `secao`
- `local_votacao`
- `candidate_id`
- `numero_candidato`
- `partido`
- `cd_setor`
- `territorial_cluster_id`
- `join_strategy`
- `join_confidence`
- `source_coverage_score`

## Relacoes Exatas

- Votacao por secao para perfil do eleitorado: `ano_eleicao + uf + cod_municipio_tse + zona + secao`.
- Local de votacao: mesma chave de secao; `local_votacao` e preenchido quando a fonte possuir o dado.
- Candidato: `ano_eleicao + uf + candidate_id`.
- Prestacao de contas: `ano_eleicao + uf + candidate_id`; isso indica cobertura de gasto, nao atribuicao territorial exata.
- Municipio TSE-IBGE por codigo: `cod_municipio_tse` em crosswalk governado.
- Setor censitario exato: somente quando `cd_setor` vem declarado na linha de origem.

## Relacoes Aproximadas ou Inferidas

- Municipio por nome normalizado unico: `uf + municipio_nome_normalizado`; confianca menor.
- Municipio por fuzzy name: similaridade textual acima do threshold; confianca ainda menor.
- Setor censitario por municipio com setor unico: permitido apenas quando a tabela de setores fornecida tem um unico `cd_setor` para o municipio. Em municipios reais com multiplos setores, a linha fica sem setor ate haver geocoding/localizacao.

## Join Confidence

O `join_confidence` e uma media dos componentes aplicados na linha:

- base de resultado por secao: `1.00`
- perfil/local por secao exata: `1.00`
- candidato exato: `1.00`
- prestacao de contas por candidato: `0.92`
- TSE-IBGE por codigo: `0.98`
- nome normalizado unico: `0.86`
- fuzzy name: similaridade reduzida
- setor exato: `1.00`
- setor aproximado por municipio unico: `0.72`
- sem match de setor ou municipio: `0.00`

## Source Coverage Score

O `source_coverage_score` mede quantas familias de fonte cobrem a linha:

1. resultados por secao
2. eleitorado por secao
3. locais de votacao
4. cadastro de candidatos
5. prestacao de contas
6. setores censitarios

O score e `fontes_presentes / 6`.

## Execucao

```powershell
python scripts/build_master_index.py `
  --resultados-secao lake/silver/tse_resultados_secao/tse_resultados_secao.parquet `
  --eleitorado-secao lake/silver/tse_eleitorado_secao/tse_eleitorado_secao.parquet `
  --locais-votacao lake/silver/tse_eleitorado_local_votacao/tse_eleitorado_local_votacao.parquet `
  --candidatos lake/silver/tse_candidatos/tse_candidatos.parquet `
  --prestacao-contas lake/silver/tse_prestacao_contas/tse_prestacao_contas.parquet `
  --setores-censitarios lake/silver/ibge_malha_setores/ibge_malha_setores.parquet `
  --municipio-crosswalk lake/gold/territorial_auxiliar_crosswalk.parquet `
  --dataset-version 2024_sp_v1
```

Saida padrao:

```text
lake/gold/territorial_electoral_master_index/
  territorial_electoral_master_index_{version}.parquet
  territorial_electoral_master_index_{version}_manifest.json
```

## Metricas de Qualidade

O manifest inclui:

- linhas e registros unicos;
- cobertura IBGE;
- cobertura de setor censitario;
- cobertura de candidato;
- media de `join_confidence`;
- media de `source_coverage_score`;
- contagem por `join_strategy`;
- limitacoes explicitas.

## Limitacoes

- Setor censitario exige geocoding ou `cd_setor` explicito; nao e correto forcar setor em municipios com multiplos setores.
- Gasto eleitoral por candidato nao implica gasto por secao ou zona.
- Nome de municipio e fallback, nao fonte primaria de verdade; codigo governado deve prevalecer.
- Fuzzy matching deve ser auditado em recomendacoes e relatorios.
