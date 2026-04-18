# Semantic Layer Registry

- Version: `semantic_registry_v1`
- Entities: `12`
- Metrics: `10`
- Dimensions: `6`

## Entities

| Entity | Canonical table | Primary key | Description |
| --- | --- | --- | --- |
| `candidato` | `gold_candidate_context` | `candidate_id` | Ator politico analisado pela plataforma em nivel agregado. |
| `territorio` | `gold_territory_profile` | `territorio_id` | Unidade territorial agregada para ranking, score e alocacao. |
| `secao_eleitoral` | `gold_territorial_electoral_master_index` | `ano_eleicao, uf, cod_municipio_tse, zona, secao` | Menor unidade eleitoral agregada usada no master index. |
| `local_votacao` | `gold_territorial_electoral_master_index` | `ano_eleicao, uf, cod_municipio_tse, zona, local_votacao` | Local fisico agregado que concentra secoes eleitorais. |
| `municipio` | `gold_territory_profile` | `uf, cod_municipio_tse, cod_municipio_ibge` | Municipio harmonizado entre codigos TSE e IBGE. |
| `cluster_territorial` | `gold_territorial_clusters` | `territorial_cluster_id` | Agrupamento operacional de territorios para leitura executiva. |
| `tema` | `gold_thematic_affinity` | `territorio_id, tema` | Tema publico agregado usado para afinidade territorial e narrativa. |
| `recomendacao` | `gold_allocation_recommendations` | `scenario_id, candidate_id, territorio_id` | Recomendacao agregada de acao e recurso para territorio/candidato/cenario. |
| `cenario` | `gold_allocation_recommendations` | `scenario_id` | Configuracao de simulacao de alocacao. |
| `gasto` | `gold_campaign_finance_efficiency` | `candidate_id` | Resumo financeiro agregado de campanha e eficiencia. |
| `base_eleitoral` | `gold_electoral_base_strength` | `candidate_id, territorio_id` | Forca eleitoral observada ou estimada por candidato e territorio. |
| `concorrencia` | `gold_competition_landscape` | `territorio_id` | Paisagem competitiva agregada por territorio. |

## Metrics

| Metric | Source | Grain | Formula | Consumers |
| --- | --- | --- | --- | --- |
| `forca_base` | `gold_electoral_base_strength` | candidate_id + territorio_id | `base_strength_score` | scoring, ranking, dashboard |
| `potencial_expansao` | `gold_priority_score` | candidate_id + territorio_id | `potencial_expansao_score` | scoring, simulador |
| `intensidade_competitiva` | `gold_competition_landscape` | territorio_id | `competition_score` | scoring, relatorios |
| `aderencia_tematica` | `gold_thematic_affinity` | territorio_id | `AVG(thematic_affinity_score)` | mensagem, copiloto |
| `eficiencia_gasto` | `gold_campaign_finance_efficiency` | candidate_id | `finance_efficiency_score` | simulador, allocation |
| `prioridade_territorial` | `gold_priority_score` | candidate_id + territorio_id | `score_prioridade_final` | ranking, allocation, dashboard |
| `confianca_recomendacao` | `gold_allocation_recommendations` | scenario_id + candidate_id + territorio_id | `COALESCE(confidence_score, score_prioridade_final * data_quality_score)` | copiloto, relatorios |
| `cobertura_territorial` | `gold_territory_profile` | territorio_id | `data_quality_score` | data quality, dashboard |
| `custo_por_voto_estimado` | `gold_campaign_finance_efficiency` | candidate_id | `custo_por_voto_estimado` | simulador, relatorios |
| `share_potencial` | `territorial_recommendation_features` | candidate_id + territorio_id | `historical_vote_share` | feature store, scoring |

## Dimensions

| Dimension | Entity | Source | Description |
| --- | --- | --- | --- |
| `candidate_id` | `candidato` | `gold_candidate_context.candidate_id` | Identificador canonico do candidato. |
| `territorio_id` | `territorio` | `gold_territory_profile.territorio_id` | Identificador canonico de territorio agregado. |
| `municipio_nome` | `municipio` | `gold_territory_profile.municipio_nome` | Nome harmonizado do municipio. |
| `zona` | `territorio` | `gold_territory_profile.zona` | Zona eleitoral padronizada. |
| `tema` | `tema` | `gold_thematic_affinity.tema` | Tema publico agregado. |
| `scenario_id` | `cenario` | `gold_allocation_recommendations.scenario_id` | Identificador do cenario de alocacao. |