# Analytical Feature Store

## Objetivo

A feature store separa feature engineering do scoring final. Ela cria features reutilizaveis para score eleitoral, contexto do candidato, simuladores e recomendacao de alocacao.

## Familias

| Familia | Exemplos |
| --- | --- |
| `base_eleitoral` | share historico, concentracao territorial, volatilidade, retencao, reduto |
| `competicao` | competidores relevantes, fragmentacao, intensidade competitiva, incumbencia |
| `territorial` | densidade proxy, urbanizacao proxy, presenca institucional, indicadores sociais |
| `tematica` | aderencia candidato-territorio, coerencia tematica, saturacao competitiva |
| `eficiencia` | custo por voto, elasticidade gasto-resultado, intensidade financeira |
| `operacional` | proximidade de base, complexidade logistica, tamanho, centralidade de local |

## Principios

- Toda feature tem definicao, grain, lineage, versao e politica de recomputacao.
- O pipeline calcula features; scoring apenas consome.
- Recomputacao e feita por `feature_version`.
- Outputs sao parquet e, quando DuckDB esta disponivel, tambem uma base local de consulta.

## Inputs Esperados

O pipeline aceita um dicionario de marts gold:

- `gold_electoral_base_strength`
- `gold_competition_landscape`
- `gold_territory_profile`
- `gold_thematic_affinity`
- `gold_campaign_finance_efficiency`
- `gold_priority_score`

Nem todos sao obrigatorios; features sem fonte recebem defaults conservadores e mantem lineage no manifest.

## Execucao

```powershell
python scripts/build_feature_store.py `
  --feature-version 2024_sp_v1 `
  --gold-table gold_electoral_base_strength=lake/gold/marts/2024_sp_v1/gold_electoral_base_strength/gold_electoral_base_strength.parquet `
  --gold-table gold_competition_landscape=lake/gold/marts/2024_sp_v1/gold_competition_landscape/gold_competition_landscape.parquet `
  --gold-table gold_territory_profile=lake/gold/marts/2024_sp_v1/gold_territory_profile/gold_territory_profile.parquet `
  --gold-table gold_thematic_affinity=lake/gold/marts/2024_sp_v1/gold_thematic_affinity/gold_thematic_affinity.parquet `
  --gold-table gold_campaign_finance_efficiency=lake/gold/marts/2024_sp_v1/gold_campaign_finance_efficiency/gold_campaign_finance_efficiency.parquet
```

## Saida

```text
lake/semantic/feature_store/{feature_version}/territorial_recommendation_features/
  features.parquet
  feature_registry.json
  manifest.json
  feature_store.duckdb
  duckdb_feature_examples.sql
```

## Exemplo de Consumo pela Scoring Engine

```python
from feature_store import AnalyticalFeatureStore
from scoring.priority_score import ScoringEngine

store = AnalyticalFeatureStore()
result = store.compute(gold_tables=gold_tables, feature_version="2024_sp_v1")
scoring_input = store.scoring_frame(result.features)
scored = ScoringEngine().score(scoring_input, thematic_vector={"geral": 0.5})
```

## Governanca

O registry e governado em `feature_store/registry.py`. Cada feature declara:

- nome;
- familia;
- definicao;
- grain;
- dtype;
- versao;
- lineage;
- politica de recomputacao;
- checks de qualidade.

## Limites

- `competitor_incumbency_pressure` depende de contexto de incumbencia confiavel; default atual e `0.0`.
- Proxies territoriais usam os marts disponiveis; quando Censo/CNES/INEP entrarem em silver/gold, as formulas devem ser enriquecidas.
- As features sao agregadas por territorio/candidato e nao autorizam microtargeting individual.
