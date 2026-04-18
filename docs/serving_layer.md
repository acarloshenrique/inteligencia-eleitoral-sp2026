# Serving Layer

A camada `serving` materializa saídas finais para API, UI, relatórios comerciais e motores de recomendação.
Ela não substitui gold nem semantic: ela congela uma visão consumível por `tenant_id`, `campaign_id`,
`snapshot_id` e `dataset_version`.

## Outputs

- `serving_territory_ranking`: ranking territorial por candidato, com score, confiança, território e limitações de join.
- `serving_allocation_recommendations`: recomendações de alocação por cenário, candidato e território.
- `serving_data_readiness`: prontidão comercial e operacional do lake para a campanha/snapshot.

## Isolamento e rastreabilidade

Os arquivos são gravados em:

```text
lake/tenants/<tenant_id>/serving/campaign_id=<campaign_id>/snapshot_id=<snapshot_id>/
```

Cada output é exportado em Parquet, CSV e JSON. O diretório também recebe:

- `serving_manifest.json`
- `serving_output_specs.json`

## Regras de modelagem

- Todo registro carrega `tenant_id`, `campaign_id`, `snapshot_id`, `dataset_version` e `generated_at_utc`.
- Toda recomendação carrega `confidence_score` e `evidence_ids`.
- Quando gold ainda não existir, a camada serving retorna `Not found in repo` em vez de inferir silenciosamente.
- Joins aproximados e lacunas de setor/local de votação são expostos no readiness.

## Comando

```bash
python scripts/build_serving_outputs.py \
  --tenant-id cliente_demo \
  --campaign-id campanha_sp_2026 \
  --snapshot-id s001 \
  --dataset-version gold_2026_04_18 \
  --table gold_priority_score=lake/gold/gold_priority_score.parquet \
  --table gold_allocation_recommendations=lake/gold/gold_allocation_recommendations.parquet
```

## Riscos conhecidos

- `confidence_score` é proxy quando gold não fornece confiança explícita.
- ROI político é estimativo e agregado; não deve ser interpretado como preferência individual.
- Se `gold_territorial_electoral_master_index` estiver ausente, a confiança média de join fica 0 e o manifesto emite warning.
