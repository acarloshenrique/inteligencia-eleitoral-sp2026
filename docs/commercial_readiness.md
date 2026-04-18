# Commercial Readiness do Data Lake

Este documento define como o Data Lake vira ativo comercial do produto: demo, relatório premium, operação
multi-cliente e snapshots auditáveis por campanha.

## Ativos de maior vantagem competitiva

1. **Master Index territorial-eleitoral**: prova de granularidade em seção, zona, município, candidato e setor
   censitário quando houver chave disponível.
2. **Marts de priorização e alocação**: transformam score em decisão, verba sugerida e justificativa.
3. **Feature store territorial**: cria reuso analítico e base para evolução de modelos sem misturar feature
   engineering com scoring final.
4. **Semantic registry**: mantém métricas canônicas para API, UI, relatórios e pitch comercial.
5. **Lake Health Report**: permite vender com transparência sobre cobertura, qualidade e confiança dos joins.

## Modelagem multi-tenant

- Todo artefato comercial deve carregar `tenant_id`, `campaign_id` e `snapshot_id`.
- O isolamento físico usa `lake/tenants/<tenant_id>/...` para clientes reais; `default` fica no root local.
- O isolamento lógico usa filtros obrigatórios por `tenant_id` nas tabelas de serving, semantic e exports.
- Tabelas multi-candidato devem suportar `candidate_id` como dimensão obrigatória, sem assumir candidato único por tenant.
- Snapshots congelam uma campanha em uma versão reproduzível: inputs, marts, exports e manifesto.

Entidades que precisam estar prontas para multi-tenant:

- candidato
- campanha
- território
- recomendação
- cenário
- feature set
- relatório/export
- data quality report

Tabelas que precisam suportar múltiplos candidatos/clientes:

- `gold_candidate_context`
- `gold_electoral_base_strength`
- `gold_priority_score`
- `gold_allocation_inputs`
- `gold_allocation_recommendations`
- `gold_candidate_comparisons`
- `territorial_recommendation_features`

## Marts comerciais

O pacote `commercial` gera três marts de produto:

- `commercial_demo_summary`: ranking curto para demo executiva e validação comercial.
- `premium_report_tables`: tabela operacional para relatório premium, planilha e recomendação de ação.
- `commercial_pitch_metrics`: números de pitch, incluindo candidatos suportados, territórios ranqueados,
  recomendações geradas, confiança de join e saúde do lake.

Esses marts consomem tabelas gold quando disponíveis e marcam lacunas como `Not found in repo` quando a evidência
operacional ainda não existe.

## Estrutura de exportação

Exports gerados:

- `commercial_demo_summary.json`
- `commercial_demo_summary.md`
- `commercial_pitch_metrics.json`
- `ranking_operacional.csv`
- `allocation_recommendations.csv`
- `premium_report_tables.xlsx`
- `commercial_assets.md`
- `commercial_export_manifest.json`

Esses arquivos sustentam:

- pitch comercial
- relatório executivo
- planilha operacional
- demo guiada
- handoff para time de campanha
- auditoria de entregáveis

## Snapshots por campanha

Cada snapshot é salvo em:

```text
lake/tenants/<tenant_id>/commercial_snapshots/<campaign_id>/<snapshot_id>/
```

Conteúdo esperado:

- marts comerciais em Parquet
- `snapshot.json` com candidatos, versão de dataset e tabelas fonte
- exports em `output/commercial/<tenant>/<campaign>/<snapshot>/`

## Política LGPD e produto

- A camada comercial não exporta microtargeting individual.
- Recomendação deve operar em território agregado.
- Dados pessoais de eleitores não devem aparecer em logs, exports ou pitch.
- Confiança e limitações de join devem ser mostradas, não escondidas.

## Como executar

Exemplo:

```bash
python scripts/build_commercial_assets.py \
  --tenant-id cliente_demo \
  --campaign-id campanha_sp_2026 \
  --snapshot-id s001 \
  --dataset-version gold_2026_04_17 \
  --gold-table gold_priority_score=lake/gold/gold_priority_score.parquet \
  --gold-table gold_allocation_recommendations=lake/gold/gold_allocation_recommendations.parquet
```

## Próximos gargalos comerciais

- Conectar estes marts à UI Streamlit como página de demo e relatórios.
- Gerar PDF executivo usando os mesmos marts comerciais.
- Criar contrato formal de `tenant_id/campaign_id/snapshot_id` nas APIs.
- Versionar score/pesos por campanha para explicar mudanças entre snapshots.
