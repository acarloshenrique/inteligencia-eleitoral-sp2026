# Data Quality and Lake Observability

## Objetivo

O framework `data_quality/` torna o lake auditavel e vendavel: cada dataset recebe checks, score, readiness comercial, limitacoes e joins confiaveis.

## Dimensoes

- `completeness`
- `uniqueness`
- `validity`
- `freshness`
- `referential_integrity`
- `territorial_coverage`
- `temporal_coverage`
- `joinability`
- `distribution`
- `drift`

## Readiness

| Status | Criterio |
| --- | --- |
| `production_ready` | score >= 0.90 e sem falhas |
| `limited_use` | score >= 0.70 ou checks com limitacoes nao bloqueantes |
| `not_ready` | falha de completude, unicidade ou validade, ou score baixo |

## Uso Programatico

```python
from data_quality import DataQualityRunner

report = DataQualityRunner().run_lake({
    "gold_territorial_electoral_master_index": master_df,
    "gold_priority_score": priority_df,
})
```

## CLI

```powershell
python scripts/run_data_quality.py `
  --dataset gold_territorial_electoral_master_index=lake/gold/master.parquet `
  --dataset gold_priority_score=lake/gold/priority.parquet `
  --output-dir output/data_quality `
  --fail-under 0.80
```

## Relatorios

O runner gera:

```text
output/data_quality/lake_health_report.json
output/data_quality/lake_health_report.md
```

O Markdown responde diretamente:

- quais bases estao prontas para producao;
- quais tem limitacoes;
- quais joins sao confiaveis;
- quais checks falharam ou geraram alerta.

## Integracao com CI

O script `scripts/run_data_quality.py` aceita `--fail-under`. Em CI, use um conjunto pequeno de fixtures ou marts de exemplo:

```yaml
- run: python scripts/run_data_quality.py --dataset gold_priority_score=tests/fixtures/gold_priority_score.parquet --fail-under 0.80
```

Para ambientes com dados reais, rode em schedule ou job operacional e publique os relatorios como artefatos.

## Limites

- Freshness depende de metadata de manifest; sem timestamp, o check fica limitado.
- Drift passa no primeiro run porque nao ha baseline anterior.
- Referencial integrity exige dataset de referencia carregado no mesmo run.
- Joinability mede chaves e `join_confidence`; nao substitui auditoria semantica de joins aproximados.
