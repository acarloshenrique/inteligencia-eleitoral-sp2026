# Semantic Layer

## Objetivo

A camada semantica define entidades, metricas e dimensoes canonicas para API, UI, relatorios, scoring e copiloto analitico. Ela evita divergencia de nomes, formulas e grains entre partes do produto.

## Entidades

- `candidato`
- `territorio`
- `secao_eleitoral`
- `local_votacao`
- `municipio`
- `cluster_territorial`
- `tema`
- `recomendacao`
- `cenario`
- `gasto`
- `base_eleitoral`
- `concorrencia`

## Metricas Canonicas

- `forca_base`
- `potencial_expansao`
- `intensidade_competitiva`
- `aderencia_tematica`
- `eficiencia_gasto`
- `prioridade_territorial`
- `confianca_recomendacao`
- `cobertura_territorial`
- `custo_por_voto_estimado`
- `share_potencial`

## Consumo Programatico

```python
from semantic_layer import SemanticQueryService

service = SemanticQueryService(tables=gold_tables)
ranking = service.territory_ranking(candidate_id="123", limit=20)
recommendations = service.allocation_recommendations(candidate_id="123", scenario_id="baseline")
```

## Export

```powershell
python scripts/export_semantic_registry.py --output-dir lake/semantic/registry
```

Saidas:

```text
lake/semantic/registry/semantic_registry.json
lake/semantic/registry/semantic_registry.md
```

## Regras

- Toda metrica declara formula, grain, tabela fonte e colunas fonte.
- Toda entidade declara chave primaria e tabela canonica.
- Queries reutilizaveis devem passar por `SemanticQueryService`, nao por SQL solto na UI.
- Metricas derivadas de proxy indicam limitacoes em `quality_notes`.

## Exemplos de Queries Reutilizaveis

- ranking territorial por `prioridade_territorial`;
- recomendacoes de alocacao por candidato/cenario;
- catalogo de metricas para API e UI;
- catalogo de entidades e dimensoes.
