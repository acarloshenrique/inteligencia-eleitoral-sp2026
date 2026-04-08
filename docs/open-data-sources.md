# Priorizacao de Fontes Open Data

Este projeto usa priorizacao formal de fontes por impacto:

- Prioridade A (alto sinal):
  - TSE resultados por municipio
  - TSE resultados por secao
  - Correspondencia oficial TSE/IBGE
  - IBGE Localidades + SIDRA
  - SEADE municipal (SP)
- Prioridade B (complementar):
  - Portal da Transparencia (transferencias, emendas, despesas)

## Criterio de entrada

Toda fonte e avaliada com os seguintes campos:

- cobertura_municipal
- atualizacao_dias
- licenca_aberta
- schema_quality

Regras:

- baseline minimo:
  - cobertura >= 0.80
  - atualizacao <= 120 dias
  - licenca aberta obrigatoria
  - schema_quality >= 0.70
- fontes A exigem adicionalmente:
  - cobertura >= 0.95
  - atualizacao <= 45 dias
  - schema_quality >= 0.85

## Catalogo e execucao

- Catalogo versionado: `config/open_data_sources.json`
- Script de priorizacao:

```bash
python scripts/prioritize_open_data_sources.py
```

Saida padrao:

- `docs/open_data_sources_report.json`
