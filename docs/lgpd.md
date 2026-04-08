# LGPD Baseline

## Minimização de dados

- Exportações assíncronas aplicam minimização por padrão (`minimize=true`).
- Campos não essenciais devem ficar fora do payload de jobs e logs.

## Retenção

- Retenção padrão: `RETENTION_DAYS=180`.
- Limpeza periódica: `python scripts/retention_cleanup.py`.

## Anonimização

- Export pode habilitar anonimização (`anonymize=true`).
- Colunas sensíveis (ex.: `municipio`) podem ser hash com sal (`LGPD_ANONYMIZATION_SALT`).

## Base legal e responsabilidade

- Operador deve registrar finalidade antes de exportar datasets.
- Logs de auditoria devem ser preservados conforme política interna.
