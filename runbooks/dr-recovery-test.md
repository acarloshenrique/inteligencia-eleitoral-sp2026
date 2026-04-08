# Runbook: Teste de Recuperação (DR)

Periodicidade recomendada: mensal.

## Plano de teste

1. Simular indisponibilidade do `api` e `worker`.
2. Restaurar metadata DB a partir de backup.
3. Reprocessar 1 job de `reindex` e 1 job de `export`.
4. Validar:
   - status `finished`
   - artefato acessível
   - trilha de auditoria registrada

## Critérios de sucesso

- RTO <= 30 min
- RPO <= 24 h
