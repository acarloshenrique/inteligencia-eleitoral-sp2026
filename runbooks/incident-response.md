# Runbook: Resposta a Incidentes

1. Classificar severidade (`SEV1`, `SEV2`, `SEV3`).
2. Congelar mudanças (pausar deploy/rollout).
3. Coletar evidências:
   - status da API (`/health`)
   - fila (`redis`, backlog de jobs)
   - últimos eventos de auditoria (`GET /v1/audit`)
4. Mitigar:
   - rollback canary/blue-green
   - desligar rotas sensíveis se necessário
5. Comunicação:
   - atualizar stakeholders com ETA
   - abrir postmortem
