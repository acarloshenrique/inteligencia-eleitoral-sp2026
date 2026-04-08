# Estratégia Blue/Green

1. Publicar nova versão com sufixo `-green` em paralelo ao `-blue`.
2. Rodar smoke tests na `green` (health, jobs API, fila, worker, read/write de artefatos).
3. Trocar o Service para apontar para `green`.
4. Monitorar SLO por 15 minutos.
5. Se erro > limiar, rollback imediato voltando Service para `blue`.

## Recomendação de gatilhos de rollback

- Taxa de fallback LLM > 40%
- Latência p95 API > 1.5s
- Erro de jobs assíncronos > 3%
