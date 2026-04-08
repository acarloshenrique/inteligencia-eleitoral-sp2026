# Deploy Gerenciado e Autoscaling

## Opções suportadas

- Kubernetes: manifests em `deploy/k8s/` com `Deployment` separado por serviço e `HPA` para autoscaling.
- ECS/Fargate: mapear `web-ui`, `api`, `worker` em tasks distintas com autoscaling por CPU/RPS.
- Cloud Run: separar `web-ui` e `api` como serviços independentes; `worker` via Cloud Run Jobs ou serviço dedicado.

## Rollout

- Canary: `deploy/k8s/rollout-canary.yaml` (Argo Rollouts).
- Blue/Green: guia operacional em `deploy/k8s/rollout-bluegreen.md`.

## Dependências de plataforma

- Redis gerenciado (ElastiCache/Memorystore/Redis Enterprise)
- Bucket S3-compatible para artefatos
- Banco transacional para metadados (pode iniciar com SQLite em volume persistente; produção recomendada: Postgres gerenciado)
