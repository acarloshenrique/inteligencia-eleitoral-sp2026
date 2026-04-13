# Production Hardening

## Redis

In `APP_ENV=prod`, bootstrap fails unless Redis uses TLS and password authentication. Configure:

```env
APP_ENV=prod
REDIS_URL=rediss://:<strong-password>@redis.example.com:6379/0
REQUIRE_REDIS_TLS_IN_PROD=true
REQUIRE_REDIS_AUTH_IN_PROD=true
```

Use a managed Redis service or terminate TLS inside the private network with certificate rotation. Do not store the password in the repository; load it from the deployment secret manager.

## ChromaDB / Vector Store

For local ChromaDB in production, each tenant must use an isolated volume under `data/tenants/<tenant>/chromadb`. Configure a dedicated tenant id:

```env
APP_ENV=prod
TENANT_ID=cliente-a
CHROMA_VECTOR_BACKEND=local
CHROMA_ALLOW_SHARED_VOLUME=false
```

For a managed vector database, set:

```env
CHROMA_VECTOR_BACKEND=external
```

The bootstrap guard blocks `TENANT_ID=default` with local ChromaDB in production because that layout is shared and not appropriate for SaaS isolation.

## Operational Alerts

Alert evaluation persists records in the metadata database and sends notifications through configured channels. Configure at least one channel in production:

```env
OPS_ALERT_WEBHOOK_URL=https://ops.example.com/inteligencia-eleitoral-alerts
OPS_ALERT_SLACK_WEBHOOK_URL=
OPS_ALERT_TEAMS_WEBHOOK_URL=
OPS_ALERT_EMAIL_ENABLED=false
OPS_ALERT_DAILY_COST_USD=50
OPS_ALERT_ERROR_RATE_THRESHOLD=0.10
OPS_ALERT_LATENCY_P95_MS=30000
```

Job failures call alert evaluation automatically. Operators can also call `POST /v1/ops/alerts/evaluate` to persist and dispatch threshold-based alerts on demand.
