---
title: Alocador Inteligente Eleitoral SP 2026
emoji: 🗳
colorFrom: blue
colorTo: green
sdk: docker
app_port: 7860
pinned: false
---

# Inteligência Eleitoral SP 2026

Sistema de alocação inteligente de recursos de campanha para os 644 municípios paulistas.

## Funcionalidades

- **Ranking v2** de 644 municípios com 6 componentes: PAV, RIG, VC, ST, AC, CBO
- **Filtro de viabilidade** por eleitores mínimos e raio geográfico do candidato
- **Alocação de budget** com tetos TSE 2026 por cargo
- **Chat RAG** com Llama 3.3 70B via Groq — consultas em linguagem natural
- **Análise de seções eleitorais** — priorização operacional de campo
- **Análise histórica** 2018 × 2022 por seção e zona eleitoral

## Stack

| Componente | Tecnologia |
|---|---|
| Interface | Streamlit |
| LLM | Groq — Llama 3.3 70B |
| Embeddings | sentence-transformers/all-MiniLM-L6-v2 |
| Busca semântica | ChromaDB |
| Banco de dados | DuckDB |
| Deploy | Docker + HF Spaces |

## Rodando localmente

```bash
# Clone
git clone https://github.com/acarloshenrique/inteligencia-eleitoral-sp2026.git
cd inteligencia-eleitoral-sp2026

# Configure a chave Groq
export GROQ_API_KEY=gsk_...

# Suba com Docker Compose
docker compose up --build

# Acesse em http://localhost:8501
```

## Arquitetura de serviços

- `web-ui` (Streamlit): interface para analistas e operadores.
- `api` (FastAPI): endpoints para tarefas pesadas e controle de jobs.
- `worker` (RQ): processamento assíncrono de indexação/export.
- `redis`: broker da fila.
- `artifact store`: backend local ou S3-compatible.
- `metadata db`: banco transacional para status/resultados de jobs.

Compose da arquitetura separada:

```bash
docker compose -f docker-compose.services.yml up --build
```

API de jobs:

- `POST /v1/jobs/reindex`
- `POST /v1/jobs/export`
- `GET /v1/jobs/{job_id}`

## Configuração

Adicione o secret `GROQ_API_KEY` no HF Space:
Settings → Variables and secrets → New secret

## Dependências reproduzíveis

- `requirements.in`: dependências diretas (fonte)
- `requirements.txt`: lockfile gerado pelo `pip-tools`

Atualizar lockfile:

```bash
python -m pip install pip-tools
python -m piptools compile --resolver=backtracking --output-file requirements.txt requirements.in
```

## Dados

Os dados de eleitores e votação (parquets) não são versionados no Git por questões de privacidade.
Para rodar localmente, copie os parquets do Google Drive para a pasta `data/`.

## Estrutura

```
.
├── app.py                  # Interface Streamlit (5 tabs)
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── data/
│   ├── outputs/
│   │   ├── estado_sessao/  # df_mun, df_sp_capital (parquets)
│   │   └── relatorios/     # última alocação, seções, mapa tático
│   └── chromadb/           # índice vetorial 644 municípios
├── scripts/
│   └── prepare_data.py     # Copia dados do Drive para data/
└── tests/
    └── test_modelo.py      # Testes dos componentes do score
```
