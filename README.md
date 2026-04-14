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


## Entrypoint canonico da UI

O caminho oficial da interface Streamlit e:

```bash
streamlit run web_ui/streamlit_app.py
```

`web_ui/streamlit_app.py` chama `presentation.app_main.run_app()`. A camada `presentation/` contem a composicao e os componentes da UI. O arquivo `app.py` esta descontinuado e existe apenas para falhar com uma mensagem explicita caso alguem tente usar o entrypoint legado. Docker, HF Spaces, CI e healthcheck devem apontar para `web_ui/streamlit_app.py`.

## Rodando localmente

```bash
# Clone
git clone https://github.com/acarloshenrique/inteligencia-eleitoral-sp2026.git
cd inteligencia-eleitoral-sp2026

# Configure a chave Groq
export GROQ_API_KEY=gsk_...

# Rode a UI canonica diretamente
streamlit run web_ui/streamlit_app.py

# Ou suba com Docker Compose
docker compose up --build

# Acesse em http://localhost:8501
```

## Open data crosswalk

Novo fluxo para ampliar cobertura de dados abertos e enriquecer o ranking municipal:

- sincronizacao incremental de assets HTTP (`ETag` / `Last-Modified`)
- mapeamento canonico de municipio (`codigo_tse` + `codigo_ibge`)
- chave mestre canonica (`municipio_id_ibge7 + ano + mes + turno`)
- enriquecimento opcional com indicadores socioeconomicos
- publicacao versionada com catalogo e metricas de `join_rate`

Scripts:

```bash
python scripts/sync_open_data_assets.py --asset "nome|url|arquivo.csv"
python scripts/run_open_data_crosswalk.py --source-catalog config/ingestion_sources.example.json
python scripts/run_automated_ingestion.py --source-catalog config/ingestion_sources.example.json
```

Detalhes: `docs/open-data-crosswalk.md`
Priorizacao de fontes: `docs/open-data-sources.md`
Arquitetura medallion: `docs/medallion-pipeline.md`

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
- `GET /v1/audit` (admin)

Autenticação API:

- Header `Authorization: Bearer <token>`
- RBAC básico por role (`admin`, `operator`, `viewer`)
- Tokens podem vir de cofre (`SECRET_BACKEND=vault`) ou fallback local controlado.

Governança e operação:

- LGPD baseline: `docs/lgpd.md`
- Runbooks: `runbooks/incident-response.md`, `runbooks/backup-restore.md`, `runbooks/dr-recovery-test.md`

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
O fluxo recomendado agora é ingestão automatizada por catálogo de fontes remotas, sem cópia manual de Google Drive:

```bash
python scripts/run_automated_ingestion.py --source-catalog config/ingestion_sources.example.json
```

Catálogo de fontes por domínio:

- `eleitoral_oficial`
- `socioeconomico`
- `territorial`
- `midia_e_social`
- `operacoes_de_campanha`

Referências:

- `config/source_domains.json`
- `config/source_catalogs/`
- `config/ingestion_tse_prioritario.example.json`
- `config/ingestion_socioeconomico_sp.example.json`

Também há job assíncrono na API:

- `POST /v1/jobs/ingest`
- `workers.tasks.run_ingestion_task`

Fluxo operacional:

- `ingestion/`: download automatizado, validação e versionamento de cargas
- `lake/bronze/`: dado cru, como veio da fonte
- `lake/silver/`: schema, tipos, datas, chaves e encoding normalizados
- `lake/gold/`: tabelas prontas para decisão, score, API e dashboard
- `api/`, `workers/` e `presentation/`: consumo exclusivo da camada `gold`

## Estrutura

```
.
|-- web_ui/streamlit_app.py # Entrypoint canonico Streamlit
|-- presentation/           # Composicao e componentes da UI
|-- Dockerfile
|-- docker-compose.yml
|-- requirements.txt
|-- ingestion/              # downloads, validacao e runs de ingestao
|-- lake/
|   |-- bronze/             # dado cru vindo das fontes
|   |-- silver/             # schema padronizado e chaves canonicas
|   `-- gold/               # marts, serving, reports e catalogo
|-- chromadb/               # indice vetorial
|-- scripts/
|   `-- run_automated_ingestion.py
`-- tests/
```
