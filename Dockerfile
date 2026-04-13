FROM python:3.11.9-slim AS builder

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Dependencias do sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Dependencias Python
COPY requirements.in requirements.txt ./
RUN python -m venv /opt/venv \
    && /opt/venv/bin/pip install --upgrade pip \
    && /opt/venv/bin/pip install -r requirements.txt

# Runtime enxuto
FROM python:3.11.9-slim

ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false \
    HOME=/home/app

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    tini \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system app \
    && useradd --system --create-home --gid app --home-dir /home/app app

COPY --from=builder /opt/venv /opt/venv
COPY app.py healthcheck.py ./

RUN mkdir -p /app/data/outputs/estado_sessao \
    /app/data/outputs/relatorios \
    /app/data/chromadb \\
    /app/data/data_lake/bronze \\
    /app/data/data_lake/silver \\
    /app/data/data_lake/gold \\
    /app/data/data_lake/catalog \\
    /tmp/inteligencia_eleitoral/relatorios \
    && chown -R app:app /app /tmp/inteligencia_eleitoral /home/app

USER app

EXPOSE 7860

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s \
    CMD ["python", "healthcheck.py", "--mode", "ready"]

ENTRYPOINT ["/usr/bin/tini", "--"]

CMD ["streamlit", "run", "app.py", \
     "--server.port=7860", \
     "--server.address=0.0.0.0", \
     "--server.headless=true", \
     "--server.enableCORS=false", \
     "--server.enableXsrfProtection=true", \
     "--server.fileWatcherType=none"]
