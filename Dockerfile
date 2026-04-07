FROM python:3.11.9-slim

WORKDIR /app

# Dependências do sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl \
    && rm -rf /var/lib/apt/lists/*

# Dependências Python
COPY requirements.in requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Código e dados
COPY app.py .
COPY healthcheck.py .
RUN mkdir -p /app/data/outputs/estado_sessao \
    /app/data/outputs/relatorios \
    /app/data/chromadb \
    /tmp/inteligencia_eleitoral/relatorios

# Porta HF Spaces
EXPOSE 7860

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s \
    CMD python healthcheck.py --mode ready || exit 1

ENV PYTHONUNBUFFERED=1

CMD ["streamlit", "run", "app.py", \
     "--server.port=7860", \
     "--server.address=0.0.0.0", \
     "--server.headless=true", \
     "--server.enableCORS=false", \
     "--server.enableXsrfProtection=false"]
