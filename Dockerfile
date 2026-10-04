# Matches the platform the warehouse services run on (arm64/Graviton).
FROM --platform=linux/arm64 python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    DEPLOY_MODE=remote \
    AUTO_START_TUNNEL=false

WORKDIR /app

# libpq and ca-certificates for secure PostgreSQL RDS connection
RUN apt-get update \
 && apt-get install -y --no-install-recommends libpq5 ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Run as a non-root user for container security
RUN useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin mcpuser \
 && chown -R mcpuser:mcpuser /app
USER 10001

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s \
  CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/health').status==200 else 1)"

CMD ["uvicorn", "remote_app:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
