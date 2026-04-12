FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Minimal runtime deps; psycopg2-binary ships wheels so we don't need libpq-dev.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tini \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt

COPY vector_ingest /app/vector_ingest
COPY migrations  /app/migrations
COPY tenants.json /app/tenants.json

RUN useradd --create-home --uid 10001 vector \
    && chown -R vector:vector /app
USER vector

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "-m", "vector_ingest.main"]
