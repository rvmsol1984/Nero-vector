# -----------------------------------------------------------------------------
# frontend build stage - compile the Vite/React bundle
# -----------------------------------------------------------------------------
#
# Build context is the REPO ROOT (see vector_ui/docker-compose.yml) so we
# can COPY vector_ingest/inky_receiver.py into the runtime image and mount
# the INKY webhook router into the vector-ui FastAPI app.
# -----------------------------------------------------------------------------

FROM node:20-alpine AS frontend

WORKDIR /build
COPY vector_ui/frontend/package.json ./package.json
RUN npm install --no-audit --no-fund --loglevel=error

COPY vector_ui/frontend/ ./
RUN npm run build


# -----------------------------------------------------------------------------
# runtime - FastAPI serving /api/* and the built SPA on a single port
# -----------------------------------------------------------------------------
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tini \
    && rm -rf /var/lib/apt/lists/*

COPY vector_ui/backend/requirements.txt /app/backend/requirements.txt
RUN pip install -r /app/backend/requirements.txt

COPY vector_ui/backend/ /app/backend/
COPY --from=frontend /build/dist /app/frontend_dist

# Pull in just the INKY webhook module from the vector_ingest package so the
# vector-ui FastAPI process can serve POST /api/ingest/inky without needing
# the poller container to be a web server.
COPY vector_ingest/__init__.py     /app/vector_ingest/__init__.py
COPY vector_ingest/inky_receiver.py /app/vector_ingest/inky_receiver.py

RUN useradd --create-home --uid 10002 vector \
    && chown -R vector:vector /app
USER vector

ENV VECTOR_UI_STATIC=/app/frontend_dist
EXPOSE 3005

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "3005"]
