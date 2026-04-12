# -----------------------------------------------------------------------------
# frontend build stage - compile the Vite/React bundle
# -----------------------------------------------------------------------------
FROM node:20-alpine AS frontend

WORKDIR /build
COPY frontend/package.json ./package.json
RUN npm install --no-audit --no-fund --loglevel=error

COPY frontend/ ./
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

COPY backend/requirements.txt /app/backend/requirements.txt
RUN pip install -r /app/backend/requirements.txt

COPY backend/ /app/backend/
COPY --from=frontend /build/dist /app/frontend_dist

RUN useradd --create-home --uid 10002 vector \
    && chown -R vector:vector /app
USER vector

ENV VECTOR_UI_STATIC=/app/frontend_dist
EXPOSE 3005

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "3005"]
