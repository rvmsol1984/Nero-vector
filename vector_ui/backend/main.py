"""NERO Vector UI backend (FastAPI).

Serves the /api/* JSON endpoints that power the operator dashboard, plus
the pre-built React SPA bundle as static files. Everything runs on a
single port so the full UI fits behind a single Cloudflare Access app.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from backend import db

logging.basicConfig(
    level=os.environ.get("VECTOR_UI_LOG_LEVEL", "INFO").upper(),
    format='{"ts":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","msg":"%(message)s"}',
)
logger = logging.getLogger("vector_ui")

app = FastAPI(title="NERO Vector UI", version="0.1.0")

# CORS is wide-open because access is gated by Cloudflare Access upstream.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def _startup() -> None:
    db.init_pool()
    logger.info("vector-ui started")


@app.on_event("shutdown")
def _shutdown() -> None:
    db.close_pool()


# ----- health ---------------------------------------------------------------

@app.get("/health")
def health() -> dict:
    try:
        db.fetch_one("SELECT 1 AS ok")
        return {"status": "ok"}
    except Exception as exc:  # pragma: no cover - defensive
        logger.exception("health check failed")
        return {"status": "degraded", "error": str(exc)}


# ----- /api -----------------------------------------------------------------

@app.get("/api/stats")
def stats() -> dict:
    row = db.fetch_one(
        """
        SELECT
            COUNT(*)::bigint                                                         AS total_events,
            COUNT(DISTINCT entity_key)::bigint                                       AS unique_users,
            COUNT(DISTINCT tenant_id)::bigint                                        AS unique_tenants,
            COUNT(*) FILTER (WHERE timestamp >= now() - INTERVAL '24 hours')::bigint AS events_24h
        FROM vector_events
        """
    ) or {}
    return {
        "total_events":   int(row.get("total_events")   or 0),
        "unique_users":   int(row.get("unique_users")   or 0),
        "unique_tenants": int(row.get("unique_tenants") or 0),
        "events_24h":     int(row.get("events_24h")     or 0),
    }


@app.get("/api/events/recent")
def events_recent(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    tenant: str | None = Query(None),
    event_type: str | None = Query(None),
) -> list[dict]:
    return db.fetch_all(
        """
        SELECT id::text,
               timestamp,
               client_name,
               tenant_id,
               user_id,
               entity_key,
               event_type,
               workload,
               result_status,
               client_ip
        FROM vector_events
        WHERE (%s::text IS NULL OR client_name = %s)
          AND (%s::text IS NULL OR event_type  = %s)
        ORDER BY timestamp DESC
        LIMIT %s OFFSET %s
        """,
        (tenant, tenant, event_type, event_type, limit, offset),
    )


@app.get("/api/events/by-tenant")
def events_by_tenant() -> list[dict]:
    return db.fetch_all(
        """
        SELECT client_name, COUNT(*)::bigint AS count
        FROM vector_events
        GROUP BY client_name
        ORDER BY count DESC
        """
    )


@app.get("/api/events/by-type")
def events_by_type(limit: int = Query(20, ge=1, le=200)) -> list[dict]:
    return db.fetch_all(
        """
        SELECT event_type, COUNT(*)::bigint AS count
        FROM vector_events
        GROUP BY event_type
        ORDER BY count DESC
        LIMIT %s
        """,
        (limit,),
    )


@app.get("/api/events/users")
def events_users(
    tenant: str | None = Query(None),
    limit: int = Query(200, ge=1, le=1000),
) -> list[dict]:
    return db.fetch_all(
        """
        SELECT entity_key,
               user_id,
               client_name,
               COUNT(*)::bigint                          AS event_count,
               mode() WITHIN GROUP (ORDER BY event_type) AS top_event_type,
               MAX(timestamp)                            AS last_seen
        FROM vector_events
        WHERE (%s::text IS NULL OR client_name = %s)
        GROUP BY entity_key, user_id, client_name
        ORDER BY event_count DESC
        LIMIT %s
        """,
        (tenant, tenant, limit),
    )


# ----- static SPA -----------------------------------------------------------

_static_path = Path(os.environ.get("VECTOR_UI_STATIC", "/app/frontend_dist"))

if (_static_path / "assets").is_dir():
    app.mount(
        "/assets",
        StaticFiles(directory=str(_static_path / "assets")),
        name="assets",
    )


@app.get("/{full_path:path}", include_in_schema=False)
async def spa(full_path: str = "") -> FileResponse:
    """Catch-all for the React SPA shell.

    Any /api/* path that didn't match an explicit route above falls through
    here and returns 404 (so clients see a real API error, not the HTML
    shell). Everything else either serves a matching file from the built
    Vite bundle (favicon, vite.svg, …) or returns index.html so React
    Router can take over client-side routing.
    """
    if full_path.startswith("api/") or full_path.startswith("api"):
        raise HTTPException(status_code=404)
    if not _static_path.is_dir():
        raise HTTPException(status_code=503, detail="frontend bundle not built")
    if full_path:
        candidate = _static_path / full_path
        if candidate.is_file():
            return FileResponse(str(candidate))
    index = _static_path / "index.html"
    if index.is_file():
        return FileResponse(str(index))
    raise HTTPException(status_code=404)
