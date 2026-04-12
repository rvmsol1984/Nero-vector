"""NERO Vector UI backend (FastAPI).

Serves the /api/* JSON endpoints that power the operator dashboard plus
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

# Wide-open CORS: access is gated by Cloudflare Access upstream.
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


# ============================================================================
# health
# ============================================================================

@app.get("/health")
def health() -> dict:
    try:
        db.fetch_one("SELECT 1 AS ok")
        return {"status": "ok"}
    except Exception as exc:  # pragma: no cover - defensive
        logger.exception("health check failed")
        return {"status": "degraded", "error": str(exc)}


# ============================================================================
# global stats + breakdowns
# ============================================================================

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


# ----- /api/events/recent -- used by the Dashboard feed AND the Events page.
# IMPORTANT: keep this route (and /by-*, /users) above /api/events/{event_id}
# so FastAPI matches the literal segments before the path-variable fallback.

@app.get("/api/events/recent")
def events_recent(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    tenant: str | None = Query(None),
    event_type: str | None = Query(None),
    workload: str | None = Query(None),
    user: str | None = Query(None, description="substring search over user_id"),
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
          AND (%s::text IS NULL OR workload    = %s)
          AND (%s::text IS NULL OR user_id ILIKE '%%' || %s || '%%')
        ORDER BY timestamp DESC
        LIMIT %s OFFSET %s
        """,
        (tenant, tenant, event_type, event_type, workload, workload, user, user, limit, offset),
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


@app.get("/api/events/by-workload")
def events_by_workload() -> list[dict]:
    return db.fetch_all(
        """
        SELECT workload, COUNT(*)::bigint AS count
        FROM vector_events
        GROUP BY workload
        ORDER BY count DESC
        """
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


# ----- single-event detail (declared AFTER the literal /api/events/* routes)

@app.get("/api/events/{event_id}")
def event_detail(event_id: str) -> dict:
    row = db.fetch_one(
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
               client_ip,
               user_agent,
               raw_json
        FROM vector_events
        WHERE id = %s
        """,
        (event_id,),
    )
    if not row:
        raise HTTPException(status_code=404, detail="event not found")
    return row


# ============================================================================
# per-user detail
# ============================================================================

@app.get("/api/users/{entity_key}")
def user_profile(entity_key: str) -> dict:
    row = db.fetch_one(
        """
        SELECT
            MAX(user_id)                       AS user_id,
            MAX(client_name)                   AS client_name,
            MAX(tenant_id)                     AS tenant_id,
            MIN(timestamp)                     AS first_seen,
            MAX(timestamp)                     AS last_seen,
            COUNT(*)::bigint                   AS total_events,
            COUNT(DISTINCT event_type)::bigint AS unique_event_types,
            COUNT(DISTINCT client_ip)::bigint  AS unique_ips,
            COUNT(DISTINCT (
                SELECT dp->>'Value'
                FROM jsonb_array_elements(
                    COALESCE(raw_json->'DeviceProperties', '[]'::jsonb)
                ) dp
                WHERE dp->>'Name' = 'DisplayName'
                LIMIT 1
            ))::bigint                         AS unique_devices
        FROM vector_events
        WHERE entity_key = %s
        """,
        (entity_key,),
    )
    if not row or not row.get("total_events"):
        raise HTTPException(status_code=404, detail="user not found")
    row["entity_key"] = entity_key
    return row


@app.get("/api/users/{entity_key}/events")
def user_events(
    entity_key: str,
    workloads: str | None = Query(None, description="comma-separated"),
    event_types: str | None = Query(None, description="comma-separated"),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> list[dict]:
    return db.fetch_all(
        """
        SELECT id::text,
               timestamp,
               event_type,
               workload,
               result_status,
               client_ip,
               user_agent,
               raw_json
        FROM vector_events
        WHERE entity_key = %s
          AND (%s::text IS NULL OR workload   = ANY(string_to_array(%s, ',')))
          AND (%s::text IS NULL OR event_type = ANY(string_to_array(%s, ',')))
        ORDER BY timestamp DESC
        LIMIT %s OFFSET %s
        """,
        (entity_key, workloads, workloads, event_types, event_types, limit, offset),
    )


@app.get("/api/users/{entity_key}/stats")
def user_stats(entity_key: str) -> dict:
    by_event_type = db.fetch_all(
        """
        SELECT event_type, COUNT(*)::bigint AS count
        FROM vector_events
        WHERE entity_key = %s
        GROUP BY event_type
        ORDER BY count DESC
        """,
        (entity_key,),
    )
    by_workload = db.fetch_all(
        """
        SELECT workload, COUNT(*)::bigint AS count
        FROM vector_events
        WHERE entity_key = %s
        GROUP BY workload
        ORDER BY count DESC
        """,
        (entity_key,),
    )
    return {"by_event_type": by_event_type, "by_workload": by_workload}


# ============================================================================
# governance (all findings are UAL-derived, tenant scoping handled by caller)
# ============================================================================


@app.get("/api/governance/dlp")
def governance_dlp(tenant: str | None = Query(None)) -> list[dict]:
    """Users who copied files onto removable media."""
    return db.fetch_all(
        """
        SELECT
            entity_key,
            user_id,
            client_name,
            COUNT(*)::bigint AS event_count,
            MAX(timestamp)   AS last_seen,
            COALESCE(
                array_agg(DISTINCT raw_json->>'ObjectId')
                    FILTER (WHERE raw_json->>'ObjectId' IS NOT NULL),
                ARRAY[]::text[]
            )                AS files
        FROM vector_events
        WHERE event_type  = 'FileCreatedOnRemovableMedia'
          AND (%s::text IS NULL OR client_name = %s)
        GROUP BY entity_key, user_id, client_name
        ORDER BY event_count DESC
        LIMIT 100
        """,
        (tenant, tenant),
    )


@app.get("/api/governance/sharing")
def governance_sharing(
    tenant: str | None = Query(None),
) -> list[dict]:
    """Users who used anonymous or generated sharing links."""
    return db.fetch_all(
        """
        SELECT
            entity_key,
            user_id,
            client_name,
            event_type,
            COUNT(*)::bigint AS event_count,
            MAX(timestamp)   AS last_seen
        FROM vector_events
        WHERE event_type IN ('AnonymousLinkUsed', 'SharingLinkUsed')
          AND (%s::text IS NULL OR client_name = %s)
        GROUP BY entity_key, user_id, client_name, event_type
        ORDER BY event_count DESC
        LIMIT 100
        """,
        (tenant, tenant),
    )


@app.get("/api/governance/downloads")
def governance_downloads(
    tenant: str | None = Query(None),
    threshold: int = Query(5, ge=1, le=1000),
) -> list[dict]:
    """Users running FileDownloadedFromBrowser > threshold in the last 24h."""
    return db.fetch_all(
        """
        SELECT
            entity_key,
            user_id,
            client_name,
            COUNT(*)::bigint AS download_count,
            MAX(timestamp)   AS last_seen
        FROM vector_events
        WHERE event_type = 'FileDownloadedFromBrowser'
          AND timestamp >= now() - INTERVAL '24 hours'
          AND (%s::text IS NULL OR client_name = %s)
        GROUP BY entity_key, user_id, client_name
        HAVING COUNT(*) > %s
        ORDER BY download_count DESC
        LIMIT 100
        """,
        (tenant, tenant, threshold),
    )


# ============================================================================
# static SPA
# ============================================================================

_static_path = Path(os.environ.get("VECTOR_UI_STATIC", "/app/frontend_dist"))

if (_static_path / "assets").is_dir():
    app.mount(
        "/assets",
        StaticFiles(directory=str(_static_path / "assets")),
        name="assets",
    )


@app.get("/{full_path:path}", include_in_schema=False)
async def spa(full_path: str = "") -> FileResponse:
    """Catch-all that serves the React SPA shell.

    Any /api/* path that didn't match an explicit route above returns 404
    here (so clients see a real API error, not the HTML shell). Everything
    else either serves a matching file from the built Vite bundle or
    returns index.html so React Router can take over client-side routing.
    """
    if full_path.startswith("api/") or full_path == "api":
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
