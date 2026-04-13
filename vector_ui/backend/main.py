"""NERO Vector UI backend (FastAPI).

Serves the /api/* JSON endpoints that power the operator dashboard plus
the pre-built React SPA bundle as static files. Everything runs on a
single port so the full UI fits behind a single Cloudflare Access app.
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles

import jwt

from backend import db

# The v0.2 inky-receiver runs as its own FastAPI process in the
# vector-ingest container on port 3007, so vector-ui no longer mounts
# any /auth/ingest/inky router.

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


# ---------------------------------------------------------------------------
# Extended GCS governance signals.
# ---------------------------------------------------------------------------
#
# Every query below is hard-scoped to GameChange Solar because the current
# UI surface is the GCS-only board. Each endpoint returns per-finding rows
# that the React client renders inside a collapsible section with a
# severity pill (CRITICAL / REVIEW REQUIRED / MONITOR).

_GCS = "GameChange Solar"


@app.get("/api/governance/external-forwarding")
def governance_external_forwarding() -> list[dict]:
    """UpdateInboxRules events that mention a Forward/Redirect parameter."""
    return db.fetch_all(
        """
        SELECT
            entity_key,
            user_id,
            client_name,
            COUNT(*)::bigint AS rule_count,
            MAX(timestamp)   AS last_seen,
            COALESCE(
                array_agg(DISTINCT raw_json->>'Parameters')
                    FILTER (WHERE raw_json->>'Parameters' IS NOT NULL),
                ARRAY[]::text[]
            ) AS rule_details
        FROM vector_events
        WHERE event_type = 'UpdateInboxRules'
          AND client_name = %s
          AND (
              raw_json::text ILIKE '%%ForwardTo%%'
           OR raw_json::text ILIKE '%%RedirectTo%%'
           OR raw_json::text ILIKE '%%ForwardAsAttachmentTo%%'
          )
        GROUP BY entity_key, user_id, client_name
        ORDER BY rule_count DESC
        LIMIT 100
        """,
        (_GCS,),
    )


@app.get("/api/governance/unmanaged-devices")
def governance_unmanaged_devices() -> list[dict]:
    """Events from devices whose DeviceProperties report IsCompliant = False."""
    return db.fetch_all(
        """
        SELECT
            entity_key,
            user_id,
            client_name,
            COUNT(*)::bigint AS event_count,
            MAX(timestamp)   AS last_seen,
            COALESCE(
                array_agg(DISTINCT raw_json->'DeviceProperties')
                    FILTER (WHERE raw_json->'DeviceProperties' IS NOT NULL),
                ARRAY[]::jsonb[]
            ) AS devices
        FROM vector_events
        WHERE client_name = %s
          AND raw_json::text ILIKE '%%"IsCompliant"%%'
          AND (
              raw_json::text ILIKE '%%"IsCompliant", "Value": "False"%%'
           OR raw_json::text ILIKE '%%IsCompliant":"False"%%'
           OR raw_json::text ILIKE '%%IsCompliant": "False"%%'
          )
        GROUP BY entity_key, user_id, client_name
        ORDER BY event_count DESC
        LIMIT 100
        """,
        (_GCS,),
    )


@app.get("/api/governance/broken-inheritance")
def governance_broken_inheritance() -> list[dict]:
    """SharingInheritanceBroken events rolled up by user."""
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
            ) AS files
        FROM vector_events
        WHERE event_type = 'SharingInheritanceBroken'
          AND client_name = %s
        GROUP BY entity_key, user_id, client_name
        ORDER BY event_count DESC
        LIMIT 100
        """,
        (_GCS,),
    )


@app.get("/api/governance/oauth-apps")
def governance_oauth_apps() -> list[dict]:
    """Consent to application. events grouped by app."""
    return db.fetch_all(
        """
        SELECT
            raw_json->>'ObjectId'              AS app_name,
            COUNT(DISTINCT user_id)::bigint    AS user_count,
            COALESCE(
                array_agg(DISTINCT user_id)
                    FILTER (WHERE user_id IS NOT NULL),
                ARRAY[]::text[]
            )                                  AS users,
            MAX(timestamp)                     AS last_consent
        FROM vector_events
        WHERE event_type = 'Consent to application.'
          AND client_name = %s
        GROUP BY raw_json->>'ObjectId'
        ORDER BY user_count DESC
        LIMIT 100
        """,
        (_GCS,),
    )


@app.get("/api/governance/password-spray")
def governance_password_spray() -> list[dict]:
    """Source IPs hitting 3+ distinct users with login failures in 24h."""
    return db.fetch_all(
        """
        SELECT
            client_ip,
            COUNT(DISTINCT user_id)::bigint AS targeted_users,
            COUNT(*)::bigint                AS total_attempts,
            MIN(timestamp)                  AS first_seen,
            MAX(timestamp)                  AS last_seen,
            COALESCE(
                array_agg(DISTINCT user_id)
                    FILTER (WHERE user_id IS NOT NULL),
                ARRAY[]::text[]
            ) AS targets
        FROM vector_events
        WHERE event_type = 'UserLoginFailed'
          AND client_name = %s
          AND client_ip IS NOT NULL
          AND timestamp > now() - INTERVAL '24 hours'
        GROUP BY client_ip
        HAVING COUNT(DISTINCT user_id) >= 3
        ORDER BY targeted_users DESC, total_attempts DESC
        LIMIT 100
        """,
        (_GCS,),
    )


@app.get("/api/governance/stale-accounts")
def governance_stale_accounts() -> list[dict]:
    """Users with activity in the last 30d but no UserLoggedIn in the last 30d."""
    return db.fetch_all(
        """
        SELECT
            entity_key,
            user_id,
            client_name,
            MAX(timestamp)::timestamptz AS last_activity,
            COUNT(*)::bigint            AS total_events,
            COALESCE(
                array_agg(DISTINCT event_type)
                    FILTER (WHERE event_type IS NOT NULL),
                ARRAY[]::text[]
            ) AS event_types
        FROM vector_events
        WHERE client_name = %s
          AND user_id NOT IN (
              SELECT DISTINCT user_id
              FROM vector_events
              WHERE event_type = 'UserLoggedIn'
                AND client_name = %s
                AND timestamp > now() - INTERVAL '30 days'
                AND user_id IS NOT NULL
          )
        GROUP BY entity_key, user_id, client_name
        HAVING MAX(timestamp) > now() - INTERVAL '30 days'
        ORDER BY last_activity DESC
        LIMIT 100
        """,
        (_GCS, _GCS),
    )


@app.get("/api/governance/mfa-changes")
def governance_mfa_changes() -> list[dict]:
    """StrongAuthentication / MFA config mutations."""
    return db.fetch_all(
        """
        SELECT
            entity_key,
            user_id,
            client_name,
            COUNT(*)::bigint AS change_count,
            MAX(timestamp)   AS last_seen,
            COALESCE(
                array_agg(DISTINCT raw_json->>'Operation')
                    FILTER (WHERE raw_json->>'Operation' IS NOT NULL),
                ARRAY[]::text[]
            ) AS operations
        FROM vector_events
        WHERE client_name = %s
          AND (
              event_type               ILIKE '%%StrongAuthentication%%'
           OR event_type               ILIKE '%%MFA%%'
           OR raw_json->>'Operation'   ILIKE '%%StrongAuthentication%%'
           OR (event_type = 'Update user.' AND raw_json::text ILIKE '%%StrongAuth%%')
          )
        GROUP BY entity_key, user_id, client_name
        ORDER BY last_seen DESC
        LIMIT 100
        """,
        (_GCS,),
    )


@app.get("/api/governance/privileged-roles")
def governance_privileged_roles() -> list[dict]:
    """Raw role add/remove events (no roll-up; operators want the audit trail)."""
    return db.fetch_all(
        """
        SELECT
            entity_key,
            user_id,
            client_name,
            event_type,
            raw_json->>'Operation' AS operation,
            raw_json->>'ObjectId'  AS role,
            raw_json->>'Actor'     AS actor,
            timestamp
        FROM vector_events
        WHERE client_name = %s
          AND event_type IN (
              'Add member to role.',
              'Remove member from role.',
              'Add eligible member to role.',
              'Remove eligible member from role.'
          )
        ORDER BY timestamp DESC
        LIMIT 100
        """,
        (_GCS,),
    )


# ---- guest users (Graph API) -----------------------------------------------

_GCS_TENANT_ID = "07b4c47a-e461-493e-91c4-90df73e2ebc6"
_GRAPH_TOKEN_CACHE: dict = {"token": None, "expires_at": 0.0}


def _get_graph_token() -> str:
    """Client credentials token for the GCS tenant, cached until ~1m before expiry."""
    now = time.monotonic()
    cached_token = _GRAPH_TOKEN_CACHE.get("token")
    if cached_token and _GRAPH_TOKEN_CACHE.get("expires_at", 0.0) > now + 60:
        return cached_token

    client_id = os.environ.get("VECTOR_CLIENT_ID")
    client_secret = os.environ.get("VECTOR_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise HTTPException(
            status_code=503,
            detail="VECTOR_CLIENT_ID / VECTOR_CLIENT_SECRET not configured",
        )

    url = f"https://login.microsoftonline.com/{_GCS_TENANT_ID}/oauth2/v2.0/token"
    body = urllib.parse.urlencode(
        {
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "https://graph.microsoft.com/.default",
            "grant_type": "client_credentials",
        }
    ).encode("utf-8")
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as exc:
        logger.exception("graph token request failed")
        raise HTTPException(status_code=502, detail=f"graph token failed: {exc}")

    token = data.get("access_token")
    if not token:
        raise HTTPException(status_code=502, detail="graph token response missing access_token")

    _GRAPH_TOKEN_CACHE["token"] = token
    _GRAPH_TOKEN_CACHE["expires_at"] = now + int(data.get("expires_in", 3600))
    return token


def _graph_get(path_with_query: str) -> dict:
    token = _get_graph_token()
    url = f"https://graph.microsoft.com/v1.0{path_with_query}"
    req = urllib.request.Request(url, method="GET")
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Accept", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        try:
            body = exc.read().decode("utf-8", errors="replace")[:300]
        except Exception:
            body = ""
        logger.error("graph %s -> %s %s", path_with_query, exc.code, body)
        raise HTTPException(status_code=502, detail=f"graph {exc.code}: {body}")
    except Exception as exc:
        logger.exception("graph request failed")
        raise HTTPException(status_code=502, detail=f"graph failed: {exc}")


@app.get("/api/governance/guest-users")
def governance_guest_users() -> list[dict]:
    """List guest users in the GCS tenant via Microsoft Graph."""
    query = urllib.parse.urlencode(
        {
            "$filter": "userType eq 'Guest'",
            "$select": "id,displayName,mail,userPrincipalName,createdDateTime,signInActivity",
        }
    )
    data = _graph_get(f"/users?{query}")
    out: list[dict] = []
    for u in data.get("value", []) or []:
        sign_in = u.get("signInActivity") or {}
        out.append(
            {
                "id": u.get("id"),
                "displayName": u.get("displayName"),
                "mail": u.get("mail") or u.get("userPrincipalName"),
                "userPrincipalName": u.get("userPrincipalName"),
                "createdDateTime": u.get("createdDateTime"),
                "lastSignIn": sign_in.get("lastSignInDateTime"),
            }
        )
    return out


# ============================================================================
# JWT auth middleware
# ============================================================================
#
# /api/* is gated by a Bearer token issued by the vector-auth-server sidecar.
# The payload looks like { email, role, name, initials, exp, iat }. The
# decoded dict is stashed on request.state.user so individual endpoints can
# gate on role without re-parsing the token.
#
# /api/ingest/* is exempt -- those endpoints (currently only the INKY
# webhook) carry their own shared-secret header and are NOT called by the
# browser, so they must not require a user JWT.

_JWT_SECRET = os.environ.get("JWT_SECRET")
_JWT_ALGORITHM = "HS256"

if not _JWT_SECRET:
    logger.error(
        "JWT_SECRET not set -- /api/* requests will be rejected with 500",
    )


@app.middleware("http")
async def jwt_auth_middleware(request: Request, call_next):
    path = request.url.path
    if path.startswith("/api/") and not path.startswith("/api/ingest/"):
        if not _JWT_SECRET:
            return JSONResponse(
                {"detail": "server misconfigured: JWT_SECRET unset"},
                status_code=500,
            )
        header = request.headers.get("authorization", "")
        if not header.lower().startswith("bearer "):
            return JSONResponse(
                {"detail": "unauthorized"},
                status_code=401,
            )
        token = header[7:].strip()
        try:
            payload = jwt.decode(token, _JWT_SECRET, algorithms=[_JWT_ALGORITHM])
        except jwt.ExpiredSignatureError:
            return JSONResponse({"detail": "token expired"}, status_code=401)
        except jwt.PyJWTError:
            return JSONResponse({"detail": "invalid token"}, status_code=401)
        request.state.user = payload
    return await call_next(request)


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
    here (so clients see a real API error, not the HTML shell). /auth/* is
    NOT served by FastAPI at all -- the React frontend calls the auth
    sidecar directly. Everything else either serves a matching file from
    the built Vite bundle or returns index.html so React Router can take
    over client-side routing.
    """
    if full_path.startswith("api/") or full_path == "api":
        raise HTTPException(status_code=404)
    if full_path.startswith("auth/") or full_path == "auth":
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
