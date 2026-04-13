"""NERO Vector UI backend (FastAPI).

Serves the /api/* JSON endpoints that power the operator dashboard plus
the pre-built React SPA bundle as static files. Everything runs on a
single port so the full UI fits behind a single Cloudflare Access app.
"""

from __future__ import annotations

import json
import logging
import os
import re
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


@app.get("/api/users/{entity_key}/emails")
def user_emails(
    entity_key: str,
    direction: str | None = Query(None),
    search: str | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> list[dict]:
    """Email envelope metadata from vector_message_trace for this user.

    entity_key is the standard tenant_id::user_id composite; we pull the
    email out of the right-hand side and match it against both
    sender_address and recipient_address.
    """
    user_email = entity_key.split("::", 1)[1] if "::" in entity_key else entity_key
    return db.fetch_all(
        """
        SELECT
            id::text,
            message_id,
            sender_address,
            recipient_address,
            subject,
            received,
            status,
            size_bytes,
            direction
        FROM vector_message_trace
        WHERE (sender_address = %s OR recipient_address = %s)
          AND (%s::text IS NULL OR direction = %s)
          AND (%s::text IS NULL OR subject ILIKE '%%' || %s || '%%')
        ORDER BY received DESC
        LIMIT %s OFFSET %s
        """,
        (
            user_email, user_email,
            direction, direction,
            search, search,
            limit, offset,
        ),
    )


# ============================================================================
# unified feed (UAL + INKY together, sorted by timestamp)
# ============================================================================

@app.get("/api/feed/recent")
def feed_recent(limit: int = Query(25, ge=1, le=200)) -> list[dict]:
    """Combined recent-activity feed. UAL and INKY rows are unioned into
    a single list so the Dashboard feed can render both in one stream.
    Each row carries a ``kind`` of "ual" or "inky" which the frontend
    uses to pick the right card treatment."""
    ual_rows = db.fetch_all(
        """
        SELECT
            id::text,
            'ual'::text      AS kind,
            timestamp,
            client_name,
            tenant_id,
            user_id,
            entity_key,
            event_type,
            workload,
            result_status,
            client_ip,
            NULL::text       AS subject,
            NULL::text       AS sender,
            NULL::text       AS verdict,
            NULL::boolean    AS aitm_detected
        FROM vector_events
        ORDER BY timestamp DESC
        LIMIT %s
        """,
        (limit,),
    )
    inky_rows = db.fetch_all(
        """
        SELECT
            id::text,
            'inky'::text     AS kind,
            timestamp,
            client_name,
            tenant_id,
            recipient        AS user_id,
            CASE
                WHEN tenant_id IS NOT NULL AND recipient IS NOT NULL
                THEN tenant_id || '::' || recipient
                ELSE NULL
            END              AS entity_key,
            event_type,
            'INKY'::text     AS workload,
            verdict          AS result_status,
            NULL::text       AS client_ip,
            subject,
            sender,
            verdict,
            aitm_detected
        FROM vector_inky_events
        ORDER BY timestamp DESC
        LIMIT %s
        """,
        (limit,),
    )
    combined = (ual_rows or []) + (inky_rows or [])
    combined.sort(key=lambda r: r.get("timestamp") or "", reverse=True)
    return combined[:limit]


# ============================================================================
# watchlist (active INKY correlation pins)
# ============================================================================

@app.get("/api/watchlist")
def watchlist(status: str | None = Query(None)) -> list[dict]:
    """Watchlist pins with the latest INKY event (if any) joined in for
    display. Accepts an optional ``?status=`` filter
    (``active``/``escalated``/``expired``); anything else (including
    ``all`` or missing) returns every pin. The Phase-2 correlation
    engine reads the same table to escalate on anomalous auth."""
    normalized = (status or "").strip().lower()
    status_filter = normalized if normalized in {"active", "escalated", "expired"} else None
    return db.fetch_all(
        """
        SELECT
            w.id::text,
            w.tenant_id,
            w.client_name,
            w.user_email,
            w.trigger_type,
            w.trigger_details,
            w.created_at,
            w.expires_at,
            w.status,
            w.incident_id::text,
            (SELECT i.sender  FROM vector_inky_events i
              WHERE i.recipient = w.user_email
                AND i.ingested_at > now() - INTERVAL '24 hours'
              ORDER BY i.timestamp DESC LIMIT 1) AS latest_sender,
            (SELECT i.subject FROM vector_inky_events i
              WHERE i.recipient = w.user_email
                AND i.ingested_at > now() - INTERVAL '24 hours'
              ORDER BY i.timestamp DESC LIMIT 1) AS latest_subject,
            (SELECT i.verdict FROM vector_inky_events i
              WHERE i.recipient = w.user_email
                AND i.ingested_at > now() - INTERVAL '24 hours'
              ORDER BY i.timestamp DESC LIMIT 1) AS latest_verdict
        FROM vector_watchlist w
        WHERE (%s::text IS NULL OR w.status = %s)
        ORDER BY w.created_at DESC
        LIMIT 200
        """,
        (status_filter, status_filter),
    )


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


def _parse_device_properties(dp: Any) -> dict | None:
    """Flatten a UAL DeviceProperties array-of-{Name,Value} into a dict.

    Returns None if ``dp`` isn't a list (or nothing usable came out of it).
    Values are left as-is except booleans, which are normalised to real
    Python bools so the frontend doesn't have to stringcase them.
    """
    if not isinstance(dp, list):
        return None
    out: dict = {
        "display_name":  None,
        "name":          None,
        "os":            None,
        "is_compliant":  None,
        "is_managed":    None,
        "browser":       None,
        "device_id":     None,
    }
    for prop in dp:
        if not isinstance(prop, dict):
            continue
        key = prop.get("Name")
        val = prop.get("Value")
        if key == "DisplayName":
            out["display_name"] = val
        elif key == "Name":
            out["name"] = val
        elif key == "OS":
            out["os"] = val
        elif key == "IsCompliant":
            out["is_compliant"] = str(val).strip().lower() == "true"
        elif key == "IsCompliantAndManaged":
            # Some UAL payloads use this combined flag. If it's False, the
            # device is either non-compliant or unmanaged; leave the two
            # individual flags at their current value if we haven't seen
            # them explicitly.
            if out["is_compliant"] is None and out["is_managed"] is None:
                both = str(val).strip().lower() == "true"
                out["is_compliant"] = both
                out["is_managed"] = both
        elif key == "IsManaged":
            out["is_managed"] = str(val).strip().lower() == "true"
        elif key == "BrowserType":
            out["browser"] = val
        elif key == "DeviceId":
            out["device_id"] = val
    if not any(out.values()):
        return None
    return out


@app.get("/api/governance/unmanaged-devices/{user_id}/devices")
def governance_unmanaged_devices_detail(user_id: str) -> list[dict]:
    """Per-device breakdown for a given user on the Unmanaged Devices tab.

    Pulls every distinct DeviceProperties blob across that user's UAL
    events in GCS, parses each one, drops anything that's both
    compliant *and* managed, and returns a deduplicated list keyed by
    device display name / id. Each row carries last_seen timestamp.
    """
    rows = db.fetch_all(
        """
        SELECT
            raw_json->'DeviceProperties'::text AS device_properties,
            MAX(timestamp)                     AS last_seen
        FROM vector_events
        WHERE client_name = %s
          AND user_id     = %s
          AND raw_json ? 'DeviceProperties'
        GROUP BY raw_json->'DeviceProperties'::text
        ORDER BY last_seen DESC
        LIMIT 200
        """,
        (_GCS, user_id),
    )

    # The GROUP BY returned JSONB::text -- re-parse each blob once.
    by_device: dict[str, dict] = {}
    for row in rows:
        raw = row.get("device_properties")
        if not raw:
            continue
        try:
            parsed_list = json.loads(raw) if isinstance(raw, str) else raw
        except (ValueError, TypeError):
            continue
        parsed = _parse_device_properties(parsed_list)
        if not parsed:
            continue
        # Keep only devices that are non-compliant OR unmanaged --
        # anything else doesn't belong in the Unmanaged Devices view.
        if parsed["is_compliant"] is not False and parsed["is_managed"] is not False:
            continue
        key = (
            parsed.get("display_name")
            or parsed.get("name")
            or parsed.get("device_id")
            or "unknown"
        )
        existing = by_device.get(key)
        last_seen = row.get("last_seen")
        if existing is None or (last_seen and last_seen > existing.get("last_seen")):
            parsed["last_seen"] = last_seen
            by_device[key] = parsed

    # Sort most-recently-seen first.
    return sorted(
        by_device.values(),
        key=lambda d: d.get("last_seen") or "",
        reverse=True,
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


# NERO Vector's own app registration — excluded from OAuth Apps findings
# because it's expected to collect consent (it's literally this process).
_NERO_VECTOR_APP_ID = "d6cf81b1-3067-46b2-96ef-8c1e946c55de"

_GUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)

# Cache of resolved app_id -> displayName. Negative lookups are stored as
# "" so we don't hammer Graph on every page load for missing apps.
_SP_NAME_CACHE: dict[str, str] = {}


def _resolve_app_display_name(app_id: str | None) -> str | None:
    """Look up a servicePrincipal displayName by appId via Microsoft Graph.

    Returns None for non-GUID input or when Graph returns nothing. Uses
    the same VECTOR_CLIENT_ID/SECRET client credentials as the guest
    users endpoint, with responses cached for the lifetime of the
    process.
    """
    if not app_id or not _GUID_RE.match(app_id):
        return None
    if app_id in _SP_NAME_CACHE:
        cached = _SP_NAME_CACHE[app_id]
        return cached or None
    try:
        query = urllib.parse.urlencode(
            {
                "$filter": f"appId eq '{app_id}'",
                "$select": "displayName,appId",
            }
        )
        data = _graph_get(f"/servicePrincipals?{query}")
    except HTTPException:
        # _graph_get already logged; remember the failure so we don't retry
        # every single page load.
        _SP_NAME_CACHE[app_id] = ""
        return None

    values = data.get("value") or []
    if not values:
        _SP_NAME_CACHE[app_id] = ""
        return None
    name = str(values[0].get("displayName") or "").strip()
    _SP_NAME_CACHE[app_id] = name
    return name or None


@app.get("/api/governance/oauth-apps")
def governance_oauth_apps() -> list[dict]:
    """Consent to application. events grouped by app, with the NERO
    Vector app itself filtered out and every remaining GUID resolved
    to its servicePrincipal displayName (cached)."""
    rows = db.fetch_all(
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
          AND COALESCE(raw_json->>'ObjectId', '') <> %s
        GROUP BY raw_json->>'ObjectId'
        ORDER BY user_count DESC
        LIMIT 100
        """,
        (_GCS, _NERO_VECTOR_APP_ID),
    )
    for row in rows:
        app_id = row.get("app_name")
        row["app_id"] = app_id
        resolved = _resolve_app_display_name(app_id) if app_id else None
        row["display_name"] = resolved or app_id
    return rows


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


_STALE_REQUIRED_DAYS = 14


@app.get("/api/governance/stale-accounts")
def governance_stale_accounts() -> dict:
    """Users with activity in the last 30d but no UserLoggedIn in the last
    30d. Guarded by a baseline check: if we have less than
    ``_STALE_REQUIRED_DAYS`` of data for GCS, return an
    ``insufficient_data`` envelope so the UI can render a
    "check back later" state instead of listing every active user as
    "stale" during the initial monitoring window.
    """
    baseline = db.fetch_one(
        """
        SELECT
            COALESCE(
                EXTRACT(EPOCH FROM (now() - MIN(timestamp))) / 86400.0,
                0
            )::float AS days_of_data
        FROM vector_events
        WHERE client_name = %s
        """,
        (_GCS,),
    ) or {}
    days_of_data = float(baseline.get("days_of_data") or 0)

    if days_of_data < _STALE_REQUIRED_DAYS:
        return {
            "sufficient_data": False,
            "days_available": round(days_of_data, 1),
            "required_days": _STALE_REQUIRED_DAYS,
            "rows": [],
        }

    rows = db.fetch_all(
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
    return {
        "sufficient_data": True,
        "days_available": round(days_of_data, 1),
        "required_days": _STALE_REQUIRED_DAYS,
        "rows": rows,
    }


@app.get("/api/governance/mfa-changes")
def governance_mfa_changes() -> list[dict]:
    """StrongAuthentication / MFA config mutations.

    Filters out ServicePrincipal_-prefixed user_id values so the view
    only shows human identities (application sign-ins / service
    principal auth lives elsewhere).
    """
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
          AND user_id NOT LIKE 'ServicePrincipal_%%'
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
    """List guest users in the GCS tenant via Microsoft Graph.

    Hand-builds the query string with literal ``$`` so Graph parses
    ``$filter`` / ``$select`` correctly (python's ``urlencode`` would
    percent-encode the ``$``), and requests ``userType`` in the
    projection so we can defensively re-check the server-side filter
    client-side -- if Graph ever returns a Member we drop it here
    instead of showing it as a guest.
    """
    filter_value = urllib.parse.quote("userType eq 'Guest'", safe="")
    path = (
        "/users"
        f"?$filter={filter_value}"
        "&$select=id,displayName,mail,userPrincipalName,"
        "createdDateTime,signInActivity,userType,accountEnabled"
        "&$top=500"
    )

    data = _graph_get(path)
    raw_values = data.get("value", []) or []

    out: list[dict] = []
    for u in raw_values:
        # Defensive: the server-side filter is the source of truth,
        # but we re-check here in case a future Graph beta quirk lets
        # a Member slip through.
        if u.get("userType") and u.get("userType") != "Guest":
            continue
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
