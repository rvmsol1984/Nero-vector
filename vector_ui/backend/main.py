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
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import Body, FastAPI, HTTPException, Query, Request
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
    ip: str | None = Query(None, description="substring search over client_ip"),
    source: str | None = Query(
        None,
        description="'ual' (default) for vector_events, 'inky' to read from vector_inky_events",
    ),
) -> list[dict]:
    normalized_source = (source or "ual").strip().lower()
    if normalized_source == "inky":
        # Source = INKY: pull from vector_inky_events and project into
        # the same row shape EventCard / Events page expects. user_id is
        # the recipient, entity_key is synthesized tenant::recipient,
        # workload is fixed to "INKY", and the verdict field doubles as
        # result_status so the status pill still has something to show.
        return db.fetch_all(
            """
            SELECT id::text,
                   timestamp,
                   client_name,
                   tenant_id,
                   recipient AS user_id,
                   CASE
                       WHEN tenant_id IS NOT NULL AND recipient IS NOT NULL
                       THEN tenant_id || '::' || recipient
                       ELSE NULL
                   END       AS entity_key,
                   event_type,
                   'INKY'::text AS workload,
                   verdict   AS result_status,
                   NULL::text AS client_ip,
                   'inky'::text AS source,
                   subject,
                   sender,
                   verdict,
                   aitm_detected
            FROM vector_inky_events
            WHERE (%s::text IS NULL OR client_name = %s)
              AND (%s::text IS NULL OR event_type  = %s)
              AND (%s::text IS NULL OR recipient ILIKE '%%' || %s || '%%')
            ORDER BY timestamp DESC
            LIMIT %s OFFSET %s
            """,
            (tenant, tenant, event_type, event_type, user, user, limit, offset),
        )

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
               client_ip,
               'ual'::text AS source
        FROM vector_events
        WHERE (%s::text IS NULL OR client_name = %s)
          AND (%s::text IS NULL OR event_type  = %s)
          AND (%s::text IS NULL OR workload    = %s)
          AND (%s::text IS NULL OR user_id ILIKE '%%' || %s || '%%')
          AND (%s::text IS NULL OR client_ip ILIKE '%%' || %s || '%%')
        ORDER BY timestamp DESC
        LIMIT %s OFFSET %s
        """,
        (tenant, tenant, event_type, event_type, workload, workload, user, user, ip, ip, limit, offset),
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


@app.get("/api/users/{entity_key}/edr")
def user_edr(entity_key: str) -> list[dict]:
    """Datto EDR events for this user.

    Matches either on user_account (case-insensitive) for alerts that
    identify a signed-in user, or on host_name against any device the
    user has been seen using in UAL (DeviceProperties -> DisplayName).
    This covers the common EDR shape where the raw alert only carries a
    hostname, not a logged-in user.
    """
    user_email = entity_key.split("::", 1)[1] if "::" in entity_key else entity_key
    return db.fetch_all(
        """
        SELECT
            id::text,
            timestamp,
            event_type,
            severity,
            host_name,
            host_ip,
            user_account,
            process_name,
            process_path,
            command_line,
            threat_name,
            threat_score,
            action_taken,
            client_name,
            tenant_id,
            raw_json
        FROM vector_edr_events
        WHERE LOWER(user_account) = LOWER(%s)
           OR LOWER(host_name) IN (
               SELECT LOWER(raw_json->>'DeviceName')
               FROM vector_events
               WHERE user_id = %s
                 AND raw_json->>'DeviceName' IS NOT NULL
           )
        ORDER BY timestamp DESC
        LIMIT 100
        """,
        (user_email, user_email),
    )


@app.get("/api/users/{entity_key}/threatlocker")
def user_threatlocker(entity_key: str) -> list[dict]:
    """ThreatLocker ActionLog events for this user.

    Matching strategy mirrors the EDR tab: fall back from an exact
    username match (case-insensitive) to a local-part substring
    match on ``username``, and union with any row whose ``hostname``
    matches a DeviceName seen for this user in UAL. This covers the
    common case where ThreatLocker records the Windows login name
    (not the UPN) while UAL carries the UPN plus a device list.
    """
    user_email = entity_key.split("::", 1)[1] if "::" in entity_key else entity_key
    local_part = user_email.split("@", 1)[0] if "@" in user_email else user_email
    return db.fetch_all(
        """
        SELECT
            id::text,
            event_time,
            hostname,
            username,
            full_path,
            process_path,
            action_type,
            action,
            action_id,
            policy_name,
            hash,
            client_name,
            tenant_id,
            raw_json
        FROM vector_threatlocker_events
        WHERE LOWER(username) = LOWER(%s)
           OR LOWER(username) LIKE '%%' || LOWER(%s) || '%%'
           OR LOWER(hostname) IN (
               SELECT LOWER(raw_json->>'DeviceName')
               FROM vector_events
               WHERE user_id = %s
                 AND raw_json->>'DeviceName' IS NOT NULL
           )
        ORDER BY event_time DESC
        LIMIT 100
        """,
        (user_email, local_part, user_email),
    )


@app.get("/api/users/{entity_key}/ioc")
def user_ioc_matches(entity_key: str) -> list[dict]:
    """OpenCTI IOC matches whose triggering event belongs to this user.

    The join goes through vector_events on id = matched_event_id and
    then filters on entity_key so a watchlist escalation on a UAL row
    shows up under the right user even if the IOC value itself has
    been seen on other tenants.
    """
    return db.fetch_all(
        """
        SELECT
            m.id::text,
            m.ioc_value,
            m.ioc_type,
            m.confidence,
            m.indicator_name,
            m.opencti_id,
            m.client_name,
            m.tenant_id,
            m.matched_at,
            m.matched_event_id::text,
            m.raw_json
        FROM vector_ioc_matches m
        JOIN vector_events ve ON ve.id = m.matched_event_id
        WHERE ve.entity_key = %s
        ORDER BY m.matched_at DESC
        LIMIT 100
        """,
        (entity_key,),
    )


@app.get("/api/users/{entity_key}/emails")
def user_emails(
    entity_key: str,
    direction: str | None = Query(None),
    search: str | None = Query(None),
    attachment: str | None = Query(
        None,
        description=(
            "Case-insensitive substring match against any element of "
            "attachment_names (populated by the MessageTraceIngestor's "
            "post-insert Graph backfill)."
        ),
    ),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> list[dict]:
    """Email envelope metadata from vector_message_trace for this user.

    entity_key is the standard tenant_id::user_id composite; we pull the
    email out of the right-hand side and match it against both
    sender_address and recipient_address.

    Filters:
      * direction  -- exact match on direction (IN/OUT/ACTIVITY)
      * search     -- ILIKE substring against subject
      * attachment -- case-insensitive substring against any element
                      of attachment_names (the MessageTraceIngestor
                      backfills names post-insert via Graph)
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
            direction,
            has_attachments,
            attachment_names
        FROM vector_message_trace
        WHERE (sender_address = %s OR recipient_address = %s)
          AND (%s::text IS NULL OR direction = %s)
          AND (%s::text IS NULL OR subject ILIKE '%%' || %s || '%%')
          AND (
              %s::text IS NULL
              OR EXISTS (
                  SELECT 1
                  FROM unnest(attachment_names) AS n
                  WHERE n ILIKE '%%' || %s || '%%'
              )
          )
        ORDER BY received DESC
        LIMIT %s OFFSET %s
        """,
        (
            user_email, user_email,
            direction, direction,
            search, search,
            attachment, attachment,
            limit, offset,
        ),
    )


@app.get("/api/users/{entity_key}/emails/{message_id}/attachments")
def user_email_attachments(entity_key: str, message_id: str) -> list[dict]:
    """Fetch attachment metadata for a single email via Microsoft Graph.

    ``message_id`` is the RFC-2822 ``Message-ID`` header stored in
    vector_message_trace (what Exchange calls ``InternetMessageId``).
    Graph doesn't expose a direct lookup on that field, so we first
    search for the matching message via ``/users/{email}/messages``
    with a ``$filter``, then pull its ``/attachments`` list if
    ``hasAttachments`` is true.

    Returns ``[]`` on any error (Graph unavailable, message not
    found, no attachments) so the frontend can degrade gracefully.
    """
    user_email = entity_key.split("::", 1)[1] if "::" in entity_key else entity_key
    if not user_email or not message_id:
        return []

    # Graph's $filter on internetMessageId needs angle brackets.
    mid = message_id.strip()
    if not mid.startswith("<"):
        mid = f"<{mid}>"
    if not mid.endswith(">"):
        mid = f"{mid}>"

    try:
        filter_str = urllib.parse.quote(
            f"internetMessageId eq '{mid}'",
            safe="",
        )
        search_path = (
            f"/users/{urllib.parse.quote(user_email, safe='@')}"
            f"/messages?$filter={filter_str}&$select=id,hasAttachments&$top=1"
        )
        data = _graph_get(search_path)
    except HTTPException:
        return []
    except Exception:
        logger.debug("email attachment search failed", exc_info=True)
        return []

    messages = data.get("value") or []
    if not messages:
        return []
    msg = messages[0]
    if not msg.get("hasAttachments"):
        return []
    graph_id = msg.get("id")
    if not graph_id:
        return []

    try:
        att_path = (
            f"/users/{urllib.parse.quote(user_email, safe='@')}"
            f"/messages/{urllib.parse.quote(graph_id, safe='')}"
            f"/attachments?$select=name,size,contentType"
        )
        att_data = _graph_get(att_path)
    except HTTPException:
        return []
    except Exception:
        logger.debug("email attachment fetch failed", exc_info=True)
        return []

    attachments = att_data.get("value") or []
    return [
        {
            "name":         a.get("name"),
            "size_bytes":   a.get("size"),
            "content_type": a.get("contentType"),
        }
        for a in attachments
        if isinstance(a, dict)
    ]


# ============================================================================
# unified feed (UAL + INKY together, sorted by timestamp)
# ============================================================================

def _fetch_ual_feed_rows(limit: int) -> list[dict]:
    """UAL rows projected into the shared feed shape. Carries both
    ``kind`` and ``source`` set to ``"ual"`` so frontends still using
    the original ``kind`` field (EventCard) keep working while new code
    can prefer the ``source`` alias."""
    return db.fetch_all(
        """
        SELECT
            id::text,
            'ual'::text      AS kind,
            'ual'::text      AS source,
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


def _fetch_inky_feed_rows(limit: int) -> list[dict]:
    """INKY rows projected into the shared feed shape. ``user_id``
    is set to the recipient email and ``entity_key`` is synthesized
    as ``tenant_id::recipient`` so the dashboard can deep-link to the
    existing user detail page."""
    return db.fetch_all(
        """
        SELECT
            id::text,
            'inky'::text     AS kind,
            'inky'::text     AS source,
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


def _fetch_edr_feed_rows(limit: int) -> list[dict]:
    """EDR rows projected into the shared feed shape. ``user_id`` is
    set to the host/user most strongly tied to the alert (user_account
    when known, otherwise host_name) so the dashboard card has something
    meaningful to print. ``entity_key`` is synthesized when we have both
    a tenant and a user_account so the card still deep-links to the
    existing user detail page."""
    return db.fetch_all(
        """
        SELECT
            id::text,
            'edr'::text      AS kind,
            'edr'::text      AS source,
            timestamp,
            client_name,
            tenant_id,
            COALESCE(user_account, host_name) AS user_id,
            CASE
                WHEN tenant_id IS NOT NULL AND user_account IS NOT NULL
                THEN tenant_id || '::' || user_account
                ELSE NULL
            END              AS entity_key,
            event_type,
            'EDR'::text      AS workload,
            severity         AS result_status,
            host_ip          AS client_ip,
            NULL::text       AS subject,
            NULL::text       AS sender,
            NULL::text       AS verdict,
            NULL::boolean    AS aitm_detected,
            host_name,
            threat_name,
            severity,
            action_taken,
            process_name
        FROM vector_edr_events
        ORDER BY timestamp DESC
        LIMIT %s
        """,
        (limit,),
    )


def _merge_feed(
    ual_rows: list[dict],
    inky_rows: list[dict],
    edr_rows: list[dict] | None = None,
) -> list[dict]:
    combined = list(ual_rows or []) + list(inky_rows or []) + list(edr_rows or [])
    combined.sort(key=lambda r: r.get("timestamp") or "", reverse=True)
    return combined


@app.get("/api/feed/recent")
def feed_recent(limit: int = Query(25, ge=1, le=200)) -> list[dict]:
    """Original combined feed used by the v0.1 dashboard -- balanced
    split between UAL, INKY and EDR rows."""
    combined = _merge_feed(
        _fetch_ual_feed_rows(limit),
        _fetch_inky_feed_rows(min(limit, 20)),
        _fetch_edr_feed_rows(min(limit, 20)),
    )
    return combined[:limit]


@app.get("/api/dashboard/feed")
def dashboard_feed(
    ual_limit: int = Query(50, ge=1, le=500),
    inky_limit: int = Query(20, ge=0, le=200),
    edr_limit: int = Query(20, ge=0, le=200),
) -> list[dict]:
    """Dashboard feed -- pulls the last 50 UAL events, the last 20
    INKY events, and the last 20 EDR alerts and returns them merged and
    timestamp-sorted. Each row carries a ``source`` field
    ("ual" | "inky" | "edr") that the frontend uses to pick the card
    treatment."""
    return _merge_feed(
        _fetch_ual_feed_rows(ual_limit),
        _fetch_inky_feed_rows(inky_limit),
        _fetch_edr_feed_rows(edr_limit),
    )


# ============================================================================
# INKY stats (used by the Sources page card)
# ============================================================================

@app.get("/api/sources/edr-count")
def sources_edr_count() -> dict:
    """Total EDR events in vector_edr_events, with a per-severity
    breakdown so the Sources card can render both a headline count and
    a severity highlight."""
    total_row = db.fetch_one(
        "SELECT COUNT(*)::bigint AS count FROM vector_edr_events"
    ) or {}
    by_severity = db.fetch_all(
        """
        SELECT severity, COUNT(*)::bigint AS count
        FROM vector_edr_events
        GROUP BY severity
        ORDER BY count DESC
        """
    )
    return {
        "count":       int(total_row.get("count") or 0),
        "by_severity": by_severity,
    }


@app.get("/api/sources/inky-count")
def sources_inky_count() -> dict:
    """Total INKY events in vector_inky_events, with a per-verdict
    breakdown so the Sources card can render both a headline count and
    a DANGER highlight."""
    total_row = db.fetch_one(
        "SELECT COUNT(*)::bigint AS count FROM vector_inky_events"
    ) or {}
    by_verdict = db.fetch_all(
        """
        SELECT verdict, COUNT(*)::bigint AS count
        FROM vector_inky_events
        GROUP BY verdict
        ORDER BY count DESC
        """
    )
    return {
        "count":      int(total_row.get("count") or 0),
        "by_verdict": by_verdict,
    }


# ============================================================================
# IOC matches (produced by vector-ingest's IocEnricher worker)
# ============================================================================

@app.get("/api/ioc/matches")
def ioc_matches(limit: int = Query(50, ge=1, le=500), tenant: str | None = Query(None)) -> list[dict]:
    """Recent OpenCTI-backed IOC matches, optionally filtered by tenant."""
    return db.fetch_all(
        """
        SELECT
            m.id::text,
            m.ioc_value,
            m.ioc_type,
            m.confidence,
            m.indicator_name,
            m.opencti_id,
            m.client_name,
            m.tenant_id,
            m.matched_at,
            m.matched_event_id::text,
            m.raw_json,
            ve.user_id,
            ve.entity_key,
            ve.event_type,
            ve.workload
        FROM vector_ioc_matches m
        LEFT JOIN vector_events ve ON ve.id = m.matched_event_id
        WHERE (%s::text IS NULL OR m.client_name = %s)
        ORDER BY m.matched_at DESC
        LIMIT %s
        """,
        (tenant, tenant, limit),
    )


@app.get("/api/ioc/matches/{ioc_value:path}")
def ioc_matches_for_value(ioc_value: str) -> list[dict]:
    """Every recorded match for a single IOC value. ``ioc_value`` is
    taken as a path segment so we can pass URLs / sha256 / etc. verbatim
    without worrying about reserved characters -- FastAPI's ``:path``
    converter keeps slashes intact."""
    return db.fetch_all(
        """
        SELECT
            m.id::text,
            m.ioc_value,
            m.ioc_type,
            m.confidence,
            m.indicator_name,
            m.opencti_id,
            m.client_name,
            m.tenant_id,
            m.matched_at,
            m.matched_event_id::text,
            m.raw_json,
            ve.user_id,
            ve.entity_key,
            ve.event_type,
            ve.workload
        FROM vector_ioc_matches m
        LEFT JOIN vector_events ve ON ve.id = m.matched_event_id
        WHERE m.ioc_value = %s
        ORDER BY m.matched_at DESC
        LIMIT 200
        """,
        (ioc_value,),
    )


# ============================================================================
# incidents (Phase 2 scoring engine output)
# ============================================================================

_INCIDENT_STATUSES = {"open", "investigating", "contained", "closed"}


@app.get("/api/incidents")
@app.get("/api/incidents/list")
def incidents_list(
    status: str | None = Query(None),
    severity: str | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
) -> list[dict]:
    """Confirmed incidents produced by the Phase 2 scoring engine.

    Ordered by newest-first so the Incidents page defaults to the
    latest escalations. ``status`` and ``severity`` are optional
    string filters.
    """
    return db.fetch_all(
        """
        SELECT
            id::text,
            tenant_id,
            client_name,
            user_id,
            entity_key,
            incident_type,
            severity,
            status,
            score,
            title,
            summary,
            patient_zero,
            dwell_time_minutes,
            first_seen,
            last_seen,
            confirmed_at,
            contained_at,
            evidence,
            raw_signals,
            created_at,
            updated_at
        FROM vector_incidents
        WHERE (%s::text IS NULL OR status   = %s)
          AND (%s::text IS NULL OR severity = %s)
        ORDER BY confirmed_at DESC
        LIMIT %s
        """,
        (status, status, severity, severity, limit),
    )


@app.get("/api/incidents/stats")
def incidents_stats() -> dict:
    """Aggregate counts for the Incidents page header cards."""
    row = db.fetch_one(
        """
        SELECT
            COUNT(*) FILTER (WHERE status = 'open')::bigint                                   AS open_count,
            COUNT(*) FILTER (WHERE severity = 'critical' AND status = 'open')::bigint         AS critical,
            COUNT(*) FILTER (WHERE severity = 'high'     AND status = 'open')::bigint         AS high,
            COUNT(*) FILTER (WHERE confirmed_at > now() - INTERVAL '24 hours')::bigint        AS today,
            COUNT(*)::bigint                                                                  AS total
        FROM vector_incidents
        """
    ) or {}
    return {
        "open":     int(row.get("open_count") or 0),
        "critical": int(row.get("critical")   or 0),
        "high":     int(row.get("high")       or 0),
        "today":    int(row.get("today")      or 0),
        "total":    int(row.get("total")      or 0),
    }


@app.get("/api/incidents/{incident_id}")
def incidents_detail(incident_id: str) -> dict:
    """Full detail for one incident including its evidence blob and
    any linked vector_incident_events rows the scoring engine
    attached via create_incident()."""
    row = db.fetch_one(
        """
        SELECT
            id::text,
            tenant_id,
            client_name,
            user_id,
            entity_key,
            incident_type,
            severity,
            status,
            score,
            title,
            summary,
            patient_zero,
            dwell_time_minutes,
            first_seen,
            last_seen,
            confirmed_at,
            contained_at,
            evidence,
            raw_signals,
            created_at,
            updated_at
        FROM vector_incidents
        WHERE id = %s
        """,
        (incident_id,),
    )
    if not row:
        raise HTTPException(status_code=404, detail="incident not found")
    row["events"] = db.fetch_all(
        """
        SELECT
            id::text,
            incident_id::text,
            event_source,
            event_id::text,
            event_type,
            timestamp,
            significance,
            raw_json,
            added_at
        FROM vector_incident_events
        WHERE incident_id = %s
        ORDER BY timestamp DESC
        """,
        (incident_id,),
    )
    return row


@app.put("/api/incidents/{incident_id}/status")
def incidents_update_status(
    incident_id: str,
    payload: dict = Body(...),
) -> dict:
    """Flip an incident's lifecycle status. Accepted values:
    ``open``, ``investigating``, ``contained``, ``closed``. The
    contained_at timestamp is auto-stamped on the first transition
    to ``contained``."""
    new_status = str((payload or {}).get("status") or "").strip().lower()
    if new_status not in _INCIDENT_STATUSES:
        raise HTTPException(
            status_code=400,
            detail=f"status must be one of {sorted(_INCIDENT_STATUSES)}",
        )
    row = db.execute_returning(
        """
        UPDATE vector_incidents
        SET
            status       = %s,
            updated_at   = now(),
            contained_at = CASE
                WHEN %s = 'contained' AND contained_at IS NULL THEN now()
                ELSE contained_at
            END
        WHERE id = %s
        RETURNING id::text, status, updated_at, contained_at
        """,
        (new_status, new_status, incident_id),
    )
    if not row:
        raise HTTPException(status_code=404, detail="incident not found")
    return row


# ============================================================================
# baseline profiles (Phase 2 scoring engine input)
# ============================================================================

@app.get("/api/baseline/stats")
def baseline_stats() -> dict:
    """Aggregate header card counts for the Baseline page.

    ``login_countries`` is stored as a JSONB object
    (``{"US": 42, ...}``) by the scoring engine's BaselineEngine, so
    we count its keys instead of its array length. ``known_ips`` is
    a flat JSONB array.
    """
    row = db.fetch_one(
        """
        SELECT
            COUNT(*)::bigint                                                 AS total_baselines,
            COUNT(*) FILTER (WHERE computed_at > now() - INTERVAL '1 hour')::bigint AS fresh,
            MAX(computed_at)                                                 AS last_computed,
            AVG(
                CASE
                    WHEN jsonb_typeof(known_ips) = 'array'
                    THEN jsonb_array_length(known_ips)
                    ELSE 0
                END
            )::float                                                         AS avg_known_ips,
            AVG(
                CASE
                    WHEN jsonb_typeof(login_countries) = 'object'
                    THEN (SELECT COUNT(*) FROM jsonb_object_keys(login_countries))
                    WHEN jsonb_typeof(login_countries) = 'array'
                    THEN jsonb_array_length(login_countries)
                    ELSE 0
                END
            )::float                                                         AS avg_countries
        FROM vector_user_baselines
        """
    ) or {}
    return {
        "total_baselines": int(row.get("total_baselines") or 0),
        "fresh":           int(row.get("fresh")           or 0),
        "last_computed":   row.get("last_computed"),
        "avg_known_ips":   float(row.get("avg_known_ips")  or 0),
        "avg_countries":   float(row.get("avg_countries")  or 0),
    }


@app.get("/api/baseline/list")
def baseline_list(
    limit: int = Query(50, ge=1, le=500),
    search: str | None = Query(None),
) -> list[dict]:
    """Baseline rows for the Baseline page table. ``search`` is a
    substring match against ``user_id``; empty/missing returns every
    row. Returns the full JSONB blobs for ``known_ips``,
    ``login_countries`` and ``known_devices`` so the expand panel
    can render them without a second round-trip.
    """
    s = (search or "").strip() or None
    return db.fetch_all(
        """
        SELECT
            user_id,
            tenant_id,
            computed_at,
            CASE
                WHEN jsonb_typeof(known_ips) = 'array'
                THEN jsonb_array_length(known_ips)
                ELSE 0
            END                                                              AS ip_count,
            CASE
                WHEN jsonb_typeof(login_countries) = 'object'
                THEN (SELECT COUNT(*)::int FROM jsonb_object_keys(login_countries))
                WHEN jsonb_typeof(login_countries) = 'array'
                THEN jsonb_array_length(login_countries)
                ELSE 0
            END                                                              AS country_count,
            CASE
                WHEN jsonb_typeof(known_devices) = 'array'
                THEN jsonb_array_length(known_devices)
                ELSE 0
            END                                                              AS device_count,
            login_countries,
            known_ips,
            known_devices,
            avg_daily_events,
            avg_daily_logins,
            baseline_days
        FROM vector_user_baselines
        WHERE (%s::text IS NULL OR user_id ILIKE '%%' || %s || '%%')
        ORDER BY computed_at DESC
        LIMIT %s
        """,
        (s, s, limit),
    )


@app.get("/api/baseline/{entity_key}")
def baseline_detail(entity_key: str) -> dict:
    """Full baseline profile for one user. ``entity_key`` is the
    standard ``tenant_id::user_id`` composite used elsewhere in the
    UI; callers can also pass a bare user_id and we'll match on
    ``user_id`` alone."""
    if "::" in entity_key:
        tenant_id, user_id = entity_key.split("::", 1)
        row = db.fetch_one(
            """
            SELECT
                user_id,
                tenant_id,
                computed_at,
                login_hours,
                login_countries,
                login_asns,
                known_devices,
                known_ips,
                avg_daily_events,
                avg_daily_logins,
                baseline_days
            FROM vector_user_baselines
            WHERE tenant_id = %s AND user_id = %s
            """,
            (tenant_id, user_id),
        )
    else:
        row = db.fetch_one(
            """
            SELECT
                user_id,
                tenant_id,
                computed_at,
                login_hours,
                login_countries,
                login_asns,
                known_devices,
                known_ips,
                avg_daily_events,
                avg_daily_logins,
                baseline_days
            FROM vector_user_baselines
            WHERE user_id = %s
            ORDER BY computed_at DESC
            LIMIT 1
            """,
            (entity_key,),
        )
    if not row:
        raise HTTPException(status_code=404, detail="baseline not found")
    return row


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


@app.get("/api/governance/threatlocker")
def governance_threatlocker(
    tenant: str | None = Query(None),
) -> list[dict]:
    """ThreatLocker block/ringfenced/elevation events grouped by
    (host, user, action). action_id filter matches the blocking
    action codes: 2=Deny, 3=Ringfenced, 6=Elevated."""
    return db.fetch_all(
        """
        SELECT
            hostname,
            username,
            action_type,
            action,
            action_id,
            policy_name,
            COUNT(*)::bigint           AS event_count,
            MAX(event_time)            AS last_seen
        FROM vector_threatlocker_events
        WHERE action_id IN (2, 3, 6)
          AND (%s::text IS NULL OR client_name = %s)
        GROUP BY hostname, username, action_type, action, action_id, policy_name
        ORDER BY event_count DESC
        LIMIT 100
        """,
        (tenant, tenant),
    )


@app.get("/api/governance/threatlocker/events")
def governance_threatlocker_events(
    hostname: str | None = Query(None),
    username: str | None = Query(None),
    action: str | None = Query(None),
    action_type: str | None = Query(None),
    policy_name: str | None = Query(None),
    limit: int = Query(10, ge=1, le=100),
) -> list[dict]:
    """Raw ThreatLocker ActionLog rows filtered to one governance
    aggregation cell. Used by the Governance ThreatLocker tab's
    in-place row expand."""
    return db.fetch_all(
        """
        SELECT
            id::text,
            event_time,
            hostname,
            username,
            full_path,
            process_path,
            action_type,
            action,
            action_id,
            policy_name,
            hash,
            raw_json
        FROM vector_threatlocker_events
        WHERE client_name = %s
          AND (%s::text IS NULL OR hostname    = %s)
          AND (%s::text IS NULL OR username    = %s)
          AND (%s::text IS NULL OR action      = %s)
          AND (%s::text IS NULL OR action_type = %s)
          AND (%s::text IS NULL OR policy_name = %s)
        ORDER BY event_time DESC
        LIMIT %s
        """,
        (
            _GCS,
            hostname, hostname,
            username, username,
            action, action,
            action_type, action_type,
            policy_name, policy_name,
            limit,
        ),
    )


@app.get("/api/governance/edr-alerts")
def governance_edr_alerts(
    tenant: str | None = Query(None),
) -> list[dict]:
    """Datto EDR alerts aggregated by (host, user, threat, severity)."""
    return db.fetch_all(
        """
        SELECT
            host_name,
            user_account,
            threat_name,
            severity,
            COUNT(*)::bigint            AS alert_count,
            MAX(timestamp)              AS last_seen,
            COALESCE(
                array_agg(DISTINCT action_taken)
                    FILTER (WHERE action_taken IS NOT NULL),
                ARRAY[]::text[]
            )                           AS actions
        FROM vector_edr_events
        WHERE (%s::text IS NULL OR client_name = %s)
        GROUP BY host_name, user_account, threat_name, severity
        ORDER BY alert_count DESC
        LIMIT 200
        """,
        (tenant, tenant),
    )


@app.get("/api/governance/edr-alerts/events")
def governance_edr_alerts_events(
    hostname: str | None = Query(None),
    username: str | None = Query(None),
    threat_name: str | None = Query(None),
    severity: str | None = Query(None),
    limit: int = Query(10, ge=1, le=100),
) -> list[dict]:
    """Raw EDR events filtered to one governance aggregation cell.
    Used by the Governance EDR Alerts tab's in-place row expand."""
    return db.fetch_all(
        """
        SELECT
            id::text,
            timestamp,
            event_type,
            severity,
            host_name,
            host_ip,
            user_account,
            process_name,
            process_path,
            threat_name,
            threat_score,
            action_taken,
            raw_json
        FROM vector_edr_events
        WHERE client_name = %s
          AND (%s::text IS NULL OR host_name    = %s)
          AND (%s::text IS NULL OR user_account = %s)
          AND (%s::text IS NULL OR threat_name  = %s)
          AND (%s::text IS NULL OR severity     = %s)
        ORDER BY timestamp DESC
        LIMIT %s
        """,
        (
            _GCS,
            hostname, hostname,
            username, username,
            threat_name, threat_name,
            severity, severity,
            limit,
        ),
    )


@app.get("/api/governance/events/by-ip")
def governance_events_by_ip(
    ip: str = Query(..., description="client_ip to filter on"),
    event_type: str | None = Query(None),
    limit: int = Query(10, ge=1, le=100),
) -> list[dict]:
    """Recent UAL rows by source IP. Backs the Password Spray row
    expand so an operator can see which users the source IP actually
    hit without leaving the governance tab."""
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
        WHERE client_ip = %s
          AND (%s::text IS NULL OR event_type = %s)
        ORDER BY timestamp DESC
        LIMIT %s
        """,
        (ip, event_type, event_type, limit),
    )


@app.get("/api/governance/oauth-apps/{app_id}/events")
def governance_oauth_apps_events(
    app_id: str,
    limit: int = Query(10, ge=1, le=100),
) -> list[dict]:
    """Recent 'Consent to application.' rows for a single OAuth app.
    Matches the `ObjectId` value used by the governance aggregate so
    expanding a row on the OAuth Apps tab renders the actual consent
    audit log for that specific application."""
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
               client_ip,
               raw_json
        FROM vector_events
        WHERE event_type = 'Consent to application.'
          AND client_name = %s
          AND COALESCE(raw_json->>'ObjectId', '') = %s
        ORDER BY timestamp DESC
        LIMIT %s
        """,
        (_GCS, app_id, limit),
    )


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
def governance_unmanaged_devices(
    tenant: str | None = Query(None),
) -> list[dict]:
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
        WHERE (%s::text IS NULL OR client_name = %s)
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
        (tenant, tenant),
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

    Preferred source: Microsoft Intune. We fetch the cached fleet list
    from ``_fetch_intune_devices``, pick out the rows whose
    ``userPrincipalName`` matches this user, and build merged records
    with the real device name / OS / compliance state / encryption
    status plus the UAL last-seen timestamp. Anything that's fully
    compliant AND encrypted AND not stale is dropped -- this view is
    the non-clean queue.

    Fallback (no Intune data for this user): parse UAL DeviceProperties
    the old way so a Graph outage doesn't blank the panel.
    """
    upn_lower = str(user_id or "").strip().lower()

    # ---- 1. Intune devices for this user (if Graph is reachable) ----
    intune_user: list[dict] = []
    try:
        all_intune = _fetch_intune_devices()
    except HTTPException as exc:
        logger.warning(
            "intune fetch failed for unmanaged-devices detail; "
            "falling back to UAL-only parsing: %s",
            exc.detail,
        )
        all_intune = []
    except Exception:
        logger.exception("intune fetch crashed for unmanaged-devices detail")
        all_intune = []

    for d in all_intune or []:
        if (d.get("userPrincipalName") or "").strip().lower() == upn_lower:
            intune_user.append(d)

    # ---- 2. Shared UAL signals for this user (one query either way) ----
    ual_row = db.fetch_one(
        """
        SELECT
            MAX(timestamp)    AS last_seen,
            COUNT(*)::bigint  AS event_count
        FROM vector_events
        WHERE client_name = %s
          AND user_id     = %s
        """,
        (_GCS, user_id),
    ) or {}
    ual_last_seen = ual_row.get("last_seen")
    ual_event_count = int(ual_row.get("event_count") or 0)

    # ---- 3. Intune path: enrich UAL with Graph device metadata ----
    if intune_user:
        stale_threshold = (
            datetime.now(tz=timezone.utc) - timedelta(days=_STALE_SYNC_DAYS)
        )
        merged: list[dict] = []
        for d in intune_user:
            state = (d.get("complianceState") or "").strip().lower()
            # Only expose the boolean if Graph was definitive ("compliant"
            # vs. everything else). "unknown" / empty leaves it None so
            # the frontend renders an em-dash instead of a wrong red "no".
            is_compliant_bool: bool | None
            if state == "compliant":
                is_compliant_bool = True
            elif state and state != "unknown":
                is_compliant_bool = False
            else:
                is_compliant_bool = None

            is_encrypted = d.get("isEncrypted")
            is_stale = _device_is_stale(d, stale_threshold)

            # Non-clean filter: surface anything that's explicitly
            # non-compliant, explicitly unencrypted, or hasn't synced in
            # >30d. Anything passing all three tests is clean and drops
            # out of this view.
            if (
                is_compliant_bool is not False
                and is_encrypted is not False
                and not is_stale
            ):
                continue

            os_label = (d.get("operatingSystem") or "").strip()
            if d.get("osVersion"):
                os_label = f"{os_label} {d['osVersion']}".strip()

            merged.append(
                {
                    # Keep the same field names the existing frontend
                    # already renders (display_name, name, os,
                    # is_compliant, is_managed, last_seen) so this
                    # endpoint is a drop-in enrichment.
                    "display_name":     d.get("deviceName"),
                    "name":             d.get("deviceName"),
                    "os":               os_label or None,
                    "is_compliant":     is_compliant_bool,
                    "is_managed":       True,  # Intune-enrolled ⇒ managed by definition
                    "last_seen":        ual_last_seen,
                    # Extended Intune fields used by the richer device panel:
                    "compliance_state":    d.get("complianceState"),
                    "is_encrypted":        is_encrypted,
                    "last_sync_date_time": d.get("lastSyncDateTime"),
                    "os_version":          d.get("osVersion"),
                    "managed_owner_type":  d.get("managedDeviceOwnerType"),
                    "ual_event_count":     ual_event_count,
                    "source":              "intune",
                }
            )

        if merged:
            def _severity(dev: dict) -> int:
                nc = dev.get("is_compliant") is False
                ne = dev.get("is_encrypted") is False
                if nc and ne:
                    return 0
                if nc or ne:
                    return 1
                return 2

            merged.sort(
                key=lambda dev: (
                    _severity(dev),
                    dev.get("last_sync_date_time") or "",
                )
            )
            return merged
        # Intune returned devices for this user but they're all clean --
        # fall through to the UAL parse so we still surface whatever
        # DeviceProperties hinted at in the audit events.

    # ---- 4. Fallback: pure-UAL DeviceProperties parsing ----
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
            parsed["source"] = "ual"
            by_device[key] = parsed

    return sorted(
        by_device.values(),
        key=lambda d: d.get("last_seen") or "",
        reverse=True,
    )


@app.get("/api/governance/broken-inheritance")
def governance_broken_inheritance(
    tenant: str | None = Query(None),
) -> list[dict]:
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
          AND (%s::text IS NULL OR client_name = %s)
        GROUP BY entity_key, user_id, client_name
        ORDER BY event_count DESC
        LIMIT 100
        """,
        (tenant, tenant),
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
def governance_oauth_apps(
    tenant: str | None = Query(None),
) -> list[dict]:
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
          AND (%s::text IS NULL OR client_name = %s)
          AND COALESCE(raw_json->>'ObjectId', '') <> %s
        GROUP BY raw_json->>'ObjectId'
        ORDER BY user_count DESC
        LIMIT 100
        """,
        (tenant, tenant, _NERO_VECTOR_APP_ID),
    )
    for row in rows:
        app_id = row.get("app_name")
        row["app_id"] = app_id
        resolved = _resolve_app_display_name(app_id) if app_id else None
        row["display_name"] = resolved or app_id
    return rows


@app.get("/api/governance/password-spray")
def governance_password_spray(
    tenant: str | None = Query(None),
) -> list[dict]:
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
          AND (%s::text IS NULL OR client_name = %s)
          AND client_ip IS NOT NULL
          AND timestamp > now() - INTERVAL '24 hours'
        GROUP BY client_ip
        HAVING COUNT(DISTINCT user_id) >= 3
        ORDER BY targeted_users DESC, total_attempts DESC
        LIMIT 100
        """,
        (tenant, tenant),
    )


_STALE_REQUIRED_DAYS = 14


@app.get("/api/governance/stale-accounts")
def governance_stale_accounts(
    tenant: str | None = Query(None),
) -> dict:
    """Users with activity in the last 30d but no UserLoggedIn in the last
    30d. Guarded by a baseline check: if we have less than
    ``_STALE_REQUIRED_DAYS`` of data for the selected tenant, return an
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
        WHERE (%s::text IS NULL OR client_name = %s)
        """,
        (tenant, tenant),
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
        WHERE (%s::text IS NULL OR client_name = %s)
          AND user_id NOT IN (
              SELECT DISTINCT user_id
              FROM vector_events
              WHERE event_type = 'UserLoggedIn'
                AND (%s::text IS NULL OR client_name = %s)
                AND timestamp > now() - INTERVAL '30 days'
                AND user_id IS NOT NULL
          )
        GROUP BY entity_key, user_id, client_name
        HAVING MAX(timestamp) > now() - INTERVAL '30 days'
        ORDER BY last_activity DESC
        LIMIT 100
        """,
        (tenant, tenant, tenant, tenant),
    )
    return {
        "sufficient_data": True,
        "days_available": round(days_of_data, 1),
        "required_days": _STALE_REQUIRED_DAYS,
        "rows": rows,
    }


@app.get("/api/governance/mfa-changes")
def governance_mfa_changes(
    tenant: str | None = Query(None),
) -> list[dict]:
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
        WHERE (%s::text IS NULL OR client_name = %s)
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
        (tenant, tenant),
    )


@app.get("/api/governance/privileged-roles")
def governance_privileged_roles(
    tenant: str | None = Query(None),
) -> list[dict]:
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
        WHERE (%s::text IS NULL OR client_name = %s)
          AND event_type IN (
              'Add member to role.',
              'Remove member from role.',
              'Add eligible member to role.',
              'Remove eligible member from role.'
          )
        ORDER BY timestamp DESC
        LIMIT 100
        """,
        (tenant, tenant),
    )


# ---- guest users (Graph API) -----------------------------------------------

_GCS_TENANT_ID = "07b4c47a-e461-493e-91c4-90df73e2ebc6"
_GRAPH_TOKEN_CACHE: dict = {}


def _get_graph_token(tenant_id: str | None = None) -> str:
    """Client credentials token for the given tenant, cached per tenant_id."""
    tid = tenant_id or _GCS_TENANT_ID
    now = time.monotonic()
    cached = _GRAPH_TOKEN_CACHE.get(tid, {})
    if cached.get("token") and cached.get("expires_at", 0.0) > now + 60:
        return cached["token"]

    client_id = os.environ.get("VECTOR_CLIENT_ID")
    client_secret = os.environ.get("VECTOR_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise HTTPException(
            status_code=503,
            detail="VECTOR_CLIENT_ID / VECTOR_CLIENT_SECRET not configured",
        )

    url = f"https://login.microsoftonline.com/{tid}/oauth2/v2.0/token"
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

    _GRAPH_TOKEN_CACHE[tid] = {"token": token, "expires_at": now + int(data.get("expires_in", 3600))}
    return token


def _graph_get(path_with_query: str, tenant_id: str | None = None) -> dict:
    token = _get_graph_token(tenant_id=tenant_id)
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


# ---- Defender Advanced Hunting client -------------------------------------
#
# Separate from the Graph helper because Defender uses a different audience
# (api.securitycenter.microsoft.com) and therefore a different token. Cached
# per-process with the usual refresh skew.

_DEFENDER_RESOURCE = "https://api.securitycenter.microsoft.com"
_DEFENDER_HUNTING_URL = f"{_DEFENDER_RESOURCE}/api/advancedqueries/run"
_DEFENDER_TOKEN_CACHE: dict = {"token": None, "expires_at": 0.0}


def _get_defender_token() -> str:
    now = time.monotonic()
    cached = _DEFENDER_TOKEN_CACHE.get("token")
    if cached and _DEFENDER_TOKEN_CACHE.get("expires_at", 0.0) > now + 60:
        return cached

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
            "scope": f"{_DEFENDER_RESOURCE}/.default",
            "grant_type": "client_credentials",
        }
    ).encode("utf-8")
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as exc:
        logger.exception("defender token request failed")
        raise HTTPException(status_code=502, detail=f"defender token failed: {exc}")

    token = data.get("access_token")
    if not token:
        raise HTTPException(
            status_code=502, detail="defender token response missing access_token"
        )
    _DEFENDER_TOKEN_CACHE["token"] = token
    _DEFENDER_TOKEN_CACHE["expires_at"] = now + int(data.get("expires_in", 3600))
    return token


def _defender_run_hunting(query: str) -> list[dict]:
    """POST a KQL query to /api/advancedqueries/run and return the Results array."""
    token = _get_defender_token()
    body = json.dumps({"Query": query}).encode("utf-8")
    req = urllib.request.Request(_DEFENDER_HUNTING_URL, data=body, method="POST")
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        try:
            resp_body = exc.read().decode("utf-8", errors="replace")[:300]
        except Exception:
            resp_body = ""
        logger.error(
            "defender hunting %s %s", exc.code, resp_body
        )
        raise HTTPException(
            status_code=502, detail=f"defender {exc.code}: {resp_body}"
        )
    except Exception as exc:
        logger.exception("defender hunting request failed")
        raise HTTPException(status_code=502, detail=f"defender failed: {exc}")
    return data.get("Results") or data.get("results") or []


# ---- AI Activity governance endpoint --------------------------------------

_AI_DOMAINS_SUMMARY_QUERY = """
DeviceNetworkEvents
| where Timestamp > ago(7d)
| where RemoteUrl has_any (
    "chat.openai.com", "api.openai.com", "chatgpt.com",
    "claude.ai", "anthropic.com",
    "gemini.google.com", "bard.google.com",
    "deepseek.com",
    "perplexity.ai",
    "copilot.microsoft.com",
    "huggingface.co",
    "mistral.ai",
    "grok.x.ai",
    "you.com",
    "poe.com"
  )
| summarize
    visit_count=count(),
    last_visit=max(Timestamp),
    devices=make_set(DeviceName)
  by InitiatingProcessAccountUpn, RemoteUrl
| where InitiatingProcessAccountUpn != ""
| order by visit_count desc
| limit 200
""".strip()

_AI_DOMAINS_RAW_QUERY = """
DeviceNetworkEvents
| where Timestamp > ago(7d)
| where RemoteUrl has_any (
    "chat.openai.com", "api.openai.com", "chatgpt.com",
    "claude.ai", "anthropic.com",
    "gemini.google.com", "bard.google.com",
    "deepseek.com",
    "perplexity.ai",
    "copilot.microsoft.com",
    "huggingface.co",
    "mistral.ai",
    "grok.x.ai",
    "you.com",
    "poe.com"
  )
| project Timestamp, DeviceName, InitiatingProcessAccountUpn,
          InitiatingProcessFileName, RemoteUrl, RemoteIP
| order by Timestamp desc
| limit 500
""".strip()


@app.get("/api/governance/ai-activity")
def governance_ai_activity(tenant: str | None = Query(None)) -> dict:
    """Combined Microsoft Copilot usage (from UAL) + external AI tool
    access (from Defender Advanced Hunting)."""
    tenant_filter = tenant or _GCS
    copilot = db.fetch_all(
        """
        SELECT
            user_id,
            MAX(tenant_id) || '::' || user_id AS entity_key,
            MAX(client_name) AS client_name,
            COUNT(*)::bigint AS event_count,
            MAX(timestamp)   AS last_seen,
            COALESCE(
                array_agg(DISTINCT event_type)
                    FILTER (WHERE event_type IS NOT NULL),
                ARRAY[]::text[]
            ) AS event_types
        FROM vector_events
        WHERE client_name = %s
          AND workload    = 'Copilot'
          AND user_id IS NOT NULL
        GROUP BY user_id
        ORDER BY event_count DESC
        LIMIT 200
        """,
        (tenant_filter,),
    )

    external_ai: list[dict] = []
    external_error: str | None = None
    try:
        rows = _defender_run_hunting(_AI_DOMAINS_SUMMARY_QUERY)
        for r in rows:
            if not isinstance(r, dict):
                continue
            visit_raw = r.get("visit_count")
            try:
                visit_count = int(visit_raw) if visit_raw is not None else 0
            except (TypeError, ValueError):
                visit_count = 0
            devices_raw = r.get("devices")
            if isinstance(devices_raw, list):
                devices = [str(d) for d in devices_raw if d]
            elif isinstance(devices_raw, str):
                devices = [devices_raw] if devices_raw else []
            else:
                devices = []
            external_ai.append(
                {
                    "user":        r.get("InitiatingProcessAccountUpn"),
                    "tool":        r.get("RemoteUrl"),
                    "visit_count": visit_count,
                    "last_visit":  r.get("last_visit"),
                    "devices":     devices,
                }
            )
    except HTTPException as exc:
        external_error = str(exc.detail)
        logger.warning(
            "defender hunting failed for AI activity: %s", exc.detail
        )

    return {
        "copilot":        copilot,
        "external_ai":    external_ai,
        "external_error": external_error,
    }


@app.get("/api/governance/ai-activity/external-raw")
def governance_ai_activity_external_raw() -> list[dict]:
    """Raw Defender rows (timestamp, device, upn, url, ip) for drill-down."""
    rows = _defender_run_hunting(_AI_DOMAINS_RAW_QUERY)
    return [r for r in rows if isinstance(r, dict)]


@app.get("/api/governance/guest-users")
def governance_guest_users(
    tenant: str | None = Query(None),
) -> list[dict]:
    """List guest users via Microsoft Graph.

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


# ---- Intune managed devices (Graph: /deviceManagement/managedDevices) -------
#
# Fetches the full managedDevices collection for the GCS tenant, groups by
# userPrincipalName, and flags users with at least one non-compliant,
# unencrypted, or stale (>30d since last sync) device. The raw response is
# cached in-process for 5 minutes so the two endpoints below can share it
# without hammering Graph on every tab click.

_INTUNE_CACHE: dict = {"data": None, "fetched_at": 0.0}
_INTUNE_CACHE_TTL = 300  # seconds
_STALE_SYNC_DAYS = 30


def _parse_graph_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    raw = str(value)
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _fetch_intune_devices(force: bool = False, tenant_id: str | None = None) -> list[dict]:
    """Return the cached /deviceManagement/managedDevices payload."""
    tid = tenant_id or _GCS_TENANT_ID
    now_mono = time.monotonic()
    cache_key = f"data_{tid}"
    cached = _INTUNE_CACHE.get(cache_key)
    if (
        not force
        and cached is not None
        and now_mono - _INTUNE_CACHE.get(f"fetched_at_{tid}", 0.0) < _INTUNE_CACHE_TTL
    ):
        return cached

    select_fields = ",".join(
        [
            "id",
            "deviceName",
            "complianceState",
            "userPrincipalName",
            "lastSyncDateTime",
            "operatingSystem",
            "osVersion",
            "isEncrypted",
            "managedDeviceOwnerType",
        ]
    )
    path = f"/deviceManagement/managedDevices?$select={select_fields}&$top=999"
    data = _graph_get(path, tenant_id=tid)
    values = data.get("value") or []
    _INTUNE_CACHE[cache_key] = values
    _INTUNE_CACHE[f"fetched_at_{tid}"] = now_mono
    return values


def _device_is_noncompliant(device: dict) -> bool:
    state = (device.get("complianceState") or "").strip().lower()
    # "compliant" and "unknown"/"" are treated as OK; everything else
    # (noncompliant, conflict, error, inGracePeriod, configManager) is a
    # finding the operator should triage.
    return state not in ("", "compliant", "unknown")


def _device_is_stale(
    device: dict, threshold: datetime
) -> bool:
    last_sync = _parse_graph_iso(device.get("lastSyncDateTime"))
    if last_sync is None:
        return False
    return last_sync < threshold


def _project_device(d: dict) -> dict:
    return {
        "deviceName":             d.get("deviceName"),
        "complianceState":        d.get("complianceState"),
        "lastSyncDateTime":       d.get("lastSyncDateTime"),
        "operatingSystem":        d.get("operatingSystem"),
        "osVersion":              d.get("osVersion"),
        "isEncrypted":            d.get("isEncrypted"),
        "managedDeviceOwnerType": d.get("managedDeviceOwnerType"),
    }


def _group_intune_by_user(
    devices: list[dict], only_with_issues: bool = True
) -> list[dict]:
    stale_threshold = datetime.now(tz=timezone.utc) - timedelta(days=_STALE_SYNC_DAYS)

    by_user: dict[str, list[dict]] = {}
    for d in devices:
        upn = (d.get("userPrincipalName") or "").strip().lower()
        if not upn:
            continue
        by_user.setdefault(upn, []).append(d)

    out: list[dict] = []
    for upn, user_devices in by_user.items():
        noncompliant = sum(1 for d in user_devices if _device_is_noncompliant(d))
        unencrypted = sum(1 for d in user_devices if d.get("isEncrypted") is False)
        stale = sum(
            1 for d in user_devices if _device_is_stale(d, stale_threshold)
        )
        total_issues = noncompliant + unencrypted + stale
        if only_with_issues and total_issues == 0:
            continue

        out.append(
            {
                "user":                upn,
                "devices":             [_project_device(d) for d in user_devices],
                "device_count":        len(user_devices),
                "noncompliant_count":  noncompliant,
                "unencrypted_count":   unencrypted,
                "stale_count":         stale,
                "total_issues":        total_issues,
            }
        )

    # Sort: most issues first, then oldest last sync across this user's
    # devices, so the operator's eye lands on the worst offenders.
    def _oldest_sync(entry: dict) -> str:
        candidates = [
            d.get("lastSyncDateTime")
            for d in entry.get("devices", [])
            if d.get("lastSyncDateTime")
        ]
        return min(candidates) if candidates else "9999-12-31T00:00:00Z"

    out.sort(key=lambda e: (-e["total_issues"], _oldest_sync(e)))
    return out


@app.get("/api/governance/intune-devices")
def governance_intune_devices(
    tenant: str | None = Query(None),
) -> list[dict]:
    """Users with at least one non-compliant, unencrypted, or stale
    Intune-managed device."""
    # Look up tenant_id from tenant name
    tenant_id = None
    if tenant:
        row = db.fetch_one("SELECT tenant_id FROM vector_events WHERE client_name = %s LIMIT 1", (tenant,))
        if row:
            tenant_id = row.get("tenant_id")
    devices = _fetch_intune_devices(tenant_id=tenant_id)
    return _group_intune_by_user(devices, only_with_issues=True)


@app.get("/api/governance/intune-devices/{upn}")
def governance_intune_devices_user(upn: str) -> dict:
    """All Intune-managed devices for a single user (used by the
    Intune Devices expandable row)."""
    devices = _fetch_intune_devices()
    upn_lower = upn.strip().lower()
    user_devices = [
        d
        for d in devices
        if (d.get("userPrincipalName") or "").strip().lower() == upn_lower
    ]
    grouped = _group_intune_by_user(user_devices, only_with_issues=False)
    if grouped:
        return grouped[0]
    return {
        "user":               upn_lower,
        "devices":            [],
        "device_count":       0,
        "noncompliant_count": 0,
        "unencrypted_count":  0,
        "stale_count":        0,
        "total_issues":       0,
    }


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
