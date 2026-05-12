"""INKY MailShield SIEM webhook receiver.

Standalone FastAPI app that runs in its own container alongside the
vector-ingest poller. Listens on :3007 for INKY SIEM feed webhooks,
stores every accepted event in ``vector_inky_events``, and for any
Danger-verdict Inbound Analysis or Link Click, stages a 60-minute
pin in ``vector_watchlist`` that the Phase-2 correlation engine will
join against UAL auth anomalies.

Auth is a flat shared secret delivered in the ``X-Vector-Token``
header, timing-safe-compared against the ``INKY_WEBHOOK_SECRET``
environment variable (falls back to ``VECTOR_INKY_TOKEN`` for
backward compat with the v0.1 receiver).

Run with:
    uvicorn vector_ingest.inky_receiver:app --host 0.0.0.0 --port 3007
"""

from __future__ import annotations

import hmac
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

import psycopg2
import psycopg2.extras
import psycopg2.pool
from fastapi import FastAPI, Header, HTTPException, Request

# ---------------------------------------------------------------------------
# logging (JSON to stdout so container log drivers pick it up)
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=os.environ.get("INKY_LOG_LEVEL", "INFO").upper(),
    stream=sys.stdout,
    format='{"ts":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","msg":"%(message)s"}',
)
logger = logging.getLogger("vector_ingest.inky")

# ---------------------------------------------------------------------------
# app + db pool
# ---------------------------------------------------------------------------

app = FastAPI(title="NERO Vector INKY Receiver", version="0.2.0")

_POOL: psycopg2.pool.ThreadedConnectionPool | None = None


@app.on_event("startup")
def _startup() -> None:
    global _POOL
    _POOL = psycopg2.pool.ThreadedConnectionPool(
        minconn=1,
        maxconn=4,
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ.get("POSTGRES_DB", "nero_vector"),
        user=os.environ.get("POSTGRES_USER", "nero_vector"),
        password=os.environ.get("POSTGRES_PASSWORD", ""),
        application_name="inky-receiver",
        options="-c timezone=UTC",
    )
    logger.info("inky-receiver db pool initialized")


@app.on_event("shutdown")
def _shutdown() -> None:
    global _POOL
    if _POOL is not None:
        _POOL.closeall()
        _POOL = None


# ---------------------------------------------------------------------------
# constants
# ---------------------------------------------------------------------------

CORRELATION_WINDOW = timedelta(hours=48)

# Map INKY SIEM feed eventType strings to our canonical event_type values.
EVENT_TYPE_MAP = {
    "Inbound Analysis Results":  "InboundAnalysis",
    "Link Clicks":               "LinkClick",
    "User Reports":              "UserReport",
    "Outbound Analysis Results": "OutboundAnalysis",
    # INKY actually sends these lowercase event_type values in production
    "analysis_result":           "InboundAnalysis",
    "link_click":                "LinkClick",
    "user_report":               "UserReport",
    "outbound_analysis_result":  "OutboundAnalysis",
}

SUPPORTED_CANONICAL = set(EVENT_TYPE_MAP.values())

DANGER_VERDICTS  = {"danger", "malicious", "phish", "phishing"}
CAUTION_VERDICTS = {"caution", "suspicious"}
NEUTRAL_VERDICTS = {"neutral", "safe", "clean"}

INSERT_INKY_EVENT_SQL = """
INSERT INTO vector_inky_events (
    tenant_id, client_name, event_type, recipient, sender, subject,
    verdict, url, aitm_detected, threat_level, policy, timestamp, raw_json
) VALUES (
    %(tenant_id)s, %(client_name)s, %(event_type)s, %(recipient)s,
    %(sender)s, %(subject)s, %(verdict)s, %(url)s, %(aitm_detected)s,
    %(threat_level)s, %(policy)s, %(timestamp)s, %(raw_json)s
)
RETURNING id
"""

INSERT_WATCHLIST_SQL = """
INSERT INTO vector_watchlist (
    tenant_id, client_name, user_email, trigger_type, trigger_details,
    expires_at, status
) VALUES (
    %(tenant_id)s, %(client_name)s, %(user_email)s, %(trigger_type)s,
    %(trigger_details)s, %(expires_at)s, 'active'
)
RETURNING id
"""

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _check_token(token: str | None) -> None:
    expected = (
        os.environ.get("INKY_WEBHOOK_SECRET")
        or os.environ.get("VECTOR_INKY_TOKEN")
    )
    if not expected:
        logger.error("INKY_WEBHOOK_SECRET not configured; refusing all traffic")
        raise HTTPException(status_code=503, detail="receiver not configured")
    if not token or not hmac.compare_digest(str(token), str(expected)):
        raise HTTPException(status_code=401, detail="bad token")


def _as_events(payload: Any) -> list[dict]:
    """Normalise to a list of event dicts.

    INKY posts either a bare event object, a list, or ``{"events": [...]}``.
    """
    if isinstance(payload, list):
        return [e for e in payload if isinstance(e, dict)]
    if isinstance(payload, dict):
        events = payload.get("events")
        if isinstance(events, list):
            return [e for e in events if isinstance(e, dict)]
        return [payload]
    return []


def _parse_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        dt = value
    elif value:
        raw = str(value)
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(raw)
        except Exception:
            dt = datetime.now(timezone.utc)
    else:
        dt = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    # Convert to naive UTC so psycopg2 stores it cleanly in TIMESTAMPTZ
    # against our UTC-pinned session.
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def _first(event: dict, keys: tuple[str, ...]) -> str:
    for k in keys:
        v = event.get(k)
        if isinstance(v, str) and v:
            return v
        if isinstance(v, list) and v and isinstance(v[0], str):
            return v[0]
    return ""


def _normalize_event_type(raw_type: str) -> str:
    return EVENT_TYPE_MAP.get(raw_type, raw_type or "Unknown")


def _normalize_verdict(v: str) -> str:
    if not v:
        return ""
    low = v.strip().lower()
    if low in DANGER_VERDICTS:
        return "Danger"
    if low in CAUTION_VERDICTS:
        return "Caution"
    if low in NEUTRAL_VERDICTS:
        return "Neutral"
    return v.strip().capitalize()


def _extract_url(event: dict) -> str:
    url = _first(event, ("url", "URL", "linkUrl", "clickedUrl", "targetUrl"))
    if url:
        return url
    for key in ("links", "Links"):
        links = event.get(key)
        if isinstance(links, list) and links:
            first = links[0]
            if isinstance(first, dict):
                return str(first.get("url") or first.get("URL") or "")
            if isinstance(first, str):
                return first
    return ""


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes", "y"}
    return bool(value)


def _db_insert(sql: str, params: dict) -> dict | None:
    if _POOL is None:
        raise RuntimeError("db pool not initialized")
    conn = _POOL.getconn()
    try:
        if not conn.autocommit:
            conn.rollback()
            conn.autocommit = True
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            return dict(row) if row else None
    finally:
        _POOL.putconn(conn)


# ---------------------------------------------------------------------------
# routes
# ---------------------------------------------------------------------------

@app.get("/health")
def health() -> dict:
    return {"status": "ok", "service": "vector-inky-receiver"}


@app.post("/api/ingest/inky")
async def ingest_inky(
    request: Request,
    x_vector_token: str | None = Header(default=None, alias="X-Vector-Token"),
) -> dict:
    _check_token(x_vector_token)

    try:
        payload = await request.json()
    except Exception as exc:
        logger.warning("invalid json body: %s", exc)
        raise HTTPException(status_code=400, detail="invalid json body")

    events = _as_events(payload)
    logger.info(f"[inky] received payload, event_count={len(events)}, sample={str(events[:1])[:500]}")

    accepted = 0
    stored = 0
    staged = 0
    skipped = 0

    for event in events:
        raw_type = _first(
            event, ("eventType", "EventType", "event_type", "type")
        )
        event_type = _normalize_event_type(raw_type)
        if event_type not in SUPPORTED_CANONICAL:
            skipped += 1
            continue
        accepted += 1

        verdict = _normalize_verdict(
            _first(event, ("verdict", "Verdict", "result", "disposition"))
        )
        recipient = _first(
            event, ("recipient", "Recipient", "to", "toAddress", "recipientAddress")
        )
        sender = _first(
            event, ("sender", "Sender", "from", "fromAddress", "senderAddress")
        )
        subject = _first(event, ("subject", "Subject", "mailSubject"))
        url = _extract_url(event)
        aitm = _as_bool(
            event.get("aitmDetected")
            or event.get("AiTMDetected")
            or event.get("isAiTM")
            or event.get("aitm")
        )
        threat_level = _first(event, ("threatLevel", "ThreatLevel", "severity"))
        policy = _first(event, ("policy", "Policy", "policyName"))
        tenant_id = _first(
            event, ("tenantId", "tenant_id", "TenantId", "customerId", "customer_id")
        )
        client_name = _first(
            event, ("clientName", "client_name", "customerName", "teamid", "teamName")
        )
        # Map known INKY teamids to canonical Vector client names
        INKY_TEAM_MAP = {
            "gamechange": "GameChange Solar",
            "gamechangesolar": "GameChange Solar",
            "londonfischer": "London Fischer",
            "london-fischer": "London Fischer",
            "lf": "London Fischer",
            "nero": "NERO",
        }
        if client_name:
            client_name = INKY_TEAM_MAP.get(str(client_name).lower(), client_name) or None
        timestamp = _parse_timestamp(
            event.get("timestamp") or event.get("eventTime") or event.get("time")
        )

        event_row = {
            "tenant_id":     tenant_id or None,
            "client_name":   client_name,
            "event_type":    event_type,
            "recipient":     recipient or None,
            "sender":        sender or None,
            "subject":       subject or None,
            "verdict":       verdict or None,
            "url":           url or None,
            "aitm_detected": aitm,
            "threat_level":  threat_level or None,
            "policy":        policy or None,
            "timestamp":     timestamp,
            "raw_json":      json.dumps(event),
        }

        try:
            _db_insert(INSERT_INKY_EVENT_SQL, event_row)
            stored += 1
        except Exception:
            logger.exception(
                "inky event insert failed event_type=%s recipient=%s",
                event_type,
                recipient,
            )
            continue

        # Watchlist pin for Danger verdicts on delivered mail / clicked links.
        if verdict == "Danger" and event_type in {"LinkClick", "InboundAnalysis"}:
            trigger_type = (
                "inky_click" if event_type == "LinkClick" else "inky_phish_delivered"
            )
            trigger_details = {
                "sender":         sender or None,
                "subject":        subject or None,
                "url":            url or None,
                "verdict":        verdict,
                "aitm_detected":  aitm,
                "threat_level":   threat_level or None,
                "policy":         policy or None,
                "event_type":     event_type,
                "timestamp":      timestamp.isoformat() + "Z",
            }
            watchlist_row = {
                "tenant_id":       tenant_id or None,
                "client_name":     client_name,
                "user_email":      recipient or None,
                "trigger_type":    trigger_type,
                "trigger_details": json.dumps(trigger_details),
                "expires_at":      timestamp + CORRELATION_WINDOW,
            }
            try:
                _db_insert(INSERT_WATCHLIST_SQL, watchlist_row)
                staged += 1
                logger.info(
                    "inky watchlist pin staged type=%s user=%s expires_in=48h",
                    trigger_type,
                    recipient,
                )
            except Exception:
                logger.exception(
                    "watchlist insert failed type=%s user=%s",
                    trigger_type,
                    recipient,
                )

    return {
        "accepted": accepted,
        "stored":   stored,
        "staged":   staged,
        "skipped":  skipped,
    }
