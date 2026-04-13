"""Datto EDR (Infocyte) webhook receiver.

Standalone FastAPI app that runs in its own container alongside the
vector-ingest poller. Listens on :3008 for Infocyte SIEM v2 webhooks,
normalises each accepted alert/observable into ``vector_edr_events``,
and uses a SHA-256 dedup fingerprint (5-minute bucket) so webhook
retries or duplicate deliveries collapse into a single row.

Auth is a flat shared secret delivered in the ``X-Vector-Token``
header, timing-safe-compared against the ``EDR_WEBHOOK_SECRET``
environment variable. Missing secret fails closed with 503.

Run with:
    uvicorn vector_ingest.edr_receiver:app --host 0.0.0.0 --port 3008
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any

import psycopg2
import psycopg2.extras
import psycopg2.pool
from fastapi import FastAPI, Header, HTTPException, Request

# ---------------------------------------------------------------------------
# logging (JSON to stdout so container log drivers pick it up)
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=os.environ.get("EDR_LOG_LEVEL", "INFO").upper(),
    stream=sys.stdout,
    format='{"ts":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","msg":"%(message)s"}',
)
logger = logging.getLogger("vector_ingest.edr")

# ---------------------------------------------------------------------------
# app + db pool
# ---------------------------------------------------------------------------

app = FastAPI(title="NERO Vector EDR Receiver", version="0.1.0")

_POOL: psycopg2.pool.ThreadedConnectionPool | None = None
_TABLE_ENSURED: bool = False


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
        application_name="edr-receiver",
        options="-c timezone=UTC",
    )
    logger.info("edr-receiver db pool initialized")
    try:
        _ensure_table()
    except Exception:
        logger.exception("edr-receiver ensure_table failed at startup")


@app.on_event("shutdown")
def _shutdown() -> None:
    global _POOL
    if _POOL is not None:
        _POOL.closeall()
        _POOL = None


# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

# Safety-net DDL -- CREATE TABLE IF NOT EXISTS + indexes + best-effort
# self-grant. Idempotent: harmless if migration 006 has already run.
_ENSURE_TABLE_SQL = """
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS vector_edr_events (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id           VARCHAR(128),
    client_name         VARCHAR(256),
    event_type          VARCHAR(64)  NOT NULL,
    severity            VARCHAR(64),
    host_name           VARCHAR(256),
    host_ip             VARCHAR(128),
    user_account        VARCHAR(512),
    process_name        VARCHAR(256),
    process_path        TEXT,
    command_line        TEXT,
    threat_name         VARCHAR(256),
    threat_score        INTEGER,
    action_taken        VARCHAR(128),
    timestamp           TIMESTAMPTZ  NOT NULL,
    raw_json            JSONB        NOT NULL,
    ingested_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    dedup_fingerprint   VARCHAR(64)  UNIQUE
);

CREATE INDEX IF NOT EXISTS idx_edr_events_tenant_time
    ON vector_edr_events (tenant_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_edr_events_severity
    ON vector_edr_events (severity);
CREATE INDEX IF NOT EXISTS idx_edr_events_host
    ON vector_edr_events (host_name);
CREATE INDEX IF NOT EXISTS idx_edr_events_event_type
    ON vector_edr_events (event_type);

DO $$
BEGIN
    EXECUTE 'GRANT ALL PRIVILEGES ON TABLE vector_edr_events TO '
         || quote_ident(current_user);
EXCEPTION WHEN OTHERS THEN
    NULL;
END $$;
"""

_INSERT_SQL = """
INSERT INTO vector_edr_events (
    tenant_id, client_name, event_type, severity, host_name, host_ip,
    user_account, process_name, process_path, command_line,
    threat_name, threat_score, action_taken, timestamp, raw_json,
    dedup_fingerprint
) VALUES (
    %(tenant_id)s, %(client_name)s, %(event_type)s, %(severity)s,
    %(host_name)s, %(host_ip)s, %(user_account)s, %(process_name)s,
    %(process_path)s, %(command_line)s, %(threat_name)s, %(threat_score)s,
    %(action_taken)s, %(timestamp)s, %(raw_json)s, %(dedup_fingerprint)s
)
ON CONFLICT (dedup_fingerprint) DO NOTHING
RETURNING id
"""


def _ensure_table() -> None:
    global _TABLE_ENSURED
    if _TABLE_ENSURED or _POOL is None:
        return
    conn = _POOL.getconn()
    try:
        if not conn.autocommit:
            conn.rollback()
            conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(_ENSURE_TABLE_SQL)
    finally:
        _POOL.putconn(conn)
    _TABLE_ENSURED = True


def _db_insert(params: dict) -> dict | None:
    if _POOL is None:
        raise RuntimeError("db pool not initialized")
    conn = _POOL.getconn()
    try:
        if not conn.autocommit:
            conn.rollback()
            conn.autocommit = True
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(_INSERT_SQL, params)
            row = cur.fetchone()
            return dict(row) if row else None
    finally:
        _POOL.putconn(conn)


# ---------------------------------------------------------------------------
# auth
# ---------------------------------------------------------------------------

def _check_token(token: str | None) -> None:
    expected = os.environ.get("EDR_WEBHOOK_SECRET")
    if not expected:
        logger.error("EDR_WEBHOOK_SECRET not configured; refusing all traffic")
        raise HTTPException(status_code=503, detail="receiver not configured")
    if not token or not hmac.compare_digest(str(token), str(expected)):
        raise HTTPException(status_code=401, detail="bad token")


# ---------------------------------------------------------------------------
# payload parsing helpers
# ---------------------------------------------------------------------------

SUPPORTED_EVENT_TYPES = {"alert", "observable"}


def _as_events(payload: Any) -> list[dict]:
    """Normalise an Infocyte webhook body to a list of event dicts.

    Infocyte's v2 webhook has been observed to post bare objects,
    bare lists, and envelopes like ``{"events": [...]}`` /
    ``{"alerts": [...]}`` / ``{"observables": [...]}``. Accept all.
    """
    if isinstance(payload, list):
        return [e for e in payload if isinstance(e, dict)]
    if isinstance(payload, dict):
        for key in ("events", "data", "results", "alerts", "observables"):
            val = payload.get(key)
            if isinstance(val, list):
                return [e for e in val if isinstance(e, dict)]
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
    # naive-UTC for TIMESTAMPTZ against the UTC-pinned session
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def _first(event: dict, keys: tuple[str, ...]) -> str:
    for k in keys:
        v = event.get(k)
        if isinstance(v, str) and v:
            return v
        if isinstance(v, (int, float)) and v:
            return str(v)
    return ""


def _first_int(event: dict, keys: tuple[str, ...]) -> int | None:
    for k in keys:
        v = event.get(k)
        if v is None or v == "":
            continue
        try:
            return int(v)
        except (TypeError, ValueError):
            try:
                return int(float(v))
            except (TypeError, ValueError):
                continue
    return None


def _normalize_event_type(raw: str) -> str:
    low = (raw or "").strip().lower()
    if "observ" in low:
        return "observable"
    if "alert" in low or "detection" in low or low == "threat":
        return "alert"
    return low or "alert"


def _normalize(event: dict) -> dict | None:
    event_type = _normalize_event_type(
        _first(event, ("eventType", "type", "alertType", "category"))
    )
    if event_type not in SUPPORTED_EVENT_TYPES:
        return None

    timestamp = _parse_timestamp(
        event.get("timestamp")
        or event.get("eventTime")
        or event.get("createdAt")
        or event.get("detectedAt")
        or event.get("firstSeen")
    )

    # Host / device info is usually nested.
    host = event.get("host") or event.get("device") or {}
    if not isinstance(host, dict):
        host = {}
    host_name = _first(
        host, ("hostname", "name", "displayName", "computerName")
    ) or _first(event, ("hostname", "host", "hostName", "computerName"))
    host_ip = _first(host, ("ipAddress", "ip", "ipv4")) or _first(
        event, ("ipAddress", "clientIp", "sourceIp")
    )

    # Process info is usually nested under "process" or
    # "initiatingProcess".
    process = event.get("process") or event.get("initiatingProcess") or {}
    if not isinstance(process, dict):
        process = {}
    process_name = _first(
        process, ("name", "fileName", "processName")
    ) or _first(event, ("processName", "process"))
    process_path = _first(
        process, ("path", "fullPath", "filePath", "imagePath")
    ) or _first(event, ("processPath", "filePath", "imagePath"))
    command_line = _first(
        process, ("commandLine", "cmdLine", "arguments")
    ) or _first(event, ("commandLine", "cmdLine"))

    user_inner = event.get("user")
    if isinstance(user_inner, dict):
        user_account = _first(user_inner, ("name", "upn", "userName"))
    else:
        user_account = ""
    user_account = user_account or _first(
        event, ("userAccount", "user", "userName", "account", "upn")
    )

    # Threat / detection info.
    threat = event.get("threat") or event.get("detection") or {}
    if not isinstance(threat, dict):
        threat = {}
    threat_name = _first(
        threat, ("name", "threatName", "family", "ruleName")
    ) or _first(event, ("threatName", "detectionName", "ruleName", "title"))
    threat_score = _first_int(
        {**event, **threat},
        ("score", "threatScore", "severityScore", "riskScore", "confidence"),
    )

    severity = _first(event, ("severity", "level", "priority")) or _first(
        threat, ("severity", "level")
    )
    action_taken = _first(
        event, ("action", "actionTaken", "response", "responseAction")
    )
    tenant_id = _first(
        event, ("tenantId", "tenant_id", "customerId", "organizationId")
    )
    client_name = _first(
        event, ("clientName", "customerName", "organizationName")
    )

    # Dedup fingerprint: SHA-256 of tenant + event_type + host +
    # 5-minute-bucketed timestamp + threat name. Replays of the same
    # event within a 5-minute window collapse into one row, while
    # genuinely new detections on the same host still get their own.
    bucket = timestamp.replace(
        minute=(timestamp.minute // 5) * 5, second=0, microsecond=0
    )
    fingerprint_src = "|".join(
        [
            tenant_id or "",
            event_type,
            host_name or "",
            bucket.isoformat(),
            threat_name or "",
        ]
    )
    fingerprint = hashlib.sha256(fingerprint_src.encode("utf-8")).hexdigest()

    return {
        "tenant_id":         tenant_id or None,
        "client_name":       client_name or None,
        "event_type":        event_type,
        "severity":          severity or None,
        "host_name":         host_name or None,
        "host_ip":           host_ip or None,
        "user_account":      user_account or None,
        "process_name":      process_name or None,
        "process_path":      process_path or None,
        "command_line":      command_line or None,
        "threat_name":       threat_name or None,
        "threat_score":      threat_score,
        "action_taken":      action_taken or None,
        "timestamp":         timestamp,
        "raw_json":          json.dumps(event),
        "dedup_fingerprint": fingerprint,
    }


# ---------------------------------------------------------------------------
# routes
# ---------------------------------------------------------------------------

@app.get("/health")
def health() -> dict:
    return {"status": "ok", "service": "vector-edr-receiver"}


@app.post("/api/ingest/edr")
async def ingest_edr(
    request: Request,
    x_vector_token: str | None = Header(default=None, alias="X-Vector-Token"),
) -> dict:
    _check_token(x_vector_token)

    try:
        payload = await request.json()
    except Exception as exc:
        logger.warning("invalid json body: %s", exc)
        raise HTTPException(status_code=400, detail="invalid json body")

    try:
        _ensure_table()
    except Exception:
        logger.exception("edr ensure_table failed")
        raise HTTPException(status_code=500, detail="database error")

    events = _as_events(payload)

    accepted = 0
    stored = 0
    duplicates = 0
    skipped = 0

    for event in events:
        row = _normalize(event)
        if not row:
            skipped += 1
            continue
        accepted += 1
        try:
            result = _db_insert(row)
        except Exception:
            logger.exception(
                "edr event insert failed event_type=%s host=%s",
                row.get("event_type"),
                row.get("host_name"),
            )
            continue
        if result:
            stored += 1
            logger.info(
                "edr event staged type=%s severity=%s host=%s threat=%s",
                row.get("event_type"),
                row.get("severity"),
                row.get("host_name"),
                row.get("threat_name"),
            )
        else:
            duplicates += 1

    return {
        "accepted":   accepted,
        "stored":     stored,
        "duplicates": duplicates,
        "skipped":    skipped,
    }
