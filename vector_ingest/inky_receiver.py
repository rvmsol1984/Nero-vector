"""INKY MailShield webhook receiver.

Exposes POST /api/ingest/inky on the vector-ui FastAPI app. Parses the
two INKY feed event types we currently care about -- "Inbound Analysis
Results" and "Link Clicks" -- and for any event whose verdict resolves
to Danger, stages a provisional entry in vector_watchlist with a 60
minute correlation window so the correlation engine can bind later UAL
activity to the original verdict while the pin is still live.

Auth is a flat shared secret sent in the X-Vector-Token header, compared
against the VECTOR_INKY_TOKEN environment variable with a timing-safe
compare. If the env var is unset the endpoint fails closed with 503.
"""

from __future__ import annotations

import hmac
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Header, HTTPException, Request

from backend import db  # shares the vector-ui psycopg2 pool

logger = logging.getLogger("vector_ingest.inky")

router = APIRouter()

# 60 minute default correlation window. Tuned for MailShield -> UAL
# binding: INKY fires before the user clicks, and most malicious click
# / login / share events land within the hour.
CORRELATION_WINDOW = timedelta(minutes=60)

# Verdicts we treat as "Danger". INKY has historically used a mix of
# these strings across mailbox vs. link-click flows; normalise to lower.
DANGER_VERDICTS = {"danger", "malicious", "phish", "phishing", "aitm"}

SUPPORTED_EVENT_TYPES = {
    "Inbound Analysis Results",
    "Link Clicks",
}

_INSERT_SQL = """
INSERT INTO vector_watchlist (
    tenant_id, source, verdict, recipient, sender, url,
    event_type, timestamp, correlation_window_expires_at, raw_json
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
RETURNING id
"""


# ---------------------------------------------------------------------------
# auth
# ---------------------------------------------------------------------------

def _check_token(token: str | None) -> None:
    expected = os.environ.get("VECTOR_INKY_TOKEN")
    if not expected:
        logger.error("VECTOR_INKY_TOKEN not configured; refusing all traffic")
        raise HTTPException(status_code=503, detail="receiver not configured")
    if not token or not hmac.compare_digest(str(token), str(expected)):
        raise HTTPException(status_code=401, detail="bad token")


# ---------------------------------------------------------------------------
# payload helpers
# ---------------------------------------------------------------------------

def _as_events(payload: Any) -> list[dict]:
    """Normalise the INKY payload to a list of event dicts.

    INKY has historically posted either a single event object or an
    ``{"events": [...]}`` envelope. Accept both; anything else yields [].
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
        except ValueError:
            dt = datetime.now(timezone.utc)
    else:
        dt = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _first_string(event: dict, keys: tuple[str, ...]) -> str:
    for key in keys:
        v = event.get(key)
        if isinstance(v, str) and v:
            return v
        if isinstance(v, list) and v:
            first = v[0]
            if isinstance(first, str):
                return first
    return ""


def _extract_event_type(event: dict) -> str:
    return _first_string(event, ("eventType", "EventType", "event_type", "type"))


def _extract_verdict(event: dict) -> str:
    return _first_string(event, ("verdict", "Verdict", "result", "disposition"))


def _extract_tenant_id(event: dict) -> str:
    return _first_string(
        event,
        ("tenantId", "tenant_id", "TenantId", "customerId", "customer_id"),
    )


def _extract_recipient(event: dict) -> str:
    return _first_string(
        event,
        ("recipient", "Recipient", "to", "toAddress", "recipientAddress"),
    )


def _extract_sender(event: dict) -> str:
    return _first_string(
        event,
        ("sender", "Sender", "from", "fromAddress", "senderAddress"),
    )


def _extract_url(event: dict) -> str:
    url = _first_string(event, ("url", "URL", "linkUrl", "clickedUrl", "targetUrl"))
    if url:
        return url
    # Inbound Analysis Results often nests links under "links": [{url: ...}]
    for key in ("links", "Links"):
        links = event.get(key)
        if isinstance(links, list) and links:
            first = links[0]
            if isinstance(first, dict):
                return str(first.get("url") or first.get("URL") or "")
            if isinstance(first, str):
                return first
    return ""


def _is_danger(verdict: str) -> bool:
    return verdict.strip().lower() in DANGER_VERDICTS


# ---------------------------------------------------------------------------
# route
# ---------------------------------------------------------------------------

@router.post("/api/ingest/inky")
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
    accepted = 0
    staged = 0
    skipped = 0

    for event in events:
        event_type = _extract_event_type(event)
        if event_type not in SUPPORTED_EVENT_TYPES:
            skipped += 1
            continue
        accepted += 1

        verdict = _extract_verdict(event)
        if not _is_danger(verdict):
            # We only stage watchlist pins for Danger. Everything else is
            # acknowledged but dropped -- future phases may persist the
            # full audit trail, but today that would just bloat the table.
            continue

        tenant_id = _extract_tenant_id(event)
        recipient = _extract_recipient(event)
        sender = _extract_sender(event)
        url = _extract_url(event)
        timestamp = _parse_timestamp(
            event.get("timestamp") or event.get("eventTime") or event.get("time")
        )
        expires_at = timestamp + CORRELATION_WINDOW

        try:
            row = db.execute_returning(
                _INSERT_SQL,
                (
                    tenant_id,
                    "INKY",
                    verdict,
                    recipient,
                    sender,
                    url,
                    event_type,
                    timestamp,
                    expires_at,
                    json.dumps(event),
                ),
            )
        except Exception as exc:
            logger.exception(
                "inky stage failed: %s (event_type=%s tenant_id=%s)",
                exc,
                event_type,
                tenant_id,
            )
            raise HTTPException(status_code=500, detail="database error")

        staged += 1
        logger.info(
            "inky watchlist entry staged id=%s tenant_id=%s recipient=%s "
            "verdict=%s event_type=%s window_expires_at=%s",
            row.get("id") if row else None,
            tenant_id,
            recipient,
            verdict,
            event_type,
            expires_at.isoformat(),
        )

    return {"accepted": accepted, "staged": staged, "skipped": skipped}
