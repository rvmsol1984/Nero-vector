"""UAL event normalization.

Converts a raw Office 365 Management Activity API event into the
flat shape persisted in the vector_events table, and computes the
dedup fingerprint used to drop duplicate events on insert.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any


def _parse_timestamp(value: Any) -> datetime:
    """Parse a UAL 'CreationTime' value. UAL uses UTC with no offset."""
    if isinstance(value, datetime):
        dt = value
    else:
        if not value:
            raise ValueError("missing CreationTime on UAL event")
        raw = str(value)
        # UAL returns e.g. "2024-04-12T19:25:03" — assume UTC.
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(raw)
        except ValueError:
            # Fallback: strip fractional seconds if malformed
            dt = datetime.fromisoformat(raw.split(".")[0])
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _extract_user_id(event: dict) -> str:
    # UAL typically populates UserId; fall back to UserKey or UserPrincipalName.
    for key in ("UserId", "UserPrincipalName", "UserKey"):
        val = event.get(key)
        if val:
            return str(val)
    return "unknown"


def _extract_result_status(event: dict) -> str | None:
    for key in ("ResultStatus", "Result", "Outcome"):
        val = event.get(key)
        if val:
            return str(val)
    return None


def _extract_client_ip(event: dict) -> str | None:
    for key in ("ClientIP", "ClientIp", "ActorIpAddress"):
        val = event.get(key)
        if val:
            return str(val)
    return None


def _extract_user_agent(event: dict) -> str | None:
    ua = event.get("UserAgent")
    if ua:
        return str(ua)
    # Some Exchange events nest UA inside ExtendedProperties
    extended = event.get("ExtendedProperties") or []
    if isinstance(extended, list):
        for prop in extended:
            if isinstance(prop, dict) and prop.get("Name") == "UserAgent":
                return str(prop.get("Value", "")) or None
    return None


def _truncate_to_5min(dt: datetime) -> datetime:
    minute = (dt.minute // 5) * 5
    return dt.replace(minute=minute, second=0, microsecond=0)


def compute_fingerprint(entity_key: str, event_type: str | None, ts: datetime) -> str:
    bucket = _truncate_to_5min(ts).strftime("%Y-%m-%dT%H:%M:%SZ")
    payload = f"{entity_key}|{event_type or ''}|{bucket}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def normalize(event: dict, tenant_id: str, client_name: str) -> dict:
    """Convert a raw UAL event dict to the vector_events row shape."""
    user_id = _extract_user_id(event)
    entity_key = f"{tenant_id}::{user_id}"
    event_type = event.get("Operation")
    workload = event.get("Workload")
    result_status = _extract_result_status(event)
    client_ip = _extract_client_ip(event)
    user_agent = _extract_user_agent(event)
    ts = _parse_timestamp(event.get("CreationTime"))
    fingerprint = compute_fingerprint(entity_key, event_type, ts)

    return {
        "tenant_id": tenant_id,
        "client_name": client_name,
        "user_id": user_id,
        "entity_key": entity_key,
        "event_type": event_type,
        "workload": workload,
        "result_status": result_status,
        "client_ip": client_ip,
        "user_agent": user_agent,
        "timestamp": ts,
        "source": "UAL",
        "dedup_fingerprint": fingerprint,
        "raw_json": event,
    }
