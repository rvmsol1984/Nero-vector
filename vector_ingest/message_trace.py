"""Office 365 Reporting Web Service -- MessageTrace poller.

Polls every 15 minutes per tenant to pull email envelope metadata
(from / to / subject / size / status / direction) into
vector_message_trace. Uses the same client_credentials flow as the
UAL poller but requests a token scoped to manage.office.com.

Pagination: the Reporting Web Service returns at most 1000 rows per
page; we walk $skip until a short page (or an empty page) comes back.

Endpoint:
    https://reports.office365.com/ecp/reportingwebservice/reporting.svc/MessageTrace
        ?$format=json
        &StartDate=YYYY-MM-DDTHH:MM:SSZ
        &EndDate=YYYY-MM-DDTHH:MM:SSZ
        &$skip=<int>
        &$top=1000

Checkpoint stored as content_type = 'MessageTrace' in
vector_ingest_state.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import requests

from vector_ingest.db import Database

logger = logging.getLogger(__name__)


REPORTING_BASE = "https://reports.office365.com/ecp/reportingwebservice/reporting.svc"
REPORTING_MT_URL = f"{REPORTING_BASE}/MessageTrace"
MANAGE_RESOURCE = "https://manage.office.com"

TOKEN_REFRESH_SKEW = timedelta(minutes=5)
POLL_INTERVAL = timedelta(minutes=15)
PAGE_SIZE = 1000
# Safety cap: ~20k messages per tenant per cycle before we bail out and
# rely on the next tick to catch up.
MAX_PAGES_PER_CYCLE = 20
DEFAULT_LOOKBACK = timedelta(hours=1)


def _parse_iso(value: Any) -> datetime | None:
    """Parse a MessageTrace timestamp into a naive-UTC datetime."""
    if not value:
        return None
    raw = str(value)
    # Reporting API sometimes wraps Date fields as /Date(1713000000000)/
    if raw.startswith("/Date(") and raw.endswith(")/"):
        try:
            millis = int(raw[6:-2].split("+", 1)[0].split("-", 1)[0])
            return datetime.fromtimestamp(millis / 1000, tz=timezone.utc).replace(tzinfo=None)
        except Exception:
            return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def _to_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


class MessageTraceIngestor:
    """One instance per tenant."""

    def __init__(
        self,
        tenant_id: str,
        client_name: str,
        client_id: str,
        client_secret: str,
        db: Database,
    ) -> None:
        self.tenant_id = tenant_id
        self.client_name = client_name
        self.client_id = client_id
        self.client_secret = client_secret
        self.db = db

        self._token: str | None = None
        self._token_expiry: datetime = datetime.fromtimestamp(0, tz=timezone.utc)
        self._session = requests.Session()
        self._last_poll: datetime = datetime.fromtimestamp(0, tz=timezone.utc)

    # ---------- auth
    def _get_token(self) -> str:
        now = datetime.now(timezone.utc)
        if self._token and now + TOKEN_REFRESH_SKEW < self._token_expiry:
            return self._token

        logger.info(
            "message_trace token refresh",
            extra={"tenant_id": self.tenant_id, "client_name": self.client_name},
        )
        url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        resp = self._session.post(
            url,
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "scope": f"{MANAGE_RESOURCE}/.default",
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        self._token = data["access_token"]
        expires_in = int(data.get("expires_in", 3600))
        self._token_expiry = now + timedelta(seconds=expires_in)
        return self._token

    def _auth_headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self._get_token()}",
            "Accept": "application/json",
        }

    # ---------- extraction helpers
    def _extract_direction(self, msg: dict) -> str | None:
        """Infer IN/OUT from sender/recipient when Direction isn't explicit."""
        direct = msg.get("Direction") or msg.get("direction")
        if direct:
            s = str(direct).strip().lower()
            if s.startswith("in"):
                return "IN"
            if s.startswith("out"):
                return "OUT"
            return str(direct)
        # Heuristic: if original_client_ip is present and sender domain != tenant
        # verified domain list we don't know, so leave null.
        return None

    def _normalize(self, msg: dict) -> dict | None:
        if not isinstance(msg, dict):
            return None
        message_id = (
            msg.get("MessageId")
            or msg.get("message_id")
            or msg.get("MessageTraceId")
            or msg.get("messageTraceId")
        )
        if not message_id:
            return None
        received = _parse_iso(msg.get("Received") or msg.get("received") or msg.get("DateReceived"))
        return {
            "tenant_id":         self.tenant_id,
            "client_name":       self.client_name,
            "message_id":        str(message_id),
            "sender_address":    msg.get("SenderAddress") or msg.get("Sender"),
            "recipient_address": msg.get("RecipientAddress") or msg.get("Recipient"),
            "subject":           msg.get("Subject"),
            "received":          received,
            "status":            msg.get("Status"),
            "size_bytes":        _to_int(msg.get("Size") or msg.get("SizeBytes")),
            "direction":         self._extract_direction(msg),
            "original_client_ip": msg.get("OriginalClientIp") or msg.get("FromIP"),
        }

    # ---------- orchestration
    def poll_once(self) -> None:
        now = datetime.now(timezone.utc)
        if now - self._last_poll < POLL_INTERVAL:
            return
        self._last_poll = now

        checkpoint = self.db.get_checkpoint(self.tenant_id, "MessageTrace")
        if checkpoint is None:
            checkpoint = (now - DEFAULT_LOOKBACK).replace(tzinfo=None)
        start = checkpoint
        end = now.replace(tzinfo=None, microsecond=0)
        if end <= start:
            return

        logger.info(
            "[message_trace] starting poll",
            extra={
                "tenant_id": self.tenant_id,
                "client_name": self.client_name,
                "start": start.isoformat(),
                "end": end.isoformat(),
            },
        )

        skip = 0
        pages = 0
        written = 0
        seen = 0
        max_received = start

        while pages < MAX_PAGES_PER_CYCLE:
            params = {
                "$format": "json",
                "StartDate": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "EndDate":   end.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "$skip":     skip,
                "$top":      PAGE_SIZE,
            }
            try:
                resp = self._session.get(
                    REPORTING_MT_URL,
                    headers=self._auth_headers(),
                    params=params,
                    timeout=60,
                )
                if resp.status_code == 401:
                    self._token = None
                    resp = self._session.get(
                        REPORTING_MT_URL,
                        headers=self._auth_headers(),
                        params=params,
                        timeout=60,
                    )
                resp.raise_for_status()
            except requests.HTTPError as exc:
                logger.error(
                    "message_trace fetch failed",
                    extra={
                        "tenant_id": self.tenant_id,
                        "status": exc.response.status_code if exc.response is not None else None,
                        "error": str(exc),
                    },
                )
                return
            except requests.RequestException as exc:
                logger.error(
                    "message_trace request error",
                    extra={"tenant_id": self.tenant_id, "error": str(exc)},
                )
                return

            try:
                payload = resp.json() or {}
            except ValueError:
                logger.error(
                    "message_trace non-json response",
                    extra={"tenant_id": self.tenant_id, "body_head": resp.text[:200]},
                )
                return

            # Response envelope variants across OData versions.
            messages = (
                payload.get("value")
                or (payload.get("d") or {}).get("results")
                or (payload.get("d") or {}).get("value")
                or []
            )
            if not messages:
                break

            for msg in messages:
                seen += 1
                row = self._normalize(msg)
                if not row:
                    continue
                try:
                    if self.db.insert_message_trace(row):
                        written += 1
                    received = row.get("received")
                    if received and received > max_received:
                        max_received = received
                except Exception:
                    logger.exception(
                        "message_trace insert failed",
                        extra={"tenant_id": self.tenant_id, "message_id": row.get("message_id")},
                    )

            if len(messages) < PAGE_SIZE:
                break
            skip += len(messages)
            pages += 1

        if max_received > start:
            self.db.update_checkpoint(
                self.tenant_id, self.client_name, "MessageTrace", max_received
            )

        logger.info(
            "[message_trace] poll complete",
            extra={
                "tenant_id": self.tenant_id,
                "client_name": self.client_name,
                "seen": seen,
                "written": written,
                "pages": pages + 1,
            },
        )
