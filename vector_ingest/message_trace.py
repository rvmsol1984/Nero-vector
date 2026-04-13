"""Microsoft Graph-based message metadata ingestion.

Replaces the deprecated reports.office365.com MessageTrace endpoint
(which doesn't support app-only auth) with a two-track Graph approach
depending on the tenant's license tier:

    E5             -> POST /v1.0/security/runHuntingQuery with an
                      EmailEvents KQL query. Returns up to 1000 rows
                      per cycle with real from/to/subject and
                      Defender threat classifications. Requires
                      ThreatHunting.Read.All plus a Microsoft 365
                      Defender / XDR entitlement.

    BizPremium etc -> GET /v1.0/reports/getEmailActivityUserDetail
                      (period='D7'). Returns per-user send/receive/
                      read counts -- no individual messages, but
                      available on every license tier with
                      Reports.Read.All. Rows are stored in
                      vector_message_trace as one synthetic "activity"
                      row per user per report refresh date.

Auth: client_credentials against https://graph.microsoft.com/.default
(same pattern as the UAL ingestor, different scope). Token cached
per-instance with a 5-minute refresh skew.

Polls every 15 minutes per tenant. Checkpoint lives in
vector_ingest_state with content_type='MessageTrace'.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import requests

from vector_ingest.db import Database

logger = logging.getLogger(__name__)


GRAPH_RESOURCE = "https://graph.microsoft.com"
GRAPH_HUNTING_URL = f"{GRAPH_RESOURCE}/v1.0/security/runHuntingQuery"
GRAPH_EMAIL_ACTIVITY_URL = (
    f"{GRAPH_RESOURCE}/v1.0/reports/getEmailActivityUserDetail(period='D7')"
    "?$format=application/json"
)

TOKEN_REFRESH_SKEW = timedelta(minutes=5)
POLL_INTERVAL = timedelta(minutes=15)

HUNTING_QUERY = (
    "EmailEvents "
    "| where Timestamp > ago(1h) "
    "| project Timestamp, SenderFromAddress, RecipientEmailAddress, "
    "Subject, DeliveryAction, ThreatTypes, NetworkMessageId "
    "| limit 1000"
)

CHECKPOINT_KEY = "MessageTrace_v2"


class HuntingUnavailable(Exception):
    """Raised when the E5 hunting path can't be used and we should fall back."""


def _parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    raw = str(value)
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        try:
            dt = datetime.fromisoformat(raw.split(".")[0] + "+00:00")
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
        license_tier: str = "BizPremium",
    ) -> None:
        self.tenant_id = tenant_id
        self.client_name = client_name
        self.client_id = client_id
        self.client_secret = client_secret
        self.db = db
        self.license_tier = str(license_tier or "").upper()

        self._token: str | None = None
        self._token_expiry: datetime = datetime.fromtimestamp(0, tz=timezone.utc)
        self._session = requests.Session()
        self._last_poll: datetime = datetime.fromtimestamp(0, tz=timezone.utc)

        # Sticky method choice. Set after the first successful cycle so we
        # don't pay the hunting 403 round-trip on every poll for tenants
        # that don't have the XDR entitlement.
        self._method: str | None = None

        # One-shot schema self-heal (idempotent), runs on first poll.
        self._table_ensured: bool = False

    # ------------------------------------------------------------------ auth
    def _get_token(self) -> str:
        now = datetime.now(timezone.utc)
        if self._token and now + TOKEN_REFRESH_SKEW < self._token_expiry:
            return self._token

        logger.info(
            "[message_trace] token refresh",
            extra={"tenant_id": self.tenant_id, "client_name": self.client_name},
        )
        url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        resp = self._session.post(
            url,
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "scope": f"{GRAPH_RESOURCE}/.default",
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

    # ------------------------------------------------------------------ hunting path (E5)
    def _poll_hunting(self) -> int:
        """Run the EmailEvents KQL query. Returns the number of rows written.

        Raises HuntingUnavailable on any response that looks like "your
        tenant doesn't have this feature" (401/403, or a Graph error
        body mentioning the subscription) so the caller can fall back
        to the activity report.
        """
        logger.info(
            "[message_trace] hunting poll start",
            extra={
                "tenant_id": self.tenant_id,
                "client_name": self.client_name,
                "method": "graph_hunting",
            },
        )

        try:
            resp = self._session.post(
                GRAPH_HUNTING_URL,
                headers={**self._auth_headers(), "Content-Type": "application/json"},
                json={"Query": HUNTING_QUERY},
                timeout=60,
            )
        except requests.RequestException as exc:
            logger.error(
                "[message_trace] hunting request failed",
                extra={"tenant_id": self.tenant_id, "error": str(exc)},
            )
            raise HuntingUnavailable(str(exc)) from exc

        if resp.status_code in (401, 403):
            if resp.status_code == 403:
                logger.warning(
                    "[message_trace] EmailEvents requires XDR license -- "
                    "tenant does not have Microsoft 365 Defender / Threat "
                    "Hunting. Falling back to activity report.",
                    extra={
                        "tenant_id": self.tenant_id,
                        "client_name": self.client_name,
                        "body_head": resp.text[:200],
                    },
                )
            else:
                logger.warning(
                    "[message_trace] hunting auth denied, body: %s",
                    resp.text[:200],
                    extra={"tenant_id": self.tenant_id, "status": 401},
                )
            # Clear the token so the next attempt re-auths cleanly.
            if resp.status_code == 401:
                self._token = None
            raise HuntingUnavailable(
                f"{resp.status_code}: {resp.text[:200]}"
            )
        if resp.status_code >= 400:
            logger.error(
                "[message_trace] hunting http error",
                extra={
                    "tenant_id": self.tenant_id,
                    "status": resp.status_code,
                    "body_head": resp.text[:200],
                },
            )
            raise HuntingUnavailable(
                f"{resp.status_code}: {resp.text[:200]}"
            )

        try:
            body = resp.json() or {}
        except ValueError:
            logger.error(
                "[message_trace] hunting non-json response",
                extra={"tenant_id": self.tenant_id, "body_head": resp.text[:200]},
            )
            raise HuntingUnavailable("non-json response")

        results = body.get("results") or body.get("Results") or []
        written = 0
        max_received = None
        for r in results:
            row = self._normalize_hunting(r)
            if not row:
                continue
            try:
                if self.db.insert_message_trace(row):
                    written += 1
                received = row.get("received")
                if received and (max_received is None or received > max_received):
                    max_received = received
            except Exception:
                logger.exception(
                    "[message_trace] hunting insert failed",
                    extra={"tenant_id": self.tenant_id},
                )

        if max_received is not None:
            self.db.update_checkpoint(
                self.tenant_id, self.client_name, CHECKPOINT_KEY, max_received
            )

        logger.info(
            "[message_trace] hunting poll complete",
            extra={
                "tenant_id": self.tenant_id,
                "client_name": self.client_name,
                "method": "graph_hunting",
                "seen": len(results),
                "written": written,
            },
        )
        return written

    def _normalize_hunting(self, r: dict) -> dict | None:
        if not isinstance(r, dict):
            return None
        received = _parse_iso(r.get("Timestamp"))
        if received is None:
            return None
        sender = r.get("SenderFromAddress") or ""
        recipient = r.get("RecipientEmailAddress") or ""
        subject = r.get("Subject") or ""

        # Use Defender's NetworkMessageId as the canonical message id
        # (stable across retries, matches the email in the mailbox).
        # Only fall back to a composite hash if Graph somehow omits it
        # so the ON CONFLICT dedup still has a unique key.
        network_message_id = (r.get("NetworkMessageId") or "").strip()
        if network_message_id:
            message_id = network_message_id[:500]
        else:
            message_id = (
                f"graph-hunt-{self.tenant_id}-{received.isoformat()}"
                f"-{sender}-{recipient}-{hash(subject) & 0xffffffff:08x}"
            )[:500]

        threat_types = r.get("ThreatTypes") or ""
        status = (
            str(r.get("DeliveryAction") or threat_types or "").strip() or None
        )

        return {
            "tenant_id":         self.tenant_id,
            "client_name":       self.client_name,
            "message_id":        message_id,
            "sender_address":    sender or None,
            "recipient_address": recipient or None,
            "subject":           subject or None,
            "received":          received,
            "status":            status,
            "size_bytes":        None,
            # EmailEvents is strictly inbound in the Defender schema, so
            # hardcode the direction rather than trying to derive it.
            "direction":         "Inbound",
            "original_client_ip": None,
        }

    # ------------------------------------------------------------------ activity report (fallback)
    def _poll_activity_report(self) -> int:
        """Per-user 7-day email activity rollup. Available on every tier
        with Reports.Read.All. Stored as one synthetic row per user per
        refresh date so downstream joins still work even though we don't
        have individual messages on this path."""
        logger.info(
            "[message_trace] activity-report poll start",
            extra={
                "tenant_id": self.tenant_id,
                "client_name": self.client_name,
                "method": "graph_activity_report",
            },
        )

        try:
            resp = self._session.get(
                GRAPH_EMAIL_ACTIVITY_URL,
                headers=self._auth_headers(),
                timeout=60,
            )
        except requests.RequestException as exc:
            logger.error(
                "[message_trace] activity-report request failed",
                extra={"tenant_id": self.tenant_id, "error": str(exc)},
            )
            return 0

        if resp.status_code == 401:
            self._token = None
            try:
                resp = self._session.get(
                    GRAPH_EMAIL_ACTIVITY_URL,
                    headers=self._auth_headers(),
                    timeout=60,
                )
            except requests.RequestException as exc:
                logger.error(
                    "[message_trace] activity-report retry failed",
                    extra={"tenant_id": self.tenant_id, "error": str(exc)},
                )
                return 0

        if resp.status_code >= 400:
            logger.error(
                "[message_trace] activity-report http error",
                extra={
                    "tenant_id": self.tenant_id,
                    "status": resp.status_code,
                    "body_head": resp.text[:200],
                },
            )
            return 0

        try:
            body = resp.json() or {}
        except ValueError:
            logger.error(
                "[message_trace] activity-report non-json response",
                extra={"tenant_id": self.tenant_id, "body_head": resp.text[:200]},
            )
            return 0

        values = body.get("value") or []
        written = 0
        for u in values:
            row = self._normalize_activity(u)
            if not row:
                continue
            try:
                if self.db.insert_message_trace(row):
                    written += 1
            except Exception:
                logger.exception(
                    "[message_trace] activity-report insert failed",
                    extra={"tenant_id": self.tenant_id},
                )

        # Use the report refresh date as the checkpoint so we don't
        # re-ingest the same daily rollup over and over. The activity
        # report is a daily snapshot, so once per UTC day is enough.
        today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        self.db.update_checkpoint(
            self.tenant_id, self.client_name, "MessageTrace", today
        )

        logger.info(
            "[message_trace] activity-report poll complete",
            extra={
                "tenant_id": self.tenant_id,
                "client_name": self.client_name,
                "method": "graph_activity_report",
                "users": len(values),
                "written": written,
            },
        )
        return written

    def _normalize_activity(self, u: dict) -> dict | None:
        if not isinstance(u, dict):
            return None
        upn = u.get("userPrincipalName")
        refresh = u.get("reportRefreshDate")
        if not upn or not refresh:
            return None
        received = _parse_iso(refresh) or datetime.utcnow().replace(microsecond=0)

        send = _to_int(u.get("sendCount")) or 0
        recv = _to_int(u.get("receiveCount")) or 0
        read = _to_int(u.get("readCount")) or 0
        subject = f"Activity D7: send={send} recv={recv} read={read}"

        # Synthetic idempotent id: one row per user per refresh date.
        message_id = f"activity-{self.tenant_id}-{upn}-{refresh}"[:500]

        return {
            "tenant_id":         self.tenant_id,
            "client_name":       self.client_name,
            "message_id":        message_id,
            "sender_address":    upn,
            "recipient_address": upn,
            "subject":           subject,
            "received":          received,
            "status":            "ACTIVITY",
            "size_bytes":        send + recv,
            "direction":         "ACTIVITY",
            "original_client_ip": None,
        }

    # ------------------------------------------------------------------ orchestration
    def poll_once(self) -> None:
        now = datetime.now(timezone.utc)
        if now - self._last_poll < POLL_INTERVAL:
            return
        self._last_poll = now

        # Safety net: ensure vector_message_trace + indexes exist before
        # the first insert. Belt-and-braces on top of the migration
        # runner -- harmless if 005 already ran.
        if not self._table_ensured:
            try:
                self.db.ensure_message_trace_table()
                self._table_ensured = True
            except Exception:
                logger.exception(
                    "[message_trace] ensure_message_trace_table failed",
                    extra={"tenant_id": self.tenant_id},
                )
                return

        # License-tier routing. E5 gets the real hunting query; everything
        # else (BizPremium, E3, …) gets the activity rollup fallback.
        if self.license_tier == "E5" and self._method != "activity":
            logger.info(
                "[message_trace] using hunting path for E5 tenant",
                extra={"tenant_id": self.tenant_id, "client_name": self.client_name},
            )
            try:
                self._poll_hunting()
                self._method = "hunting"
                return
            except HuntingUnavailable as exc:
                logger.warning(
                    "[message_trace] hunting unavailable, falling back to activity report",
                    extra={
                        "tenant_id": self.tenant_id,
                        "client_name": self.client_name,
                        "error": str(exc),
                    },
                )
                self._method = "activity"
                # fall through to activity report below

        logger.info(
            "[message_trace] using activity-report path",
            extra={
                "tenant_id": self.tenant_id,
                "client_name": self.client_name,
                "license_tier": self.license_tier,
            },
        )
        self._poll_activity_report()
        if self._method is None:
            self._method = "activity"
