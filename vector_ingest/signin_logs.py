"""Microsoft Graph ``/auditLogs/signIns`` ingestion.

Polls the Graph sign-in log every 5 minutes for each tenant that
has consented to ``AuditLog.Read.All``. Results are normalised into
``vector_events`` as synthetic ``UserLoggedIn`` rows so the scoring
engine and baseline engine see interactive + non-interactive sign-ins
within minutes of the user actually authenticating -- the UAL
``Audit.AzureActiveDirectory`` feed typically lags by 15-60 minutes,
so this poller is what makes impossible-travel and AiTM correlation
rules work on fresh data.

Auth uses the same ``VECTOR_CLIENT_ID`` / ``VECTOR_CLIENT_SECRET``
app registration as the UAL poller but requests a token against the
``https://graph.microsoft.com/.default`` scope. Tenants without the
``AuditLog.Read.All`` application permission return 403 on the first
poll; the ingestor logs a warning and deactivates itself so the main
loop doesn't keep hammering a tenant that can't serve us.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import requests

from vector_ingest.db import Database
from vector_ingest.normalizer import compute_fingerprint

logger = logging.getLogger(__name__)


GRAPH_RESOURCE = "https://graph.microsoft.com"
SIGNINS_URL = f"{GRAPH_RESOURCE}/v1.0/auditLogs/signIns"

POLL_INTERVAL = timedelta(minutes=5)
DEFAULT_LOOKBACK = timedelta(minutes=10)
TOKEN_REFRESH_SKEW = timedelta(minutes=5)

# Checkpoint content_type key stored in vector_ingest_state. One row
# per (tenant_id, content_type) pair is the existing convention.
CHECKPOINT_KEY = "SignInLogs_v1"

# Max rows to pull per poll. Graph caps at 1000 but 500 keeps each
# response small enough to process well under the 5-minute cadence.
PAGE_SIZE = 500


def _parse_iso(value: Any) -> datetime | None:
    """Parse an ISO-8601 timestamp from Graph into a naive-UTC
    datetime suitable for TIMESTAMPTZ against our UTC-pinned session."""
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


def _fmt_graph_ts(dt: datetime) -> str:
    """Format a naive-UTC datetime as the ``YYYY-MM-DDTHH:MM:SSZ``
    shape Graph's ``$filter createdDateTime gt`` clause expects."""
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


class SignInLogPoller:
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

        # Sticky deactivation flag. Flipped to True the first time
        # Graph returns a 403 "permission not granted" so subsequent
        # main-loop cycles skip this tenant silently instead of
        # logging a warning every 5 minutes.
        self._disabled: bool = False

    # ------------------------------------------------------------------ auth
    def _get_token(self) -> str:
        now = datetime.now(timezone.utc)
        if self._token and now + TOKEN_REFRESH_SKEW < self._token_expiry:
            return self._token

        logger.info(
            "[signin] token refresh",
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

    # ------------------------------------------------------------------ checkpoint
    def _checkpoint(self) -> datetime:
        last = self.db.get_checkpoint(self.tenant_id, CHECKPOINT_KEY)
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        if last is None:
            return now - DEFAULT_LOOKBACK
        return last

    # ------------------------------------------------------------------ fetch
    def _fetch(self, params: dict) -> tuple[list[dict], int]:
        """GET /v1.0/auditLogs/signIns with ``params``. Returns
        ``(results, status_code)``. Status is returned so the caller
        can disable the tenant on a hard 403."""
        try:
            resp = self._session.get(
                SIGNINS_URL,
                headers=self._auth_headers(),
                params=params,
                timeout=45,
            )
        except requests.RequestException as exc:
            logger.error(
                "[signin] request error",
                extra={"tenant_id": self.tenant_id, "error": str(exc)},
            )
            return [], 0

        if resp.status_code == 401:
            # Token almost certainly expired between refresh checks;
            # drop it and let the next cycle re-auth.
            self._token = None
            logger.warning(
                "[signin] 401 from Graph, clearing cached token",
                extra={"tenant_id": self.tenant_id},
            )
            return [], 401
        if resp.status_code == 403:
            logger.warning(
                "[signin] 403 from Graph -- tenant is missing "
                "AuditLog.Read.All permission; disabling poller",
                extra={
                    "tenant_id": self.tenant_id,
                    "client_name": self.client_name,
                    "body_head": resp.text[:200],
                },
            )
            return [], 403
        if not resp.ok:
            logger.error(
                "[signin] Graph non-2xx",
                extra={
                    "tenant_id": self.tenant_id,
                    "status": resp.status_code,
                    "body_head": resp.text[:200],
                },
            )
            return [], resp.status_code

        try:
            body = resp.json() or {}
        except ValueError:
            logger.error(
                "[signin] non-json response",
                extra={"tenant_id": self.tenant_id},
            )
            return [], resp.status_code

        value = body.get("value") or []
        return [v for v in value if isinstance(v, dict)], resp.status_code

    # ------------------------------------------------------------------ normalize
    def _normalize(self, s: dict) -> dict | None:
        """Convert one signIn dict into a vector_events-shaped row.

        ``userPrincipalName`` is required -- non-interactive sign-ins
        sometimes fire without a UPN and there's nothing useful the
        scoring engine can do with them.
        """
        ts = _parse_iso(s.get("createdDateTime"))
        if ts is None:
            return None
        upn = (s.get("userPrincipalName") or "").strip()
        if not upn:
            return None
        # Flag B2B guest accounts so downstream filtering knows they
        # aren't tenant-internal identities, but still ingest them --
        # the Guest Users governance tab and several correlation
        # rules want to see guest sign-ins.
        is_guest = "#EXT#" in upn.upper()

        location = s.get("location") or {}
        if not isinstance(location, dict):
            location = {}
        device_detail = s.get("deviceDetail") or {}
        if not isinstance(device_detail, dict):
            device_detail = {}

        client_ip = (s.get("ipAddress") or "").strip() or None
        event_type = "UserLoggedIn"
        entity_key = f"{self.tenant_id}::{upn}"

        # The scoring engine reads Country / City / ASN keys (written
        # by ingestor.py's GeoEnricher for UAL rows) to run the
        # impossible-travel rule -- populate the same shape here so
        # both data sources are comparable.
        raw_json = {
            "Country":                 (location.get("countryOrRegion") or "").strip() or None,
            "City":                    (location.get("city")            or "").strip() or None,
            "State":                   (location.get("state")           or "").strip() or None,
            "AppDisplayName":          (s.get("appDisplayName")         or "").strip() or None,
            "ClientAppUsed":           (s.get("clientAppUsed")          or "").strip() or None,
            "DeviceDetail":            (device_detail.get("displayName") or "").strip() or None,
            "ConditionalAccessStatus": (s.get("conditionalAccessStatus") or "").strip() or None,
            "RiskLevelDuringSignIn":   (s.get("riskLevelDuringSignIn")   or "").strip() or None,
            "RiskState":               (s.get("riskState")               or "").strip() or None,
            "IsGuest":                 is_guest,
            "SignInId":                s.get("id"),
            "source":                  "graph_signin",
        }

        # Pull a result_status string out of the signIn's status
        # envelope. "0" errorCode means success; anything else is
        # surfaced with the failureReason so the UI's status pill
        # renders a meaningful label.
        status = s.get("status") or {}
        if isinstance(status, dict):
            err = status.get("errorCode")
            if err == 0 or err == "0" or err is None:
                result_status = "Success"
            else:
                reason = (status.get("failureReason") or "").strip()
                result_status = f"Failure: {reason}" if reason else "Failure"
        else:
            result_status = None

        return {
            "tenant_id":         self.tenant_id,
            "client_name":       self.client_name,
            "user_id":           upn,
            "entity_key":        entity_key,
            "event_type":        event_type,
            "workload":          "AzureActiveDirectory",
            "result_status":     result_status,
            "client_ip":         client_ip,
            "user_agent":        None,
            "timestamp":         ts,
            "source":            "graph_signin",
            "dedup_fingerprint": compute_fingerprint(entity_key, event_type, ts),
            "raw_json":          raw_json,
        }

    # ------------------------------------------------------------------ orchestration
    def poll_once(self) -> None:
        """Called by the main vector-ingest loop. Internally rate-
        limits to POLL_INTERVAL so the loop can invoke us at any
        cadence without overshooting Graph."""
        if self._disabled:
            return
        now = datetime.now(timezone.utc)
        if now - self._last_poll < POLL_INTERVAL:
            return
        self._last_poll = now

        checkpoint = self._checkpoint()
        filter_str = f"createdDateTime gt {_fmt_graph_ts(checkpoint)}"
        params = {
            "$top":     PAGE_SIZE,
            "$orderby": "createdDateTime desc",
            "$filter":  filter_str,
        }

        logger.info(
            "[signin] poll start",
            extra={
                "tenant_id":   self.tenant_id,
                "client_name": self.client_name,
                "since":       _fmt_graph_ts(checkpoint),
            },
        )

        results, status = self._fetch(params)
        if status == 403:
            # AuditLog.Read.All not granted -- never try this tenant
            # again for the life of the process.
            self._disabled = True
            return
        if not results:
            logger.info(
                "[signin] poll complete",
                extra={
                    "tenant_id":   self.tenant_id,
                    "client_name": self.client_name,
                    "seen":        0,
                    "written":     0,
                },
            )
            return

        rows: list[dict] = []
        max_ts: datetime | None = None
        for raw in results:
            row = self._normalize(raw)
            if row is None:
                continue
            rows.append(row)
            ts = row.get("timestamp")
            if ts and (max_ts is None or ts > max_ts):
                max_ts = ts

        written = 0
        if rows:
            try:
                written = self.db.upsert_events_geo(rows)
            except Exception:
                logger.exception(
                    "[signin] insert_events failed",
                    extra={"tenant_id": self.tenant_id},
                )

        if max_ts is not None:
            try:
                self.db.update_checkpoint(
                    self.tenant_id, self.client_name, CHECKPOINT_KEY, max_ts
                )
            except Exception:
                logger.debug(
                    "[signin] checkpoint update failed",
                    exc_info=True,
                )

        logger.info(
            "[signin] poll complete",
            extra={
                "tenant_id":   self.tenant_id,
                "client_name": self.client_name,
                "seen":        len(results),
                "written":     written,
            },
        )
