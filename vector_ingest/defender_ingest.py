"""Microsoft Defender ATP ingestion.

Runs alongside the UAL TenantIngestor for tenants whose license_tier in
tenants.json is ``E5``. Polls the Defender Alerts API every 5 minutes
and the Advanced Hunting API every 15 minutes for a small set of
security-relevant KQL queries.

Auth uses client_credentials against the same VECTOR_CLIENT_ID /
VECTOR_CLIENT_SECRET used by the UAL poller, but requests the token
against the ``https://api.securitycenter.microsoft.com/.default`` scope
so the returned access token is valid for the Defender API.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import requests

from vector_ingest.db import Database

logger = logging.getLogger(__name__)


DEFENDER_RESOURCE = "https://api.securitycenter.microsoft.com"
ALERTS_URL  = f"{DEFENDER_RESOURCE}/api/alerts"
HUNTING_URL = f"{DEFENDER_RESOURCE}/api/advancedqueries/run"

TOKEN_REFRESH_SKEW    = timedelta(minutes=5)
ALERTS_POLL_INTERVAL  = timedelta(minutes=5)
HUNTING_POLL_INTERVAL = timedelta(minutes=15)

# KQL queries executed on each hunting cycle. Each one projects a
# uniform set of columns so normalize_hunting can read them by name.
HUNTING_QUERIES: dict[str, str] = {
    "sensitive_file_read": (
        "DeviceEvents "
        "| where Timestamp > ago(15m) "
        "| where ActionType == 'SensitiveFileRead' "
        "| project Timestamp, DeviceId, DeviceName, FileName, FolderPath, "
        "AccountName, InitiatingProcessFileName, InitiatingProcessCommandLine, "
        "InitiatingProcessAccountUpn"
    ),
    "file_events": (
        "DeviceFileEvents "
        "| where Timestamp > ago(15m) "
        "| where ActionType in ('FileCreated','FileModified','FileDeleted','FileRenamed') "
        "| project Timestamp, DeviceId, DeviceName, FileName, FolderPath, "
        "SHA256, FileSize, InitiatingProcessAccountUpn, InitiatingProcessFileName, "
        "InitiatingProcessCommandLine, ActionType"
    ),
    "suspicious_network": (
        "DeviceNetworkEvents "
        "| where Timestamp > ago(15m) "
        "| where RemotePort in (4444,1337,8080,31337,6666,9001) "
        "or RemoteIPType == 'Tor' "
        "| project Timestamp, DeviceId, DeviceName, RemoteIP, RemotePort, "
        "RemoteIPType, InitiatingProcessFileName, InitiatingProcessAccountUpn"
    ),
}


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


class DefenderIngestor:
    """One instance per E5 tenant."""

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

        # Per-source cadence bookkeeping so the main poll loop can call
        # poll_once() as often as it wants without overshooting the API
        # rate limits.
        self._last_alerts_poll: datetime = datetime.fromtimestamp(0, tz=timezone.utc)
        self._last_hunting_poll: datetime = datetime.fromtimestamp(0, tz=timezone.utc)

    # ------------------------------------------------------------------ auth
    def _get_token(self) -> str:
        now = datetime.now(timezone.utc)
        if self._token and now + TOKEN_REFRESH_SKEW < self._token_expiry:
            return self._token

        logger.info(
            "defender token refresh",
            extra={"tenant_id": self.tenant_id, "client_name": self.client_name},
        )
        url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        resp = self._session.post(
            url,
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "scope": f"{DEFENDER_RESOURCE}/.default",
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
        return {"Authorization": f"Bearer {self._get_token()}"}

    # ------------------------------------------------------------------ alerts
    def _poll_alerts(self) -> None:
        checkpoint = self.db.get_checkpoint(self.tenant_id, "defender_alerts")
        if checkpoint is None:
            checkpoint = datetime.utcnow().replace(microsecond=0) - timedelta(hours=1)

        filter_str = (
            f"alertCreationTime gt {checkpoint.strftime('%Y-%m-%dT%H:%M:%SZ')}"
        )
        params = {
            "$top": 100,
            "$orderby": "alertCreationTime desc",
            "$filter": filter_str,
        }

        try:
            resp = self._session.get(
                ALERTS_URL,
                headers=self._auth_headers(),
                params=params,
                timeout=30,
            )
            if resp.status_code == 401:
                self._token = None
                resp = self._session.get(
                    ALERTS_URL,
                    headers=self._auth_headers(),
                    params=params,
                    timeout=30,
                )
            resp.raise_for_status()
        except requests.HTTPError as exc:
            logger.error(
                "defender alerts fetch failed",
                extra={
                    "tenant_id": self.tenant_id,
                    "status": exc.response.status_code if exc.response is not None else None,
                    "error": str(exc),
                },
            )
            return
        except requests.RequestException as exc:
            logger.error(
                "defender alerts request error",
                extra={"tenant_id": self.tenant_id, "error": str(exc)},
            )
            return

        body = resp.json() or {}
        alerts = body.get("value") or []

        written = 0
        max_ts = checkpoint
        for a in alerts:
            row = self._normalize_alert(a)
            if not row:
                continue
            try:
                if self.db.insert_defender_alert(row):
                    written += 1
            except Exception:
                logger.exception(
                    "defender alert insert failed",
                    extra={"tenant_id": self.tenant_id, "alert_id": row.get("id")},
                )
                continue
            ts = row.get("alert_creation_time")
            if ts and ts > max_ts:
                max_ts = ts

        if max_ts > checkpoint:
            self.db.update_checkpoint(
                self.tenant_id, self.client_name, "defender_alerts", max_ts
            )

        logger.info(
            "defender alerts poll",
            extra={
                "tenant_id": self.tenant_id,
                "client_name": self.client_name,
                "count": len(alerts),
                "written": written,
            },
        )

    def _normalize_alert(self, a: dict) -> dict | None:
        if not isinstance(a, dict) or not a.get("id"):
            return None
        return {
            "id":                  str(a["id"]),
            "tenant_id":           self.tenant_id,
            "client_name":         self.client_name,
            "incident_id":         a.get("incidentId"),
            "severity":            a.get("severity"),
            "status":              a.get("status"),
            "category":            a.get("category"),
            "threat_family":       a.get("threatFamilyName"),
            "title":               a.get("title"),
            "machine_id":          a.get("machineId"),
            "computer_name":       a.get("computerDnsName"),
            "threat_name":         a.get("threatName"),
            "logged_on_users":     a.get("loggedOnUsers") or [],
            "alert_creation_time": _parse_iso(a.get("alertCreationTime")),
            "first_event_time":    _parse_iso(a.get("firstEventTime")),
            "last_event_time":     _parse_iso(a.get("lastEventTime")),
            "detection_source":    a.get("detectionSource"),
            "investigation_state": a.get("investigationState"),
            "mitre_techniques":    a.get("mitreTechniques") or [],
            "raw_json":            a,
        }

    # ------------------------------------------------------------------ hunting
    def _poll_hunting(self) -> None:
        for query_name, query in HUNTING_QUERIES.items():
            try:
                resp = self._session.post(
                    HUNTING_URL,
                    headers={**self._auth_headers(), "Content-Type": "application/json"},
                    json={"Query": query},
                    timeout=60,
                )
                if resp.status_code == 401:
                    self._token = None
                    resp = self._session.post(
                        HUNTING_URL,
                        headers={**self._auth_headers(), "Content-Type": "application/json"},
                        json={"Query": query},
                        timeout=60,
                    )
                resp.raise_for_status()
            except requests.HTTPError as exc:
                logger.error(
                    "defender hunting fetch failed",
                    extra={
                        "tenant_id": self.tenant_id,
                        "query": query_name,
                        "status": exc.response.status_code if exc.response is not None else None,
                        "error": str(exc),
                    },
                )
                continue
            except requests.RequestException as exc:
                logger.error(
                    "defender hunting request error",
                    extra={
                        "tenant_id": self.tenant_id,
                        "query": query_name,
                        "error": str(exc),
                    },
                )
                continue

            body = resp.json() or {}
            results = body.get("Results") or body.get("results") or []

            written = 0
            for r in results:
                row = self._normalize_hunting(query_name, r)
                if not row:
                    continue
                try:
                    if self.db.insert_defender_hunting(row):
                        written += 1
                except Exception:
                    logger.exception(
                        "defender hunting insert failed",
                        extra={
                            "tenant_id": self.tenant_id,
                            "query": query_name,
                        },
                    )

            logger.info(
                "defender hunting poll",
                extra={
                    "tenant_id": self.tenant_id,
                    "client_name": self.client_name,
                    "query": query_name,
                    "count": len(results),
                    "written": written,
                },
            )

    def _normalize_hunting(self, query_name: str, r: dict) -> dict | None:
        if not isinstance(r, dict):
            return None
        ts = _parse_iso(r.get("Timestamp") or r.get("timestamp"))
        if ts is None:
            return None
        return {
            "tenant_id":   self.tenant_id,
            "client_name": self.client_name,
            "query_name":  query_name,
            "device_id":   r.get("DeviceId") or "",
            "device_name": r.get("DeviceName"),
            "account_upn": r.get("InitiatingProcessAccountUpn") or r.get("AccountName"),
            "action_type": r.get("ActionType") or query_name,
            "timestamp":   ts,
            "raw_json":    r,
        }

    # ------------------------------------------------------------------ orchestration
    def poll_once(self) -> None:
        """Called by the main poll loop. Internally rate-limits alerts vs. hunting."""
        now = datetime.now(timezone.utc)
        if now - self._last_alerts_poll >= ALERTS_POLL_INTERVAL:
            try:
                self._poll_alerts()
            except Exception:
                logger.exception(
                    "defender alerts poll crashed",
                    extra={"tenant_id": self.tenant_id},
                )
            self._last_alerts_poll = now
        if now - self._last_hunting_poll >= HUNTING_POLL_INTERVAL:
            try:
                self._poll_hunting()
            except Exception:
                logger.exception(
                    "defender hunting poll crashed",
                    extra={"tenant_id": self.tenant_id},
                )
            self._last_hunting_poll = now
