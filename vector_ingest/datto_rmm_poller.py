"""Datto RMM device and alert ingestion.

Polls two endpoints every 4 hours:
  - GET /api/v2/site/{site_uid}/devices  → vector_datto_devices
  - GET /api/v2/account/alerts/open      → vector_datto_alerts

Auth is OAuth2 password grant using the public-client credentials
documented by Datto. The token is valid for 100 hours and is cached
for the life of the process (refreshed automatically when it expires).

Site-to-client-name mapping is read from two env vars:
  DATTO_SITE_GCS  → site UID for GameChange Solar
  DATTO_SITE_LF   → site UID for London Fischer

This poller uses ``tenant_id = "*"`` (the global-worker convention in
vector-ingest) so it is placed in the ``global_ingestors`` bucket and
runs after every per-tenant UAL cycle -- matching the same pattern as
IocEnricher.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import requests

from vector_ingest.db import Database

logger = logging.getLogger(__name__)

# Datto public-client credentials (base64 of "public-client:public").
_DATTO_BASIC = "cHVibGljLWNsaWVudDpwdWJsaWM="

# Poll cadence — Datto device state changes slowly; 4 hours is plenty.
POLL_INTERVAL = timedelta(hours=4)

# Token TTL reported by Datto is 100 hours; refresh with a small skew.
_TOKEN_REFRESH_SKEW = timedelta(hours=1)


def _epoch_ms_to_dt(value: Any) -> datetime | None:
    """Convert epoch-millisecond integer (or string) to a UTC datetime."""
    if value is None:
        return None
    try:
        ms = int(value)
    except (TypeError, ValueError):
        return None
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


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
    return dt.astimezone(timezone.utc)


class DattoRmmPoller:
    """Global worker — polls all configured Datto RMM sites.

    ``tenant_id = "*"`` places this in the ``global_ingestors`` bucket
    so it runs after per-tenant UAL ingest each cycle, matching the
    IocEnricher / ScoringEngine convention.
    """

    tenant_id   = "*"
    client_name = "datto-rmm"

    def __init__(self, db: Database) -> None:
        self.db = db

        self._base_url  = (os.environ.get("DATTO_RMM_BASE_URL") or "").rstrip("/")
        self._api_key   = os.environ.get("DATTO_RMM_API_KEY", "")
        self._secret    = os.environ.get("DATTO_RMM_SECRET_KEY", "")

        # Map of site_uid → client_name populated from env at construction.
        self._sites: dict[str, str] = {}
        gcs_uid = (os.environ.get("DATTO_SITE_GCS") or "").strip()
        lf_uid  = (os.environ.get("DATTO_SITE_LF")  or "").strip()
        if gcs_uid:
            self._sites[gcs_uid] = "GameChange Solar"
        if lf_uid:
            self._sites[lf_uid]  = "London Fischer"

        self._token: str | None = None
        self._token_expiry: datetime = datetime.fromtimestamp(0, tz=timezone.utc)
        self._session = requests.Session()
        self._last_poll: datetime = datetime.fromtimestamp(0, tz=timezone.utc)

    # ------------------------------------------------------------------ auth

    def _get_token(self) -> str:
        now = datetime.now(timezone.utc)
        if self._token and now + _TOKEN_REFRESH_SKEW < self._token_expiry:
            return self._token

        logger.info("[datto] refreshing OAuth token")
        url  = f"{self._base_url}/auth/oauth/token"
        resp = self._session.post(
            url,
            headers={
                "Authorization": f"Basic {_DATTO_BASIC}",
                "Content-Type":  "application/x-www-form-urlencoded",
            },
            data={
                "grant_type": "password",
                "username":   self._api_key,
                "password":   self._secret,
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        self._token = data["access_token"]
        expires_in  = int(data.get("expires_in", 360000))
        self._token_expiry = now + timedelta(seconds=expires_in)
        logger.info(
            "[datto] token obtained",
            extra={"expires_in_hours": round(expires_in / 3600, 1)},
        )
        return self._token

    def _auth_headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self._get_token()}",
            "Accept":        "application/json",
        }

    # ------------------------------------------------------------------ API helpers

    def _get(self, path: str, params: dict | None = None) -> dict | list:
        url  = f"{self._base_url}{path}"
        resp = self._session.get(
            url,
            headers=self._auth_headers(),
            params=params or {},
            timeout=45,
        )
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------ devices

    def _poll_devices(self) -> int:
        """Fetch all devices for each configured site and upsert into
        vector_datto_devices. Returns total devices written."""
        total = 0
        for site_uid, client_name in self._sites.items():
            try:
                total += self._poll_site_devices(site_uid, client_name)
            except Exception as exc:
                logger.exception(
                    "[datto] device poll failed",
                    extra={"site_uid": site_uid, "client_name": client_name, "error": str(exc)},
                )
        return total

    def _poll_site_devices(self, site_uid: str, client_name: str) -> int:
        logger.info(
            "[datto] polling devices",
            extra={"site_uid": site_uid, "client_name": client_name},
        )
        devices = []
        page = 1
        while True:
            data = self._get(f"/api/v2/site/{site_uid}/devices", params={"page": page, "pageSize": 250})
            page_devices = data if isinstance(data, list) else (data.get("devices") or data.get("data") or [])
            if not page_devices:
                break
            devices.extend(page_devices)
            if isinstance(data, dict) and data.get("pageDetails", {}).get("nextPageUrl"):
                page += 1
            else:
                break

        rows: list[dict] = []
        for dev in devices:
            if not isinstance(dev, dict):
                continue
            uid = (dev.get("uid") or dev.get("id") or "").strip()
            if not uid:
                continue

            last_seen_raw = dev.get("lastSeen") or dev.get("last_seen")
            last_seen: datetime | None = None
            if isinstance(last_seen_raw, (int, float)):
                last_seen = _epoch_ms_to_dt(last_seen_raw)
            elif last_seen_raw:
                last_seen = _parse_iso(str(last_seen_raw))

            rows.append({
                "uid":                 uid,
                "site_uid":            site_uid,
                "client_name":         client_name,
                "hostname":            (dev.get("hostname") or dev.get("computerName") or "").strip() or None,
                "operating_system":    (dev.get("operatingSystem") or dev.get("os") or "").strip() or None,
                "online":              bool(dev.get("online") or dev.get("isOnline")),
                "last_seen":           last_seen,
                "last_logged_in_user": (dev.get("lastLoggedInUser") or dev.get("lastUser") or "").strip() or None,
                "raw_json":            json.dumps(dev),
            })

        if not rows:
            return 0

        written = self._upsert_devices(rows)
        logger.info(
            "[datto] devices upserted",
            extra={"site_uid": site_uid, "client_name": client_name, "count": written},
        )
        return written

    def _upsert_devices(self, rows: list[dict]) -> int:
        sql = """
            INSERT INTO vector_datto_devices
                (uid, site_uid, client_name, hostname, operating_system,
                 online, last_seen, last_logged_in_user, raw_json, updated_at)
            VALUES
                (%(uid)s, %(site_uid)s, %(client_name)s, %(hostname)s,
                 %(operating_system)s, %(online)s, %(last_seen)s,
                 %(last_logged_in_user)s, %(raw_json)s::jsonb, NOW())
            ON CONFLICT (uid) DO UPDATE SET
                online               = EXCLUDED.online,
                last_seen            = EXCLUDED.last_seen,
                last_logged_in_user  = EXCLUDED.last_logged_in_user,
                hostname             = EXCLUDED.hostname,
                operating_system     = EXCLUDED.operating_system,
                raw_json             = EXCLUDED.raw_json,
                updated_at           = NOW()
        """
        with self.db.conn.cursor() as cur:
            for row in rows:
                cur.execute(sql, row)
        self.db.conn.commit()
        return len(rows)

    # ------------------------------------------------------------------ alerts

    def _poll_alerts(self) -> int:
        logger.info("[datto] polling open alerts")
        try:
            all_alerts = []
            page = 1
            while True:
                data = self._get("/api/v2/account/alerts/open", params={"pageSize": 200, "page": page})
                page_alerts = data if isinstance(data, list) else (data.get("alerts") or data.get("data") or [])
                if not page_alerts:
                    break
                all_alerts.extend(page_alerts)
                if isinstance(data, dict) and data.get("pageDetails", {}).get("nextPageUrl"):
                    page += 1
                else:
                    break
            data = {"alerts": all_alerts}
            alerts = data if isinstance(data, list) else (data.get("alerts") or data.get("data") or [])
        except Exception as exc:
            logger.exception("[datto] alert poll failed", extra={"error": str(exc)})
            return 0

        # Resolve the site_uid → client_name map for alert attribution.
        site_to_client = self._sites

        rows: list[dict] = []
        for alert in alerts:
            if not isinstance(alert, dict):
                continue
            uid = (alert.get("alertUid") or alert.get("uid") or alert.get("id") or "").strip()
            if not uid:
                continue

            # Best-effort client_name from the alert's siteUid.
            site_uid    = (alert.get("siteUid") or alert.get("site_uid") or "").strip()
            client_name = site_to_client.get(site_uid, "")

            ts_raw  = alert.get("timestamp") or alert.get("createdAt")
            ts: datetime | None = None
            if isinstance(ts_raw, (int, float)):
                ts = _epoch_ms_to_dt(ts_raw)
            elif ts_raw:
                ts = _parse_iso(str(ts_raw))

            rows.append({
                "uid":             uid,
                "device_uid":      (alert.get("deviceUid") or alert.get("device_uid") or "").strip() or None,
                "client_name":     client_name or None,
                "hostname":        (alert.get("hostname") or alert.get("computerName") or "").strip() or None,
                "alert_type":      (alert.get("alertType") or alert.get("alert_type") or "").strip() or None,
                "message":         (alert.get("message") or alert.get("description") or "").strip() or None,
                "priority":        (alert.get("priority") or alert.get("severity") or "").strip() or None,
                "alert_timestamp": ts,
                "resolved":        False,
            })

        if not rows:
            return 0

        written = self._upsert_alerts(rows)
        logger.info("[datto] alerts upserted", extra={"count": written})
        return written

    def _upsert_alerts(self, rows: list[dict]) -> int:
        sql = """
            INSERT INTO vector_datto_alerts
                (uid, device_uid, client_name, hostname, alert_type,
                 message, priority, alert_timestamp, resolved)
            VALUES
                (%(uid)s, %(device_uid)s, %(client_name)s, %(hostname)s,
                 %(alert_type)s, %(message)s, %(priority)s,
                 %(alert_timestamp)s, %(resolved)s)
            ON CONFLICT (uid) DO UPDATE SET
                client_name     = EXCLUDED.client_name,
                hostname        = EXCLUDED.hostname,
                alert_type      = EXCLUDED.alert_type,
                message         = EXCLUDED.message,
                priority        = EXCLUDED.priority,
                alert_timestamp = EXCLUDED.alert_timestamp
        """
        with self.db.conn.cursor() as cur:
            for row in rows:
                cur.execute(sql, row)
        self.db.conn.commit()
        return len(rows)

    # ------------------------------------------------------------------ orchestration

    def poll_once(self) -> None:
        """Called by the main vector-ingest loop.

        Rate-limits internally to POLL_INTERVAL (4 hours) so the
        5-minute main-loop cadence doesn't over-poll Datto.
        """
        if not self._base_url or not self._api_key or not self._secret:
            logger.debug("[datto] env not configured, skipping poll")
            return

        now = datetime.now(timezone.utc)
        if now - self._last_poll < POLL_INTERVAL:
            return
        self._last_poll = now

        logger.info("[datto] poll start")
        try:
            devices_written = self._poll_devices()
            alerts_written  = self._poll_alerts()
            logger.info(
                "[datto] poll complete",
                extra={"devices": devices_written, "alerts": alerts_written},
            )
        except Exception as exc:
            logger.exception("[datto] poll cycle failed", extra={"error": str(exc)})
