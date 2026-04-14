"""ThreatLocker ActionLog ingestion.

Polls the ThreatLocker Portal API every 5 minutes for ActionLog
entries (Permit / Deny / Ringfenced / Elevation events) and stores
them in ``vector_threatlocker_events`` keyed on the vendor-supplied
``eActionLogId`` so duplicate deliveries collapse automatically.

Auth model is a flat API token in the ``Authorization`` header
(no Bearer prefix), plus a ``ManagedOrganizationId`` header that
tells the portal which tenant's rows to return. The v2 endpoint
takes a JSON body with start/end window and pagination controls.

Checkpoint: ``vector_ingest_state`` row with
``content_type='ThreatLocker'`` and ``tenant_id`` set to the
configured ``THREATLOCKER_ORG_ID`` so one row per org is tracked.

This module is only instantiated by ``vector_ingest.main`` when
``THREATLOCKER_API_TOKEN`` is set in the environment; otherwise the
ingestor is skipped entirely (a log line is emitted at startup).
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


PORTAL_URL = (
    "https://portalapi.c.threatlocker.com/portalapi/ActionLog/"
    "ActionLogGetByParametersV2"
)

# Poll cadence matches the main vector-ingest loop; the main loop
# runs us every 5 minutes already, we only use this value to cap the
# lookback window when no checkpoint exists yet.
DEFAULT_LOOKBACK = timedelta(minutes=15)

# Safety cap so a completely empty checkpoint never asks for an
# unbounded window on first boot.
MAX_LOOKBACK = timedelta(hours=24)

# Page size per the vendor docs; anything larger returns HTTP 400.
PAGE_SIZE = 1000

# Hard cap on pages per cycle so a runaway response doesn't block the
# main loop indefinitely.
MAX_PAGES = 50

CONTENT_TYPE = "ThreatLocker"


def _parse_iso(value: Any) -> datetime | None:
    """Parse an ISO-8601 timestamp returned by the ThreatLocker API
    into a naive-UTC datetime suitable for TIMESTAMPTZ against the
    UTC-pinned session."""
    if not value:
        return None
    raw = str(value)
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        # Some portal responses carry fractional seconds; strip them
        # and retry as a last resort.
        try:
            dt = datetime.fromisoformat(raw.split(".")[0] + "+00:00")
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def _fmt_iso_z(dt: datetime) -> str:
    """Format a naive-UTC datetime as the ``YYYY-MM-DDTHH:MM:SSZ``
    shape ThreatLocker's body parser expects."""
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _to_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None


def _first(obj: dict, keys: tuple[str, ...]) -> Any:
    for k in keys:
        if k in obj and obj[k] not in (None, ""):
            return obj[k]
    return None


INSERT_SQL = """
INSERT INTO vector_threatlocker_events (
    tenant_id, client_name, action_log_id, event_time, hostname,
    username, full_path, process_path, action_type, action, action_id,
    policy_name, hash, raw_json
) VALUES (
    %(tenant_id)s, %(client_name)s, %(action_log_id)s, %(event_time)s,
    %(hostname)s, %(username)s, %(full_path)s, %(process_path)s,
    %(action_type)s, %(action)s, %(action_id)s, %(policy_name)s,
    %(hash)s, %(raw_json)s
)
ON CONFLICT (action_log_id) DO NOTHING
"""


class ThreatLockerIngestor:
    """One instance per ThreatLocker organization.

    The portal scopes by header (``ManagedOrganizationId``), so even
    though a partner can manage many orgs we construct one ingestor
    per configured ``THREATLOCKER_ORG_ID`` and let the checkpoint
    table disambiguate.
    """

    def __init__(
        self,
        tenant_id: str,
        client_name: str,
        api_token: str,
        db: Database,
    ) -> None:
        self.tenant_id = tenant_id
        self.client_name = client_name
        self._api_token = api_token
        self._db = db
        self._session = requests.Session()

    # ------------------------------------------------------------------
    def _headers(self) -> dict:
        return {
            "Authorization":         self._api_token,
            "ManagedOrganizationId": self.tenant_id,
            "usenewsearch":          "true",
            "Content-Type":          "application/json",
            "Accept":                "application/json",
        }

    # ------------------------------------------------------------------
    def _checkpoint(self) -> datetime:
        last = self._db.get_checkpoint(self.tenant_id, CONTENT_TYPE)
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        if last is None:
            return now - DEFAULT_LOOKBACK
        # Never allow the checkpoint to drift more than 24h into the
        # past -- if the worker was offline for longer than that we'd
        # rather lose old rows than issue a multi-day query.
        earliest = now - MAX_LOOKBACK
        return max(last, earliest)

    def _fetch_page(
        self,
        start_dt: datetime,
        end_dt: datetime,
        page_number: int,
    ) -> list[dict]:
        body = {
            "startDate":       _fmt_iso_z(start_dt),
            "endDate":         _fmt_iso_z(end_dt),
            "pageNumber":      page_number,
            "pageSize":        PAGE_SIZE,
            "paramsFieldsDto": [],
        }
        try:
            resp = self._session.post(
                PORTAL_URL,
                headers=self._headers(),
                data=json.dumps(body),
                timeout=60,
            )
        except requests.RequestException as exc:
            logger.error(
                "[threatlocker] request failed",
                extra={
                    "tenant_id":   self.tenant_id,
                    "client_name": self.client_name,
                    "page":        page_number,
                    "error":       str(exc),
                },
            )
            return []

        if resp.status_code == 401:
            logger.warning(
                "[threatlocker] 401 from portal -- check THREATLOCKER_API_TOKEN",
                extra={
                    "tenant_id":   self.tenant_id,
                    "client_name": self.client_name,
                    "body_head":   resp.text[:200],
                },
            )
            return []
        if not resp.ok:
            logger.warning(
                "[threatlocker] portal returned non-2xx",
                extra={
                    "tenant_id":   self.tenant_id,
                    "client_name": self.client_name,
                    "status":      resp.status_code,
                    "body_head":   resp.text[:200],
                },
            )
            return []

        try:
            payload = resp.json()
        except ValueError:
            logger.warning(
                "[threatlocker] portal returned non-JSON body",
                extra={"tenant_id": self.tenant_id, "body_head": resp.text[:200]},
            )
            return []

        # The documented response shape is a bare list. Some partner
        # deployments wrap the list under ``data`` / ``results``; be
        # permissive so a portal upgrade doesn't break us.
        if isinstance(payload, list):
            return [r for r in payload if isinstance(r, dict)]
        if isinstance(payload, dict):
            for key in ("data", "results", "items", "actionLogs"):
                val = payload.get(key)
                if isinstance(val, list):
                    return [r for r in val if isinstance(r, dict)]
        return []

    # ------------------------------------------------------------------
    def _normalize(self, row: dict) -> dict | None:
        action_log_id = _first(
            row,
            ("eActionLogId", "actionLogId", "eActionLogID", "id"),
        )
        if action_log_id is None:
            return None
        event_time = _parse_iso(
            _first(row, ("dateTime", "eventTime", "createdDateTime"))
        )
        return {
            "tenant_id":     self.tenant_id,
            "client_name":   self.client_name,
            "action_log_id": str(action_log_id),
            "event_time":    event_time,
            "hostname":      _first(row, ("hostname", "computerName", "deviceName")),
            "username":      _first(row, ("username", "userName", "user")),
            "full_path":     _first(row, ("fullPath", "filePath", "path")),
            "process_path":  _first(row, ("processPath", "parentProcessPath")),
            "action_type":   _first(row, ("actionType", "type")),
            "action":        _first(row, ("action",)),
            "action_id":     _to_int(_first(row, ("actionId", "action_id"))),
            "policy_name":   _first(row, ("policyName", "policy")),
            "hash":          _first(row, ("hash", "fileHash", "sha256")),
            "raw_json":      json.dumps(row),
        }

    def _insert_rows(self, rows: list[dict]) -> int:
        if not rows:
            return 0
        written = 0
        with self._db.conn.cursor() as cur:
            for row in rows:
                try:
                    cur.execute(INSERT_SQL, row)
                    written += max(cur.rowcount, 0)
                except Exception:
                    logger.exception(
                        "[threatlocker] insert failed",
                        extra={
                            "tenant_id":     self.tenant_id,
                            "action_log_id": row.get("action_log_id"),
                        },
                    )
        self._db.conn.commit()
        return written

    # ------------------------------------------------------------------
    def poll_once(self) -> None:
        start_dt = self._checkpoint()
        end_dt = datetime.now(timezone.utc).replace(tzinfo=None)
        if end_dt <= start_dt:
            end_dt = start_dt + timedelta(seconds=1)

        logger.info(
            "[threatlocker] poll start",
            extra={
                "tenant_id":   self.tenant_id,
                "client_name": self.client_name,
                "start":       _fmt_iso_z(start_dt),
                "end":         _fmt_iso_z(end_dt),
            },
        )

        total_seen = 0
        total_written = 0
        latest_event_time: datetime | None = None
        page_number = 1

        while page_number <= MAX_PAGES:
            page = self._fetch_page(start_dt, end_dt, page_number)
            if not page:
                break
            normalized: list[dict] = []
            for raw in page:
                row = self._normalize(raw)
                if row is None:
                    continue
                normalized.append(row)
                evt = row.get("event_time")
                if evt and (latest_event_time is None or evt > latest_event_time):
                    latest_event_time = evt
            total_seen += len(page)
            total_written += self._insert_rows(normalized)

            if len(page) < PAGE_SIZE:
                # Short page -- we've drained the window.
                break
            page_number += 1

        if page_number > MAX_PAGES:
            logger.warning(
                "[threatlocker] hit MAX_PAGES cap; checkpoint will advance "
                "only as far as the last page ingested",
                extra={"tenant_id": self.tenant_id, "pages": page_number},
            )

        if latest_event_time is not None:
            # Advance the checkpoint one second past the last event so
            # the next cycle's start boundary is strictly exclusive.
            self._db.update_checkpoint(
                self.tenant_id,
                self.client_name,
                CONTENT_TYPE,
                latest_event_time + timedelta(seconds=1),
            )
        else:
            # Even with an empty response, advance the checkpoint up to
            # the window end so we don't keep re-querying the same
            # range on the next cycle.
            self._db.update_checkpoint(
                self.tenant_id,
                self.client_name,
                CONTENT_TYPE,
                end_dt,
            )

        logger.info(
            "[threatlocker] poll complete",
            extra={
                "tenant_id":   self.tenant_id,
                "client_name": self.client_name,
                "pages":       page_number - 1 if total_seen else 0,
                "seen":        total_seen,
                "written":     total_written,
            },
        )
