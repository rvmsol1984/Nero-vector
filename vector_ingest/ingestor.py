"""Office 365 Management Activity API ingestor.

One TenantIngestor per tenant. Responsibilities:
    - obtain and cache a tenant-scoped OAuth token (auto-refresh)
    - ensure each Audit.* content subscription is started
    - poll available content blobs for a (start, end) window
    - fetch each blob and hand the raw events off to the normalizer + db
    - advance the per-content-type checkpoint
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Iterable

import requests

from vector_ingest.db import Database
from vector_ingest.normalizer import normalize

logger = logging.getLogger(__name__)


CONTENT_TYPES = (
    "Audit.AzureActiveDirectory",
    "Audit.Exchange",
    "Audit.SharePoint",
    "Audit.General",
)

MANAGEMENT_RESOURCE = "https://manage.office.com"
PUBLISHER_ID = "vector-ingest"

# Microsoft requires start/end inside the last 7 days and windows <= 24h.
MAX_WINDOW = timedelta(hours=24)
DEFAULT_LOOKBACK = timedelta(hours=1)
TOKEN_REFRESH_SKEW = timedelta(minutes=5)


class TenantIngestor:
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
        self._subscriptions_started: set[str] = set()
        self._session = requests.Session()

    # ------------------------------------------------------------------ auth
    def _get_token(self) -> str:
        now = datetime.now(timezone.utc)
        if self._token and now + TOKEN_REFRESH_SKEW < self._token_expiry:
            return self._token

        logger.info(
            "requesting new tenant token",
            extra={"tenant_id": self.tenant_id, "client_name": self.client_name},
        )
        url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        resp = self._session.post(
            url,
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "scope": f"{MANAGEMENT_RESOURCE}/.default",
            },
            timeout=30,
        )
        resp.raise_for_status()
        payload = resp.json()
        self._token = payload["access_token"]
        expires_in = int(payload.get("expires_in", 3600))
        self._token_expiry = now + timedelta(seconds=expires_in)
        return self._token

    def _auth_headers(self) -> dict:
        return {"Authorization": f"Bearer {self._get_token()}"}

    # ------------------------------------------------------------------ subscriptions
    def _ensure_subscription(self, content_type: str) -> None:
        if content_type in self._subscriptions_started:
            return

        url = (
            f"{MANAGEMENT_RESOURCE}/api/v1.0/{self.tenant_id}/activity/feed/subscriptions/start"
        )
        resp = self._session.post(
            url,
            headers=self._auth_headers(),
            params={"contentType": content_type, "PublisherIdentifier": PUBLISHER_ID},
            timeout=30,
        )
        # 200 = started, 400 with AF20024 = already enabled, both are fine.
        if resp.status_code == 200:
            logger.info(
                "subscription started",
                extra={
                    "tenant_id": self.tenant_id,
                    "client_name": self.client_name,
                    "content_type": content_type,
                },
            )
        elif resp.status_code == 400 and "AF20024" in resp.text:
            logger.info(
                "subscription already active",
                extra={
                    "tenant_id": self.tenant_id,
                    "client_name": self.client_name,
                    "content_type": content_type,
                },
            )
        else:
            resp.raise_for_status()

        self._subscriptions_started.add(content_type)

    # ------------------------------------------------------------------ polling
    def _list_content(
        self, content_type: str, start: datetime, end: datetime
    ) -> Iterable[dict]:
        """Yield content blobs (each is a dict with contentUri) for a window."""
        url = (
            f"{MANAGEMENT_RESOURCE}/api/v1.0/{self.tenant_id}/activity/feed/subscriptions/content"
        )
        params = {
            "contentType": content_type,
            "PublisherIdentifier": PUBLISHER_ID,
            "startTime": start.strftime("%Y-%m-%dT%H:%M:%S"),
            "endTime": end.strftime("%Y-%m-%dT%H:%M:%S"),
        }

        while True:
            resp = self._session.get(
                url, headers=self._auth_headers(), params=params, timeout=60
            )
            resp.raise_for_status()
            blobs = resp.json() or []
            for blob in blobs:
                yield blob

            next_page = resp.headers.get("NextPageUri")
            if not next_page:
                break
            # NextPageUri already contains all needed query params
            url = next_page
            params = None

    def _fetch_blob(self, content_uri: str) -> list[dict]:
        resp = self._session.get(
            content_uri, headers=self._auth_headers(), timeout=120
        )
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list):
            return data
        return []

    # ------------------------------------------------------------------ orchestration
    def _poll_content_type(self, content_type: str) -> None:
        self._ensure_subscription(content_type)

        now = datetime.now(timezone.utc).replace(microsecond=0)
        checkpoint = self.db.get_checkpoint(self.tenant_id, content_type)

        if checkpoint is None:
            start = now - DEFAULT_LOOKBACK
            logger.info(
                "no checkpoint, starting fresh",
                extra={
                    "tenant_id": self.tenant_id,
                    "client_name": self.client_name,
                    "content_type": content_type,
                    "start": start.isoformat(),
                },
            )
        else:
            start = checkpoint

        # The API rejects windows older than 7 days or longer than 24h.
        earliest = now - timedelta(days=7) + timedelta(minutes=1)
        if start < earliest:
            logger.warning(
                "checkpoint older than 7d, clamping",
                extra={
                    "tenant_id": self.tenant_id,
                    "content_type": content_type,
                    "checkpoint": start.isoformat(),
                },
            )
            start = earliest

        end = min(now, start + MAX_WINDOW)
        if end <= start:
            return

        logger.info(
            "polling window",
            extra={
                "tenant_id": self.tenant_id,
                "client_name": self.client_name,
                "content_type": content_type,
                "start": start.isoformat(),
                "end": end.isoformat(),
            },
        )

        blob_count = 0
        event_count = 0
        written_count = 0
        batch: list[dict] = []

        for blob in self._list_content(content_type, start, end):
            content_uri = blob.get("contentUri")
            if not content_uri:
                continue
            blob_count += 1
            try:
                raw_events = self._fetch_blob(content_uri)
            except requests.RequestException as exc:
                logger.error(
                    "blob fetch failed",
                    extra={
                        "tenant_id": self.tenant_id,
                        "content_type": content_type,
                        "content_uri": content_uri,
                        "error": str(exc),
                    },
                )
                continue

            for raw in raw_events:
                event_count += 1
                try:
                    normalized = normalize(raw, self.tenant_id, self.client_name)
                except Exception as exc:
                    logger.exception(
                        "normalize failed",
                        extra={
                            "tenant_id": self.tenant_id,
                            "content_type": content_type,
                            "error": str(exc),
                        },
                    )
                    continue
                batch.append(normalized)

                if len(batch) >= 500:
                    written_count += self.db.insert_events(batch)
                    batch.clear()

        if batch:
            written_count += self.db.insert_events(batch)

        self.db.update_checkpoint(self.tenant_id, self.client_name, content_type, end)

        logger.info(
            "poll complete",
            extra={
                "tenant_id": self.tenant_id,
                "client_name": self.client_name,
                "content_type": content_type,
                "blobs": blob_count,
                "events_seen": event_count,
                "events_written": written_count,
            },
        )

    def poll_once(self) -> None:
        for content_type in CONTENT_TYPES:
            try:
                self._poll_content_type(content_type)
            except requests.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else None
                logger.error(
                    "content poll http error",
                    extra={
                        "tenant_id": self.tenant_id,
                        "client_name": self.client_name,
                        "content_type": content_type,
                        "status": status,
                        "error": str(exc),
                    },
                )
                # On 401, drop the token so we force a refresh next loop.
                if status == 401:
                    self._token = None
            except Exception as exc:
                logger.exception(
                    "content poll failed",
                    extra={
                        "tenant_id": self.tenant_id,
                        "client_name": self.client_name,
                        "content_type": content_type,
                        "error": str(exc),
                    },
                )
