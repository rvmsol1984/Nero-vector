"""Office 365 Management Activity API ingestor.

One TenantIngestor per tenant. Responsibilities:
    - obtain and cache a tenant-scoped OAuth token (auto-refresh)
    - ensure each Audit.* content subscription is started
    - poll available content blobs for a (start, end) window
    - fetch each blob and hand the raw events off to the normalizer + db
    - advance the per-content-type checkpoint
"""

from __future__ import annotations

import ipaddress
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


# ---------------------------------------------------------------------------
# ipinfo.io geo-enrichment for UserLoggedIn events
# ---------------------------------------------------------------------------
#
# Adds Country / City / ASN to each UserLoggedIn event's raw_json based
# on the client_ip. Runs inside the ingest hot path so it's built to
# fail open: any network / parse / rate-limit error is logged at
# DEBUG and the event is left unchanged. Results are cached per-IP
# with a 24h TTL so the second UserLoggedIn from the same IP doesn't
# touch the network. Private and loopback addresses are skipped
# entirely via the stdlib ``ipaddress`` module.
#
# A single module-level instance is shared across every
# TenantIngestor so the cache is effective across tenants -- two
# different tenants hitting the same shared egress IP both get one
# lookup, not two.

_GEO_POSITIVE_TTL = timedelta(hours=24)
_GEO_NEGATIVE_TTL = timedelta(hours=1)
_GEO_RATE_LIMIT_SEC = 1.0


class GeoEnricher:
    """Lazy ipinfo.io enrichment with a 24h in-memory cache."""

    def __init__(self, token: str | None = None) -> None:
        self._token = token or None
        # ip -> (expires_at, geo_dict or None). ``None`` is a cached
        # negative result so we don't hammer ipinfo for IPs it
        # doesn't know about.
        self._cache: dict[str, tuple[datetime, dict | None]] = {}
        self._last_call_at: float = 0.0
        self._session = requests.Session()

    # ----- public api ----------------------------------------------------
    def enrich_event(self, normalized: dict) -> None:
        """Mutate ``normalized['raw_json']`` in place with Country /
        City / ASN fields when the event is a UserLoggedIn with a
        public client_ip. All errors are swallowed."""
        if normalized.get("event_type") != "UserLoggedIn":
            return
        client_ip = normalized.get("client_ip")
        if not client_ip:
            return
        raw = normalized.get("raw_json")
        if not isinstance(raw, dict):
            return
        try:
            geo = self._lookup(client_ip)
        except Exception:
            logger.debug(
                "[geo] unexpected enrichment failure ip=%s", client_ip, exc_info=True,
            )
            return
        if not geo:
            return
        # Only fill fields that aren't already present so a
        # UAL-provided Country keeps precedence over the ipinfo hit.
        for key, value in geo.items():
            if value and not raw.get(key):
                raw[key] = value

    # ----- internals -----------------------------------------------------
    @staticmethod
    def _is_skippable(ip: str) -> bool:
        """Return True for any address we should NOT look up --
        private ranges, loopback, link-local, multicast, unspecified,
        and anything we can't even parse."""
        try:
            addr = ipaddress.ip_address(str(ip).strip())
        except (ValueError, TypeError):
            return True
        return (
            addr.is_private
            or addr.is_loopback
            or addr.is_link_local
            or addr.is_unspecified
            or addr.is_multicast
            or addr.is_reserved
        )

    def _cache_get(self, ip: str) -> tuple[bool, dict | None]:
        """Return (hit, value). hit=True means we can short-circuit."""
        entry = self._cache.get(ip)
        if not entry:
            return (False, None)
        expires_at, value = entry
        if datetime.now(timezone.utc) > expires_at:
            self._cache.pop(ip, None)
            return (False, None)
        return (True, value)

    def _cache_put(self, ip: str, value: dict | None) -> None:
        ttl = _GEO_POSITIVE_TTL if value else _GEO_NEGATIVE_TTL
        self._cache[ip] = (datetime.now(timezone.utc) + ttl, value)

    def _rate_limit(self) -> None:
        """Block the calling thread until the next slot is available.
        Ingest runs on a single thread per tenant cycle so this is a
        simple monotonic sleep."""
        elapsed = time.monotonic() - self._last_call_at
        if elapsed < _GEO_RATE_LIMIT_SEC:
            time.sleep(_GEO_RATE_LIMIT_SEC - elapsed)
        self._last_call_at = time.monotonic()

    def _lookup(self, ip: str) -> dict | None:
        if self._is_skippable(ip):
            return None

        hit, value = self._cache_get(ip)
        if hit:
            return value

        self._rate_limit()
        url = f"https://ipinfo.io/{ip}/json"
        params = {"token": self._token} if self._token else None
        try:
            resp = self._session.get(url, params=params, timeout=5)
        except requests.RequestException as exc:
            logger.debug("[geo] ipinfo request failed ip=%s err=%s", ip, exc)
            # Don't cache transport errors -- retry on next cycle.
            return None

        if not resp.ok:
            logger.debug(
                "[geo] ipinfo non-2xx ip=%s status=%s body=%s",
                ip, resp.status_code, resp.text[:120],
            )
            if resp.status_code == 429:
                # Rate limited — fall back to ip-api.com
                return self._lookup_fallback(ip)
            if 400 <= resp.status_code < 500:
                self._cache_put(ip, None)
            return None

        try:
            data = resp.json()
        except ValueError:
            self._cache_put(ip, None)
            return None

        country = str(data.get("country") or "").strip() or None
        city    = str(data.get("city")    or "").strip() or None
        org     = str(data.get("org")     or "").strip() or None
        geo = {
            "Country": country,
            "City":    city,
            "ASN":     org,
        }
        # If ipinfo returned an empty envelope (no usable fields) cache
        # it as a negative so we don't keep retrying.
        if not any(geo.values()):
            self._cache_put(ip, None)
            return None

        self._cache_put(ip, geo)
        return geo

    def _lookup_fallback(self, ip: str) -> dict | None:
        """ip-api.com fallback when ipinfo is rate-limited. Free, no token needed."""
        try:
            resp = self._session.get(
                f"http://ip-api.com/json/{ip}?fields=status,country,city,org,countryCode",
                timeout=5,
            )
            if not resp.ok:
                return None
            data = resp.json()
            if data.get("status") != "success":
                return None
            country = str(data.get("countryCode") or "").strip() or None
            city    = str(data.get("city")        or "").strip() or None
            org     = str(data.get("org")         or "").strip() or None
            geo = {"Country": country, "City": city, "ASN": org}
            if not any(geo.values()):
                return None
            self._cache_put(ip, geo)
            logger.debug("[geo] ip-api fallback hit ip=%s country=%s", ip, country)
            return geo
        except Exception as exc:
            logger.debug("[geo] ip-api fallback failed ip=%s err=%s", ip, exc)
            return None


# Module-level singleton. Constructed on first access so
# IPINFO_TOKEN can be picked up after process start (e.g. via an
# .env file loaded between import and first poll).
_GEO_ENRICHER: GeoEnricher | None = None


def _get_geo_enricher() -> GeoEnricher:
    global _GEO_ENRICHER
    if _GEO_ENRICHER is None:
        _GEO_ENRICHER = GeoEnricher(token=os.environ.get("IPINFO_TOKEN") or None)
    return _GEO_ENRICHER


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

        # Work in naive UTC throughout this function so that both the DB
        # checkpoint write and the O365 API startTime/endTime params are
        # always plain UTC wall-clock, regardless of the container's local
        # timezone. Mixing an aware local-tz datetime with strftime() silently
        # produced a local-offset window (e.g. +02:00 CEST) against the API,
        # which is why the fix here is to strip tzinfo after converting to UTC.
        now = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
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
                # Geo-enrich UserLoggedIn events in place before batching.
                # Fail-open: any ipinfo error is swallowed inside enrich_event
                # so a CTI outage never blocks UAL ingest.
                _get_geo_enricher().enrich_event(normalized)
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
