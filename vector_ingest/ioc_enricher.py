"""OpenCTI-backed IOC enricher.

Every 5 minutes this poller scans recent telemetry for unique
observables worth looking up, asks OpenCTI whether it has any
indicators matching them, and records hits >= confidence 50 in
``vector_ioc_matches``. Hits >= confidence 75 additionally stage
a ``trigger_type='ioc_match'`` row in ``vector_watchlist`` with
``status='escalated'`` so the correlation engine picks them up
immediately.

Data sources (last 5 minutes only):
    - ``vector_events.client_ip``                -> ipv4 / ipv6
    - ``vector_events.raw_json->>'SenderFromAddress'`` -> email
    - ``vector_defender_hunting.raw_json->>'SHA256'`` -> file hash

OpenCTI is reached over GraphQL at
``OPENCTI_URL`` (defaults to ``http://127.0.0.1:8080/graphql``)
with a bearer token from ``OPENCTI_TOKEN``. Auth failure or any
non-2xx response degrades gracefully -- the enricher logs and
skips that tick rather than crashing the poll loop.

Rate-limiting: a fixed 100ms sleep between queries plus an
in-process cache (24h TTL for hits, 1h for misses) so the
same IOC is never queried twice in quick succession.
"""

from __future__ import annotations

import ipaddress
import json
import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import requests

from vector_ingest.db import Database

logger = logging.getLogger(__name__)


DEFAULT_OPENCTI_URL = "http://127.0.0.1:8080/graphql"
POLL_INTERVAL = timedelta(minutes=5)
LOOKBACK_WINDOW = timedelta(minutes=5)

QUERY_RATE_LIMIT_SEC = 0.1  # 100ms between OpenCTI queries

CACHE_TTL_HIT = timedelta(hours=24)
CACHE_TTL_MISS = timedelta(hours=1)

CONFIDENCE_STORE = 50       # >= 50 -> insert into vector_ioc_matches
CONFIDENCE_ESCALATE = 75    # >= 75 -> also stage a watchlist pin

# -- recognisers -------------------------------------------------------------

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")


def _looks_like_ipv4(value: str) -> bool:
    try:
        return isinstance(ipaddress.ip_address(value), ipaddress.IPv4Address)
    except (ValueError, TypeError):
        return False


def _looks_like_ipv6(value: str) -> bool:
    try:
        return isinstance(ipaddress.ip_address(value), ipaddress.IPv6Address)
    except (ValueError, TypeError):
        return False


def _looks_like_email(value: str) -> bool:
    return bool(value) and bool(_EMAIL_RE.match(value))


def _looks_like_sha256(value: str) -> bool:
    return bool(value) and bool(_SHA256_RE.match(value))


# -- GraphQL query templates -------------------------------------------------
#
# The confirmed-working query from the spec is for IPv4. We derive the
# other variants from the same filter pattern, just targeting a
# different entity type. OpenCTI is flexible about which filter key it
# accepts ("value" works for observables across types).

_GQL_QUERY_TMPL = """
query {
  stixCyberObservables(
    filters: {
      mode: and
      filters: [{ key: "value", values: ["%s"] }]
      filterGroups: []
    }
  ) {
    edges {
      node {
        id
        entity_type
        ... on IPv4Addr { value }
        ... on IPv6Addr { value }
        ... on EmailAddr { value }
        ... on StixFile { hashes { algorithm hash } name }
        indicators {
          edges {
            node {
              id
              name
              confidence
              description
              valid_from
              valid_until
            }
          }
        }
      }
    }
  }
}
""".strip()


class IocEnricher:
    """Poll OpenCTI every 5 minutes for the last 5 minutes of observables."""

    def __init__(self, db: Database) -> None:
        self.db = db
        self._session = requests.Session()
        self._last_poll: datetime = datetime.fromtimestamp(0, tz=timezone.utc)

        # { ioc_value -> (result_dict | None, expires_at) }
        # ``result_dict`` holds the best-matching indicator when found,
        # or None for a negative cache entry.
        self._cache: dict[str, tuple[dict | None, datetime]] = {}

        # Lazy-discovered watchlist schema (differs between migration
        # 002 and 004). Set on first insert attempt.
        self._watchlist_flavor: str | None = None

    # ------------------------------------------------------------------ config
    @property
    def _url(self) -> str:
        return os.environ.get("OPENCTI_URL", DEFAULT_OPENCTI_URL)

    @property
    def _token(self) -> str | None:
        return os.environ.get("OPENCTI_TOKEN")

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self._token}",
            "Content-Type":  "application/json",
            "Accept":        "application/json",
        }

    # ------------------------------------------------------------------ cache
    def _cache_get(self, value: str) -> dict | None | object:
        """Return the cached result for ``value`` or the sentinel
        ``_MISS`` if we've never looked it up (or the entry has
        expired). A cached negative lookup returns ``None``."""
        entry = self._cache.get(value)
        if entry is None:
            return _MISS
        result, expires_at = entry
        if datetime.now(timezone.utc) >= expires_at:
            del self._cache[value]
            return _MISS
        return result

    def _cache_put(self, value: str, result: dict | None) -> None:
        ttl = CACHE_TTL_HIT if result else CACHE_TTL_MISS
        self._cache[value] = (result, datetime.now(timezone.utc) + ttl)

    # ------------------------------------------------------------------ queries
    def _query_opencti(self, value: str) -> dict | None:
        """Run a single stixCyberObservables query. Returns a dict
        describing the best matching indicator (highest confidence)
        or None when the IOC is unknown / the query failed."""
        if not self._token:
            logger.error("OPENCTI_TOKEN not configured; skipping enrichment")
            return None
        try:
            resp = self._session.post(
                self._url,
                headers=self._headers(),
                json={"query": _GQL_QUERY_TMPL % value},
                timeout=15,
            )
        except requests.RequestException as exc:
            logger.warning("[ioc] opencti request failed for %s: %s", value, exc)
            return None

        if resp.status_code >= 400:
            logger.warning(
                "[ioc] opencti http error value=%s status=%d body=%s",
                value,
                resp.status_code,
                resp.text[:200],
            )
            return None

        try:
            body = resp.json()
        except ValueError:
            logger.warning(
                "[ioc] opencti non-json response value=%s body=%s",
                value,
                resp.text[:200],
            )
            return None

        if "errors" in body and body["errors"]:
            logger.warning(
                "[ioc] opencti graphql errors value=%s errors=%s",
                value,
                str(body["errors"])[:200],
            )
            # fall through: data may still be partially useful

        data = (body.get("data") or {}).get("stixCyberObservables") or {}
        edges = data.get("edges") or []
        if not edges:
            return None

        # Pick the first matching observable that has an indicator with
        # non-zero confidence. OpenCTI can return multiple observables
        # with the same value across tenants; we score on the highest
        # indicator confidence we can find.
        best: dict | None = None
        for edge in edges:
            node = (edge or {}).get("node") or {}
            observable_id = node.get("id")
            entity_type = node.get("entity_type")
            indicators = ((node.get("indicators") or {}).get("edges") or [])
            for ind_edge in indicators:
                ind = (ind_edge or {}).get("node") or {}
                confidence = ind.get("confidence")
                try:
                    confidence_int = int(confidence) if confidence is not None else 0
                except (TypeError, ValueError):
                    confidence_int = 0
                candidate = {
                    "opencti_observable_id": observable_id,
                    "entity_type": entity_type,
                    "opencti_id": ind.get("id"),
                    "indicator_name": ind.get("name"),
                    "confidence": confidence_int,
                    "description": ind.get("description"),
                    "valid_from": ind.get("valid_from"),
                    "valid_until": ind.get("valid_until"),
                }
                if best is None or candidate["confidence"] > best["confidence"]:
                    best = candidate
        return best

    def _lookup(self, value: str) -> dict | None:
        """Cache-backed wrapper around _query_opencti."""
        cached = self._cache_get(value)
        if cached is not _MISS:
            return cached  # type: ignore[return-value]
        time.sleep(QUERY_RATE_LIMIT_SEC)
        result = self._query_opencti(value)
        self._cache_put(value, result)
        return result

    # ------------------------------------------------------------------ collection
    def _collect_iocs(self) -> list[dict]:
        """Pull distinct observable candidates from the last 5 minutes
        of vector_events + vector_defender_hunting. Returns a list of
        ``{type, value, event_id, tenant_id, client_name}`` rows --
        one per distinct IOC, keeping the first event_id seen so the
        match row can point back to it."""
        now_window = datetime.now(timezone.utc) - LOOKBACK_WINDOW

        # IPs + sender emails from vector_events.
        with self.db.conn.cursor() as _cur:
            _cur.execute(
                """
                SELECT id, tenant_id, client_name, client_ip,
                       raw_json->>'SenderFromAddress' AS sender_email
                FROM vector_events
                WHERE timestamp >= %s
                  AND (
                    client_ip IS NOT NULL
                    OR raw_json ? 'SenderFromAddress'
                  )
                """,
                (now_window,),
            )
            ip_and_email_rows = _cur.fetchall()
            ip_and_email_rows = [
                {"id": r[0], "tenant_id": r[1], "client_name": r[2],
                 "client_ip": r[3], "sender_email": r[4]}
                for r in ip_and_email_rows
            ]

        # SHA256 hashes from defender hunting results.
        with self.db.conn.cursor() as _cur2:
            _cur2.execute(
                """
                SELECT id, tenant_id, client_name,
                       raw_json->>'SHA256' AS sha256
                FROM vector_defender_hunting
                WHERE timestamp >= %s
                  AND raw_json ? 'SHA256'
                """,
                (now_window,),
            )
            hash_rows = _cur2.fetchall()
            hash_rows = [
                {"id": r[0], "tenant_id": r[1], "client_name": r[2], "sha256": r[3]}
                for r in hash_rows
            ]

        seen: set[tuple[str, str]] = set()
        out: list[dict] = []

        def _add(ioc_type: str, value: str, row: dict) -> None:
            if not value:
                return
            value = value.strip()
            if not value:
                return
            key = (ioc_type, value.lower())
            if key in seen:
                return
            seen.add(key)
            out.append(
                {
                    "type":        ioc_type,
                    "value":       value,
                    "event_id":    row.get("id"),
                    "tenant_id":   row.get("tenant_id"),
                    "client_name": row.get("client_name"),
                }
            )

        for row in ip_and_email_rows or []:
            ip = (row.get("client_ip") or "").strip()
            if _looks_like_ipv4(ip):
                _add("ipv4-addr", ip, row)
            elif _looks_like_ipv6(ip):
                _add("ipv6-addr", ip, row)
            email = (row.get("sender_email") or "").strip()
            if _looks_like_email(email):
                _add("email-addr", email, row)

        for row in hash_rows or []:
            sha = (row.get("sha256") or "").strip()
            if _looks_like_sha256(sha):
                _add("file-sha256", sha, row)

        return out

    # ------------------------------------------------------------------ persistence
    def _insert_match(self, ioc: dict, match: dict) -> bool:
        payload = {
            "tenant_id":        ioc.get("tenant_id"),
            "client_name":      ioc.get("client_name"),
            "ioc_type":         ioc.get("type"),
            "ioc_value":        ioc.get("value"),
            "opencti_id":       match.get("opencti_id"),
            "indicator_name":   match.get("indicator_name"),
            "confidence":       match.get("confidence"),
            "matched_event_id": ioc.get("event_id"),
            "raw_json":         json.dumps(match),
        }
        sql = """
        INSERT INTO vector_ioc_matches (
            tenant_id, client_name, ioc_type, ioc_value, opencti_id,
            indicator_name, confidence, matched_event_id, raw_json
        ) VALUES (
            %(tenant_id)s, %(client_name)s, %(ioc_type)s, %(ioc_value)s,
            %(opencti_id)s, %(indicator_name)s, %(confidence)s,
            %(matched_event_id)s, %(raw_json)s
        )
        ON CONFLICT (ioc_value, matched_event_id) DO NOTHING
        """
        with self.db.conn.cursor() as cur:
            cur.execute(sql, payload)
            written = cur.rowcount
        self.db.conn.commit()
        return written > 0

    def _detect_watchlist_flavor(self) -> str:
        """Figure out whether vector_watchlist is the v0.2 schema
        (trigger_type / trigger_details / status) or the v0.1 schema
        (source / verdict / recipient / ...). Cached on the instance.
        """
        if self._watchlist_flavor is not None:
            return self._watchlist_flavor
        with self.db.conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'vector_watchlist'
                """
            )
            cols = {row[0] for row in cur.fetchall()}
        if "trigger_type" in cols and "trigger_details" in cols:
            self._watchlist_flavor = "v2"
        elif "verdict" in cols or "source" in cols:
            self._watchlist_flavor = "v1"
        else:
            self._watchlist_flavor = "unknown"
        return self._watchlist_flavor

    def _stage_watchlist(self, ioc: dict, match: dict) -> None:
        flavor = self._detect_watchlist_flavor()
        if flavor == "unknown":
            logger.warning(
                "[ioc] vector_watchlist schema unrecognised; "
                "skipping escalation for %s",
                ioc.get("value"),
            )
            return

        now = datetime.now(timezone.utc)
        expires_at = now + timedelta(hours=48)

        details = {
            "ioc_type":         ioc.get("type"),
            "ioc_value":        ioc.get("value"),
            "opencti_id":       match.get("opencti_id"),
            "indicator_name":   match.get("indicator_name"),
            "confidence":       match.get("confidence"),
            "matched_event_id": str(ioc.get("event_id")) if ioc.get("event_id") else None,
            "description":      match.get("description"),
        }

        try:
            if flavor == "v2":
                sql = """
                INSERT INTO vector_watchlist (
                    tenant_id, client_name, user_email, trigger_type,
                    trigger_details, expires_at, status
                ) VALUES (
                    %s, %s, %s, 'ioc_match', %s, %s, 'escalated'
                )
                """
                user_email = (
                    ioc["value"] if ioc["type"] == "email-addr" else None
                )
                with self.db.conn.cursor() as cur:
                    cur.execute(
                        sql,
                        (
                            ioc.get("tenant_id"),
                            ioc.get("client_name"),
                            user_email,
                            json.dumps(details),
                            expires_at.replace(tzinfo=None),
                        ),
                    )
            else:  # v1 schema
                sql = """
                INSERT INTO vector_watchlist (
                    tenant_id, source, verdict, recipient, sender, url,
                    event_type, timestamp, correlation_window_expires_at,
                    raw_json
                ) VALUES (
                    %s, 'OpenCTI', 'ioc_match', %s, NULL, NULL,
                    %s, %s, %s, %s
                )
                """
                recipient = ioc["value"] if ioc["type"] == "email-addr" else None
                with self.db.conn.cursor() as cur:
                    cur.execute(
                        sql,
                        (
                            ioc.get("tenant_id"),
                            recipient,
                            f"ioc_match:{ioc.get('type')}",
                            now.replace(tzinfo=None),
                            expires_at.replace(tzinfo=None),
                            json.dumps(details),
                        ),
                    )
            self.db.conn.commit()
            logger.info(
                "[ioc] escalated ioc=%s confidence=%s indicator=%s",
                ioc.get("value"),
                match.get("confidence"),
                match.get("indicator_name"),
            )
        except Exception:
            logger.exception(
                "[ioc] failed to stage watchlist pin for %s",
                ioc.get("value"),
            )
            self.db.conn.rollback()

    # ------------------------------------------------------------------ orchestration
    @property
    def tenant_id(self) -> str:
        # Surface a pseudo tenant id for main.py's log lines.
        return "(all tenants)"

    @property
    def client_name(self) -> str:
        return "ioc-enricher"

    def poll_once(self) -> None:
        now = datetime.now(timezone.utc)
        if now - self._last_poll < POLL_INTERVAL:
            return
        self._last_poll = now

        if not self._token:
            logger.warning(
                "[ioc] OPENCTI_TOKEN not set, skipping enrichment cycle"
            )
            return

        iocs = self._collect_iocs()
        if not iocs:
            logger.info("[ioc] no fresh observables in the last 5 minutes")
            return

        seen_count = len(iocs)
        matched_count = 0
        stored_count = 0
        escalated_count = 0

        for ioc in iocs:
            match = self._lookup(ioc["value"])
            if not match:
                continue
            confidence = int(match.get("confidence") or 0)
            if confidence < CONFIDENCE_STORE:
                continue
            matched_count += 1
            try:
                if self._insert_match(ioc, match):
                    stored_count += 1
            except Exception:
                logger.exception("[ioc] insert_match failed for %s", ioc["value"])
                continue
            if confidence >= CONFIDENCE_ESCALATE:
                self._stage_watchlist(ioc, match)
                escalated_count += 1

        logger.info(
            "[ioc] cycle complete scanned=%d matched=%d stored=%d escalated=%d",
            seen_count,
            matched_count,
            stored_count,
            escalated_count,
        )


# Sentinel used by _cache_get to distinguish "not cached" from
# "cached negative lookup".
_MISS = object()
