"""OpenCTI-backed IOC enrichment worker.

Runs on the same 5-minute cadence as the rest of the vector-ingest
pollers. Each cycle:

  1. Pull the last ~5 minutes of IOCs out of Postgres:
       - client_ip values from vector_events
       - SenderFromAddress / RecipientEmailAddress /
         ExtendedProperties URLs from vector_events.raw_json
       - ObjectId values from vector_events.raw_json that look like
         URLs
       - sender_address values from vector_message_trace
       - SHA256 hashes from vector_defender_hunting.raw_json
       - SHA256 hashes from vector_edr_events.raw_json

  2. For each unique IOC, query OpenCTI's stixCyberObservables
     GraphQL endpoint for the value and inspect any linked
     indicators.

  3. When OpenCTI returns a linked indicator with confidence >= 50
     the row is inserted into vector_ioc_matches. When confidence
     >= 75 a vector_watchlist row is also created with
     trigger_type='ioc_match' / status='escalated' so the UI's
     correlation pins pick it up immediately.

Rate limits / caching:

  * Queries are issued one at a time with a 100ms pause in between
    so we don't hammer the OpenCTI API. IPv4 lookups are still
    batched client-side into groups of 10 so we can log progress.
  * Negative lookups (clean IOC, no linked indicators) are cached
    in-memory for 1 hour.
  * Positive lookups are cached in-memory for 24 hours.

This module intentionally uses its own OpenCTI HTTP client rather
than piggybacking on the existing TenantIngestor so it doesn't block
UAL ingestion on a slow CTI backend.
"""

from __future__ import annotations

import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

import requests

from vector_ingest.db import Database

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# tunables
# ---------------------------------------------------------------------------

# How far back to look for fresh IOCs each cycle. Slightly wider than
# the 5-minute poll cadence so that any late-arriving rows from a
# previous cycle are still picked up.
LOOKBACK = timedelta(minutes=6)

# Per-query pacing against OpenCTI (milliseconds between calls).
QUERY_DELAY_MS = 100

# Group IPs into chunks of this size for progress logging. (OpenCTI
# is queried one value at a time; this is purely a batch-reporting
# granularity.)
IP_BATCH_SIZE = 10

# Cache TTLs.
NEGATIVE_TTL = timedelta(hours=1)
POSITIVE_TTL = timedelta(hours=24)

# Confidence thresholds.
MIN_CONFIDENCE = 50
ESCALATE_CONFIDENCE = 75

# Watchlist correlation window for IOC-triggered pins (24 hours).
WATCHLIST_WINDOW = timedelta(hours=24)

# GraphQL lookup timeout.
GRAPHQL_TIMEOUT = 15


# ---------------------------------------------------------------------------
# OpenCTI GraphQL client
# ---------------------------------------------------------------------------

# The spec confirms that the stixCyberObservables query with a "value"
# filter works for every observable type we care about. OpenCTI picks
# the right observable class by filter value; we then union-select the
# value field from each type so the response shape is uniform.
_GRAPHQL_QUERY = """
query VectorIocLookup($value: String!) {
  stixCyberObservables(
    filters: {
      mode: and,
      filters: [{ key: "value", values: [$value] }],
      filterGroups: []
    }
  ) {
    edges {
      node {
        id
        entity_type
        ... on IPv4Addr  { value }
        ... on IPv6Addr  { value }
        ... on DomainName { value }
        ... on Url       { value }
        ... on EmailAddr { value }
        ... on StixFile  { hashes { algorithm hash } }
        indicators {
          edges {
            node {
              id
              name
              confidence
              description
              pattern
              valid_from
              valid_until
            }
          }
        }
      }
    }
  }
}
"""


class OpenCTIClient:
    """Small GraphQL client for the OpenCTI API."""

    def __init__(self, url: str, token: str, timeout: int = GRAPHQL_TIMEOUT) -> None:
        self._url = url
        self._token = token
        self._timeout = timeout
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Content-Type":  "application/json",
                "Accept":        "application/json",
            }
        )

    def lookup(self, value: str) -> list[dict]:
        """Return the list of edges[].node dicts for ``value``.

        Raises on transport / HTTP errors; callers are expected to
        swallow the exception and log it so one bad query never
        blocks the rest of the enrichment cycle.
        """
        payload = {"query": _GRAPHQL_QUERY, "variables": {"value": value}}
        resp = self._session.post(self._url, json=payload, timeout=self._timeout)
        resp.raise_for_status()
        body = resp.json()
        if body.get("errors"):
            logger.warning(
                "[ioc] opencti returned errors value=%s errors=%s",
                value,
                body["errors"],
            )
            return []
        edges = (
            body.get("data", {})
            .get("stixCyberObservables", {})
            .get("edges", [])
            or []
        )
        return [e.get("node") or {} for e in edges if isinstance(e, dict)]


# ---------------------------------------------------------------------------
# extraction helpers
# ---------------------------------------------------------------------------

_IPV4_RE = re.compile(r"^\d{1,3}(\.\d{1,3}){3}$")
_IPV6_RE = re.compile(r"^[0-9a-fA-F:]+$")
_SHA256_RE = re.compile(r"^[a-fA-F0-9]{64}$")
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_URL_RE = re.compile(r"^https?://", re.IGNORECASE)
_DOMAIN_RE = re.compile(
    r"^(?=.{1,253}$)(?:[a-zA-Z0-9](?:[a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?\.)+"
    r"[a-zA-Z]{2,63}$"
)


def _classify_ip(raw: str) -> str | None:
    raw = (raw or "").strip()
    if not raw:
        return None
    if _IPV4_RE.match(raw):
        parts = raw.split(".")
        if all(0 <= int(p) <= 255 for p in parts):
            return "ipv4"
        return None
    if ":" in raw and _IPV6_RE.match(raw):
        return "ipv6"
    return None


def _classify_string(raw: str) -> tuple[str, str] | None:
    """Return (ioc_type, normalized_value) for a raw string, or None
    if the value doesn't look like a supported IOC shape."""
    if raw is None:
        return None
    value = str(raw).strip()
    if not value:
        return None
    if _SHA256_RE.match(value):
        return ("sha256", value.lower())
    ip_type = _classify_ip(value)
    if ip_type:
        return (ip_type, value)
    if _EMAIL_RE.match(value):
        return ("email", value.lower())
    if _URL_RE.match(value):
        return ("url", value)
    if _DOMAIN_RE.match(value) and "." in value:
        return ("domain", value.lower())
    return None


def _iter_url_like_from_extended(raw_json: Any) -> Iterable[str]:
    """Walk a UAL ExtendedProperties array (list of {Name, Value}) and
    yield every Value that looks like a URL."""
    if not isinstance(raw_json, dict):
        return []
    out: list[str] = []
    props = raw_json.get("ExtendedProperties")
    if isinstance(props, list):
        for p in props:
            if not isinstance(p, dict):
                continue
            val = p.get("Value")
            if isinstance(val, str) and _URL_RE.match(val):
                out.append(val)
    obj_id = raw_json.get("ObjectId")
    if isinstance(obj_id, str) and _URL_RE.match(obj_id):
        out.append(obj_id)
    return out


# ---------------------------------------------------------------------------
# main worker
# ---------------------------------------------------------------------------

class IocEnricher:
    """Runs one enrichment cycle per ``poll_once()`` call."""

    # tenant_id / client_name are kept on the instance only so the
    # main loop's structured log lines have fields to print -- this
    # worker is global, not per-tenant.
    tenant_id = "*"
    client_name = "global"

    def __init__(
        self,
        db: Database,
        url: str | None = None,
        token: str | None = None,
    ) -> None:
        self._db = db
        self._url = url or os.environ.get(
            "OPENCTI_URL", "http://127.0.0.1:8080/graphql"
        )
        self._token = token or os.environ.get("OPENCTI_TOKEN", "")
        self._client: OpenCTIClient | None = None
        # Cache: ioc_value -> (expires_at, list_of_indicator_dicts).
        # An empty list indicates a negative result.
        self._cache: dict[str, tuple[datetime, list[dict]]] = {}

    # ----- cache ---------------------------------------------------------

    def _cache_get(self, value: str) -> list[dict] | None:
        hit = self._cache.get(value)
        if not hit:
            return None
        expires_at, result = hit
        if datetime.now(timezone.utc) > expires_at:
            self._cache.pop(value, None)
            return None
        return result

    def _cache_put(self, value: str, result: list[dict]) -> None:
        ttl = POSITIVE_TTL if result else NEGATIVE_TTL
        self._cache[value] = (datetime.now(timezone.utc) + ttl, result)

    # ----- extraction ----------------------------------------------------

    def _recent_events(self) -> list[dict]:
        """Load recent UAL events with their raw_json so we can pull
        out client_ip plus whatever lives in ExtendedProperties /
        ObjectId / SenderFromAddress / RecipientEmailAddress."""
        cutoff = datetime.now(timezone.utc) - LOOKBACK
        with self._db.conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, tenant_id, client_name, client_ip, raw_json, user_id
                FROM vector_events
                WHERE timestamp >= %s
                """,
                (cutoff,),
            )
            rows = cur.fetchall()
        out: list[dict] = []
        for row in rows:
            rid, tenant_id, client_name, client_ip, raw_json, user_id = row
            out.append(
                {
                    "id":         rid,
                    "tenant_id":  tenant_id,
                    "client_name": client_name,
                    "client_ip":  client_ip,
                    "raw_json":   raw_json,
                    "user_id":    user_id,
                }
            )
        return out

    def _recent_message_trace_senders(self) -> list[dict]:
        cutoff = datetime.now(timezone.utc) - LOOKBACK
        with self._db.conn.cursor() as cur:
            cur.execute(
                """
                SELECT tenant_id, client_name, sender_address
                FROM vector_message_trace
                WHERE received >= %s
                  AND sender_address IS NOT NULL
                """,
                (cutoff,),
            )
            return [
                {
                    "tenant_id":  r[0],
                    "client_name": r[1],
                    "value":      r[2],
                }
                for r in cur.fetchall()
            ]

    def _recent_defender_hunting_hashes(self) -> list[dict]:
        cutoff = datetime.now(timezone.utc) - LOOKBACK
        with self._db.conn.cursor() as cur:
            cur.execute(
                """
                SELECT tenant_id, client_name, raw_json
                FROM vector_defender_hunting
                WHERE timestamp >= %s
                """,
                (cutoff,),
            )
            rows = cur.fetchall()
        out: list[dict] = []
        for tenant_id, client_name, raw_json in rows:
            if not isinstance(raw_json, dict):
                continue
            for key in ("SHA256", "Sha256", "sha256", "InitiatingProcessSHA256"):
                val = raw_json.get(key)
                if isinstance(val, str) and _SHA256_RE.match(val):
                    out.append(
                        {
                            "tenant_id":  tenant_id,
                            "client_name": client_name,
                            "value":      val.lower(),
                        }
                    )
        return out

    def _recent_edr_hashes(self) -> list[dict]:
        cutoff = datetime.now(timezone.utc) - LOOKBACK
        with self._db.conn.cursor() as cur:
            cur.execute(
                """
                SELECT tenant_id, client_name, raw_json
                FROM vector_edr_events
                WHERE timestamp >= %s
                """,
                (cutoff,),
            )
            rows = cur.fetchall()
        out: list[dict] = []
        for tenant_id, client_name, raw_json in rows:
            if not isinstance(raw_json, dict):
                continue
            # Recurse one level to find any sha256-looking string.
            for val in _walk_strings(raw_json):
                if _SHA256_RE.match(val):
                    out.append(
                        {
                            "tenant_id":  tenant_id,
                            "client_name": client_name,
                            "value":      val.lower(),
                        }
                    )
        return out

    def _collect_iocs(self) -> dict[tuple[str, str], dict]:
        """Return a {(ioc_type, ioc_value): context} map.

        ``context`` carries the first tenant_id / client_name / event
        id / user_id we saw the value on so the resulting
        vector_ioc_matches row has something to point at. Events
        without a matching database row still produce a match with a
        NULL matched_event_id.
        """
        iocs: dict[tuple[str, str], dict] = {}

        def add(key: tuple[str, str], ctx: dict) -> None:
            iocs.setdefault(key, ctx)

        for ev in self._recent_events():
            tenant_id = ev.get("tenant_id")
            client_name = ev.get("client_name")
            event_id = ev.get("id")
            user_id = ev.get("user_id")
            ctx = {
                "tenant_id":  tenant_id,
                "client_name": client_name,
                "event_id":   event_id,
                "user_id":    user_id,
            }

            # client_ip
            ip_type = _classify_ip(ev.get("client_ip") or "")
            if ip_type:
                add((ip_type, ev["client_ip"]), ctx)

            raw = ev.get("raw_json")
            if isinstance(raw, dict):
                for key in ("SenderFromAddress", "RecipientEmailAddress"):
                    val = raw.get(key)
                    if isinstance(val, str):
                        classified = _classify_string(val)
                        if classified and classified[0] == "email":
                            add(classified, ctx)
                for url in _iter_url_like_from_extended(raw):
                    classified = _classify_string(url)
                    if classified:
                        add(classified, ctx)

        for row in self._recent_message_trace_senders():
            classified = _classify_string(row["value"])
            if classified and classified[0] == "email":
                add(
                    classified,
                    {
                        "tenant_id":  row["tenant_id"],
                        "client_name": row["client_name"],
                        "event_id":   None,
                        "user_id":    None,
                    },
                )

        for row in self._recent_defender_hunting_hashes():
            add(
                ("sha256", row["value"]),
                {
                    "tenant_id":  row["tenant_id"],
                    "client_name": row["client_name"],
                    "event_id":   None,
                    "user_id":    None,
                },
            )

        for row in self._recent_edr_hashes():
            add(
                ("sha256", row["value"]),
                {
                    "tenant_id":  row["tenant_id"],
                    "client_name": row["client_name"],
                    "event_id":   None,
                    "user_id":    None,
                },
            )

        return iocs

    # ----- OpenCTI lookup ------------------------------------------------

    def _ensure_client(self) -> OpenCTIClient | None:
        if not self._token:
            logger.warning(
                "[ioc] OPENCTI_TOKEN not set -- IOC enrichment disabled"
            )
            return None
        if self._client is None:
            self._client = OpenCTIClient(self._url, self._token)
        return self._client

    def _best_indicator(self, nodes: list[dict]) -> dict | None:
        """Pick the highest-confidence linked indicator from a list
        of stixCyberObservables nodes. Returns a flat dict that the
        caller inserts into vector_ioc_matches."""
        best: dict | None = None
        for node in nodes:
            if not isinstance(node, dict):
                continue
            observable_id = node.get("id")
            indicators = (
                (node.get("indicators") or {}).get("edges") or []
            )
            for edge in indicators:
                inner = (edge or {}).get("node") or {}
                try:
                    confidence = int(inner.get("confidence") or 0)
                except (TypeError, ValueError):
                    confidence = 0
                if confidence < MIN_CONFIDENCE:
                    continue
                if best and confidence <= (best.get("confidence") or 0):
                    continue
                best = {
                    "opencti_id":     inner.get("id") or observable_id,
                    "indicator_name": inner.get("name"),
                    "confidence":     confidence,
                    "raw":            {"observable": node, "indicator": inner},
                }
        return best

    def _lookup(self, value: str) -> dict | None:
        cached = self._cache_get(value)
        if cached is not None:
            if not cached:
                return None
            # Cached positive hits are stored as the already-picked
            # best indicator wrapped in a single-item list so a cache
            # hit still returns the richest match we've seen.
            return cached[0]

        client = self._ensure_client()
        if client is None:
            return None

        try:
            nodes = client.lookup(value)
        except requests.RequestException as exc:
            logger.warning("[ioc] opencti request failed value=%s err=%s", value, exc)
            # Don't cache transport errors -- retry next cycle.
            return None
        except Exception:
            logger.exception("[ioc] unexpected opencti lookup failure value=%s", value)
            return None

        best = self._best_indicator(nodes)
        self._cache_put(value, [best] if best else [])
        time.sleep(QUERY_DELAY_MS / 1000.0)
        return best

    # ----- writes --------------------------------------------------------

    def _insert_match(
        self,
        ctx: dict,
        ioc_type: str,
        ioc_value: str,
        match: dict,
    ) -> bool:
        import json as _json

        payload = {
            "tenant_id":        ctx.get("tenant_id"),
            "client_name":      ctx.get("client_name"),
            "ioc_type":         ioc_type,
            "ioc_value":        ioc_value,
            "opencti_id":       match.get("opencti_id"),
            "indicator_name":   match.get("indicator_name"),
            "confidence":       match.get("confidence"),
            "matched_event_id": ctx.get("event_id"),
            "raw_json":         _json.dumps(match.get("raw") or {}),
        }
        try:
            with self._db.conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO vector_ioc_matches (
                        tenant_id, client_name, ioc_type, ioc_value,
                        opencti_id, indicator_name, confidence,
                        matched_event_id, raw_json
                    ) VALUES (
                        %(tenant_id)s, %(client_name)s, %(ioc_type)s,
                        %(ioc_value)s, %(opencti_id)s, %(indicator_name)s,
                        %(confidence)s, %(matched_event_id)s, %(raw_json)s
                    )
                    ON CONFLICT (ioc_value, matched_event_id) DO NOTHING
                    RETURNING id
                    """,
                    payload,
                )
                row = cur.fetchone()
            self._db.conn.commit()
            return bool(row)
        except Exception:
            self._db.conn.rollback()
            logger.exception(
                "[ioc] failed to insert ioc match value=%s event=%s",
                ioc_value,
                ctx.get("event_id"),
            )
            return False

    def _escalate_watchlist(
        self,
        ctx: dict,
        ioc_type: str,
        ioc_value: str,
        match: dict,
    ) -> None:
        import json as _json

        trigger_details = {
            "ioc_type":       ioc_type,
            "ioc_value":      ioc_value,
            "indicator":      match.get("indicator_name"),
            "opencti_id":     match.get("opencti_id"),
            "confidence":     match.get("confidence"),
            "matched_event":  str(ctx.get("event_id")) if ctx.get("event_id") else None,
        }
        try:
            with self._db.conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO vector_watchlist (
                        tenant_id, client_name, user_email,
                        trigger_type, trigger_details,
                        expires_at, status
                    ) VALUES (
                        %s, %s, %s, %s, %s::jsonb, now() + %s, %s
                    )
                    """,
                    (
                        ctx.get("tenant_id"),
                        ctx.get("client_name"),
                        ctx.get("user_id"),
                        "ioc_match",
                        _json.dumps(trigger_details),
                        WATCHLIST_WINDOW,
                        "escalated",
                    ),
                )
            self._db.conn.commit()
        except Exception:
            self._db.conn.rollback()
            logger.exception(
                "[ioc] watchlist escalation failed value=%s",
                ioc_value,
            )

    # ----- main entrypoint -----------------------------------------------

    def poll_once(self) -> None:
        if not self._token:
            logger.info("[ioc] OPENCTI_TOKEN unset, skipping cycle")
            return

        iocs = self._collect_iocs()
        if not iocs:
            logger.info("[ioc] no recent IOCs to enrich")
            return

        # Break out IPs for progress logging so the log line cadence
        # matches the IP_BATCH_SIZE batching requirement in the spec.
        ip_values = [v for (t, v) in iocs.keys() if t in ("ipv4", "ipv6")]
        other_count = len(iocs) - len(ip_values)
        logger.info(
            "[ioc] enrichment cycle starting iocs=%d (ips=%d other=%d)",
            len(iocs),
            len(ip_values),
            other_count,
        )

        total_checked = 0
        total_matched = 0
        total_escalated = 0
        batch_index = 0

        for (ioc_type, ioc_value), ctx in iocs.items():
            total_checked += 1
            if ioc_type in ("ipv4", "ipv6") and total_checked % IP_BATCH_SIZE == 0:
                batch_index += 1
                logger.info(
                    "[ioc] IP batch %d checked=%d matched=%d",
                    batch_index,
                    total_checked,
                    total_matched,
                )

            match = self._lookup(ioc_value)
            if not match:
                continue
            confidence = int(match.get("confidence") or 0)
            if confidence < MIN_CONFIDENCE:
                continue

            inserted = self._insert_match(ctx, ioc_type, ioc_value, match)
            if inserted:
                total_matched += 1
                logger.info(
                    "[ioc] MATCH %s=%s confidence=%d indicator=%s",
                    ioc_type,
                    ioc_value,
                    confidence,
                    match.get("indicator_name"),
                )
            if confidence >= ESCALATE_CONFIDENCE:
                self._escalate_watchlist(ctx, ioc_type, ioc_value, match)
                total_escalated += 1

        logger.info(
            "[ioc] cycle complete checked=%d matched=%d escalated=%d "
            "cache_entries=%d",
            total_checked,
            total_matched,
            total_escalated,
            len(self._cache),
        )


def _walk_strings(obj: Any) -> Iterable[str]:
    """Yield every string embedded anywhere in ``obj``. Used so we
    can scrape SHA256 values out of opaque EDR raw_json blobs."""
    if isinstance(obj, str):
        yield obj
    elif isinstance(obj, dict):
        for v in obj.values():
            yield from _walk_strings(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _walk_strings(v)
