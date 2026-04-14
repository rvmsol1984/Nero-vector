"""BaselineEngine + ScoringEngine for NERO Vector.

Both run in the same poll loop as the rest of the ingestors via
vector_ingest.main.build_ingestors. BaselineEngine refreshes
per-user "known good" state every 60 minutes; ScoringEngine runs
every 5 minutes, promotes high-confidence IOC / Defender hits to
incidents immediately, and otherwise sums a set of signal weights
per user to decide whether to escalate their watchlist pin or
stand up a new incident.

Neither class bypasses the existing Database helpers — we just
reuse ``db.fetch_all`` / ``db.fetch_one`` / ``db.conn`` for the
write paths so the session timezone pinning and connection
lifecycle are inherited from the UAL ingestor.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from vector_ingest.db import Database

logger = logging.getLogger(__name__)


BASELINE_POLL_INTERVAL = timedelta(minutes=60)
SCORING_POLL_INTERVAL  = timedelta(minutes=5)

# Lookback windows used by each engine.
BASELINE_MIN_HISTORY = timedelta(days=7)
BASELINE_WINDOW      = timedelta(days=14)
SCORING_WINDOW       = timedelta(minutes=30)
IMMEDIATE_WINDOW     = timedelta(minutes=5)

# ---- signal weights + labels -----------------------------------------------

SIGNAL_WEIGHTS = {
    "unknown_ip":           15,
    "unknown_country":      35,
    "inbox_rule_change":    45,
    "watchlist_active":     50,
    "threatlocker_deny":    30,
    "defender_medium_alert": 35,
}

SIGNAL_LABELS = {
    "unknown_ip":            "Sign-in from unknown IP",
    "unknown_country":       "Sign-in from unknown country",
    "inbox_rule_change":     "Inbox rule modified",
    "watchlist_active":      "Active watchlist entry",
    "threatlocker_deny":     "ThreatLocker policy deny",
    "defender_medium_alert": "Defender medium alert",
}

SCORE_INCIDENT_THRESHOLD = 80
SCORE_ESCALATE_THRESHOLD = 50


# ---------------------------------------------------------------------------
# helpers shared across the two engines
# ---------------------------------------------------------------------------

def _table_exists(db: Database, name: str) -> bool:
    try:
        row = db.fetch_one(
            "SELECT to_regclass(%s) IS NOT NULL AS exists", (name,)
        )
        return bool(row and row.get("exists"))
    except Exception:
        return False


def _column_type(db: Database, table: str, column: str) -> str:
    try:
        row = db.fetch_one(
            """
            SELECT data_type
            FROM information_schema.columns
            WHERE table_name = %s AND column_name = %s
            """,
            (table, column),
        )
        return (row.get("data_type") or "").lower() if row else ""
    except Exception:
        return ""


def _parse_json_maybe(value: Any) -> Any:
    """psycopg2 usually decodes JSONB for us, but raw rows fetched
    through some drivers come back as strings. Coerce either shape to
    a Python object."""
    if value is None or isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (TypeError, ValueError):
            return None
    return None


def _logged_on_upn(logged_on_users: Any) -> str | None:
    """Pull the first accountName / userPrincipalName out of a
    Defender logged_on_users array."""
    parsed = _parse_json_maybe(logged_on_users) or []
    if not isinstance(parsed, list) or not parsed:
        return None
    first = parsed[0]
    if not isinstance(first, dict):
        return None
    return (
        first.get("accountName")
        or first.get("userPrincipalName")
        or first.get("upn")
    )


# ---------------------------------------------------------------------------
# BaselineEngine -- refreshes per-user known IPs / countries / devices
# ---------------------------------------------------------------------------

class BaselineEngine:
    """Every 60 minutes, rebuild a row in vector_user_baselines for every
    user that has at least 7 days of history. Each row captures the last
    14 days of distinct client IPs, distinct UserLoggedIn Country values,
    and distinct DeviceName values so ScoringEngine can tell what's
    "known-good" for that user."""

    def __init__(self, db: Database) -> None:
        self.db = db
        self._last_poll: datetime = datetime.fromtimestamp(0, tz=timezone.utc)
        # Cached column-kind for known_ips / login_countries /
        # known_devices: "jsonb" => json.dumps(list), else Python list.
        self._array_encoding: str | None = None

    @property
    def tenant_id(self) -> str:
        return "(all)"

    @property
    def client_name(self) -> str:
        return "baseline-engine"

    # ------------------------------------------------------------------ cadence
    def poll_once(self) -> None:
        now = datetime.now(timezone.utc)
        if now - self._last_poll < BASELINE_POLL_INTERVAL:
            return
        self._last_poll = now
        try:
            self.build_baselines()
        except Exception:
            logger.exception("[baseline] cycle crashed")

    # ------------------------------------------------------------------ build
    def build_baselines(self) -> None:
        users = self.db.fetch_all(
            """
            SELECT
                user_id,
                MAX(tenant_id)   AS tenant_id,
                MAX(client_name) AS client_name,
                MIN(timestamp)   AS first_seen,
                MAX(timestamp)   AS last_seen
            FROM vector_events
            WHERE user_id IS NOT NULL
            GROUP BY user_id
            HAVING MAX(timestamp) - MIN(timestamp) >= INTERVAL '7 days'
            LIMIT 5000
            """
        )
        built = 0
        for user in users or []:
            user_id = user.get("user_id")
            if not user_id:
                continue
            tenant_id = user.get("tenant_id")
            client_name = user.get("client_name")

            known_ips = self._distinct(
                """
                SELECT DISTINCT client_ip AS val
                FROM vector_events
                WHERE user_id = %s
                  AND timestamp > now() - INTERVAL '14 days'
                  AND client_ip IS NOT NULL
                """,
                (user_id,),
            )
            login_countries = self._distinct(
                """
                SELECT DISTINCT raw_json->>'Country' AS val
                FROM vector_events
                WHERE user_id = %s
                  AND event_type = 'UserLoggedIn'
                  AND timestamp > now() - INTERVAL '14 days'
                  AND raw_json ? 'Country'
                """,
                (user_id,),
            )
            known_devices = self._distinct(
                """
                SELECT DISTINCT raw_json->>'DeviceName' AS val
                FROM vector_events
                WHERE user_id = %s
                  AND timestamp > now() - INTERVAL '14 days'
                  AND raw_json ? 'DeviceName'
                """,
                (user_id,),
            )

            self._upsert(
                user_id=user_id,
                tenant_id=tenant_id,
                client_name=client_name,
                known_ips=known_ips,
                login_countries=login_countries,
                known_devices=known_devices,
            )
            built += 1

        logger.info(
            "[baseline] cycle complete candidates=%d built=%d",
            len(users or []),
            built,
        )

    def _distinct(self, sql: str, params: tuple) -> list[str]:
        rows = self.db.fetch_all(sql, params) or []
        return [str(r["val"]) for r in rows if r.get("val")]

    # ------------------------------------------------------------------ encoding
    def _encoding(self) -> str:
        if self._array_encoding is not None:
            return self._array_encoding
        col_type = _column_type(self.db, "vector_user_baselines", "known_ips")
        # jsonb -> json.dumps(list); everything else (text[], varchar[], ...)
        # -> pass a Python list and let psycopg2 adapt it to PG arrays.
        self._array_encoding = "jsonb" if "json" in col_type else "array"
        logger.info(
            "[baseline] known_ips column encoding=%s", self._array_encoding
        )
        return self._array_encoding

    def _encode(self, values: list[str]) -> Any:
        return json.dumps(values) if self._encoding() == "jsonb" else list(values)

    # ------------------------------------------------------------------ upsert
    def _upsert(
        self,
        user_id: str,
        tenant_id: str | None,
        client_name: str | None,
        known_ips: list[str],
        login_countries: list[str],
        known_devices: list[str],
    ) -> None:
        params = (
            user_id,
            tenant_id,
            client_name,
            self._encode(known_ips),
            self._encode(login_countries),
            self._encode(known_devices),
        )
        try:
            with self.db.conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO vector_user_baselines (
                        user_id, tenant_id, client_name,
                        known_ips, login_countries, known_devices,
                        updated_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, now()
                    )
                    ON CONFLICT (user_id) DO UPDATE SET
                        tenant_id       = EXCLUDED.tenant_id,
                        client_name     = EXCLUDED.client_name,
                        known_ips       = EXCLUDED.known_ips,
                        login_countries = EXCLUDED.login_countries,
                        known_devices   = EXCLUDED.known_devices,
                        updated_at      = now()
                    """,
                    params,
                )
            self.db.conn.commit()
        except Exception:
            logger.exception("[baseline] upsert failed for %s", user_id)
            try:
                self.db.conn.rollback()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# ScoringEngine -- correlates recent signals and writes incidents
# ---------------------------------------------------------------------------

class ScoringEngine:
    """Every 5 minutes: promote high-confidence IOC + Defender hits to
    incidents immediately, then walk every user that was active in the
    last 30 minutes, sum their signal weights, and either stand up a
    new incident (>= 80) or escalate their watchlist pin (>= 50)."""

    def __init__(self, db: Database) -> None:
        self.db = db
        self._last_poll: datetime = datetime.fromtimestamp(0, tz=timezone.utc)
        # Detected once per process.
        self._watchlist_flavor: str | None = None
        self._has_threatlocker: bool | None = None

    @property
    def tenant_id(self) -> str:
        return "(all)"

    @property
    def client_name(self) -> str:
        return "scoring-engine"

    # ------------------------------------------------------------------ cadence
    def poll_once(self) -> None:
        now = datetime.now(timezone.utc)
        if now - self._last_poll < SCORING_POLL_INTERVAL:
            return
        self._last_poll = now
        try:
            self.run_scoring_cycle()
        except Exception:
            logger.exception("[scoring] cycle crashed")

    # ------------------------------------------------------------------ lazy schema sniffers
    def _get_watchlist_flavor(self) -> str:
        if self._watchlist_flavor is not None:
            return self._watchlist_flavor
        try:
            rows = self.db.fetch_all(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'vector_watchlist'
                """
            )
            cols = {r["column_name"] for r in (rows or [])}
        except Exception:
            cols = set()
        if "trigger_type" in cols and "user_email" in cols:
            self._watchlist_flavor = "v2"
        elif "recipient" in cols:
            self._watchlist_flavor = "v1"
        else:
            self._watchlist_flavor = "none"
        return self._watchlist_flavor

    def _threatlocker_available(self) -> bool:
        if self._has_threatlocker is None:
            self._has_threatlocker = _table_exists(self.db, "vector_threatlocker_events")
        return self._has_threatlocker

    # ------------------------------------------------------------------ cycle
    def run_scoring_cycle(self) -> None:
        immediate_count = self._run_immediate_incidents()
        incident_count, escalated_count, scored_count = self._run_score_based()
        logger.info(
            "[scoring] cycle complete scored=%d immediate_incidents=%d "
            "score_incidents=%d escalated=%d",
            scored_count,
            immediate_count,
            incident_count,
            escalated_count,
        )

    # ------------------------------------------------------------------ (1) immediate incidents
    def _run_immediate_incidents(self) -> int:
        created = 0

        # IOC matches >= 75 in the last 5 min.
        try:
            ioc_rows = self.db.fetch_all(
                """
                SELECT
                    m.id::text,
                    m.tenant_id,
                    m.client_name,
                    m.ioc_type,
                    m.ioc_value,
                    m.opencti_id,
                    m.indicator_name,
                    m.confidence,
                    m.matched_event_id::text,
                    m.matched_at,
                    ve.user_id
                FROM vector_ioc_matches m
                LEFT JOIN vector_events ve ON ve.id = m.matched_event_id
                WHERE m.confidence >= 75
                  AND m.matched_at > now() - INTERVAL '5 minutes'
                """
            )
        except Exception:
            logger.exception("[scoring] ioc immediate query failed")
            ioc_rows = []

        for r in ioc_rows or []:
            user_id = r.get("user_id") or (
                r.get("ioc_value") if r.get("ioc_type") == "email-addr" else None
            )
            if not user_id:
                continue
            confidence = int(r.get("confidence") or 0)
            severity = "critical" if confidence >= 90 else "high"
            label = (
                r.get("indicator_name")
                or r.get("ioc_value")
                or "OpenCTI indicator"
            )
            signal = {
                "name":  "ioc_match",
                "label": f"IOC match: {label}",
                "ioc_type":       r.get("ioc_type"),
                "ioc_value":      r.get("ioc_value"),
                "confidence":     confidence,
                "opencti_id":     r.get("opencti_id"),
                "indicator_name": r.get("indicator_name"),
            }
            if self.create_incident(
                user_id=user_id,
                tenant_id=r.get("tenant_id"),
                client_name=r.get("client_name"),
                score=min(100, 50 + confidence // 2),
                signals=[signal],
                severity=severity,
            ):
                created += 1

        # Defender alerts with severity High or Critical in the last 5 min.
        try:
            defender_rows = self.db.fetch_all(
                """
                SELECT
                    id,
                    tenant_id,
                    client_name,
                    severity,
                    title,
                    threat_name,
                    machine_id,
                    computer_name,
                    logged_on_users,
                    alert_creation_time
                FROM vector_defender_alerts
                WHERE LOWER(severity) IN ('high', 'critical')
                  AND alert_creation_time > now() - INTERVAL '5 minutes'
                """
            )
        except Exception:
            logger.exception("[scoring] defender immediate query failed")
            defender_rows = []

        for r in defender_rows or []:
            user_id = _logged_on_upn(r.get("logged_on_users"))
            if not user_id:
                continue
            sev = str(r.get("severity") or "").lower()
            severity = "critical" if sev == "critical" else "high"
            score = 95 if sev == "critical" else 85
            label = r.get("title") or r.get("threat_name") or "Defender alert"
            signal = {
                "name":     "defender_alert",
                "label":    f"Defender alert: {label}",
                "severity": r.get("severity"),
                "machine":  r.get("computer_name"),
            }
            if self.create_incident(
                user_id=user_id,
                tenant_id=r.get("tenant_id"),
                client_name=r.get("client_name"),
                score=score,
                signals=[signal],
                severity=severity,
            ):
                created += 1

        return created

    # ------------------------------------------------------------------ (2) score-based
    def _run_score_based(self) -> tuple[int, int, int]:
        try:
            active_users = self.db.fetch_all(
                """
                SELECT DISTINCT
                    user_id,
                    MAX(tenant_id)   AS tenant_id,
                    MAX(client_name) AS client_name
                FROM vector_events
                WHERE timestamp > now() - INTERVAL '30 minutes'
                  AND user_id IS NOT NULL
                GROUP BY user_id
                LIMIT 2000
                """
            )
        except Exception:
            logger.exception("[scoring] active-users query failed")
            return (0, 0, 0)

        incidents = 0
        escalated = 0
        for user in active_users or []:
            user_id = user.get("user_id")
            if not user_id:
                continue
            tenant_id = user.get("tenant_id")
            client_name = user.get("client_name")

            score, signals = self.score_user(user_id)
            if score >= SCORE_INCIDENT_THRESHOLD:
                if self.create_incident(
                    user_id=user_id,
                    tenant_id=tenant_id,
                    client_name=client_name,
                    score=score,
                    signals=signals,
                    severity="high",
                ):
                    incidents += 1
            elif score >= SCORE_ESCALATE_THRESHOLD:
                if self._escalate_watchlist(user_id):
                    escalated += 1

        return incidents, escalated, len(active_users or [])

    def score_user(self, user_id: str) -> tuple[int, list[dict]]:
        score = 0
        signals: list[dict] = []

        # Load baseline.
        baseline = self.db.fetch_one(
            """
            SELECT known_ips, login_countries, known_devices
            FROM vector_user_baselines
            WHERE user_id = %s
            """,
            (user_id,),
        ) or {}
        known_ips = set(baseline.get("known_ips") or [])
        login_countries = set(baseline.get("login_countries") or [])

        # Recent events.
        try:
            events = self.db.fetch_all(
                """
                SELECT event_type, client_ip, raw_json, timestamp
                FROM vector_events
                WHERE user_id = %s
                  AND timestamp > now() - INTERVAL '30 minutes'
                """,
                (user_id,),
            )
        except Exception:
            logger.exception("[scoring] score_user events query failed for %s", user_id)
            events = []

        seen_unknown_ip = False
        seen_unknown_country = False
        seen_inbox_rule = False

        for e in events or []:
            event_type = e.get("event_type")
            raw = _parse_json_maybe(e.get("raw_json")) or {}

            if event_type == "UserLoggedIn":
                ip = (e.get("client_ip") or "").strip()
                if ip and known_ips and ip not in known_ips and not seen_unknown_ip:
                    score += SIGNAL_WEIGHTS["unknown_ip"]
                    signals.append(
                        {
                            "name":  "unknown_ip",
                            "label": SIGNAL_LABELS["unknown_ip"],
                            "value": ip,
                        }
                    )
                    seen_unknown_ip = True
                country = raw.get("Country") if isinstance(raw, dict) else None
                if (
                    country
                    and login_countries
                    and country not in login_countries
                    and not seen_unknown_country
                ):
                    score += SIGNAL_WEIGHTS["unknown_country"]
                    signals.append(
                        {
                            "name":  "unknown_country",
                            "label": SIGNAL_LABELS["unknown_country"],
                            "value": country,
                        }
                    )
                    seen_unknown_country = True

            if event_type == "UpdateInboxRules" and not seen_inbox_rule:
                score += SIGNAL_WEIGHTS["inbox_rule_change"]
                signals.append(
                    {
                        "name":  "inbox_rule_change",
                        "label": SIGNAL_LABELS["inbox_rule_change"],
                    }
                )
                seen_inbox_rule = True

        # Active watchlist pin.
        if self._user_has_active_watchlist(user_id):
            score += SIGNAL_WEIGHTS["watchlist_active"]
            signals.append(
                {
                    "name":  "watchlist_active",
                    "label": SIGNAL_LABELS["watchlist_active"],
                }
            )

        # ThreatLocker Deny in the last 30 min.
        if self._threatlocker_available():
            try:
                tl = self.db.fetch_one(
                    """
                    SELECT id FROM vector_threatlocker_events
                    WHERE username ILIKE %s
                      AND action ILIKE 'deny%%'
                      AND event_time > now() - INTERVAL '30 minutes'
                    LIMIT 1
                    """,
                    (user_id,),
                )
            except Exception:
                logger.exception(
                    "[scoring] threatlocker query failed for %s", user_id
                )
                self.db.conn.rollback()
                tl = None
            if tl:
                score += SIGNAL_WEIGHTS["threatlocker_deny"]
                signals.append(
                    {
                        "name":  "threatlocker_deny",
                        "label": SIGNAL_LABELS["threatlocker_deny"],
                    }
                )

        # Defender Medium alert against this user's logged_on_users.
        try:
            defender = self.db.fetch_one(
                """
                SELECT id FROM vector_defender_alerts
                WHERE LOWER(severity) = 'medium'
                  AND alert_creation_time > now() - INTERVAL '30 minutes'
                  AND logged_on_users::text ILIKE '%%' || %s || '%%'
                LIMIT 1
                """,
                (user_id,),
            )
        except Exception:
            logger.exception(
                "[scoring] defender medium query failed for %s", user_id
            )
            defender = None
        if defender:
            score += SIGNAL_WEIGHTS["defender_medium_alert"]
            signals.append(
                {
                    "name":  "defender_medium_alert",
                    "label": SIGNAL_LABELS["defender_medium_alert"],
                }
            )

        return min(score, 100), signals

    # ------------------------------------------------------------------ watchlist
    def _user_has_active_watchlist(self, user_id: str) -> bool:
        flavor = self._get_watchlist_flavor()
        if flavor == "none":
            return False
        try:
            if flavor == "v2":
                row = self.db.fetch_one(
                    """
                    SELECT id FROM vector_watchlist
                    WHERE user_email = %s AND status = 'active'
                    LIMIT 1
                    """,
                    (user_id,),
                )
            else:  # v1
                row = self.db.fetch_one(
                    """
                    SELECT id FROM vector_watchlist
                    WHERE recipient = %s
                    LIMIT 1
                    """,
                    (user_id,),
                )
        except Exception:
            logger.exception(
                "[scoring] watchlist probe failed for %s", user_id
            )
            return False
        return bool(row)

    def _escalate_watchlist(self, user_id: str) -> bool:
        flavor = self._get_watchlist_flavor()
        if flavor == "none":
            return False
        try:
            with self.db.conn.cursor() as cur:
                if flavor == "v2":
                    cur.execute(
                        """
                        UPDATE vector_watchlist
                        SET status = 'escalated'
                        WHERE user_email = %s
                          AND status = 'active'
                        """,
                        (user_id,),
                    )
                else:
                    # v1 schema has no "status" column so this is a no-op
                    # from the escalate-path's perspective. We still
                    # return False so the caller's counter stays honest.
                    return False
                updated = cur.rowcount
            self.db.conn.commit()
            if updated:
                logger.info(
                    "[scoring] escalated watchlist entries for %s count=%d",
                    user_id,
                    updated,
                )
            return updated > 0
        except Exception:
            logger.exception(
                "[scoring] _escalate_watchlist failed for %s", user_id
            )
            try:
                self.db.conn.rollback()
            except Exception:
                pass
            return False

    # ------------------------------------------------------------------ create_incident
    def create_incident(
        self,
        user_id: str,
        tenant_id: str | None,
        client_name: str | None,
        score: int,
        signals: list[dict],
        severity: str = "high",
    ) -> bool:
        entity_key = f"{tenant_id}::{user_id}" if tenant_id else user_id
        top = signals[0] if signals else None
        title = (
            (top or {}).get("label")
            or f"Risk score {score} for {user_id}"
        )
        plural = "s" if len(signals) != 1 else ""
        summary = f"Score {score} - {len(signals)} signal{plural} detected"
        evidence = {
            "score":   int(score),
            "signals": signals,
        }

        try:
            with self.db.conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO vector_incidents (
                        tenant_id, client_name, user_id, entity_key,
                        severity, score, title, summary, evidence,
                        confirmed_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, now()
                    )
                    ON CONFLICT DO NOTHING
                    RETURNING id
                    """,
                    (
                        tenant_id,
                        client_name,
                        user_id,
                        entity_key,
                        severity,
                        int(score),
                        title,
                        summary,
                        json.dumps(evidence),
                    ),
                )
                row = cur.fetchone()
            self.db.conn.commit()
            if row:
                logger.info(
                    "[scoring] incident created user=%s score=%d severity=%s "
                    "signals=%d",
                    user_id,
                    score,
                    severity,
                    len(signals),
                )
                return True
            return False
        except Exception:
            logger.exception(
                "[scoring] create_incident failed for %s score=%s",
                user_id,
                score,
            )
            try:
                self.db.conn.rollback()
            except Exception:
                pass
            return False
