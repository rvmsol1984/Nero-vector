"""Phase 2 correlation scoring scaffold.

This module is the framework the named correlation rules hook into.
It owns three things:

    * ``RuleResult``        -- the dataclass each rule returns
    * ``CorrelationRule``   -- the abstract base class rules subclass
    * ``ScoringEngine``     -- the worker that loads active users,
                               runs every registered rule against
                               them, and writes a vector_incidents
                               row when the aggregate score clears
                               the configured threshold

No concrete rules live in this file yet. Rules are added via
``ScoringEngine.register_rule()`` from whichever module is driving
the cycle (typically ``vector_ingest/main.py`` once rule modules
are written). An engine with zero rules is a no-op per cycle and
logs a single "no rules registered" line so operators can see that
Phase 2 is wired but inert.

Expected database schema (from ``migrations/009_incidents.sql``):

    vector_user_baselines   -- input, read during each cycle
    vector_events           -- input, read during each cycle
    vector_incidents        -- output, one row per confirmed incident
"""

from __future__ import annotations

import ipaddress
import json
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

import requests

from vector_ingest.db import Database

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# tunables
# ---------------------------------------------------------------------------

# Sliding window each cycle evaluates. Rules only see events inside
# this window. Matching the main ingest loop's 5-minute cadence with
# a 30-minute window gives each event six chances to participate in
# a correlation before it ages out.
SCORING_WINDOW = timedelta(minutes=30)

# Aggregate score at which a user's evaluation produces an incident.
# Rules contribute positive integers via ``RuleResult.score_delta``;
# the total across all fired rules is compared against this value.
INCIDENT_THRESHOLD = 80

# Two confirmed incidents of the same type for the same user within
# this window collapse into one. Prevents a single ongoing anomaly
# from producing a fresh incident every cycle while it's still
# active.
DEDUP_WINDOW = timedelta(hours=4)

# Incident type stamped on rows this engine writes. Each future
# correlation rule can override by constructing a richer incident
# shape, but the scaffold itself always uses this.
DEFAULT_INCIDENT_TYPE = "score_based"


# ---------------------------------------------------------------------------
# RuleResult
# ---------------------------------------------------------------------------

@dataclass
class RuleResult:
    """The value a ``CorrelationRule.evaluate()`` call returns.

    Rules must return a ``RuleResult`` on every call, including
    calls where they didn't fire -- return ``fired=False`` +
    ``score_delta=0`` for the "no signal" case. This makes cycle
    logging and future multi-rule scoring explainable.

    Attributes
    ----------
    rule_name
        Stable identifier for the rule. ``CorrelationRule``
        populates this automatically from its class name when the
        engine calls ``evaluate``; subclasses can still set it
        explicitly if they want a prettier label.
    score_delta
        Integer points this rule contributes to the user's
        aggregate anomaly score when fired. Must be >= 0. Ignored
        when ``fired`` is False.
    fired
        True if the rule detected an anomaly worth scoring.
    evidence
        Free-form JSON-serialisable dict describing *why* the rule
        fired. Goes straight into the ``evidence`` JSONB column of
        ``vector_incidents`` so the Incidents UI can render it on
        the detail panel.
    """

    rule_name: str
    score_delta: int
    fired: bool
    evidence: dict = field(default_factory=dict)

    def as_signal(self) -> dict:
        """Serialise to the ``evidence`` column row shape."""
        return {
            "rule":     self.rule_name,
            "score":    int(self.score_delta or 0),
            "fired":    bool(self.fired),
            "evidence": dict(self.evidence or {}),
        }


# ---------------------------------------------------------------------------
# CorrelationRule
# ---------------------------------------------------------------------------

class CorrelationRule(ABC):
    """Abstract base for Phase 2 correlation rules.

    Subclass this and implement ``evaluate`` -- the engine gives
    each rule the user's recent events plus their baseline profile
    and expects a ``RuleResult`` back. Rules that need to reach
    beyond the events list (e.g. to query vector_events with a
    rule-specific lookback window) can use ``self._db`` which the
    engine populates via ``bind_db`` when the rule is registered.

    Subclasses can override ``name`` as a class attribute if they
    want a friendlier label than the class name.
    """

    #: Optional friendlier name. When ``None``, the class name is
    #: used instead via the ``rule_name`` property.
    name: str | None = None

    def __init__(self) -> None:
        # Populated by ScoringEngine.register_rule(). Rules that
        # don't need DB access can leave this alone; rules that do
        # should check for None before issuing queries so they
        # degrade to fired=False when unbound (e.g. in unit tests).
        self._db: Database | None = None

    def bind_db(self, db: Database) -> None:
        """Called by the engine during registration to wire the
        shared DB handle into the rule. Idempotent."""
        self._db = db

    @property
    def rule_name(self) -> str:
        return self.name or self.__class__.__name__

    @abstractmethod
    def evaluate(
        self,
        events: list[dict],
        user_profile: dict,
    ) -> RuleResult:
        """Evaluate this rule against ``events`` + ``user_profile``.

        Parameters
        ----------
        events
            The user's events inside ``SCORING_WINDOW``, ordered
            newest-first. Each is a dict with ``id``, ``tenant_id``,
            ``client_name``, ``user_id``, ``entity_key``,
            ``event_type``, ``workload``, ``result_status``,
            ``client_ip``, ``timestamp`` and ``raw_json``.
        user_profile
            The user's baseline profile read from
            ``vector_user_baselines`` -- ``login_hours``,
            ``login_countries``, ``login_asns``, ``known_devices``,
            ``known_ips``, ``avg_daily_events``, ``avg_daily_logins``,
            ``baseline_days``. Empty dict when no baseline has
            been computed yet.

        Returns
        -------
        RuleResult
            Must always be returned, even when the rule doesn't
            fire. The engine tolerates rules that raise by logging
            and continuing, but rules are expected to handle their
            own errors internally.
        """


# ---------------------------------------------------------------------------
# ScoringEngine
# ---------------------------------------------------------------------------

class ScoringEngine:
    """Phase 2 worker that turns correlated signals into incidents.

    The engine is a standard vector-ingest poller: ``poll_once`` is
    called by the main loop every cycle, it runs ``run_scoring_cycle``
    once, and returns. All database I/O lives on this class so rule
    classes stay pure.

    Usage::

        engine = ScoringEngine(db)
        engine.register_rule(SomeRule())
        engine.register_rule(AnotherRule())
        engine.poll_once()          # main loop calls this repeatedly
    """

    # These two attributes are read by vector_ingest/main.py's
    # structured logging so every worker in the ingestors list has
    # consistent "tenant_id" / "client_name" fields on its log
    # lines. The engine is tenant-global so we use sentinel values.
    tenant_id = "*"
    client_name = "global"

    def __init__(
        self,
        db: Database,
        rules: list[CorrelationRule] | None = None,
    ) -> None:
        self._db = db
        self._rules: list[CorrelationRule] = []
        if rules is None:
            # Default rule set. Future rules should be appended here
            # so a plain ``ScoringEngine(db)`` from main.py picks up
            # every Phase 2 detection automatically.
            rules = [
                NewCountryLoginRule(),
                OffHoursLoginRule(),
                HighVolumeFileAccessRule(),
            ]
        for r in rules:
            self.register_rule(r)

    # ----- rule registration ---------------------------------------------

    def register_rule(self, rule: CorrelationRule) -> None:
        """Append a rule to the evaluation list. Safe to call at
        any time between constructor and first ``poll_once``. The
        engine's DB handle is wired into the rule at registration
        time via ``bind_db`` so rules that need to issue their own
        queries can reach Postgres without threading a connection
        through ``evaluate``."""
        if not isinstance(rule, CorrelationRule):
            raise TypeError(
                "ScoringEngine.register_rule() expected a CorrelationRule "
                f"instance, got {type(rule).__name__}"
            )
        rule.bind_db(self._db)
        self._rules.append(rule)
        logger.info(
            "[scoring] registered rule", extra={"rule": rule.rule_name},
        )

    @property
    def rules(self) -> tuple[CorrelationRule, ...]:
        """Read-only view of the registered rules."""
        return tuple(self._rules)

    # ----- poll entrypoint -----------------------------------------------

    def poll_once(self) -> None:
        """Main-loop entrypoint. Runs one full scoring cycle and
        swallows any unexpected exception so a broken rule never
        takes the ingest loop down."""
        try:
            self.run_scoring_cycle()
        except Exception:
            logger.exception("[scoring] cycle crashed")

    def run_scoring_cycle(self) -> None:
        """Load every user active inside ``SCORING_WINDOW``, run
        every registered rule, and emit a ``vector_incidents`` row
        for anyone whose aggregate score clears
        ``INCIDENT_THRESHOLD``."""
        if not self._rules:
            logger.info("[scoring] no rules registered, skipping cycle")
            return

        active = self._active_users()
        if not active:
            logger.info("[scoring] no active users in scoring window")
            return

        evaluated = 0
        fired_any = 0
        incidents_created = 0

        for tenant_id, user_id in active:
            try:
                events = self._load_events(tenant_id, user_id)
                profile = self._load_user_profile(tenant_id, user_id)
                results, total_score = self._evaluate_user(events, profile)
            except Exception:
                logger.exception(
                    "[scoring] failed to evaluate user",
                    extra={"tenant_id": tenant_id, "user_id": user_id},
                )
                continue

            evaluated += 1
            if any(r.fired for r in results):
                fired_any += 1

            if total_score >= INCIDENT_THRESHOLD:
                created = self._create_incident(
                    tenant_id=tenant_id,
                    user_id=user_id,
                    results=results,
                    total_score=total_score,
                    events=events,
                )
                if created:
                    incidents_created += 1

        logger.info(
            "[scoring] cycle complete evaluated=%d fired_any=%d incidents=%d",
            evaluated,
            fired_any,
            incidents_created,
        )

    # ----- per-user evaluation -------------------------------------------

    def _evaluate_user(
        self,
        events: list[dict],
        profile: dict,
    ) -> tuple[list[RuleResult], int]:
        """Run every registered rule against one user's window.

        Returns ``(results, total_score)`` where ``results`` is the
        list of ``RuleResult`` values from rules that returned
        cleanly (a rule that raised is skipped and logged) and
        ``total_score`` is the sum of ``score_delta`` over all
        fired rules.
        """
        results: list[RuleResult] = []
        total = 0

        for rule in self._rules:
            try:
                result = rule.evaluate(events, profile)
            except Exception:
                logger.exception(
                    "[scoring] rule raised",
                    extra={"rule": rule.rule_name},
                )
                continue

            if not isinstance(result, RuleResult):
                logger.warning(
                    "[scoring] rule returned non-RuleResult, ignoring",
                    extra={
                        "rule":    rule.rule_name,
                        "got":     type(result).__name__,
                    },
                )
                continue

            # Force rule_name to match the class the engine actually
            # called; the rule can still override via the ``name``
            # class attribute but an unset rule_name is surfaced
            # consistently here.
            if not result.rule_name:
                result.rule_name = rule.rule_name

            results.append(result)
            if result.fired:
                total += max(0, int(result.score_delta or 0))

        return results, total

    # ----- data loading ---------------------------------------------------

    def _active_users(self) -> list[tuple[str, str]]:
        """Distinct (tenant_id, user_id) pairs with activity in the
        last ``SCORING_WINDOW``."""
        with self._db.conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT tenant_id, user_id
                FROM vector_events
                WHERE user_id IS NOT NULL
                  AND timestamp > NOW() - %s
                """,
                (SCORING_WINDOW,),
            )
            return [(row[0], row[1]) for row in cur.fetchall()]

    def _load_events(
        self,
        tenant_id: str,
        user_id: str,
    ) -> list[dict]:
        """Recent events for one user, ordered newest-first."""
        with self._db.conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, tenant_id, client_name, user_id, entity_key,
                       event_type, workload, result_status, client_ip,
                       timestamp, raw_json
                FROM vector_events
                WHERE tenant_id = %s
                  AND user_id = %s
                  AND timestamp > NOW() - %s
                ORDER BY timestamp DESC
                """,
                (tenant_id, user_id, SCORING_WINDOW),
            )
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    def _load_user_profile(
        self,
        tenant_id: str,
        user_id: str,
    ) -> dict:
        """Baseline profile for one user, or ``{}`` if none exists.

        Missing baselines aren't fatal -- rules are expected to
        handle an empty profile by either not firing or returning
        a lower-confidence result. Returning an empty dict rather
        than ``None`` keeps every rule's ``user_profile`` access
        pattern uniform.
        """
        with self._db.conn.cursor() as cur:
            cur.execute(
                """
                SELECT login_hours, login_countries, login_asns,
                       known_devices, known_ips,
                       avg_daily_events, avg_daily_logins, baseline_days
                FROM vector_user_baselines
                WHERE tenant_id = %s AND user_id = %s
                """,
                (tenant_id, user_id),
            )
            row = cur.fetchone()
        if row is None:
            return {}
        return {
            "login_hours":      row[0] or {},
            "login_countries":  row[1] or {},
            "login_asns":       row[2] or {},
            "known_devices":    row[3] or [],
            "known_ips":        row[4] or [],
            "avg_daily_events": float(row[5] or 0),
            "avg_daily_logins": float(row[6] or 0),
            "baseline_days":    int(row[7] or 0),
        }

    # ----- incident emission ---------------------------------------------

    def _create_incident(
        self,
        tenant_id: str,
        user_id: str,
        results: list[RuleResult],
        total_score: int,
        events: list[dict],
    ) -> bool:
        """Insert a ``vector_incidents`` row for this evaluation.

        Skips the insert (returns False) if an incident of type
        ``DEFAULT_INCIDENT_TYPE`` already exists for this user
        inside the ``DEDUP_WINDOW``.
        """
        if self._incident_exists(user_id, DEFAULT_INCIDENT_TYPE):
            logger.info(
                "[scoring] dedup skip",
                extra={
                    "user_id":       user_id,
                    "incident_type": DEFAULT_INCIDENT_TYPE,
                },
            )
            return False

        fired = [r for r in results if r.fired]
        severity = self._severity_for(total_score)
        client_name = self._client_name_for(events, tenant_id)
        entity_key = f"{tenant_id}::{user_id}"
        first_seen, last_seen = self._timespan(events)
        dwell_minutes = self._dwell_minutes(first_seen, last_seen)

        title = self._title_for(user_id, fired, total_score)
        summary = self._summary_for(fired, total_score)

        evidence_payload = [r.as_signal() for r in fired]
        raw_signals_payload = {
            "rule_count":    len(self._rules),
            "fired_count":   len(fired),
            "total_score":   int(total_score),
            "window_minutes": int(SCORING_WINDOW.total_seconds() // 60),
            "threshold":     INCIDENT_THRESHOLD,
        }

        try:
            with self._db.conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO vector_incidents (
                        tenant_id, client_name, user_id, entity_key,
                        incident_type, severity, status, score,
                        title, summary, patient_zero, dwell_time_minutes,
                        first_seen, last_seen, confirmed_at,
                        evidence, raw_signals
                    ) VALUES (
                        %s, %s, %s, %s,
                        %s, %s, 'open', %s,
                        %s, %s, %s, %s,
                        %s, %s, NOW(),
                        %s::jsonb, %s::jsonb
                    )
                    RETURNING id::text
                    """,
                    (
                        tenant_id, client_name, user_id, entity_key,
                        DEFAULT_INCIDENT_TYPE, severity, int(total_score),
                        title, summary, user_id, dwell_minutes,
                        first_seen, last_seen,
                        json.dumps(evidence_payload),
                        json.dumps(raw_signals_payload),
                    ),
                )
                new_id = cur.fetchone()[0]
            self._db.conn.commit()
        except Exception:
            logger.exception(
                "[scoring] incident insert failed",
                extra={"tenant_id": tenant_id, "user_id": user_id},
            )
            try:
                self._db.conn.rollback()
            except Exception:
                pass
            return False

        logger.info(
            "[scoring] MATCH user=%s score=%d severity=%s incident=%s",
            user_id,
            total_score,
            severity,
            new_id,
        )
        return True

    def _incident_exists(self, user_id: str, incident_type: str) -> bool:
        with self._db.conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM vector_incidents
                WHERE user_id = %s
                  AND incident_type = %s
                  AND confirmed_at > NOW() - %s
                LIMIT 1
                """,
                (user_id, incident_type, DEDUP_WINDOW),
            )
            return cur.fetchone() is not None

    # ----- small helpers --------------------------------------------------

    @staticmethod
    def _severity_for(score: int) -> str:
        if score >= 90:
            return "critical"
        if score >= 80:
            return "high"
        if score >= 60:
            return "medium"
        return "low"

    @staticmethod
    def _client_name_for(events: Iterable[dict], tenant_id: str) -> str | None:
        for e in events or ():
            cn = e.get("client_name")
            if cn:
                return cn
        return None

    @staticmethod
    def _timespan(events: Iterable[dict]) -> tuple[datetime | None, datetime | None]:
        first: datetime | None = None
        last: datetime | None = None
        for e in events or ():
            ts = e.get("timestamp")
            if not isinstance(ts, datetime):
                continue
            if first is None or ts < first:
                first = ts
            if last is None or ts > last:
                last = ts
        return (first, last)

    @staticmethod
    def _dwell_minutes(
        first: datetime | None,
        last: datetime | None,
    ) -> int | None:
        if first is None or last is None:
            return None
        delta = last - first
        return int(max(0, delta.total_seconds() // 60))

    @staticmethod
    def _title_for(
        user_id: str,
        fired: list[RuleResult],
        total_score: int,
    ) -> str:
        if not fired:
            return f"Anomalous activity for {user_id} (score {total_score})"
        names = ", ".join(r.rule_name for r in fired[:3])
        if len(fired) > 3:
            names = f"{names} +{len(fired) - 3} more"
        return (
            f"Anomalous activity for {user_id} — {names} (score {total_score})"
        )

    @staticmethod
    def _summary_for(
        fired: list[RuleResult],
        total_score: int,
    ) -> str:
        if not fired:
            return f"Aggregate anomaly score {total_score}."
        parts = "; ".join(
            f"{r.rule_name} +{int(r.score_delta or 0)}" for r in fired
        )
        noun = "rule" if len(fired) == 1 else "rules"
        return (
            f"Aggregate anomaly score {total_score} from {len(fired)} "
            f"fired {noun}: {parts}."
        )


# ---------------------------------------------------------------------------
# rules
# ---------------------------------------------------------------------------

class NewCountryLoginRule(CorrelationRule):
    """Fires when a user logged in from an IP whose country does
    not appear in their baseline ``login_countries`` profile.

    Flow per evaluation:

    1.  Pull every distinct ``client_ip`` off this user's
        ``UserLoggedIn`` events in the last 24 hours.
    2.  Pull the user's ``login_countries`` from
        ``vector_user_baselines`` (we handle both the object form
        ``{"US": 42, "DE": 3}`` and a flat array form so future
        schema tweaks don't break the rule).
    3.  Resolve each non-private IP's country via
        ``https://ipinfo.io/{ip}/json``. Results are cached on the
        rule instance for 24 hours and the calls are rate-limited
        to 1/sec so repeated cycles don't hammer ipinfo.
    4.  Fire the first time a resolved country doesn't match the
        baseline set.

    Safety: when the user has no baseline yet (a brand new user,
    or the BaselineEngine hasn't built one for them) the rule does
    NOT fire -- otherwise every first-ever login would trip it.
    Private / loopback / link-local / multicast addresses are
    skipped via the stdlib ``ipaddress`` module before any network
    call is made.

    Score: 25 points on a single fire. Below the 80-point incident
    threshold on its own so a benign travel day doesn't page
    anyone, but combines with other rules (impossible travel,
    IOC match, etc.) to confirm incidents.
    """

    name = "NewCountryLogin"
    SCORE_DELTA = 25
    LOOKBACK = timedelta(hours=24)

    # ipinfo plumbing
    IPINFO_URL_TEMPLATE = "https://ipinfo.io/{ip}/json"
    IPINFO_TIMEOUT = 5
    IPINFO_RATE_LIMIT_SEC = 1.0
    CACHE_TTL = timedelta(hours=24)

    def __init__(self) -> None:
        super().__init__()
        # Instance-level IP -> (expires_at, country_code | None)
        # cache. Shared across ``evaluate`` calls on the same rule
        # instance so a second user from the same egress IP in the
        # same cycle is free. ``None`` is a cached negative result
        # (ipinfo didn't know the IP / returned a blank country).
        self._cache: dict[str, tuple[datetime, str | None]] = {}
        self._last_call_at: float = 0.0
        self._session: requests.Session | None = None

    # ----- main entrypoint ----------------------------------------------

    def evaluate(
        self,
        events: list[dict],
        user_profile: dict,
    ) -> RuleResult:
        miss = RuleResult(
            rule_name=self.rule_name, score_delta=0, fired=False,
        )

        # The engine only calls rules for users with recent
        # activity so events should always be non-empty, but be
        # defensive.
        if not events:
            return miss

        first = events[0]
        tenant_id = first.get("tenant_id")
        user_id = first.get("user_id")
        if not tenant_id or not user_id:
            return miss

        # Unbound rule (unit test / misconfigured caller) -- fail
        # closed without raising so the cycle stays healthy.
        if self._db is None:
            logger.debug(
                "[new_country] rule has no DB handle, skipping",
            )
            return miss

        # 1. unique login IPs in the last 24h
        try:
            login_ips = self._fetch_login_ips(tenant_id, user_id)
        except Exception:
            logger.exception(
                "[new_country] login IP fetch failed",
                extra={"tenant_id": tenant_id, "user_id": user_id},
            )
            self._safe_rollback()
            return miss

        if not login_ips:
            return miss

        # 2. baseline country set for this user
        try:
            baseline_countries = self._fetch_baseline_countries(
                tenant_id, user_id,
            )
        except Exception:
            logger.exception(
                "[new_country] baseline fetch failed",
                extra={"tenant_id": tenant_id, "user_id": user_id},
            )
            self._safe_rollback()
            return miss

        # No baseline -> don't fire. Otherwise a brand-new user's
        # very first login would produce an incident.
        if not baseline_countries:
            return miss

        # 3. resolve each IP via ipinfo, stop on the first miss
        for ip in login_ips:
            country = self._resolve_country(ip)
            if country is None:
                continue
            if country in baseline_countries:
                continue
            return RuleResult(
                rule_name=self.rule_name,
                score_delta=self.SCORE_DELTA,
                fired=True,
                evidence={
                    "user":               user_id,
                    "new_country":        country,
                    "ip":                 ip,
                    "baseline_countries": sorted(baseline_countries),
                },
            )

        return miss

    # ----- database ------------------------------------------------------

    def _fetch_login_ips(
        self,
        tenant_id: str,
        user_id: str,
    ) -> list[str]:
        """Return unique ``client_ip`` values from this user's
        ``UserLoggedIn`` events over the lookback window."""
        with self._db.conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT client_ip
                FROM vector_events
                WHERE tenant_id = %s
                  AND user_id = %s
                  AND event_type = 'UserLoggedIn'
                  AND client_ip IS NOT NULL
                  AND timestamp > NOW() - %s
                """,
                (tenant_id, user_id, self.LOOKBACK),
            )
            return [row[0] for row in cur.fetchall() if row[0]]

    def _fetch_baseline_countries(
        self,
        tenant_id: str,
        user_id: str,
    ) -> set[str]:
        """Return the set of country codes present on the user's
        baseline profile. Tolerates both ``{"US": 42}`` and
        ``["US", "DE"]`` shapes so a schema change doesn't break
        the rule silently."""
        with self._db.conn.cursor() as cur:
            cur.execute(
                """
                SELECT login_countries
                FROM vector_user_baselines
                WHERE tenant_id = %s AND user_id = %s
                """,
                (tenant_id, user_id),
            )
            row = cur.fetchone()
        if not row:
            return set()
        raw = row[0]
        if isinstance(raw, dict):
            return {k for k in raw.keys() if k}
        if isinstance(raw, (list, tuple, set)):
            return {str(v) for v in raw if v}
        return set()

    def _safe_rollback(self) -> None:
        if self._db is None:
            return
        try:
            self._db.conn.rollback()
        except Exception:
            pass

    # ----- ipinfo resolver ----------------------------------------------

    @staticmethod
    def _is_skippable_ip(ip: str) -> bool:
        """Return True for any address we shouldn't look up --
        RFC1918, loopback, link-local, multicast, unspecified, or
        anything we can't even parse."""
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

    def _cache_get(self, ip: str) -> tuple[bool, str | None]:
        entry = self._cache.get(ip)
        if not entry:
            return (False, None)
        expires_at, country = entry
        if datetime.now(timezone.utc) > expires_at:
            self._cache.pop(ip, None)
            return (False, None)
        return (True, country)

    def _cache_put(self, ip: str, country: str | None) -> None:
        self._cache[ip] = (
            datetime.now(timezone.utc) + self.CACHE_TTL,
            country,
        )

    def _rate_limit(self) -> None:
        elapsed = time.monotonic() - self._last_call_at
        if elapsed < self.IPINFO_RATE_LIMIT_SEC:
            time.sleep(self.IPINFO_RATE_LIMIT_SEC - elapsed)
        self._last_call_at = time.monotonic()

    def _resolve_country(self, ip: str) -> str | None:
        """Return the ISO-3166 alpha-2 country code ipinfo.io
        reports for ``ip``, or ``None`` when ipinfo is
        unreachable, the IP is private, or the response carries
        no country field. Fail-open semantics: a missing country
        never flags a user."""
        if self._is_skippable_ip(ip):
            return None

        hit, cached = self._cache_get(ip)
        if hit:
            return cached

        self._rate_limit()
        if self._session is None:
            self._session = requests.Session()
        url = self.IPINFO_URL_TEMPLATE.format(ip=ip)
        try:
            resp = self._session.get(url, timeout=self.IPINFO_TIMEOUT)
        except requests.RequestException as exc:
            logger.debug("[new_country] ipinfo request failed ip=%s err=%s", ip, exc)
            # Don't cache transport errors -- retry next cycle.
            return None

        if not resp.ok:
            logger.debug(
                "[new_country] ipinfo non-2xx ip=%s status=%s",
                ip, resp.status_code,
            )
            # Negative-cache 4xx (non-429) so we stop retrying.
            if 400 <= resp.status_code < 500 and resp.status_code != 429:
                self._cache_put(ip, None)
            return None

        try:
            data = resp.json()
        except ValueError:
            self._cache_put(ip, None)
            return None

        country = (data.get("country") or "").strip() or None
        self._cache_put(ip, country)
        return country


class OffHoursLoginRule(CorrelationRule):
    """Fires when a user logged in during the overnight window
    22:00-06:00 UTC inside the last 24 hours.

    A single off-hours login rarely means compromise by itself --
    night-shift admins, travelling execs, and scheduled service
    accounts all trip this regularly -- so the rule is intentionally
    low-weight. It exists to stack with other signals: a new-country
    login *at 03:00 UTC* is a lot more suspicious than either signal
    alone, and that's the shape the aggregate scoring model is
    designed to catch.

    Time handling: event timestamps on ``vector_events`` are stored
    as TIMESTAMPTZ and Postgres sessions in vector-ingest are pinned
    to UTC in db.py's ``connect()``, so ``EXTRACT(HOUR FROM timestamp)``
    returns the UTC hour directly. We also double-check client-side
    using the ``datetime`` object psycopg2 hands back, in case a
    future migration changes the session tz.
    """

    name = "OffHoursLogin"
    SCORE_DELTA = 15
    LOOKBACK = timedelta(hours=24)

    # Off-hours window: 22:00 (inclusive) through 06:00 (exclusive),
    # UTC. The window wraps midnight, so membership is
    # ``hour >= 22 or hour < 6``.
    OFF_HOURS_START = 22  # 22:00 UTC
    OFF_HOURS_END = 6     # 06:00 UTC

    @classmethod
    def _is_off_hours(cls, hour: int) -> bool:
        return hour >= cls.OFF_HOURS_START or hour < cls.OFF_HOURS_END

    def evaluate(
        self,
        events: list[dict],
        user_profile: dict,
    ) -> RuleResult:
        miss = RuleResult(
            rule_name=self.rule_name, score_delta=0, fired=False,
        )
        if not events:
            return miss

        first = events[0]
        tenant_id = first.get("tenant_id")
        user_id = first.get("user_id")
        if not tenant_id or not user_id:
            return miss

        if self._db is None:
            logger.debug(
                "[off_hours] rule has no DB handle, skipping",
            )
            return miss

        try:
            logins = self._fetch_logins(tenant_id, user_id)
        except Exception:
            logger.exception(
                "[off_hours] login fetch failed",
                extra={"tenant_id": tenant_id, "user_id": user_id},
            )
            try:
                self._db.conn.rollback()
            except Exception:
                pass
            return miss

        # First off-hours login wins. We take the earliest qualifying
        # login so the evidence's ``login_time`` is deterministic and
        # reproducible across repeated cycles.
        for ts, client_ip in logins:
            if not isinstance(ts, datetime):
                continue
            hour = int(ts.hour)
            if not self._is_off_hours(hour):
                continue
            return RuleResult(
                rule_name=self.rule_name,
                score_delta=self.SCORE_DELTA,
                fired=True,
                evidence={
                    "user":       user_id,
                    "login_time": ts.isoformat(),
                    "hour":       hour,
                    "client_ip":  client_ip,
                },
            )

        return miss

    def _fetch_logins(
        self,
        tenant_id: str,
        user_id: str,
    ) -> list[tuple[datetime, str | None]]:
        """Return ``(timestamp, client_ip)`` tuples for this user's
        ``UserLoggedIn`` events in the lookback window, ordered
        earliest-first."""
        with self._db.conn.cursor() as cur:
            cur.execute(
                """
                SELECT timestamp, client_ip
                FROM vector_events
                WHERE tenant_id = %s
                  AND user_id = %s
                  AND event_type = 'UserLoggedIn'
                  AND timestamp > NOW() - %s
                ORDER BY timestamp ASC
                """,
                (tenant_id, user_id, self.LOOKBACK),
            )
            return [(row[0], row[1]) for row in cur.fetchall()]


class HighVolumeFileAccessRule(CorrelationRule):
    """Fires when a user touched more than 50 files in a 1-hour
    window across SharePoint / OneDrive. Classic post-credential-
    compromise behaviour (bulk enumeration of a compromised user's
    drive) and also the first-stage signal for ransomware staging.

    The matched events are UAL ``FileAccessed``, ``FileDownloaded``,
    and ``FileModified`` rows on the ``SharePoint`` or ``OneDrive``
    workloads. We also accept ``OneDriveForBusiness`` because that's
    the actual workload string UAL emits for business OneDrive
    activity; "OneDrive" at the spec level maps to both.

    Score is 20 -- intentionally below the 80-point incident
    threshold on its own so a legitimate bulk-sync session doesn't
    page operators, but combines with anomaly signals (new country,
    off-hours, impossible travel) to confirm a real incident.
    """

    name = "HighVolumeFileAccess"
    SCORE_DELTA = 20
    LOOKBACK = timedelta(minutes=60)
    THRESHOLD = 50

    MATCHED_EVENT_TYPES = ("FileAccessed", "FileDownloaded", "FileModified")
    MATCHED_WORKLOADS = ("SharePoint", "OneDrive", "OneDriveForBusiness")

    def evaluate(
        self,
        events: list[dict],
        user_profile: dict,
    ) -> RuleResult:
        miss = RuleResult(
            rule_name=self.rule_name, score_delta=0, fired=False,
        )
        if not events:
            return miss

        first = events[0]
        tenant_id = first.get("tenant_id")
        user_id = first.get("user_id")
        if not tenant_id or not user_id:
            return miss

        if self._db is None:
            logger.debug(
                "[high_volume_file] rule has no DB handle, skipping",
            )
            return miss

        try:
            count = self._fetch_event_count(tenant_id, user_id)
        except Exception:
            logger.exception(
                "[high_volume_file] count fetch failed",
                extra={"tenant_id": tenant_id, "user_id": user_id},
            )
            try:
                self._db.conn.rollback()
            except Exception:
                pass
            return miss

        if count <= self.THRESHOLD:
            return miss

        return RuleResult(
            rule_name=self.rule_name,
            score_delta=self.SCORE_DELTA,
            fired=True,
            evidence={
                "user":             user_id,
                "file_event_count": int(count),
                "threshold":        self.THRESHOLD,
                "window_minutes":   60,
            },
        )

    def _fetch_event_count(
        self,
        tenant_id: str,
        user_id: str,
    ) -> int:
        """Return the count of matching file-access events for this
        user in the lookback window."""
        with self._db.conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)::bigint
                FROM vector_events
                WHERE tenant_id = %s
                  AND user_id = %s
                  AND event_type = ANY(%s)
                  AND workload = ANY(%s)
                  AND timestamp > NOW() - %s
                """,
                (
                    tenant_id,
                    user_id,
                    list(self.MATCHED_EVENT_TYPES),
                    list(self.MATCHED_WORKLOADS),
                    self.LOOKBACK,
                ),
            )
            row = cur.fetchone()
        return int(row[0] or 0) if row else 0
