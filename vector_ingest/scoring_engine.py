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

import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

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
            rules = [ImpossibleTravelRule()]
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

class ImpossibleTravelRule(CorrelationRule):
    """Fires when a user authenticated from two distinct countries
    within a 2-hour window -- physically impossible for a
    legitimate session and a high-confidence indicator of
    credential compromise or a hijacked access token.

    Unlike most rules, ImpossibleTravel needs a wider lookback
    window than the engine's default 30 minutes so it can still
    catch "logged in from Chicago at 10:05, logged in from Lagos
    at 11:50" cases where the two sign-ins straddle the window.
    We issue our own SQL query against ``vector_events`` using
    ``self._db`` populated by ``ScoringEngine.register_rule``.

    Scoring: a single fired result is worth 40 points, well under
    the 80-point incident threshold on its own so operators don't
    get paged for every corporate VPN misroute. When it fires
    alongside a second signal (new country, off-hours, anomalous
    download volume, etc.) the combined score crosses threshold
    and produces an incident.
    """

    name = "ImpossibleTravel"
    SCORE_DELTA = 40
    LOOKBACK = timedelta(hours=2)

    def evaluate(
        self,
        events: list[dict],
        user_profile: dict,
    ) -> RuleResult:
        # No events -> nothing to rule on. The engine only asks
        # rules to evaluate users who had activity inside the
        # scoring window so this branch is mostly belt-and-braces.
        if not events:
            return RuleResult(
                rule_name=self.rule_name, score_delta=0, fired=False,
            )

        # All events in the list belong to the same user, so
        # (tenant_id, user_id) can be read off any one of them.
        first = events[0]
        tenant_id = first.get("tenant_id")
        user_id = first.get("user_id")
        if not tenant_id or not user_id:
            return RuleResult(
                rule_name=self.rule_name, score_delta=0, fired=False,
            )

        # The engine wires its DB handle in during register_rule;
        # an unbound rule (e.g. during unit tests) simply doesn't
        # fire instead of crashing the cycle.
        if self._db is None:
            logger.debug(
                "[impossible_travel] rule has no DB handle, skipping",
            )
            return RuleResult(
                rule_name=self.rule_name, score_delta=0, fired=False,
            )

        try:
            logins = self._fetch_logins(tenant_id, user_id)
        except Exception:
            logger.exception(
                "[impossible_travel] login fetch failed",
                extra={"tenant_id": tenant_id, "user_id": user_id},
            )
            try:
                self._db.conn.rollback()
            except Exception:
                pass
            return RuleResult(
                rule_name=self.rule_name, score_delta=0, fired=False,
            )

        # Collapse into {country: (first_seen_ip, first_seen_ts)}.
        # Keeping only the first occurrence per country makes the
        # time-delta we emit reflect "when did we first see country
        # A vs country B", which is the operator-useful framing.
        first_by_country: dict[str, tuple[str | None, datetime]] = {}
        for country, client_ip, ts in logins:
            if not country:
                continue
            if country not in first_by_country:
                first_by_country[country] = (client_ip, ts)

        if len(first_by_country) < 2:
            return RuleResult(
                rule_name=self.rule_name, score_delta=0, fired=False,
            )

        # Pick the two earliest country appearances so the
        # time_delta_minutes evidence field is signed positive and
        # the "from X at Ta to Y at Tb" narrative reads naturally
        # for the Incidents UI.
        ordered = sorted(
            first_by_country.items(),
            key=lambda kv: kv[1][1],
        )
        (country_a, (ip_a, t_a)), (country_b, (ip_b, t_b)) = ordered[:2]
        delta_seconds = (t_b - t_a).total_seconds()
        time_delta_minutes = int(max(0, delta_seconds // 60))

        return RuleResult(
            rule_name=self.rule_name,
            score_delta=self.SCORE_DELTA,
            fired=True,
            evidence={
                "user":               user_id,
                "country_a":          country_a,
                "country_b":          country_b,
                "ip_a":               ip_a,
                "ip_b":               ip_b,
                "time_delta_minutes": time_delta_minutes,
            },
        )

    # ----- internals -----------------------------------------------------

    def _fetch_logins(
        self,
        tenant_id: str,
        user_id: str,
    ) -> list[tuple[str | None, str | None, datetime]]:
        """Return ``(geo_country, client_ip, timestamp)`` tuples for
        every ``UserLoggedIn`` event for this user in the last
        ``LOOKBACK`` window, ordered earliest-first. Rows with NULL
        ``geo_country`` are filtered server-side so the caller can
        rely on country being populated."""
        with self._db.conn.cursor() as cur:
            cur.execute(
                """
                SELECT geo_country, client_ip, timestamp
                FROM vector_events
                WHERE tenant_id = %s
                  AND user_id = %s
                  AND event_type = 'UserLoggedIn'
                  AND timestamp > NOW() - %s
                  AND geo_country IS NOT NULL
                ORDER BY timestamp ASC
                """,
                (tenant_id, user_id, self.LOOKBACK),
            )
            return [(row[0], row[1], row[2]) for row in cur.fetchall()]
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
            HAVING MAX(timestamp) - MIN(timestamp) >= INTERVAL '2 days' OR COUNT(*) >= 50
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
                    ON CONFLICT (tenant_id, user_id) DO UPDATE SET
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

