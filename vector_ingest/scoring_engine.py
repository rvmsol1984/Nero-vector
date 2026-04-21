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
import math
import os
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
        # Set by register_rule so rules can call
        # self._engine._is_excepted() for the allowlist check.
        self._engine: Any = None

    def bind_db(self, db: Database) -> None:
        """Called by the engine during registration to wire the
        shared DB handle into the rule. Idempotent."""
        self._db = db

    def bind_engine(self, engine: Any) -> None:
        """Called by the engine during registration so rules can
        access the exception allowlist via ``_engine._is_excepted``."""
        self._engine = engine

    def is_excepted(
        self,
        tenant_id: str,
        facts: dict[str, str | None],
    ) -> bool:
        """Convenience wrapper for the engine's allowlist check.
        Returns False when the engine isn't wired (unit tests)."""
        if self._engine is None:
            return False
        return self._engine._is_excepted(tenant_id, self.rule_name, facts)

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
                SuspiciousMailboxRule(),
                MalwareDetectedRule(),
                IOCMatchRule(),
                HighRiskCountryLoginRule(),
                VPNLoginRule(),
                ImpossibleTravelRule(),
                InboxRuleCreatedRule(),
                MassEmailDeleteRule(),
                NewDeviceLoginRule(),
                PrivilegedRoleAssignedRule(),
                MFAMethodChangedRule(),
                ServicePrincipalLoginRule(),
                PasswordSprayRule(),
                ExternalSharingSpikeRule(),
                AiTMDetectionRule(),
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
        rule.bind_engine(self)
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

        self._load_exceptions()

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

        After the incident row is written, each fired rule gets a
        companion ``vector_incident_events`` row so the Incidents
        UI's evidence timeline can render a per-signal breakdown.
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

        # Enrich fired rules' evidence with VPN info before
        # serialising. Any IP in the evidence dict (keyed as "ip",
        # "matched_ip", or "client_ip") is looked up once and the
        # result is merged into the evidence so operators see VPN
        # status on the Incidents UI without a second click.
        for r in fired:
            ip = (
                r.evidence.get("ip")
                or r.evidence.get("matched_ip")
                or r.evidence.get("client_ip")
            )
            if ip:
                vpn_info = self._check_vpn(ip)
                if vpn_info:
                    r.evidence["vpn_info"] = vpn_info

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

                # Write one vector_incident_events row per fired rule
                # so the Incidents UI's evidence timeline can render
                # a per-signal breakdown with source badges and score
                # contributions.
                for r in fired:
                    cur.execute(
                        """
                        INSERT INTO vector_incident_events (
                            incident_id, event_source, event_type,
                            significance, raw_json, timestamp
                        ) VALUES (
                            %s::uuid, 'scoring_engine', %s,
                            'high', %s::jsonb, NOW()
                        )
                        """,
                        (
                            new_id,
                            r.rule_name,
                            json.dumps(r.evidence or {}),
                        ),
                    )

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

    # ----- rule exception / allowlist ------------------------------------
    #
    # Operators can suppress specific rule triggers per tenant by
    # inserting rows into ``vector_rule_exceptions``.  The full set is
    # loaded once per scoring cycle (via _load_exceptions at the top of
    # run_scoring_cycle) and cached for EXCEPTION_CACHE_TTL so a mid-
    # cycle DB outage doesn't re-enable suppressed alerts.
    #
    # Shape:
    #   _exceptions_cache = {
    #     (tenant_id, rule_name): [
    #       {"type": "country", "value": "CN"},
    #       {"type": "ip",      "value": "1.2.3.4"},
    #       {"type": "any",     "value": "*"},
    #       ...
    #     ]
    #   }
    #
    # ``_is_excepted`` checks whether a particular (tenant, rule, facts)
    # triple matches any stored exception. ``facts`` is a dict like
    # ``{"country": "CN", "ip": "1.2.3.4", "user": "alice@example.com"}``
    # — each key is matched against exceptions of the corresponding
    # ``exception_type``.

    EXCEPTION_CACHE_TTL = timedelta(minutes=5)

    def _load_exceptions(self) -> None:
        """Refresh the exception cache from Postgres. Called once at
        the top of ``run_scoring_cycle``; any DB error is swallowed so
        the previous cache survives."""
        now = datetime.now(timezone.utc)
        if (
            hasattr(self, "_exceptions_loaded_at")
            and self._exceptions_loaded_at
            and now - self._exceptions_loaded_at < self.EXCEPTION_CACHE_TTL
        ):
            return
        try:
            with self._db.conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT tenant_id::text, rule_name,
                           exception_type, exception_value
                    FROM vector_rule_exceptions
                    """
                )
                rows = cur.fetchall()
        except Exception:
            logger.debug("[scoring] exception cache refresh failed", exc_info=True)
            return
        cache: dict[tuple[str, str], list[dict]] = {}
        for tenant_id, rule_name, exc_type, exc_value in rows:
            key = (str(tenant_id), rule_name)
            cache.setdefault(key, []).append({
                "type":  exc_type,
                "value": (exc_value or "").strip().lower(),
            })
        self._exceptions_cache = cache
        self._exceptions_loaded_at = now

    def _is_excepted(
        self,
        tenant_id: str,
        rule_name: str,
        facts: dict[str, str | None],
    ) -> bool:
        """Return True if any stored exception matches the given
        (tenant, rule, facts) triple. ``facts`` keys are exception
        types; values are the runtime values to compare against.

        An exception with ``type='any'`` matches unconditionally
        (i.e. the rule is fully disabled for that tenant).
        """
        cache = getattr(self, "_exceptions_cache", None) or {}
        entries = cache.get((str(tenant_id), rule_name), [])
        for entry in entries:
            if entry["type"] == "any":
                return True
            fact_value = (facts.get(entry["type"]) or "").strip().lower()
            if fact_value and fact_value == entry["value"]:
                return True
        return False

    # ----- VPN detection -------------------------------------------------

    # Instance-level cache: ip -> (expires_at, result_dict | None).
    # Constructed lazily on first _check_vpn call so the engine
    # doesn't carry empty dicts when VPN checks never fire.
    _vpn_cache: dict[str, tuple[datetime, dict | None]] | None = None
    _vpn_session: requests.Session | None = None

    VPN_API_URL = "https://ipapi.is/json/"
    VPN_CACHE_TTL = timedelta(hours=24)
    VPN_REQUEST_TIMEOUT = 5

    def _check_vpn(self, ip: str) -> dict | None:
        """Check whether ``ip`` is a known VPN endpoint via the
        ipapi.is API. Returns ``{is_vpn, vpn_name, asn}`` on
        success, ``None`` when the IP is private / unreachable /
        unparseable. All errors are swallowed -- VPN enrichment is
        best-effort and must never block incident creation.

        Results are cached per-IP for 24 hours on the engine
        instance so the same IP across multiple rules in the same
        cycle only produces one outbound request.
        """
        if not ip:
            return None
        try:
            addr = ipaddress.ip_address(str(ip).strip())
        except (ValueError, TypeError):
            return None
        if (
            addr.is_private
            or addr.is_loopback
            or addr.is_link_local
            or addr.is_unspecified
            or addr.is_multicast
            or addr.is_reserved
        ):
            return None

        # Lazy-init cache + session so a ScoringEngine that never
        # fires any IP-bearing rules costs nothing.
        if self._vpn_cache is None:
            self._vpn_cache = {}
        hit = self._vpn_cache.get(ip)
        if hit:
            expires_at, cached = hit
            if datetime.now(timezone.utc) < expires_at:
                return cached
            self._vpn_cache.pop(ip, None)

        if self._vpn_session is None:
            self._vpn_session = requests.Session()
        try:
            resp = self._vpn_session.get(
                f"{self.VPN_API_URL}?ip={ip}",
                timeout=self.VPN_REQUEST_TIMEOUT,
            )
        except requests.RequestException as exc:
            logger.debug("[vpn_check] request failed ip=%s err=%s", ip, exc)
            return None

        if not resp.ok:
            logger.debug("[vpn_check] non-2xx ip=%s status=%s", ip, resp.status_code)
            return None

        try:
            data = resp.json()
        except ValueError:
            return None

        is_vpn = bool(data.get("is_vpn"))
        vpn_block = data.get("vpn") or {}
        vpn_name = (
            vpn_block.get("name")
            if isinstance(vpn_block, dict)
            else None
        )
        asn_block = data.get("asn") or {}
        asn = (
            asn_block.get("org") or asn_block.get("asn")
            if isinstance(asn_block, dict)
            else None
        )
        result = {
            "is_vpn":  is_vpn,
            "vpn_name": vpn_name or None,
            "asn":     str(asn) if asn else None,
        }

        self._vpn_cache[ip] = (
            datetime.now(timezone.utc) + self.VPN_CACHE_TTL,
            result,
        )
        return result

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

        if self.is_excepted(tenant_id, {"user": user_id}):
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
    """Fires when a user logged in at a time they don't normally
    authenticate.

    The rule has two modes, selected automatically based on how much
    history the user's baseline profile has accumulated:

    **Personal baseline** (>= 7 distinct hours in ``login_hours``):
        ``login_hours`` is a dict ``{"8": 5, "9": 4, ...}`` whose
        keys are UTC hours and values are event counts.  A login is
        flagged when the hour it occurred in has 0 appearances or
        accounts for < 2% of the user's total login volume.  This
        respects shift workers, global travellers, and service
        accounts that legitimately authenticate overnight.

    **Fixed fallback** (< 7 distinct baseline hours):
        Falls back to the static 22:00-06:00 UTC window from the
        original v1 rule. Seven distinct hours is the minimum where
        we can trust the distribution enough to flag a *missing*
        hour as anomalous -- fewer than that and most normal users
        would trigger on any new hour they hadn't been active
        during yet.

    Score: 15 points. Intentionally low so a single off-hours
    login doesn't page anyone. Stacks with NewCountryLogin (+25),
    HighRiskCountry (+35), or IOCMatch (+50) to cross the 80-point
    incident threshold.
    """

    name = "OffHoursLogin"
    SCORE_DELTA = 15
    LOOKBACK = timedelta(hours=24)

    # Fixed fallback window (v1 behaviour) used when the user's
    # baseline is too thin for a personal distribution check.
    OFF_HOURS_START = 22  # 22:00 UTC
    OFF_HOURS_END = 6     # 06:00 UTC

    # Minimum number of distinct hours in the baseline before we
    # trust the personal distribution. Anything below this count
    # falls back to the fixed window.
    MIN_BASELINE_HOURS = 7

    # Fraction of total logins below which an hour is considered
    # "anomalous" in the personal baseline mode.
    RARE_THRESHOLD = 0.02  # 2%

    @classmethod
    def _is_off_hours_fixed(cls, hour: int) -> bool:
        """v1 fixed-window check (22:00-06:00 UTC)."""
        return hour >= cls.OFF_HOURS_START or hour < cls.OFF_HOURS_END

    @classmethod
    def _is_off_hours_personal(
        cls,
        hour: int,
        login_hours: dict,
    ) -> bool:
        """Personal-baseline check. Returns True when ``hour`` is
        absent from the user's distribution or represents < 2% of
        their total login volume."""
        total = sum(int(v or 0) for v in login_hours.values())
        if total <= 0:
            # Degenerate baseline (all counts zero) -- treat as
            # insufficient data and let the caller fall through to
            # the fixed window.
            return False
        count = int(login_hours.get(str(hour)) or 0)
        if count == 0:
            return True
        return (count / total) < cls.RARE_THRESHOLD

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

        if self.is_excepted(tenant_id, {"user": user_id}):
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

        # Decide which check mode to use for this user based on the
        # richness of their login_hours baseline.
        login_hours = user_profile.get("login_hours") or {}
        if not isinstance(login_hours, dict):
            login_hours = {}
        baseline_hours_count = len(login_hours)
        use_personal = baseline_hours_count >= self.MIN_BASELINE_HOURS

        # First off-hours login wins. We take the earliest qualifying
        # login so the evidence's ``login_time`` is deterministic and
        # reproducible across repeated cycles.
        for ts, client_ip in logins:
            if not isinstance(ts, datetime):
                continue
            hour = int(ts.hour)
            if use_personal:
                flagged = self._is_off_hours_personal(hour, login_hours)
            else:
                flagged = self._is_off_hours_fixed(hour)
            if not flagged:
                continue
            return RuleResult(
                rule_name=self.rule_name,
                score_delta=self.SCORE_DELTA,
                fired=True,
                evidence={
                    "user":                 user_id,
                    "login_time":           ts.isoformat(),
                    "hour":                 hour,
                    "client_ip":            client_ip,
                    "baseline_hours_count": baseline_hours_count,
                    "mode":                 "personal" if use_personal else "fixed",
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

        if self.is_excepted(tenant_id, {"user": user_id}):
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


class SuspiciousMailboxRule(CorrelationRule):
    """Fires when a user triggered mailbox-recon + mailbox-mutation
    events in the same 2-hour window.

    The shape we're looking for:

    * ``FolderBind`` -- UAL record of a mailbox folder being opened
      by a non-owner or a service identity. Normal users don't
      usually generate ``FolderBind`` for their own mailbox; when
      they do, it shows up alongside other mailbox reads. When we
      see a ``FolderBind`` paired with a mailbox mutation event
      inside a small window it's almost always either a legitimate
      admin doing a mailbox audit or an attacker enumerating
      folders before setting up forwarding / an inbox rule. The
      enumeration -> action pair is the tell.

    * ``MessageForward`` or ``New-InboxRule`` (``NewInboxRule``) --
      the mutation side: the attacker adjusts the mailbox to
      exfiltrate messages or hide sent email from the victim.
      Either one on its own is noisy; paired with a preceding
      folder enumeration it's a strong BEC precursor.

    Score: 30 points. High on its own because the combination is
    genuinely rare for legitimate users; still below the 80-point
    incident threshold so a second signal (new-country login,
    off-hours, high-volume downloads, etc.) is needed to confirm.
    """

    name = "SuspiciousMailbox"
    SCORE_DELTA = 30
    LOOKBACK = timedelta(hours=2)

    FOLDER_BIND_TYPE = "FolderBind"
    MUTATION_TYPES = ("MessageForward", "NewInboxRule", "New-InboxRule")

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
                "[suspicious_mailbox] rule has no DB handle, skipping",
            )
            return miss

        if self.is_excepted(tenant_id, {"user": user_id}):
            return miss

        try:
            counts = self._fetch_event_counts(tenant_id, user_id)
        except Exception:
            logger.exception(
                "[suspicious_mailbox] event count fetch failed",
                extra={"tenant_id": tenant_id, "user_id": user_id},
            )
            try:
                self._db.conn.rollback()
            except Exception:
                pass
            return miss

        folder_bind_count = int(counts.get("folder_bind") or 0)
        mutation_count = int(counts.get("mutation") or 0)

        if folder_bind_count <= 0 or mutation_count <= 0:
            return miss

        return RuleResult(
            rule_name=self.rule_name,
            score_delta=self.SCORE_DELTA,
            fired=True,
            evidence={
                "user":           user_id,
                "events_found":   {
                    "FolderBind":               folder_bind_count,
                    "MessageForward/NewInboxRule": mutation_count,
                },
                "window_minutes": 120,
            },
        )

    def _fetch_event_counts(
        self,
        tenant_id: str,
        user_id: str,
    ) -> dict:
        """Return ``{folder_bind, mutation}`` counts for this user
        inside the lookback window.

        A single COUNT(*) FILTER query keeps the whole check to one
        round-trip and never materialises the row list -- the rule
        only needs "does at least one of each exist" so this is
        strictly faster than pulling event metadata.
        """
        with self._db.conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*) FILTER (WHERE event_type = %s)::bigint AS folder_bind,
                    COUNT(*) FILTER (WHERE event_type = ANY(%s))::bigint AS mutation
                FROM vector_events
                WHERE tenant_id = %s
                  AND user_id = %s
                  AND event_type IN (%s, %s, %s, %s)
                  AND timestamp > NOW() - %s
                """,
                (
                    self.FOLDER_BIND_TYPE,
                    list(self.MUTATION_TYPES),
                    tenant_id,
                    user_id,
                    self.FOLDER_BIND_TYPE,
                    self.MUTATION_TYPES[0],
                    self.MUTATION_TYPES[1],
                    self.MUTATION_TYPES[2],
                    self.LOOKBACK,
                ),
            )
            row = cur.fetchone()
        if not row:
            return {"folder_bind": 0, "mutation": 0}
        return {
            "folder_bind": int(row[0] or 0),
            "mutation":    int(row[1] or 0),
        }


class MalwareDetectedRule(CorrelationRule):
    """Fires when ``vector_defender_alerts`` contains any alert
    for this user in the last 24 hours.

    Auto-fires regardless of other signals: a Defender ATP alert
    is already a vetted detection from Microsoft's security stack,
    so the scoring engine's role here is just to lift the detection
    into a Phase 2 incident with proper dedup + ownership + UI
    surface. The 60-point weight is intentionally high so a single
    fire plus even one low-weight signal (off-hours login, +15)
    pushes the aggregate over the 80-point incident threshold.

    Matching: Defender alerts identify users via the
    ``logged_on_users`` JSONB array (each element is
    ``{accountName, domainName}``) and various paths in
    ``raw_json``. We do the pragmatic thing and substring-match
    both against the user's UPN and against the UPN's local part
    -- false positives are inherently scoped to one tenant and
    the severity of a real hit justifies the looser match.
    """

    name = "MalwareDetected"
    SCORE_DELTA = 60
    LOOKBACK = timedelta(hours=24)
    MAX_ALERTS = 100  # hard cap on inspected rows per cycle

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
                "[malware] rule has no DB handle, skipping",
            )
            return miss

        if self.is_excepted(tenant_id, {"user": user_id}):
            return miss

        try:
            alerts = self._fetch_alerts(tenant_id, user_id)
        except Exception:
            logger.exception(
                "[malware] alert fetch failed",
                extra={"tenant_id": tenant_id, "user_id": user_id},
            )
            try:
                self._db.conn.rollback()
            except Exception:
                pass
            return miss

        if not alerts:
            return miss

        # Rows come back ordered newest-first, so index 0 is the
        # latest. We surface its title + severity in the evidence
        # so the Incidents UI's evidence timeline has something
        # operator-useful to show without clicking through.
        latest = alerts[0]
        return RuleResult(
            rule_name=self.rule_name,
            score_delta=self.SCORE_DELTA,
            fired=True,
            evidence={
                "user":                  user_id,
                "alert_count":           len(alerts),
                "latest_alert_title":    latest.get("title"),
                "latest_alert_severity": latest.get("severity"),
            },
        )

    def _fetch_alerts(
        self,
        tenant_id: str,
        user_id: str,
    ) -> list[dict]:
        """Return matching Defender alerts for this user inside
        the lookback window, newest-first.

        ``logged_on_users`` is cast to text and substring-matched
        against the local part of the UPN (that's what Defender
        carries in accountName). ``raw_json`` is substring-matched
        against the full UPN for the cases where the alert payload
        embeds the email somewhere in its nested structure.
        """
        local_part = user_id.split("@", 1)[0] if "@" in user_id else user_id
        with self._db.conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, severity, title, alert_creation_time
                FROM vector_defender_alerts
                WHERE tenant_id = %s
                  AND alert_creation_time > NOW() - %s
                  AND (
                        logged_on_users::text ILIKE '%%' || %s || '%%'
                     OR raw_json::text        ILIKE '%%' || %s || '%%'
                  )
                ORDER BY alert_creation_time DESC
                LIMIT %s
                """,
                (
                    tenant_id,
                    self.LOOKBACK,
                    local_part,
                    user_id,
                    self.MAX_ALERTS,
                ),
            )
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]


class IOCMatchRule(CorrelationRule):
    """Fires when any of the user's recent activity IPs shows up in
    ``vector_ioc_matches`` with confidence >= 50.

    ``vector_ioc_matches`` is populated by two independent workers:

    * ``IocEnricher`` (vector_ingest/ioc_enricher.py) -- live
      enrichment on freshly-ingested events, running every 5
      minutes alongside the rest of the ingest loop.
    * ``ThreatIntelMonitor`` (below) -- daily retroactive sweep
      that catches IPs which became indicators AFTER they were
      first ingested. This is the "indicator published yesterday,
      we saw the traffic three days ago" case.

    This rule is the consumer that lifts matches from either source
    into Phase 2 incident scoring. Score is 50 -- high enough that
    even a single low-weight co-signal (off-hours login +15) pushes
    the aggregate past the 80-point incident threshold, but below
    threshold on its own so a noisy upstream feed never pages
    operators all by itself.

    The join goes via ``vector_ioc_matches.ioc_value = vector_events
    .client_ip`` because the only IOC type currently populated for
    IPv4 observables is the flat IP string. If/when the schema grows
    a dedicated ``ioc_type = 'ipv4'`` filter, this query can add a
    ``WHERE m.ioc_type = 'ipv4'`` clause to keep the index usage
    tight; today there's nothing to exclude.
    """

    name = "IOCMatch"
    SCORE_DELTA = 50
    LOOKBACK = timedelta(hours=24)
    MIN_CONFIDENCE = 50

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
                "[ioc_match] rule has no DB handle, skipping",
            )
            return miss

        if self.is_excepted(tenant_id, {"user": user_id}):
            return miss

        try:
            match = self._fetch_match(tenant_id, user_id)
        except Exception:
            logger.exception(
                "[ioc_match] query failed",
                extra={"tenant_id": tenant_id, "user_id": user_id},
            )
            try:
                self._db.conn.rollback()
            except Exception:
                pass
            return miss

        if match is None:
            return miss

        # ``threat_type`` is the operator-facing label shown on the
        # Incidents UI's evidence timeline. Prefer the concrete
        # ``ioc_type`` field when it's set (e.g. "ipv4") because
        # that's what downstream UI code already knows how to render;
        # fall back to the human-readable indicator_name and finally
        # to a literal string when both are missing.
        threat_type = (
            (match.get("ioc_type") or "").strip()
            or (match.get("indicator_name") or "").strip()
            or "unknown"
        )

        return RuleResult(
            rule_name=self.rule_name,
            score_delta=self.SCORE_DELTA,
            fired=True,
            evidence={
                "user":        user_id,
                "matched_ip":  match.get("matched_ip"),
                "ioc_value":   match.get("ioc_value"),
                "confidence":  int(match.get("confidence") or 0),
                "threat_type": threat_type,
            },
        )

    def _fetch_match(
        self,
        tenant_id: str,
        user_id: str,
    ) -> dict | None:
        """Find the single highest-confidence IOC match for any IP
        this user used in the lookback window. Returns a dict or
        ``None`` if no match exists.

        We use LIMIT 1 because the rule only needs to know "does at
        least one IOC match exist" -- the evidence dict reports a
        single representative hit to keep the Incidents UI readable.
        """
        with self._db.conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    e.client_ip     AS matched_ip,
                    m.ioc_value     AS ioc_value,
                    m.confidence    AS confidence,
                    m.ioc_type      AS ioc_type,
                    m.indicator_name AS indicator_name
                FROM vector_events e
                JOIN vector_ioc_matches m ON m.ioc_value = e.client_ip
                WHERE e.tenant_id = %s
                  AND e.user_id = %s
                  AND e.client_ip IS NOT NULL
                  AND e.timestamp > NOW() - %s
                  AND m.confidence >= %s
                ORDER BY m.confidence DESC
                LIMIT 1
                """,
                (tenant_id, user_id, self.LOOKBACK, self.MIN_CONFIDENCE),
            )
            row = cur.fetchone()
        if not row:
            return None
        return {
            "matched_ip":     row[0],
            "ioc_value":      row[1],
            "confidence":     row[2],
            "ioc_type":       row[3],
            "indicator_name": row[4],
        }


# ISO-3166 alpha-2 → English name for the high-risk set so the
# evidence dict carries a human-readable label the Incidents UI can
# show without an extra lookup.
_COUNTRY_NAMES: dict[str, str] = {
    "CN": "China",
    "RU": "Russia",
    "IR": "Iran",
    "KP": "North Korea",
    "BY": "Belarus",
    "CU": "Cuba",
    "SY": "Syria",
    "VE": "Venezuela",
    "MM": "Myanmar",
    "SD": "Sudan",
}


class HighRiskCountryLoginRule(CorrelationRule):
    """Fires when a user logged in from an IP geolocated to a
    country on the high-risk list AND that country is not already
    in the user's baseline login_countries profile.

    The high-risk set is the union of OFAC-sanctioned jurisdictions
    and countries with well-documented state-sponsored cyber
    programmes. Any tenant can carve out legitimate exceptions via
    ``TENANT_EXCLUSIONS`` -- for example, GameChange Solar has a
    China office so CN is excluded for that tenant only.

    The baseline guard stops this rule from firing on a legitimate
    global organisation whose users routinely authenticate from a
    high-risk jurisdiction -- if the country is already a known
    part of their login pattern, the signal is redundant with the
    existing sign-ins and would just produce noise. First-time
    hits from a high-risk country still fire cleanly.

    IP resolution reuses the same ``ipinfo.io`` cache, rate-limit,
    and private-IP-skip logic as ``NewCountryLoginRule``. A single
    ``HighRiskCountryLoginRule`` instance shares its own cache so
    the same IP evaluated across multiple users in one cycle is
    resolved exactly once.

    Score: 40 points. Intentionally higher than ``NewCountryLogin``
    (+25) because a sanctioned-country origin is a stronger signal,
    but still below the 80-point incident threshold on its own so a
    legitimate traveller whose tenant exclusion list simply hasn't
    been updated doesn't auto-page.
    """

    name = "HighRiskCountryLogin"
    SCORE_DELTA = 40
    LOOKBACK = timedelta(hours=24)

    HIGH_RISK_COUNTRIES: set[str] = {
        "CN",  # China
        "RU",  # Russia
        "KP",  # North Korea
        "IR",  # Iran
        "BY",  # Belarus
        "CU",  # Cuba
        "SY",  # Syria
        "VE",  # Venezuela
        "MM",  # Myanmar
        "SD",  # Sudan
    }

    # Per-tenant exclusion overrides. Keys are tenant_id strings;
    # values are sets of ISO-3166 alpha-2 codes that are considered
    # legitimate for that tenant and should NOT fire.
    TENANT_EXCLUSIONS: dict[str, set[str]] = {
        "07b4c47a-e461-493e-91c4-90df73e2ebc6": {"CN"},  # GameChange Solar China office
    }

    # ipinfo plumbing (same tunables as NewCountryLoginRule so
    # operators see consistent behaviour across the two rules)
    IPINFO_URL_TEMPLATE = "https://ipinfo.io/{ip}/json"
    IPINFO_TIMEOUT = 5
    IPINFO_RATE_LIMIT_SEC = 1.0
    CACHE_TTL = timedelta(hours=24)

    def __init__(self) -> None:
        super().__init__()
        self._cache: dict[str, tuple[datetime, str | None]] = {}
        self._last_call_at: float = 0.0
        self._session: requests.Session | None = None

    # ----- evaluate -------------------------------------------------------

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
            logger.debug("[high_risk_country] no DB handle, skipping")
            return miss

        if self.is_excepted(tenant_id, {"user": user_id}):
            return miss

        try:
            login_ips = self._fetch_login_ips(tenant_id, user_id)
        except Exception:
            logger.exception(
                "[high_risk_country] login IP fetch failed",
                extra={"tenant_id": tenant_id, "user_id": user_id},
            )
            try:
                self._db.conn.rollback()
            except Exception:
                pass
            return miss

        if not login_ips:
            return miss

        exclusions = self.TENANT_EXCLUSIONS.get(tenant_id) or set()
        baseline_countries = self._baseline_country_set(user_profile)

        for ip in login_ips:
            country = self._resolve_country(ip)
            if country is None:
                continue
            upper = country.upper()
            if upper not in self.HIGH_RISK_COUNTRIES:
                continue
            if upper in exclusions:
                continue
            # Baseline gate: only fire if this high-risk country is
            # NOT already in the user's known login distribution.
            # A user who has legitimately been authenticating from
            # this country shouldn't trigger a new incident every
            # cycle just because the code is high-risk.
            if upper in baseline_countries:
                continue
            return RuleResult(
                rule_name=self.rule_name,
                score_delta=self.SCORE_DELTA,
                fired=True,
                evidence={
                    "user":               user_id,
                    "country_code":       upper,
                    "ip":                 ip,
                    "baseline_countries": sorted(baseline_countries),
                    "is_new_country":     True,
                    "country_name":       _COUNTRY_NAMES.get(upper, upper),
                },
            )

        return miss

    # ----- database -------------------------------------------------------

    @staticmethod
    def _baseline_country_set(user_profile: dict) -> set[str]:
        """Extract the user's known-country set from the baseline
        profile. Tolerates both the object shape
        ``{"US": 42, "DE": 3}`` (what BaselineEngine currently
        writes) and a flat array shape so a future schema tweak
        doesn't silently break the rule. Codes are upper-cased so
        matching against ``HIGH_RISK_COUNTRIES`` works regardless
        of how the baseline was stored."""
        raw = user_profile.get("login_countries") if user_profile else None
        if isinstance(raw, dict):
            return {str(k).upper() for k in raw.keys() if k}
        if isinstance(raw, (list, tuple, set)):
            return {str(v).upper() for v in raw if v}
        return set()

    def _fetch_login_ips(
        self,
        tenant_id: str,
        user_id: str,
    ) -> list[str]:
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

    # ----- ipinfo resolver (mirrors NewCountryLoginRule) ------------------

    @staticmethod
    def _is_skippable_ip(ip: str) -> bool:
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
            logger.debug("[high_risk_country] ipinfo failed ip=%s err=%s", ip, exc)
            return None

        if not resp.ok:
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


class VPNLoginRule(CorrelationRule):
    """Fires when a user's recent login originated from an IP whose
    ASN is a commercial VPN exit or a hosting/cloud provider.

    Commodity VPN traffic is a strong pre-compromise tell: attackers
    routinely route credential-stuffing and session-replay attempts
    through NordVPN / ExpressVPN / Mullvad / ProtonVPN / PIA /
    Surfshark so their true origin doesn't land in ``vector_events``.
    Hosting ASNs (``asn.type == "hosting"``) catch the "bot traffic
    bounced off a rented VPS" variant of the same pattern.

    Score: 20 points. Deliberately low on its own -- many legitimate
    employees use a personal VPN, and some tenants require one for
    remote work. The rule is intended to stack with NewCountryLogin
    (+25), OffHoursLogin (+15), or HighRiskCountry (+40) to push a
    suspicious session over the 80-point incident threshold.

    IP resolution reuses the ``ipinfo.io`` pattern already in
    ``NewCountryLoginRule`` / ``HighRiskCountryLoginRule`` -- same
    timeout, rate-limit, and private-IP-skip logic. A single rule
    instance shares a per-cycle cache so multiple users hitting the
    same VPN exit only produce one outbound lookup.

    When ipinfo.io returns a paid-tier ``asn`` sub-object, the rule
    uses ``asn.type`` / ``asn.name`` directly. On the free tier the
    ``asn`` block is absent and we fall back to parsing the free
    ``org`` field (format: ``"AS15169 Google LLC"``) -- ``asn_type``
    is then ``None`` and detection relies solely on the VPN-provider
    name match.
    """

    name = "VPNLogin"
    SCORE_DELTA = 20
    LOOKBACK = timedelta(hours=24)

    # Case-insensitive substrings matched against the ASN org name.
    # Covers the six operators the spec calls out plus obvious
    # aliases so tenants that resolve ProtonVPN as "Proton AG" or
    # PIA as "Private Internet Access" still fire cleanly.
    VPN_PROVIDER_MARKERS: tuple[str, ...] = (
        "nordvpn",
        "expressvpn",
        "mullvad",
        "protonvpn",
        "proton ag",
        "private internet access",
        "surfshark",
    )

    IPINFO_URL_TEMPLATE = "https://ipinfo.io/{ip}/json"
    IPINFO_TIMEOUT = 5
    IPINFO_RATE_LIMIT_SEC = 1.0
    CACHE_TTL = timedelta(hours=24)

    def __init__(self) -> None:
        super().__init__()
        # ip -> (expires_at, {asn_name, asn_type} | None)
        self._cache: dict[str, tuple[datetime, dict | None]] = {}
        self._last_call_at: float = 0.0
        self._session: requests.Session | None = None

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
            logger.debug("[vpn_login] no DB handle, skipping")
            return miss

        if self.is_excepted(tenant_id, {"user": user_id}):
            return miss

        try:
            login_ips = self._fetch_login_ips(tenant_id, user_id)
        except Exception:
            logger.exception(
                "[vpn_login] login IP fetch failed",
                extra={"tenant_id": tenant_id, "user_id": user_id},
            )
            try:
                self._db.conn.rollback()
            except Exception:
                pass
            return miss

        if not login_ips:
            return miss

        for ip in login_ips:
            if self.is_excepted(tenant_id, {"ip": ip}):
                continue
            info = self._resolve_asn(ip)
            if not info:
                continue
            asn_name = info.get("asn_name") or ""
            asn_type = (info.get("asn_type") or "").lower()
            name_lower = asn_name.lower()
            is_hosting = asn_type == "hosting"
            matched_provider = next(
                (
                    p for p in self.VPN_PROVIDER_MARKERS
                    if p in name_lower
                ),
                None,
            )
            if not is_hosting and matched_provider is None:
                continue
            return RuleResult(
                rule_name=self.rule_name,
                score_delta=self.SCORE_DELTA,
                fired=True,
                evidence={
                    "user":     user_id,
                    "ip":       ip,
                    "asn_name": asn_name or None,
                    "asn_type": info.get("asn_type"),
                },
            )

        return miss

    # ----- database -------------------------------------------------------

    def _fetch_login_ips(
        self,
        tenant_id: str,
        user_id: str,
    ) -> list[str]:
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

    # ----- ipinfo resolver ------------------------------------------------

    @staticmethod
    def _is_skippable_ip(ip: str) -> bool:
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
        entry = self._cache.get(ip)
        if not entry:
            return (False, None)
        expires_at, info = entry
        if datetime.now(timezone.utc) > expires_at:
            self._cache.pop(ip, None)
            return (False, None)
        return (True, info)

    def _cache_put(self, ip: str, info: dict | None) -> None:
        self._cache[ip] = (
            datetime.now(timezone.utc) + self.CACHE_TTL,
            info,
        )

    def _rate_limit(self) -> None:
        elapsed = time.monotonic() - self._last_call_at
        if elapsed < self.IPINFO_RATE_LIMIT_SEC:
            time.sleep(self.IPINFO_RATE_LIMIT_SEC - elapsed)
        self._last_call_at = time.monotonic()

    def _resolve_asn(self, ip: str) -> dict | None:
        """Return ``{asn_name, asn_type}`` for ``ip``.

        Prefers the paid-tier ``asn`` sub-object; falls back to
        parsing the free-tier ``org`` field (``"AS15169 Google LLC"``
        shape) in which case ``asn_type`` is ``None``. Returns
        ``None`` when the IP is private / unresolved / the network
        call fails.
        """
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
            logger.debug("[vpn_login] ipinfo failed ip=%s err=%s", ip, exc)
            return None

        if not resp.ok:
            if 400 <= resp.status_code < 500 and resp.status_code != 429:
                self._cache_put(ip, None)
            return None

        try:
            data = resp.json()
        except ValueError:
            self._cache_put(ip, None)
            return None

        info = self._extract_asn(data)
        self._cache_put(ip, info)
        return info

    @staticmethod
    def _extract_asn(data: dict) -> dict | None:
        asn_block = data.get("asn") if isinstance(data, dict) else None
        if isinstance(asn_block, dict):
            return {
                "asn_name": (asn_block.get("name") or "").strip() or None,
                "asn_type": (asn_block.get("type") or "").strip() or None,
            }
        # Free-tier fallback: parse "AS15169 Google LLC" out of org.
        org = (data.get("org") or "").strip() if isinstance(data, dict) else ""
        if not org:
            return None
        parts = org.split(" ", 1)
        name = parts[1].strip() if len(parts) > 1 else org
        return {"asn_name": name or None, "asn_type": None}


class ImpossibleTravelRule(CorrelationRule):
    """Fires when two of a user's logins happen in different countries
    inside a 2-hour window and the physical distance between the two
    IPs exceeds 500 km -- i.e. the user would need to move faster
    than any commercial flight to have been at both endpoints.

    The check:

    1. Pull every ``UserLoggedIn`` row with a ``client_ip`` in the
       lookback window, earliest-first.
    2. Geolocate each IP via ``ipinfo.io`` (country + ``loc``
       ``"lat,lng"`` string).
    3. Scan consecutive pairs; fire on the first pair that satisfies
       ``different countries AND distance > 500 km AND time_delta <
       2h``.

    Distance uses the haversine formula on the ``loc`` coordinates.
    An IP without a ``loc`` is skipped (country-only ipinfo responses
    are rare but do happen for anycast prefixes). The earliest
    qualifying pair wins so evidence is deterministic across cycles.

    Score: 50 points. High-confidence on its own -- there are very
    few false-positive paths once the geo + time check both pass.
    Still below the 80-point incident threshold by design so a
    mobile-data reroute followed by a desktop login doesn't auto-
    page; one co-signal (new country, off-hours, VPN) confirms.
    """

    name = "ImpossibleTravel"
    SCORE_DELTA = 50
    LOOKBACK = timedelta(hours=24)
    MAX_WINDOW = timedelta(hours=2)
    MIN_DISTANCE_KM = 500.0

    IPINFO_URL_TEMPLATE = "https://ipinfo.io/{ip}/json"
    IPINFO_TIMEOUT = 5
    IPINFO_RATE_LIMIT_SEC = 1.0
    CACHE_TTL = timedelta(hours=24)
    EARTH_RADIUS_KM = 6371.0

    def __init__(self) -> None:
        super().__init__()
        # ip -> (expires_at, {country, lat, lng} | None)
        self._cache: dict[str, tuple[datetime, dict | None]] = {}
        self._last_call_at: float = 0.0
        self._session: requests.Session | None = None

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
            logger.debug("[impossible_travel] no DB handle, skipping")
            return miss

        if self.is_excepted(tenant_id, {"user": user_id}):
            return miss

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
            return miss

        # Resolve each login's IP once, preserving chronological order.
        resolved: list[tuple[datetime, str, dict]] = []
        for ts, ip in logins:
            if not isinstance(ts, datetime) or not ip:
                continue
            info = self._resolve_geo(ip)
            if not info or not info.get("country"):
                continue
            if info.get("lat") is None or info.get("lng") is None:
                continue
            resolved.append((ts, ip, info))

        if len(resolved) < 2:
            return miss

        for i in range(1, len(resolved)):
            ts_b, ip_b, info_b = resolved[i]
            ts_a, ip_a, info_a = resolved[i - 1]
            country_a = (info_a.get("country") or "").upper()
            country_b = (info_b.get("country") or "").upper()
            if not country_a or not country_b:
                continue
            if country_a == country_b:
                continue
            time_delta = ts_b - ts_a
            if time_delta <= timedelta(0) or time_delta > self.MAX_WINDOW:
                continue
            distance_km = self._haversine_km(
                info_a["lat"], info_a["lng"],
                info_b["lat"], info_b["lng"],
            )
            if distance_km <= self.MIN_DISTANCE_KM:
                continue
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
                    "distance_km":        round(distance_km, 1),
                    "time_delta_minutes": int(time_delta.total_seconds() // 60),
                },
            )

        return miss

    # ----- database -------------------------------------------------------

    def _fetch_logins(
        self,
        tenant_id: str,
        user_id: str,
    ) -> list[tuple[datetime, str]]:
        with self._db.conn.cursor() as cur:
            cur.execute(
                """
                SELECT timestamp, client_ip
                FROM vector_events
                WHERE tenant_id = %s
                  AND user_id = %s
                  AND event_type = 'UserLoggedIn'
                  AND client_ip IS NOT NULL
                  AND timestamp > NOW() - %s
                ORDER BY timestamp ASC
                """,
                (tenant_id, user_id, self.LOOKBACK),
            )
            return [(row[0], row[1]) for row in cur.fetchall()]

    # ----- math -----------------------------------------------------------

    @classmethod
    def _haversine_km(
        cls,
        lat1: float,
        lng1: float,
        lat2: float,
        lng2: float,
    ) -> float:
        phi1 = math.radians(lat1)
        phi2 = math.radians(lat2)
        d_phi = math.radians(lat2 - lat1)
        d_lambda = math.radians(lng2 - lng1)
        a = (
            math.sin(d_phi / 2) ** 2
            + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2
        )
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return cls.EARTH_RADIUS_KM * c

    # ----- ipinfo resolver ------------------------------------------------

    @staticmethod
    def _is_skippable_ip(ip: str) -> bool:
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
        entry = self._cache.get(ip)
        if not entry:
            return (False, None)
        expires_at, info = entry
        if datetime.now(timezone.utc) > expires_at:
            self._cache.pop(ip, None)
            return (False, None)
        return (True, info)

    def _cache_put(self, ip: str, info: dict | None) -> None:
        self._cache[ip] = (
            datetime.now(timezone.utc) + self.CACHE_TTL,
            info,
        )

    def _rate_limit(self) -> None:
        elapsed = time.monotonic() - self._last_call_at
        if elapsed < self.IPINFO_RATE_LIMIT_SEC:
            time.sleep(self.IPINFO_RATE_LIMIT_SEC - elapsed)
        self._last_call_at = time.monotonic()

    def _resolve_geo(self, ip: str) -> dict | None:
        """Return ``{country, lat, lng}`` for ``ip`` or ``None``.

        The ``loc`` field from ipinfo.io is a ``"lat,lng"`` string;
        we split and parse once, stashing floats in the cache so
        subsequent pair comparisons don't re-parse.
        """
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
            logger.debug("[impossible_travel] ipinfo failed ip=%s err=%s", ip, exc)
            return None

        if not resp.ok:
            if 400 <= resp.status_code < 500 and resp.status_code != 429:
                self._cache_put(ip, None)
            return None

        try:
            data = resp.json()
        except ValueError:
            self._cache_put(ip, None)
            return None

        country = (data.get("country") or "").strip() or None
        lat: float | None = None
        lng: float | None = None
        loc = (data.get("loc") or "").strip()
        if loc:
            parts = loc.split(",", 1)
            if len(parts) == 2:
                try:
                    lat = float(parts[0])
                    lng = float(parts[1])
                except ValueError:
                    lat = None
                    lng = None
        info = {"country": country, "lat": lat, "lng": lng}
        self._cache_put(ip, info)
        return info


class InboxRuleCreatedRule(CorrelationRule):
    """Fires when the user created (or modified) an Exchange inbox
    rule in the last 24 hours whose action pattern matches known BEC
    tradecraft: forwarding to an external address, deleting matched
    messages, or moving them to low-visibility folders like RSS or
    Deleted Items.

    Inbox rules are the textbook BEC persistence mechanism. Once an
    attacker phishes credentials, step one is almost always an inbox
    rule that:

    * forwards incoming mail to a throwaway external mailbox so the
      attacker sees password-reset confirmations without being
      logged in, or
    * moves mail matching keywords like "invoice", "wire", "bank"
      into ``RSS Feeds`` or ``Deleted Items`` so the victim never
      sees finance threads while the attacker manipulates them.

    We match the UAL event type ``New-InboxRule`` (and
    ``NewInboxRule``, which Graph sometimes uses). The action payload
    lives in ``raw_json.Parameters`` as an array of
    ``{Name, Value}`` pairs -- we scan those for the suspicious
    action keywords and only fire when at least one matches. Benign
    inbox rules (e.g. "move Slack digests to a folder") therefore
    don't produce noise.

    Score: 35 points. Strong-enough on its own to stack with a
    single co-signal (new country, off-hours, VPN) and cross the 80-
    point incident threshold. Low enough that a legitimate user
    creating an auto-forward to a partner domain doesn't auto-page
    in isolation -- a second anomaly is still required.
    """

    name = "InboxRuleCreated"
    SCORE_DELTA = 45
    LOOKBACK = timedelta(hours=24)

    MATCHED_EVENT_TYPES = (
        "New-InboxRule",
        "NewInboxRule",
        "Set-InboxRule",
        "Enable-InboxRule",
    )

    # Case-insensitive substrings on the *parameter name* side of
    # the UAL Parameters array that indicate suspicious actions.
    SUSPICIOUS_PARAM_MARKERS: tuple[str, ...] = (
        "forwardto",
        "forwardasattachmentto",
        "redirectto",
        "deletemessage",
        "movetofolder",
        "markasread",
    )

    # Case-insensitive substrings on the *parameter value* side that
    # indicate a hide-messages destination. Applied only when the
    # parameter name is ``MoveToFolder`` so we don't over-match.
    HIDING_FOLDER_MARKERS: tuple[str, ...] = (
        "archive",
        "rss",
        "deleted items",
        "junk",
        "conversation history",
    )

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
            logger.debug("[inbox_rule] no DB handle, skipping")
            return miss

        if self.is_excepted(tenant_id, {"user": user_id}):
            return miss

        try:
            rows = self._fetch_inbox_rule_events(tenant_id, user_id)
        except Exception:
            logger.exception(
                "[inbox_rule] fetch failed",
                extra={"tenant_id": tenant_id, "user_id": user_id},
            )
            try:
                self._db.conn.rollback()
            except Exception:
                pass
            return miss

        if not rows:
            return miss

        for ts, raw in rows:
            params = self._parameters(raw)
            if not params:
                continue
            rule_name   = self._rule_name_from_params(params, raw)
            action      = self._suspicious_action(params)
            name_sus    = self._is_suspicious_name(rule_name or "")

            if action is None and not name_sus:
                continue

            destination = None
            if action:
                if action.startswith("forward:"):
                    destination = action[len("forward:"):]
                elif action.startswith("move:"):
                    destination = action[len("move:"):]

            suspicious_reason = action or ("suspicious_name" if name_sus else "")

            return RuleResult(
                rule_name=self.rule_name,
                score_delta=self.SCORE_DELTA,
                fired=True,
                evidence={
                    "user":             user_id,
                    "rule_name":        rule_name,
                    "rule_action":      action or "suspicious_name",
                    "suspicious_reason": suspicious_reason,
                    "destination":      destination,
                    "timestamp":        ts.isoformat() if isinstance(ts, datetime) else str(ts),
                },
            )

        return miss

    # ----- database -------------------------------------------------------

    def _fetch_inbox_rule_events(
        self,
        tenant_id: str,
        user_id: str,
    ) -> list[tuple[datetime, Any]]:
        with self._db.conn.cursor() as cur:
            cur.execute(
                """
                SELECT timestamp, raw_json
                FROM vector_events
                WHERE tenant_id = %s
                  AND user_id = %s
                  AND event_type = ANY(%s)
                  AND timestamp > NOW() - %s
                ORDER BY timestamp ASC
                """,
                (
                    tenant_id,
                    user_id,
                    list(self.MATCHED_EVENT_TYPES),
                    self.LOOKBACK,
                ),
            )
            return [(row[0], row[1]) for row in cur.fetchall()]

    # ----- raw_json parsing ----------------------------------------------

    @staticmethod
    def _parameters(raw: Any) -> list[dict]:
        """UAL rows store the cmdlet arguments in
        ``raw_json.Parameters`` as an array of ``{Name, Value}``.
        Tolerate both a dict (already parsed JSON) and a JSON string,
        and return a flat list of param dicts."""
        if raw is None:
            return []
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except (ValueError, TypeError):
                return []
        if not isinstance(raw, dict):
            return []
        params = raw.get("Parameters")
        if not isinstance(params, list):
            return []
        return [p for p in params if isinstance(p, dict)]

    @staticmethod
    def _rule_name_from_params(params: list[dict], raw: Any) -> str | None:
        for p in params:
            name = (p.get("Name") or "").lower()
            if name == "name":
                val = p.get("Value")
                if isinstance(val, str) and val.strip():
                    return val.strip()
        if isinstance(raw, dict):
            obj_id = raw.get("ObjectId")
            if isinstance(obj_id, str) and obj_id.strip():
                return obj_id.strip()
        return None

    @classmethod
    def _suspicious_action(cls, params: list[dict]) -> str | None:
        """Return a short action label if ``params`` contains a
        suspicious cmdlet argument, else ``None``.

        The label is surfaced on the Incidents UI so operators can
        triage without clicking into the raw event:
        ``"forward:addr"`` / ``"delete"`` / ``"move:RSS Feeds"`` /
        ``"mark_as_read"`` etc.
        """
        for p in params:
            name = (p.get("Name") or "").lower()
            if not name:
                continue
            matched = next(
                (m for m in cls.SUSPICIOUS_PARAM_MARKERS if m in name),
                None,
            )
            if matched is None:
                continue
            if matched == "movetofolder":
                value = p.get("Value")
                value_str = value if isinstance(value, str) else ""
                value_lower = value_str.lower()
                hiding = next(
                    (h for h in cls.HIDING_FOLDER_MARKERS if h in value_lower),
                    None,
                )
                if hiding is None:
                    continue
                return f"move:{value_str or hiding}"
            if matched in ("forwardto", "forwardasattachmentto", "redirectto"):
                value = p.get("Value")
                target = value if isinstance(value, str) else ""
                return f"forward:{target}" if target else "forward"
            if matched == "deletemessage":
                return "delete"
            if matched == "markasread":
                value = p.get("Value")
                truthy = (
                    (isinstance(value, bool) and value)
                    or (isinstance(value, str) and value.lower() in ("true", "1", "yes"))
                )
                if truthy:
                    return "mark_as_read"
        return None

    @staticmethod
    def _is_suspicious_name(name: str) -> bool:
        """Return True if the rule display name looks adversarial."""
        s = name.strip()
        if not s:
            return True
        if len(s) == 1:
            return True
        if s.isdigit():
            return True
        if all(c in ".,- \t" for c in s):
            return True
        return False


class MassEmailDeleteRule(CorrelationRule):
    """Fires when a user produced more than 50 mailbox-delete events
    inside a rolling 1-hour window.

    We match the UAL event types ``HardDelete`` and ``SoftDelete`` --
    HardDelete is mailbox purge (cannot be recovered by the user),
    SoftDelete is deletion-to-Deleted-Items. Either one at volume is
    a strong post-compromise tell: an attacker who has read a
    mailbox often purges evidence of their reads, or deletes threads
    matching a keyword list ("invoice", "wire", etc.) before the
    real owner logs back in.

    The count comes from a single COUNT(*) against ``vector_events``
    with the matching event types inside ``LOOKBACK``. We return the
    earliest and latest event timestamps so the Incidents UI can
    plot the delete burst on the evidence timeline.

    Score: 40 points. Meaningful on its own but still below the 80-
    point incident threshold so a genuine mailbox-cleanup day (rare
    but it happens) doesn't auto-page operators. Stacks cleanly
    with new-country / off-hours / VPN co-signals.
    """

    name = "MassEmailDelete"
    SCORE_DELTA = 40
    LOOKBACK = timedelta(hours=1)
    THRESHOLD = 50

    MATCHED_EVENT_TYPES = ("HardDelete", "SoftDelete")

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
            logger.debug("[mass_email_delete] no DB handle, skipping")
            return miss

        if self.is_excepted(tenant_id, {"user": user_id}):
            return miss

        try:
            count, first_ts, last_ts = self._fetch_delete_stats(
                tenant_id, user_id,
            )
        except Exception:
            logger.exception(
                "[mass_email_delete] stats fetch failed",
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
                "user":         user_id,
                "delete_count": int(count),
                "window_start": first_ts.isoformat() if isinstance(first_ts, datetime) else None,
                "window_end":   last_ts.isoformat() if isinstance(last_ts, datetime) else None,
            },
        )

    def _fetch_delete_stats(
        self,
        tenant_id: str,
        user_id: str,
    ) -> tuple[int, datetime | None, datetime | None]:
        """Return ``(count, first_ts, last_ts)`` for the user's
        HardDelete/SoftDelete events inside the lookback window."""
        with self._db.conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*)::bigint,
                    MIN(timestamp),
                    MAX(timestamp)
                FROM vector_events
                WHERE tenant_id = %s
                  AND user_id = %s
                  AND event_type = ANY(%s)
                  AND timestamp > NOW() - %s
                """,
                (
                    tenant_id,
                    user_id,
                    list(self.MATCHED_EVENT_TYPES),
                    self.LOOKBACK,
                ),
            )
            row = cur.fetchone()
        if not row:
            return (0, None, None)
        return (int(row[0] or 0), row[1], row[2])


class NewDeviceLoginRule(CorrelationRule):
    """Fires when a user logged in from a device that does not
    appear in their baseline device profile.

    The baseline is read from ``vector_user_baselines`` -- the
    profile dict passed into ``evaluate`` exposes ``known_devices``
    today. We also peek at ``known_device_ids`` so a future schema
    rename doesn't silently regress this rule. Each baseline device
    list is a flat array of device-id strings (typically the Azure
    AD ``DeviceId`` GUID or a Defender device token).

    We extract the current session's device-id from the raw UAL
    row. UAL stores device identity in a few shapes depending on
    the connector; this rule tolerates all of them:

    * ``raw_json.DeviceProperties`` array containing a dict with
      ``Name == 'DeviceId'`` / ``Name == 'Id'`` (Graph / UAL shape)
    * ``raw_json.DeviceId`` (top-level, Defender shape)
    * ``raw_json.ExtendedProperties`` array with ``Name``
      matching any of the above (ECS shape)

    Fail-open contract (per spec): if the baseline is empty, the
    baseline has no device list, or the event has no parsable
    device id, the rule returns a clean miss. This avoids paging
    on a user's very first login (no baseline yet) or on event
    sources that don't carry a device identifier at all.

    Score: 25 points. Intended to stack with NewCountryLogin (+25)
    or OffHoursLogin (+15) to cross the 80-point incident threshold
    without being noisy on its own when a user legitimately buys
    a new laptop.
    """

    name = "NewDeviceLogin"
    SCORE_DELTA = 25
    LOOKBACK = timedelta(hours=24)

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
            logger.debug("[new_device] no DB handle, skipping")
            return miss

        if self.is_excepted(tenant_id, {"user": user_id}):
            return miss

        # Fail-open when no baseline device list exists yet.
        known = self._known_device_set(user_profile)
        if not known:
            return miss

        try:
            rows = self._fetch_login_events(tenant_id, user_id)
        except Exception:
            logger.exception(
                "[new_device] login fetch failed",
                extra={"tenant_id": tenant_id, "user_id": user_id},
            )
            try:
                self._db.conn.rollback()
            except Exception:
                pass
            return miss

        for ts, raw in rows:
            device_id = self._extract_device_id(raw)
            if not device_id:
                # Fail-open per spec: events without a device_id
                # field are skipped silently, not forced to fire.
                continue
            if device_id in known:
                continue
            if self.is_excepted(tenant_id, {"device": device_id}):
                continue
            return RuleResult(
                rule_name=self.rule_name,
                score_delta=self.SCORE_DELTA,
                fired=True,
                evidence={
                    "user":       user_id,
                    "device_id":  device_id,
                    "hostname":   self._extract_device_name(raw),
                    "first_seen": ts.isoformat() if isinstance(ts, datetime) else None,
                },
            )

        return miss

    # ----- database -------------------------------------------------------

    def _fetch_login_events(
        self,
        tenant_id: str,
        user_id: str,
    ) -> list[tuple[datetime, Any]]:
        with self._db.conn.cursor() as cur:
            cur.execute(
                """
                SELECT timestamp, raw_json
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

    # ----- helpers --------------------------------------------------------

    @staticmethod
    def _known_device_set(user_profile: dict) -> set[str]:
        """Flat string set of every known device id across both the
        current schema (``known_devices``) and the forward-compatible
        ``known_device_ids`` key the spec calls out."""
        if not user_profile:
            return set()
        out: set[str] = set()
        for key in ("known_device_ids", "known_devices"):
            raw = user_profile.get(key)
            if not raw:
                continue
            if isinstance(raw, dict):
                for k in raw.keys():
                    if k:
                        out.add(str(k).strip())
                continue
            if isinstance(raw, (list, tuple, set)):
                for v in raw:
                    if v:
                        out.add(str(v).strip())
        return out

    @staticmethod
    @staticmethod
    def _extract_device_name(raw: Any) -> str | None:
        """Extract DisplayName from DeviceProperties array."""
        if not isinstance(raw, dict):
            return None
        block = raw.get("DeviceProperties")
        if not isinstance(block, list):
            return None
        for entry in block:
            if not isinstance(entry, dict):
                continue
            name = (entry.get("Name") or "").strip()
            if name == "DisplayName":
                val = entry.get("Value") or ""
                return val.strip() or None
        return None

    @staticmethod
    def _extract_device_id(raw: Any) -> str | None:
        """Walk the known UAL shapes for a device identifier.
        Returns the first non-empty value found, else None."""
        if raw is None:
            return None
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except (ValueError, TypeError):
                return None
        if not isinstance(raw, dict):
            return None

        # Shape 1: top-level DeviceId / deviceId
        for key in ("DeviceId", "deviceId", "device_id"):
            val = raw.get(key)
            if isinstance(val, str) and val.strip():
                return val.strip()

        # Shape 2: DeviceProperties / ExtendedProperties arrays
        for array_key in ("DeviceProperties", "ExtendedProperties"):
            block = raw.get(array_key)
            if not isinstance(block, list):
                continue
            for entry in block:
                if not isinstance(entry, dict):
                    continue
                name = (entry.get("Name") or entry.get("name") or "").lower()
                if name not in ("deviceid", "id", "device_id"):
                    continue
                val = entry.get("Value") or entry.get("value")
                if isinstance(val, str) and val.strip():
                    return val.strip()
        return None


class PrivilegedRoleAssignedRule(CorrelationRule):
    """Fires when a user was assigned a high-privilege directory
    role in the last 24 hours.

    The UAL operation is ``Add member to role.`` (note the trailing
    period; that's the actual string Azure AD emits). The role name
    is carried in ``raw_json.ModifiedProperties`` under the
    ``Role.DisplayName`` key. The assignee's identity lives in the
    ``Target`` array; the admin who performed the assignment lives
    in either ``UserId`` at the top level or the ``Actor`` array.

    The rule only fires for the highest-risk roles -- assigning a
    low-privilege role like ``Directory Readers`` is routine and
    should not page. We match these as case-insensitive substrings
    against the role display name:

    * Global Admin / Global Administrator
    * Security Admin / Security Administrator
    * Exchange Admin / Exchange Administrator
    * SharePoint Admin / SharePoint Administrator
    * User Admin / User Administrator

    Score: 45 points. High because privileged-role assignment is
    one of the sharper post-compromise escalation signals. Still
    below the 80-point threshold on its own so a legitimate admin
    promoting a new hire doesn't auto-page in isolation -- one
    co-signal (new country, VPN, off-hours) is the gate.
    """

    name = "PrivilegedRoleAssigned"
    SCORE_DELTA = 45
    LOOKBACK = timedelta(hours=24)

    MATCHED_EVENT_TYPES = ("Add member to role.", "AddMemberToRole")

    # Case-insensitive substrings matched against the role display
    # name from ModifiedProperties. The base word ("Global Admin")
    # also matches the long form ("Global Administrator") so we
    # don't need both variants.
    HIGH_RISK_ROLE_MARKERS: tuple[str, ...] = (
        "global admin",
        "security admin",
        "exchange admin",
        "sharepoint admin",
        "user admin",
    )

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
            logger.debug("[privileged_role] no DB handle, skipping")
            return miss

        if self.is_excepted(tenant_id, {"user": user_id}):
            return miss

        try:
            rows = self._fetch_role_events(tenant_id, user_id)
        except Exception:
            logger.exception(
                "[privileged_role] fetch failed",
                extra={"tenant_id": tenant_id, "user_id": user_id},
            )
            try:
                self._db.conn.rollback()
            except Exception:
                pass
            return miss

        for ts, raw in rows:
            role_name = self._extract_role_name(raw)
            if not role_name:
                continue
            if not self._is_privileged(role_name):
                continue
            assigned_by = self._extract_actor(raw)
            return RuleResult(
                rule_name=self.rule_name,
                score_delta=self.SCORE_DELTA,
                fired=True,
                evidence={
                    "user":        user_id,
                    "role_name":   role_name,
                    "assigned_by": assigned_by,
                    "timestamp":   ts.isoformat() if isinstance(ts, datetime) else None,
                },
            )

        return miss

    # ----- database -------------------------------------------------------

    def _fetch_role_events(
        self,
        tenant_id: str,
        user_id: str,
    ) -> list[tuple[datetime, Any]]:
        with self._db.conn.cursor() as cur:
            cur.execute(
                """
                SELECT timestamp, raw_json
                FROM vector_events
                WHERE tenant_id = %s
                  AND user_id = %s
                  AND event_type = ANY(%s)
                  AND timestamp > NOW() - %s
                ORDER BY timestamp ASC
                """,
                (
                    tenant_id,
                    user_id,
                    list(self.MATCHED_EVENT_TYPES),
                    self.LOOKBACK,
                ),
            )
            return [(row[0], row[1]) for row in cur.fetchall()]

    # ----- raw_json parsing ----------------------------------------------

    @classmethod
    def _is_privileged(cls, role_name: str) -> bool:
        lower = role_name.lower()
        return any(m in lower for m in cls.HIGH_RISK_ROLE_MARKERS)

    @staticmethod
    def _coerce_dict(raw: Any) -> dict | None:
        if raw is None:
            return None
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except (ValueError, TypeError):
                return None
        return raw if isinstance(raw, dict) else None

    @classmethod
    def _extract_role_name(cls, raw: Any) -> str | None:
        """Walk ``ModifiedProperties`` for the ``Role.DisplayName``
        entry. Tolerates both UAL shapes (``NewValue`` as a quoted
        JSON array string and as a plain string)."""
        data = cls._coerce_dict(raw)
        if data is None:
            return None
        mods = data.get("ModifiedProperties")
        if not isinstance(mods, list):
            return None
        for entry in mods:
            if not isinstance(entry, dict):
                continue
            name = (entry.get("Name") or "").strip()
            if name not in ("Role.DisplayName", "RoleName", "Role.Name"):
                continue
            new_val = entry.get("NewValue")
            parsed = cls._unquote_ual_value(new_val)
            if parsed:
                return parsed
        return None

    @classmethod
    def _extract_actor(cls, raw: Any) -> str | None:
        data = cls._coerce_dict(raw)
        if data is None:
            return None
        # Top-level UserId is the most common for "Add member to role."
        uid = data.get("UserId")
        if isinstance(uid, str) and uid.strip():
            return uid.strip()
        actor = data.get("Actor")
        if isinstance(actor, list):
            for entry in actor:
                if not isinstance(entry, dict):
                    continue
                aid = entry.get("ID") or entry.get("Id") or entry.get("id")
                if isinstance(aid, str) and aid.strip():
                    return aid.strip()
        return None

    @staticmethod
    def _unquote_ual_value(val: Any) -> str | None:
        """UAL sometimes stores display names as a JSON-encoded
        string containing a single-element array -- e.g. the raw
        value is literally ``'["Global Administrator"]'``. Handle
        both that shape and the plain-string shape without double-
        decoding."""
        if val is None:
            return None
        if isinstance(val, list):
            for item in val:
                if isinstance(item, str) and item.strip():
                    return item.strip()
            return None
        if isinstance(val, str):
            stripped = val.strip()
            if not stripped:
                return None
            if stripped.startswith("[") and stripped.endswith("]"):
                try:
                    parsed = json.loads(stripped)
                except (ValueError, TypeError):
                    return stripped
                if isinstance(parsed, list):
                    for item in parsed:
                        if isinstance(item, str) and item.strip():
                            return item.strip()
                    return None
                if isinstance(parsed, str):
                    return parsed.strip() or None
                return None
            return stripped
        return None


class MFAMethodChangedRule(CorrelationRule):
    """Fires when a user's MFA method was changed inside the lookback window.

    Monitors UAL "Update user." events whose ModifiedProperties list
    contains a StrongAuthenticationMethod entry. MFA method changes are
    a reliable post-compromise signal: an attacker who has obtained
    initial access will reset the victim's MFA to one they control so
    they can maintain persistence even after the password is rotated.

    Score: 40 points. High enough that a second co-signal (new country,
    VPN, off-hours) immediately clears the incident threshold.
    """

    name = "MFAMethodChanged"
    SCORE_DELTA = 40
    LOOKBACK = timedelta(hours=24)

    MFA_PROP_KEY = "StrongAuthenticationMethod"
    EVENT_TYPE = "Update user."

    def evaluate(
        self,
        events: list[dict],
        user_profile: dict,
    ) -> RuleResult:
        miss = RuleResult(rule_name=self.rule_name, score_delta=0, fired=False)
        if not events:
            return miss

        first = events[0]
        tenant_id = first.get("tenant_id")
        user_id = first.get("user_id")
        if not tenant_id or not user_id:
            return miss

        if self._db is None:
            logger.debug("[mfa_changed] no DB handle, skipping")
            return miss

        if self.is_excepted(tenant_id, {"user": user_id}):
            return miss

        try:
            rows = self._fetch_mfa_events(tenant_id, user_id)
        except Exception:
            logger.exception(
                "[mfa_changed] fetch failed",
                extra={"tenant_id": tenant_id, "user_id": user_id},
            )
            try:
                self._db.conn.rollback()
            except Exception:
                pass
            return miss

        for ts, raw in rows:
            old_method, new_method = self._extract_methods(raw)
            return RuleResult(
                rule_name=self.rule_name,
                score_delta=self.SCORE_DELTA,
                fired=True,
                evidence={
                    "user":       user_id,
                    "old_method": old_method,
                    "new_method": new_method,
                    "timestamp":  ts.isoformat() if isinstance(ts, datetime) else None,
                },
            )

        return miss

    # ----- database -------------------------------------------------------

    def _fetch_mfa_events(
        self,
        tenant_id: str,
        user_id: str,
    ) -> list[tuple]:
        with self._db.conn.cursor() as cur:
            cur.execute(
                """
                SELECT timestamp, raw_json
                FROM vector_events
                WHERE tenant_id = %s
                  AND user_id = %s
                  AND event_type = %s
                  AND raw_json::text ILIKE %s
                  AND timestamp > NOW() - %s
                ORDER BY timestamp DESC
                """,
                (
                    tenant_id,
                    user_id,
                    self.EVENT_TYPE,
                    f"%{self.MFA_PROP_KEY}%",
                    self.LOOKBACK,
                ),
            )
            return [(row[0], row[1]) for row in cur.fetchall()]

    # ----- raw_json parsing -----------------------------------------------

    @classmethod
    def _extract_methods(cls, raw: Any) -> tuple[str | None, str | None]:
        """Return (old_method, new_method) from ModifiedProperties."""
        if raw is None:
            return (None, None)
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except (ValueError, TypeError):
                return (None, None)
        if not isinstance(raw, dict):
            return (None, None)
        mods = raw.get("ModifiedProperties")
        if not isinstance(mods, list):
            return (None, None)
        for entry in mods:
            if not isinstance(entry, dict):
                continue
            if cls.MFA_PROP_KEY not in (entry.get("Name") or ""):
                continue
            old_val = entry.get("OldValue") or entry.get("old_value")
            new_val = entry.get("NewValue") or entry.get("new_value")
            return (
                cls._parse_method_value(old_val),
                cls._parse_method_value(new_val),
            )
        return (None, None)

    @staticmethod
    def _parse_method_value(val: Any) -> str | None:
        """Normalise UAL's many shapes for a StrongAuthenticationMethod
        value into a human-readable string."""
        if val is None:
            return None
        if isinstance(val, list) and val:
            entry = val[0]
            if isinstance(entry, dict):
                return (
                    entry.get("MethodType")
                    or entry.get("methodType")
                    or str(entry)
                )
            return str(entry)
        if isinstance(val, str):
            stripped = val.strip()
            if not stripped:
                return None
            try:
                parsed = json.loads(stripped)
            except (ValueError, TypeError):
                return stripped
            if isinstance(parsed, list) and parsed:
                entry = parsed[0]
                if isinstance(entry, dict):
                    return (
                        entry.get("MethodType")
                        or entry.get("methodType")
                        or str(entry)
                    )
                return str(entry)
            if isinstance(parsed, str):
                return parsed.strip() or None
            return stripped
        return str(val) if val else None


class ServicePrincipalLoginRule(CorrelationRule):
    """Fires when a UserLoggedIn event comes from an unknown client app.

    Azure AD records the client application ID (AppId) on every
    sign-in. A curated list of known-safe Microsoft first-party app IDs
    covers Office, Teams, SharePoint, and common admin portals. Any
    authentication that uses an app ID outside this list indicates a
    non-standard client -- which can be a legitimate third-party
    integration, but is also a common vector for token-theft replays and
    OAuth abuse.

    Score: 30 points. Designed to combine with other signals (new
    country, off-hours, impossible travel) rather than fire alone.
    """

    name = "ServicePrincipalLogin"
    SCORE_DELTA = 30
    LOOKBACK = timedelta(hours=24)

    # Well-known Microsoft first-party app IDs that are safe to skip.
    # GUIDs are stored lowercase-normalised.
    KNOWN_SAFE_APP_IDS: frozenset[str] = frozenset({
        "d3590ed6-52b3-4102-aeff-aad2292ab01c",  # Microsoft Office
        "00000002-0000-0ff1-ce00-000000000000",  # Office 365 Exchange Online
        "00000003-0000-0ff1-ce00-000000000000",  # SharePoint Online
        "00000004-0000-0ff1-ce00-000000000000",  # Skype for Business
        "1fec8e78-bce4-4aaf-ab1b-5451cc387264",  # Microsoft Teams
        "5e3ce6c0-2b1f-4285-8d4b-75ee78787346",  # Teams Web Client
        "4765445b-32c6-49b0-83e6-1d93765276ca",  # Office Online
        "cc15fd57-2c6c-4117-a88c-83b1d56b4bbe",  # Teams Desktop
        "57fb890c-0dab-4253-a5e0-7188c88b2bb4",  # SharePoint Online Client
        "b26aadf8-566f-4478-926f-589f601d9c74",  # OneDrive SyncEngine
        "ab9b8c07-8f02-4f72-87fa-80105867a763",  # OneDrive for Business
        "00000006-0000-0ff1-ce00-000000000000",  # Office 365 Portal
        "89bee1f7-5e6e-4d8a-9f3d-ecd601259da7",  # Office 365 Management APIs
        "797f4846-ba00-4fd7-ba43-dac1f8f63013",  # Azure AD Joined Devices
        "a0c73c16-a7e3-4564-9a95-2bdf47383716",  # Exchange ActiveSync
        "04b07795-8ddb-461a-bbee-02f9e1bf7b46",  # Microsoft Azure CLI
        "1950a258-227b-4e31-a9cf-717495945fc2",  # Microsoft Azure PowerShell
        "26a7ee05-5602-4d76-a7ba-eae8b7b67941",  # Windows Sign In
        "27922004-5251-4030-b22d-91ecd9a37ea4",  # Outlook Mobile
        "4e291c71-d680-4d0e-9640-0a3358e31177",  # PowerApps
        "cf36b471-5b44-428c-9ce7-313bf84528de",  # Microsoft Authenticator
        "fc0f3af4-6835-4174-b806-f7db311fd2f3",  # Microsoft Intune
        "38aa3b87-a06d-4817-b275-7a316988d93b",  # Windows Sign In (AAD broker)
    })

    def evaluate(
        self,
        events: list[dict],
        user_profile: dict,
    ) -> RuleResult:
        miss = RuleResult(rule_name=self.rule_name, score_delta=0, fired=False)
        if not events:
            return miss

        first = events[0]
        tenant_id = first.get("tenant_id")
        user_id = first.get("user_id")
        if not tenant_id or not user_id:
            return miss

        if self._db is None:
            logger.debug("[sp_login] no DB handle, skipping")
            return miss

        if self.is_excepted(tenant_id, {"user": user_id}):
            return miss

        try:
            rows = self._fetch_login_events(tenant_id, user_id)
        except Exception:
            logger.exception(
                "[sp_login] fetch failed",
                extra={"tenant_id": tenant_id, "user_id": user_id},
            )
            try:
                self._db.conn.rollback()
            except Exception:
                pass
            return miss

        for ts, raw in rows:
            app_id, app_name = self._extract_app(raw)
            if not app_id:
                continue
            if app_id.lower() in self.KNOWN_SAFE_APP_IDS:
                continue
            return RuleResult(
                rule_name=self.rule_name,
                score_delta=self.SCORE_DELTA,
                fired=True,
                evidence={
                    "user":      user_id,
                    "app_id":    app_id,
                    "app_name":  app_name,
                    "timestamp": ts.isoformat() if isinstance(ts, datetime) else None,
                },
            )

        return miss

    # ----- database -------------------------------------------------------

    def _fetch_login_events(
        self,
        tenant_id: str,
        user_id: str,
    ) -> list[tuple]:
        with self._db.conn.cursor() as cur:
            cur.execute(
                """
                SELECT timestamp, raw_json
                FROM vector_events
                WHERE tenant_id = %s
                  AND user_id = %s
                  AND event_type = 'UserLoggedIn'
                  AND timestamp > NOW() - %s
                ORDER BY timestamp DESC
                """,
                (tenant_id, user_id, self.LOOKBACK),
            )
            return [(row[0], row[1]) for row in cur.fetchall()]

    # ----- raw_json parsing -----------------------------------------------

    @staticmethod
    def _extract_app(raw: Any) -> tuple[str | None, str | None]:
        """Return (app_id, app_name) from a login event's raw_json."""
        if raw is None:
            return (None, None)
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except (ValueError, TypeError):
                return (None, None)
        if not isinstance(raw, dict):
            return (None, None)
        app_id = (
            raw.get("ApplicationId")
            or raw.get("AppId")
            or raw.get("ClientAppId")
            or raw.get("client_app_id")
        )
        app_name = (
            raw.get("ApplicationName")
            or raw.get("AppName")
            or raw.get("AppDisplayName")
            or raw.get("app_name")
        )
        # Static lookup for common app IDs not labeled in UAL
        _APP_NAMES: dict[str, str] = {
            "d3590ed6-52b3-4102-aeff-aad2292ab01c": "Microsoft Office",
            "00000002-0000-0ff1-ce00-000000000000": "Exchange Online",
            "00000003-0000-0ff1-ce00-000000000000": "SharePoint Online",
            "1fec8e78-bce4-4aaf-ab1b-5451cc387264": "Microsoft Teams",
            "5e3ce6c0-2b1f-4285-8d4b-75ee78787346": "Teams Web Client",
            "4765445b-32c6-49b0-83e6-1d93765276ca": "Azure AD Join",
            "27922004-5251-4030-b22d-91ecd9a37ea4": "Outlook Mobile",
            "b26aadf8-566f-4478-926f-589f601d9c74": "OneDrive Sync",
            "ab9b8c07-8f02-4f72-87fa-80105867a763": "OneDrive for Business",
            "04b07795-8ddb-461a-bbee-02f9e1bf7b46": "Azure CLI",
            "1950a258-227b-4e31-a9cf-717495945fc2": "Azure PowerShell",
            "fc0f3af4-6835-4174-b806-f7db311fd2f3": "Microsoft Intune",
            "38aa3b87-a06d-4817-b275-7a316988d93b": "LF App (38aa)",
            "120929d6-8abb-4d6e-9bce-d8df341f45cb": "LF App (1209)",
            "9199bf20-a13f-4107-85dc-02114787ef48": "LF App (9199)",
            "cd711a14-210d-4cca-8ec7-716042ce05b4": "LF App (cd71)",
        }
        if not app_name and app_id:
            app_name = _APP_NAMES.get(str(app_id).strip().lower())
        return (
            str(app_id).strip() if app_id else None,
            str(app_name).strip() if app_name else None,
        )


class PasswordSprayRule(CorrelationRule):
    """Fires when 5+ failed logins come from one IP across 3+ user accounts.

    Password-spray attacks try a single common password against many
    accounts from one source IP to stay below per-account lockout
    thresholds. The pattern is: one IP, many distinct targets, many
    failures, all within a short window.

    For each user's failed-login source IPs, this rule queries across
    all users in the tenant to count total attempts and distinct targets
    from that IP in the last 10 minutes. When attempt_count >=
    ATTEMPT_THRESHOLD and distinct users >= USER_THRESHOLD the rule
    fires against the user being evaluated.

    Known office IPs are excluded. The exclusion list is loaded once
    from ``vector_known_ips`` (fail-open: if the table is absent all
    IPs are evaluated).

    Score: 55 points.
    """

    name = "PasswordSpray"
    SCORE_DELTA = 55
    WINDOW = timedelta(minutes=10)
    ATTEMPT_THRESHOLD = 5
    USER_THRESHOLD = 3

    FAILED_STATUSES: frozenset[str] = frozenset({"failed", "failure", "0"})

    def __init__(self) -> None:
        super().__init__()
        self._known_office_ips: set[str] = set()
        self._office_ips_loaded: bool = False

    def evaluate(
        self,
        events: list[dict],
        user_profile: dict,
    ) -> RuleResult:
        miss = RuleResult(rule_name=self.rule_name, score_delta=0, fired=False)
        if not events:
            return miss

        first = events[0]
        tenant_id = first.get("tenant_id")
        user_id = first.get("user_id")
        if not tenant_id or not user_id:
            return miss

        if self._db is None:
            logger.debug("[password_spray] no DB handle, skipping")
            return miss

        if self.is_excepted(tenant_id, {"user": user_id}):
            return miss

        if not self._office_ips_loaded:
            self._load_known_ips()

        failed_ips = self._extract_failed_ips(events)
        if not failed_ips:
            return miss

        for ip in failed_ips:
            if ip in self._known_office_ips:
                continue
            try:
                attempt_count, affected_users = self._query_spray(tenant_id, ip)
            except Exception:
                logger.exception(
                    "[password_spray] spray query failed",
                    extra={"tenant_id": tenant_id, "ip": ip},
                )
                try:
                    self._db.conn.rollback()
                except Exception:
                    pass
                continue

            if (
                attempt_count >= self.ATTEMPT_THRESHOLD
                and len(affected_users) >= self.USER_THRESHOLD
            ):
                return RuleResult(
                    rule_name=self.rule_name,
                    score_delta=self.SCORE_DELTA,
                    fired=True,
                    evidence={
                        "source_ip":      ip,
                        "affected_users": sorted(affected_users),
                        "attempt_count":  attempt_count,
                        "window_minutes": int(self.WINDOW.total_seconds() // 60),
                    },
                )

        return miss

    # ----- office IP exclusion --------------------------------------------

    def _load_known_ips(self) -> None:
        """Load office IPs from vector_known_ips. Fails open (empty set)
        when the table doesn't exist or the query errors."""
        self._office_ips_loaded = True
        if self._db is None:
            return
        try:
            with self._db.conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT ip_address FROM vector_known_ips
                    WHERE ip_type = 'office' OR ip_type IS NULL
                    """
                )
                self._known_office_ips = {
                    row[0] for row in cur.fetchall() if row[0]
                }
        except Exception:
            logger.debug(
                "[password_spray] vector_known_ips unavailable, fail-open",
            )
            try:
                self._db.conn.rollback()
            except Exception:
                pass
            self._known_office_ips = set()

    # ----- helpers --------------------------------------------------------

    def _extract_failed_ips(self, events: list[dict]) -> set[str]:
        """Return distinct source IPs from this user's failed logins."""
        ips: set[str] = set()
        for ev in events:
            if ev.get("event_type") != "UserLoggedIn":
                continue
            status = (ev.get("result_status") or "").lower()
            if status not in self.FAILED_STATUSES:
                continue
            ip = ev.get("client_ip")
            if ip:
                ips.add(str(ip))
        return ips

    def _query_spray(
        self,
        tenant_id: str,
        ip: str,
    ) -> tuple[int, set[str]]:
        """Return (total_attempt_count, distinct_user_set) for failed
        logins from this IP within WINDOW across all users."""
        with self._db.conn.cursor() as cur:
            cur.execute(
                """
                SELECT user_id, COUNT(*) AS attempts
                FROM vector_events
                WHERE tenant_id = %s
                  AND client_ip = %s
                  AND event_type = 'UserLoggedIn'
                  AND LOWER(result_status) = ANY(%s)
                  AND timestamp > NOW() - %s
                GROUP BY user_id
                """,
                (
                    tenant_id,
                    ip,
                    list(self.FAILED_STATUSES),
                    self.WINDOW,
                ),
            )
            rows = cur.fetchall()
        user_set = {row[0] for row in rows if row[0]}
        total = sum(int(row[1] or 0) for row in rows)
        return (total, user_set)


class AttachmentOpenedPostLoginRule(CorrelationRule):
    """Fires when a login from a new country or outside normal hours is
    followed within 30 minutes by a file-download or file-preview event.

    The sequence — authenticate from an unusual context, immediately
    pull an attachment — is a reliable business-email-compromise pattern.
    Legitimate travellers occasionally trigger it, but the combination
    with ExternalSharingSpike or ImpossibleTravel quickly raises the
    aggregate score above the incident threshold.

    Detection window: the login must have occurred within the last 30
    minutes, and the file event must follow the login within 30 minutes.

    Score: 30 points.
    """

    name = "AttachmentOpenedPostLogin"
    SCORE_DELTA = 30
    WINDOW = timedelta(minutes=30)

    LOGIN_EVENT_TYPE = "UserLoggedIn"
    FILE_EVENT_TYPES: frozenset[str] = frozenset({
        "FileDownloaded", "FilePreviewed", "FileAccessed",
    })

    # Country codes considered high-risk for the "new country" half of
    # the trigger. The rule also fires when the login is off-hours even
    # if the country is baseline-normal -- the country is included in
    # evidence regardless.
    HIGH_RISK_COUNTRIES: frozenset[str] = frozenset({
        "CN", "RU", "KP", "IR", "SY", "CU",
    })

    def evaluate(
        self,
        events: list[dict],
        user_profile: dict,
    ) -> RuleResult:
        miss = RuleResult(rule_name=self.rule_name, score_delta=0, fired=False)
        if not events:
            return miss

        first = events[0]
        tenant_id = first.get("tenant_id")
        user_id = first.get("user_id")
        if not tenant_id or not user_id:
            return miss

        if self._db is None:
            logger.debug("[attach_post_login] no DB handle, skipping")
            return miss

        if self.is_excepted(tenant_id, {"user": user_id}):
            return miss

        known_countries: set[str] = set()
        raw_countries = user_profile.get("login_countries") or {}
        if isinstance(raw_countries, dict):
            known_countries = {k for k in raw_countries if k}
        elif isinstance(raw_countries, (list, tuple, set)):
            known_countries = {str(v) for v in raw_countries if v}

        login_hours: dict = {}
        raw_hours = user_profile.get("login_hours") or {}
        if isinstance(raw_hours, dict):
            login_hours = raw_hours

        try:
            login_rows = self._fetch_logins(tenant_id, user_id)
            file_rows = self._fetch_file_events(tenant_id, user_id)
        except Exception:
            logger.exception(
                "[attach_post_login] fetch failed",
                extra={"tenant_id": tenant_id, "user_id": user_id},
            )
            try:
                self._db.conn.rollback()
            except Exception:
                pass
            return miss

        if not login_rows or not file_rows:
            return miss

        for login_ts, login_country, login_ip in login_rows:
            if not isinstance(login_ts, datetime):
                continue

            unusual = self._is_unusual_login(
                login_ts, login_country, known_countries, login_hours,
            )
            if not unusual:
                continue

            for file_ts, file_name, file_size in file_rows:
                if not isinstance(file_ts, datetime):
                    continue
                if file_ts < login_ts:
                    continue
                delta = file_ts - login_ts
                if delta > self.WINDOW:
                    continue

                return RuleResult(
                    rule_name=self.rule_name,
                    score_delta=self.SCORE_DELTA,
                    fired=True,
                    evidence={
                        "user":               user_id,
                        "login_country":      login_country,
                        "file_name":          file_name,
                        "file_size":          file_size,
                        "time_delta_minutes": round(
                            delta.total_seconds() / 60, 1
                        ),
                    },
                )

        return miss

    # ----- helpers --------------------------------------------------------

    @staticmethod
    def _is_unusual_login(
        ts: datetime,
        country: str | None,
        known_countries: set[str],
        login_hours: dict,
    ) -> bool:
        """Return True when the login country is new/high-risk OR the
        hour is outside the user's baseline login hours."""
        if country and known_countries and country not in known_countries:
            return True
        if country and country in AttachmentOpenedPostLoginRule.HIGH_RISK_COUNTRIES:
            return True
        if login_hours:
            hour_key = str(ts.hour)
            if hour_key not in login_hours:
                return True
        return False

    # ----- database -------------------------------------------------------

    def _fetch_logins(
        self,
        tenant_id: str,
        user_id: str,
    ) -> list[tuple]:
        with self._db.conn.cursor() as cur:
            cur.execute(
                """
                SELECT timestamp,
                       raw_json->>'Country' AS country,
                       client_ip
                FROM vector_events
                WHERE tenant_id = %s
                  AND user_id = %s
                  AND event_type = %s
                  AND timestamp > NOW() - %s
                ORDER BY timestamp DESC
                """,
                (tenant_id, user_id, self.LOGIN_EVENT_TYPE, self.WINDOW),
            )
            return [(row[0], row[1], row[2]) for row in cur.fetchall()]

    def _fetch_file_events(
        self,
        tenant_id: str,
        user_id: str,
    ) -> list[tuple]:
        with self._db.conn.cursor() as cur:
            cur.execute(
                """
                SELECT timestamp,
                       COALESCE(
                           raw_json->>'SourceFileName',
                           raw_json->>'ObjectId',
                           entity_key
                       ) AS file_name,
                       (raw_json->>'FileSize')::bigint AS file_size
                FROM vector_events
                WHERE tenant_id = %s
                  AND user_id = %s
                  AND event_type = ANY(%s)
                  AND timestamp > NOW() - %s
                ORDER BY timestamp ASC
                """,
                (
                    tenant_id,
                    user_id,
                    list(self.FILE_EVENT_TYPES),
                    self.WINDOW * 2,
                ),
            )
            return [(row[0], row[1], row[2]) for row in cur.fetchall()]


class ExternalSharingSpikeRule(CorrelationRule):
    """Fires when a user creates 3+ external shares within one hour.

    ``SharingInvitationCreated`` and ``AnonymousLinkCreated`` are the
    UAL events emitted when a user shares a file or folder externally.
    Three or more such events inside a one-hour rolling window is
    anomalous for most users and is a reliable data-exfiltration
    precursor in BEC and insider-threat scenarios.

    The rule collects distinct external recipient addresses from the
    event payloads for the evidence dict so analysts can immediately
    see who received access without pivoting to a second query.

    Score: 35 points.
    """

    name = "ExternalSharingSpike"
    SCORE_DELTA = 35
    WINDOW = timedelta(hours=1)
    SHARE_THRESHOLD = 3

    SHARING_EVENT_TYPES: frozenset[str] = frozenset({
        "SharingInvitationCreated",
        "AnonymousLinkCreated",
        "SharingLinkCreated",
        "AnonymousLinkUpdated",
        "SharingLinkUpdated",
        "AddedToSecureLink",
    })

    def evaluate(
        self,
        events: list[dict],
        user_profile: dict,
    ) -> RuleResult:
        miss = RuleResult(rule_name=self.rule_name, score_delta=0, fired=False)
        if not events:
            return miss

        first = events[0]
        tenant_id = first.get("tenant_id")
        user_id = first.get("user_id")
        if not tenant_id or not user_id:
            return miss

        if self._db is None:
            logger.debug("[ext_sharing_spike] no DB handle, skipping")
            return miss

        if self.is_excepted(tenant_id, {"user": user_id}):
            return miss

        try:
            rows = self._fetch_sharing_events(tenant_id, user_id)
        except Exception:
            logger.exception(
                "[ext_sharing_spike] fetch failed",
                extra={"tenant_id": tenant_id, "user_id": user_id},
            )
            try:
                self._db.conn.rollback()
            except Exception:
                pass
            return miss

        if len(rows) < self.SHARE_THRESHOLD:
            return miss

        # Slide a 1-hour window over the sorted events.
        rows_sorted = sorted(rows, key=lambda r: r[0] or datetime.min)
        for i, (ts_start, _, _) in enumerate(rows_sorted):
            if not isinstance(ts_start, datetime):
                continue
            window_end = ts_start + self.WINDOW
            bucket = [
                r for r in rows_sorted[i:]
                if isinstance(r[0], datetime) and r[0] <= window_end
            ]
            if len(bucket) < self.SHARE_THRESHOLD:
                continue

            recipients: list[str] = []
            for _, raw, _ in bucket:
                rcpt = self._extract_recipient(raw)
                if rcpt and rcpt not in recipients:
                    recipients.append(rcpt)

            last_ts = bucket[-1][0]
            return RuleResult(
                rule_name=self.rule_name,
                score_delta=self.SCORE_DELTA,
                fired=True,
                evidence={
                    "user":               user_id,
                    "share_count":        len(bucket),
                    "external_recipients": recipients,
                    "window_start":       ts_start.isoformat(),
                    "window_end":         last_ts.isoformat()
                                          if isinstance(last_ts, datetime)
                                          else None,
                },
            )

        return miss

    # ----- database -------------------------------------------------------

    def _fetch_sharing_events(
        self,
        tenant_id: str,
        user_id: str,
    ) -> list[tuple]:
        with self._db.conn.cursor() as cur:
            cur.execute(
                """
                SELECT timestamp, raw_json, event_type
                FROM vector_events
                WHERE tenant_id = %s
                  AND user_id = %s
                  AND event_type = ANY(%s)
                  AND timestamp > NOW() - %s
                ORDER BY timestamp ASC
                """,
                (
                    tenant_id,
                    user_id,
                    list(self.SHARING_EVENT_TYPES),
                    self.WINDOW,
                ),
            )
            return [(row[0], row[1], row[2]) for row in cur.fetchall()]

    # ----- raw_json parsing -----------------------------------------------

    @staticmethod
    def _extract_recipient(raw: Any) -> str | None:
        """Pull the external recipient from a sharing event payload."""
        if raw is None:
            return None
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except (ValueError, TypeError):
                return None
        if not isinstance(raw, dict):
            return None
        for key in (
            "TargetUserOrGroupName",
            "InviteeEmail",
            "RecipientAddress",
            "SharedWith",
            "TargetUserOrGroupType",
        ):
            val = raw.get(key)
            if isinstance(val, str) and val.strip():
                return val.strip()
        return None


class AiTMDetectionRule(CorrelationRule):
    """Fires when indicators of an Adversary-in-the-Middle (AiTM) proxy
    attack are detected for a user within the last 2 hours.

    Two independent patterns are checked:

    Pattern 1 — Automated HTTP client authenticating on behalf of the user.
        User-agents like ``axios``, ``python-requests``, and ``curl`` are
        never used by legitimate browsers. Seeing them on ``UserLoggedIn``
        or ``UserLoginFailed`` events means an AiTM proxy (e.g. Evilginx2,
        Modlishka) is relaying credentials. The rule checks both the
        top-level ``UserAgent`` field and the ``ExtendedProperties`` JSONB
        array that Office 365 uses to carry the same data.

    Pattern 2 — Rapid OAuth consent burst (3+ grants in 10 minutes from one IP).
        An AiTM proxy that successfully captures a session cookie
        immediately attempts to establish persistence via OAuth app
        consents. Three or more consent/app-role events from the same
        source IP within a 10-minute window is a reliable persistence
        fingerprint.

    Score: 85 points (fires an incident on its own).
    """

    name = "AiTMDetection"
    SCORE_DELTA = 85
    WINDOW = timedelta(hours=2)

    _BOT_UA_PATTERNS: tuple[str, ...] = ("axios", "python-requests", "curl/")

    _OAUTH_EVENT_TYPES: frozenset[str] = frozenset({
        "Add app role assignment to service principal.",
        "Consent to application.",
        "Add OAuth2PermissionGrant.",
    })

    # Minimum OAuth consent events from the same IP in 10 minutes.
    _OAUTH_BURST_THRESHOLD = 3
    _OAUTH_BURST_WINDOW = timedelta(minutes=10)

    def evaluate(
        self,
        events: list[dict],
        user_profile: dict,
    ) -> RuleResult:
        miss = RuleResult(rule_name=self.rule_name, score_delta=0, fired=False)
        if not events:
            return miss

        first = events[0]
        tenant_id = first.get("tenant_id")
        user_id = first.get("user_id")
        if not tenant_id or not user_id:
            return miss

        if self._db is None:
            logger.debug("[aitm] no DB handle, skipping")
            return miss

        if self.is_excepted(tenant_id, {"user": user_id}):
            return miss

        try:
            hit = self._check_bot_ua(tenant_id, user_id)
            if hit:
                return RuleResult(
                    rule_name=self.rule_name,
                    score_delta=self.SCORE_DELTA,
                    fired=True,
                    evidence=hit,
                )

            hit = self._check_oauth_burst(tenant_id, user_id)
            if hit:
                return RuleResult(
                    rule_name=self.rule_name,
                    score_delta=self.SCORE_DELTA,
                    fired=True,
                    evidence=hit,
                )
        except Exception:
            logger.exception(
                "[aitm] evaluation error",
                extra={"tenant_id": tenant_id, "user_id": user_id},
            )
            try:
                self._db.conn.rollback()
            except Exception:
                pass

        return miss

    # ----- pattern 1: bot user-agent on auth events -----------------------

    def _check_bot_ua(
        self,
        tenant_id: str,
        user_id: str,
    ) -> dict | None:
        """Return evidence dict if a non-browser UA is seen on auth events."""
        with self._db.conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    timestamp,
                    raw_json->>'UserAgent'    AS ua_top,
                    raw_json->>'ClientIP'     AS client_ip,
                    event_type
                FROM vector_events
                WHERE tenant_id = %s
                  AND user_id = %s
                  AND timestamp > NOW() - %s
                  AND event_type IN ('UserLoggedIn', 'UserLoginFailed')
                  AND (
                    (
                      raw_json->>'UserAgent' ILIKE '%%axios%%'
                      OR raw_json->>'UserAgent' ILIKE '%%python-requests%%'
                      OR raw_json->>'UserAgent' ILIKE '%%curl/%%'
                    )
                    OR EXISTS (
                      SELECT 1
                      FROM jsonb_array_elements(
                        CASE
                          WHEN jsonb_typeof(raw_json->'ExtendedProperties') = 'array'
                          THEN raw_json->'ExtendedProperties'
                          ELSE '[]'::jsonb
                        END
                      ) ep
                      WHERE ep->>'Name' = 'UserAgent'
                        AND (
                          ep->>'Value' ILIKE '%%axios%%'
                          OR ep->>'Value' ILIKE '%%python-requests%%'
                          OR ep->>'Value' ILIKE '%%curl/%%'
                        )
                    )
                  )
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                (tenant_id, user_id, self.WINDOW),
            )
            row = cur.fetchone()

        if row is None:
            return None

        ts, ua_top, client_ip, event_type = row
        # Try to resolve the actual UA from ExtendedProperties when the
        # top-level field was empty.
        user_agent = ua_top or self._ua_from_extended(tenant_id, user_id, ts)
        return {
            "user":       user_id,
            "pattern":    "bot_user_agent",
            "user_agent": user_agent,
            "client_ip":  client_ip,
            "event_type": event_type,
            "timestamp":  ts.isoformat() if isinstance(ts, datetime) else str(ts),
        }

    def _ua_from_extended(
        self,
        tenant_id: str,
        user_id: str,
        ts: Any,
    ) -> str | None:
        """Resolve the UA from ExtendedProperties for a specific event."""
        try:
            with self._db.conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT ep->>'Value'
                    FROM vector_events,
                         jsonb_array_elements(
                           CASE
                             WHEN jsonb_typeof(raw_json->'ExtendedProperties') = 'array'
                             THEN raw_json->'ExtendedProperties'
                             ELSE '[]'::jsonb
                           END
                         ) ep
                    WHERE tenant_id = %s
                      AND user_id = %s
                      AND timestamp = %s
                      AND ep->>'Name' = 'UserAgent'
                    LIMIT 1
                    """,
                    (tenant_id, user_id, ts),
                )
                row = cur.fetchone()
            return row[0] if row else None
        except Exception:
            return None

    # ----- pattern 2: rapid OAuth consent burst ---------------------------

    def _check_oauth_burst(
        self,
        tenant_id: str,
        user_id: str,
    ) -> dict | None:
        """Return evidence dict if 3+ OAuth consent events occur within 10 min
        from the same source IP."""
        with self._db.conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    timestamp,
                    raw_json->>'ClientIP' AS client_ip,
                    event_type
                FROM vector_events
                WHERE tenant_id = %s
                  AND user_id = %s
                  AND timestamp > NOW() - %s
                  AND (
                    event_type = ANY(%s)
                    OR event_type ILIKE '%%oauth%%'
                  )
                ORDER BY timestamp ASC
                """,
                (tenant_id, user_id, self.WINDOW, list(self._OAUTH_EVENT_TYPES)),
            )
            rows = cur.fetchall()

        if len(rows) < self._OAUTH_BURST_THRESHOLD:
            return None

        # Group by source IP and slide a 10-minute window.
        from collections import defaultdict
        by_ip: dict[str, list] = defaultdict(list)
        for ts, ip, etype in rows:
            if ip:
                by_ip[ip].append((ts, etype))

        for ip, events_for_ip in by_ip.items():
            sorted_evts = sorted(
                events_for_ip,
                key=lambda x: x[0] if isinstance(x[0], datetime) else datetime.min,
            )
            for i, (ts_start, _) in enumerate(sorted_evts):
                if not isinstance(ts_start, datetime):
                    continue
                window_end = ts_start + self._OAUTH_BURST_WINDOW
                bucket = [
                    e for e in sorted_evts[i:]
                    if isinstance(e[0], datetime) and e[0] <= window_end
                ]
                if len(bucket) >= self._OAUTH_BURST_THRESHOLD:
                    return {
                        "user":        user_id,
                        "pattern":     "oauth_burst",
                        "client_ip":   ip,
                        "grant_count": len(bucket),
                        "window_start": ts_start.isoformat(),
                        "window_end":  bucket[-1][0].isoformat(),
                        "event_types": list({e[1] for e in bucket}),
                    }

        return None


class ThreatIntelMonitor:
    """Daily proactive IOC sweep against the local OpenCTI instance.

    Unlike ``IocEnricher`` (which runs every 5 minutes and checks
    only freshly-ingested events), this worker walks the last 7 days
    of ``vector_events`` once per day at 02:00 UTC, extracts every
    unique public ``client_ip``, and re-queries OpenCTI for each
    one. Any new hit is written into ``vector_ioc_matches`` where
    ``IOCMatchRule`` picks it up on the next scoring cycle.

    This catches the "retroactive IOC" case: an IP was ingested
    cleanly on day 1, OpenCTI added it to a threat-actor indicator
    on day 3, and without this sweep the historical match would
    never surface. The 7-day lookback is a deliberate compromise
    between catching slow threat-feed updates and keeping the
    per-cycle workload bounded -- most OpenCTI feeds publish within
    72 hours of observation.

    ``ThreatIntelMonitor`` is NOT a ``CorrelationRule`` -- it's a
    stand-alone worker that fits the vector-ingest main-loop
    ``poll_once()`` contract. The main loop calls ``poll_once()``
    every cycle; the worker internally checks the clock and only
    runs a real sweep when it's past 02:00 UTC and it hasn't
    already run today.
    """

    # Structured-logging anchors so main.py's log format stays
    # consistent with the other global workers.
    tenant_id = "*"
    client_name = "global"

    OPENCTI_URL = "http://localhost:8080/graphql"
    LOOKBACK = timedelta(days=7)
    RATE_LIMIT_SEC = 1.0
    REQUEST_TIMEOUT = 10
    DAILY_RUN_HOUR_UTC = 2  # 02:00 UTC
    MIN_CONFIDENCE = 50

    def __init__(self, db: Database) -> None:
        self._db = db
        # Last successful ``poll_once`` that actually performed a
        # sweep. Used to gate "already ran today" so the worker
        # runs exactly once per day even though the main loop
        # calls it every cycle.
        self._last_run: datetime | None = None
        self._last_call_at: float = 0.0
        self._session: requests.Session | None = None

    # ----- main-loop entrypoint ------------------------------------------

    def poll_once(self) -> None:
        """Main-loop entrypoint. Runs one daily sweep when we're
        past 02:00 UTC and haven't already run today; otherwise
        returns immediately. Unexpected exceptions are swallowed
        so a misconfigured OpenCTI never takes the ingest loop
        down."""
        now = datetime.now(timezone.utc)
        today_target = now.replace(
            hour=self.DAILY_RUN_HOUR_UTC,
            minute=0,
            second=0,
            microsecond=0,
        )
        # Too early -- haven't hit the daily window yet.
        if now < today_target:
            return
        # Already ran today.
        if self._last_run is not None and self._last_run >= today_target:
            return

        try:
            self._run_sweep()
        except Exception:
            logger.exception("[threat_intel] daily sweep crashed")
        finally:
            # Always advance the last-run clock so a persistent
            # failure doesn't spin us up on every cycle.
            self._last_run = datetime.now(timezone.utc)

    # ----- sweep orchestration -------------------------------------------

    def _run_sweep(self) -> None:
        logger.info("[threat_intel] starting daily sweep")
        candidates = self._fetch_unique_ips()
        logger.info(
            "[threat_intel] pulled %d candidate IP records", len(candidates),
        )

        checked = 0
        hit = 0
        inserted = 0
        skipped_private = 0

        for tenant_id, client_name, ip, event_id in candidates:
            if self._is_skippable_ip(ip):
                skipped_private += 1
                continue
            checked += 1
            try:
                indicators = self._query_opencti(ip)
            except Exception:
                logger.exception(
                    "[threat_intel] opencti query failed ip=%s", ip,
                )
                continue
            if not indicators:
                continue
            hit += 1
            for indicator in indicators:
                if int(indicator.get("confidence") or 0) < self.MIN_CONFIDENCE:
                    continue
                if self._insert_match(
                    tenant_id=tenant_id,
                    client_name=client_name,
                    ip=ip,
                    event_id=event_id,
                    indicator=indicator,
                ):
                    inserted += 1

        logger.info(
            "[threat_intel] sweep complete checked=%d hit=%d inserted=%d "
            "skipped_private=%d",
            checked, hit, inserted, skipped_private,
        )

    # ----- database helpers ----------------------------------------------

    def _fetch_unique_ips(
        self,
    ) -> list[tuple[str | None, str | None, str, Any]]:
        """Return one ``(tenant_id, client_name, client_ip, event_id)``
        tuple per unique ``client_ip`` seen in the lookback window.

        Uses ``DISTINCT ON (client_ip)`` with ``ORDER BY client_ip,
        timestamp DESC`` so each IP is keyed to its most recent
        vector_events row. That row's id becomes the
        ``matched_event_id`` on any match we insert, which lets the
        ``UNIQUE (ioc_value, matched_event_id)`` constraint dedupe
        re-runs of the same IP against the same representative
        event.
        """
        with self._db.conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT ON (client_ip)
                    tenant_id, client_name, client_ip, id
                FROM vector_events
                WHERE client_ip IS NOT NULL
                  AND timestamp > NOW() - %s
                ORDER BY client_ip, timestamp DESC
                """,
                (self.LOOKBACK,),
            )
            return [
                (row[0], row[1], row[2], row[3])
                for row in cur.fetchall()
            ]

    def _insert_match(
        self,
        tenant_id: str | None,
        client_name: str | None,
        ip: str,
        event_id: Any,
        indicator: dict,
    ) -> bool:
        """Insert a single match row. Returns True when a new row
        was written, False when the ``UNIQUE (ioc_value,
        matched_event_id)`` constraint caused the insert to be a
        no-op (i.e. we already knew about this match)."""
        try:
            with self._db.conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO vector_ioc_matches (
                        tenant_id, client_name, ioc_type, ioc_value,
                        opencti_id, indicator_name, confidence,
                        matched_event_id, raw_json
                    ) VALUES (
                        %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s::jsonb
                    )
                    ON CONFLICT (ioc_value, matched_event_id) DO NOTHING
                    RETURNING id::text
                    """,
                    (
                        tenant_id,
                        client_name,
                        "ipv4",
                        ip,
                        indicator.get("opencti_id"),
                        indicator.get("indicator_name"),
                        int(indicator.get("confidence") or 0),
                        event_id,
                        json.dumps({
                            "indicator": indicator,
                            "source":    "threat_intel_monitor",
                            "proactive": True,
                        }),
                    ),
                )
                row = cur.fetchone()
            self._db.conn.commit()
            return row is not None
        except Exception:
            logger.exception(
                "[threat_intel] insert_match failed ip=%s", ip,
            )
            try:
                self._db.conn.rollback()
            except Exception:
                pass
            return False

    # ----- OpenCTI HTTP --------------------------------------------------

    @staticmethod
    def _is_skippable_ip(ip: str) -> bool:
        """True for any address we shouldn't query OpenCTI for --
        RFC1918, loopback, link-local, multicast, unspecified, or
        anything unparseable."""
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

    def _rate_limit(self) -> None:
        elapsed = time.monotonic() - self._last_call_at
        if elapsed < self.RATE_LIMIT_SEC:
            time.sleep(self.RATE_LIMIT_SEC - elapsed)
        self._last_call_at = time.monotonic()

    def _query_opencti(self, ip: str) -> list[dict]:
        """POST an inline GraphQL query for one IP. Returns a list
        of normalised ``{opencti_id, indicator_name, confidence,
        description}`` dicts, or ``[]`` on any error / empty
        response.

        The query is inlined as a literal string because OpenCTI's
        GraphQL parser has historically rejected some parameterised
        filter shapes we care about. The IP is embedded after a
        quick ``_is_skippable_ip`` check above, which already
        ensures we're only ever interpolating well-formed numeric
        addresses -- no user-controlled input ever reaches this
        path.
        """
        self._rate_limit()
        if self._session is None:
            self._session = requests.Session()

        query = (
            "{ stixCyberObservables("
            "  filters: {mode: and,"
            f"  filters: [{{key: \"value\", values: [\"{ip}\"]}}],"
            "   filterGroups: []}"
            ") { edges { node {"
            "  id entity_type"
            "  ... on IPv4Addr { value }"
            "  indicators { edges { node {"
            "    id name confidence description"
            "  } } }"
            "} } } }"
        )

        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        token = os.environ.get("OPENCTI_TOKEN")
        if token:
            headers["Authorization"] = f"Bearer {token}"

        try:
            resp = self._session.post(
                self.OPENCTI_URL,
                json={"query": query},
                headers=headers,
                timeout=self.REQUEST_TIMEOUT,
            )
        except requests.RequestException as exc:
            logger.debug("[threat_intel] opencti request failed ip=%s err=%s", ip, exc)
            return []

        if not resp.ok:
            logger.debug(
                "[threat_intel] opencti non-2xx ip=%s status=%s",
                ip, resp.status_code,
            )
            return []

        try:
            data = resp.json()
        except ValueError:
            return []

        indicators: list[dict] = []
        edges = (
            (data.get("data") or {})
            .get("stixCyberObservables", {})
            .get("edges", [])
            or []
        )
        for edge in edges:
            node = (edge or {}).get("node") or {}
            observable_id = node.get("id")
            inner_edges = (
                (node.get("indicators") or {}).get("edges") or []
            )
            for inner in inner_edges:
                ind = (inner or {}).get("node") or {}
                try:
                    confidence = int(ind.get("confidence") or 0)
                except (TypeError, ValueError):
                    confidence = 0
                indicators.append({
                    "opencti_id":     ind.get("id") or observable_id,
                    "indicator_name": ind.get("name"),
                    "confidence":     confidence,
                    "description":    ind.get("description"),
                })
        return indicators
