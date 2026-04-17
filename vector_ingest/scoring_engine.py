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
INCIDENT_THRESHOLD = 25

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
                SuspiciousMailboxRule(),
                MalwareDetectedRule(),
                IOCMatchRule(),
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
        # IOC match or malware always escalates to critical regardless of score
        fired_rule_names = {r.rule_name for r in fired}
        if "IOCMatch" in fired_rule_names or "MalwareDetected" in fired_rule_names:
            severity = "critical"
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
