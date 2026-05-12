"""BaselineEngine -- daily bulk behavioural baseline builder.

One single SQL statement rolls up the last 30 days of
``vector_events`` into every user's ``vector_user_baselines`` row
in a single round-trip. The previous implementation iterated users
in Python and issued 5-7 queries per user; on tenants with tens of
thousands of active identities that pattern turned into a 20-minute
stall once an hour. This replacement aggregates everything server-
side and upserts with ``ON CONFLICT DO UPDATE`` so the whole build
is a single prepared-plan execution.

Schema touched (read and write):

    vector_events           -- input, 30-day lookback
    vector_user_baselines   -- output, bulk upsert

Runs every 24 hours. ``poll_once()`` is called by the main ingest
loop every cycle; the engine internally checks whether 24 hours
have elapsed since the last successful build and short-circuits
otherwise.

The baselines produced:

    login_hours       jsonb object  {"0": count, "1": count, ...}
                                     built from UserLoggedIn events
    login_countries   jsonb array   distinct Country values seen
                                     on UserLoggedIn events
    login_asns        jsonb array   distinct ASN values seen on
                                     UserLoggedIn events
    known_devices     jsonb array   distinct raw_json->>'DeviceName'
    known_ips         jsonb array   distinct client_ip values
    avg_daily_events  float         events / 30
    avg_daily_logins  float         UserLoggedIn / 30
    baseline_days     int           days between MIN(timestamp) and
                                     now, clamped to >= 1

Log anchors ``tenant_id = "*"`` and ``client_name = "global"`` keep
structured-log fields consistent with the other global workers
(ThreatIntelMonitor, ScoringEngine) so main.py's JSON-line output
doesn't carry NULLs for this worker's lines.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone

from vector_ingest.db import Database

logger = logging.getLogger(__name__)


# Exact name specified by the main ingest loop -- do not rename.
BASELINE_POLL_INTERVAL = timedelta(hours=24)

# Event lookback window fed into the single SQL aggregation.
_LOOKBACK_DAYS = 30


# One big statement. CTEs:
#
#   events_30d   -- the raw 30-day window, filtered to rows that
#                   are usable (user_id + tenant_id not null).
#   hourly       -- per-(tenant, user) jsonb object of
#                   hour -> login_count. Separate CTE because the
#                   inner subquery groups by hour first and then
#                   rolls up by user; you can't express that in a
#                   single GROUP BY against the same row set.
#   per_user     -- per-(tenant, user) aggregates for everything
#                   else: country/asn/device/ip distinct sets plus
#                   avg_daily counters and baseline_days.
#
# The final INSERT ... SELECT ... ON CONFLICT DO UPDATE upserts
# one row per (tenant, user) with the joined CTE output.
#
# ``jsonb_agg(DISTINCT x) FILTER (WHERE ...)`` is the idiomatic
# way to produce a JSONB array of unique non-null values in one
# pass. FILTER runs before DISTINCT, so NULLs are dropped cleanly.
#
# ``INSERT ... ON CONFLICT DO UPDATE`` reports a rowcount equal to
# inserts + updates combined, which is exactly what we want to
# log as "users processed".
_BULK_UPSERT_SQL = """
WITH events_30d AS (
    SELECT tenant_id, user_id, timestamp, client_ip, event_type, raw_json
    FROM vector_events
    WHERE timestamp > NOW() - INTERVAL '30 days'
      AND user_id    IS NOT NULL
      AND tenant_id  IS NOT NULL
      AND user_id LIKE '%@%'
      AND user_id NOT LIKE '+%'
      AND user_id NOT LIKE 'ServicePrincipal_%'
),
hourly AS (
    SELECT tenant_id, user_id,
           jsonb_object_agg(hour_str, cnt) AS login_hours
    FROM (
        SELECT tenant_id, user_id,
               EXTRACT(HOUR FROM timestamp)::int::text AS hour_str,
               COUNT(*)::int                           AS cnt
        FROM events_30d
        WHERE event_type = 'UserLoggedIn'
        GROUP BY tenant_id, user_id,
                 EXTRACT(HOUR FROM timestamp)::int::text
    ) h
    GROUP BY tenant_id, user_id
),
per_user AS (
    SELECT
        tenant_id,
        user_id,
        COUNT(*)::double precision / 30.0
            AS avg_daily_events,
        COUNT(*) FILTER (WHERE event_type = 'UserLoggedIn')::double precision
            / 30.0
            AS avg_daily_logins,
        GREATEST(
            1,
            CEIL(EXTRACT(EPOCH FROM (NOW() - MIN(timestamp))) / 86400.0)::int
        ) AS baseline_days,
        COALESCE(
            jsonb_agg(DISTINCT raw_json->>'Country')
                FILTER (WHERE event_type = 'UserLoggedIn'
                          AND raw_json->>'Country' IS NOT NULL),
            '[]'::jsonb
        ) AS login_countries,
        COALESCE(
            jsonb_agg(DISTINCT raw_json->>'ASN')
                FILTER (WHERE event_type = 'UserLoggedIn'
                          AND raw_json->>'ASN' IS NOT NULL),
            '[]'::jsonb
        ) AS login_asns,
        COALESCE(
            jsonb_agg(DISTINCT client_ip)
                FILTER (WHERE client_ip IS NOT NULL),
            '[]'::jsonb
        ) AS known_ips,
        COALESCE(
            jsonb_agg(DISTINCT raw_json->>'DeviceName')
                FILTER (WHERE raw_json->>'DeviceName' IS NOT NULL),
            '[]'::jsonb
        ) AS known_devices
    FROM events_30d
    GROUP BY tenant_id, user_id
),
first_seen_all AS (
    -- Look back across ALL history (not just 30d) to find the true
    -- earliest event for each user. Kept as a separate CTE so the
    -- 30d window in events_30d doesn't affect the result.
    SELECT tenant_id, user_id, MIN(timestamp) AS first_seen
    FROM vector_events
    WHERE user_id   IS NOT NULL
      AND tenant_id IS NOT NULL
    GROUP BY tenant_id, user_id
)
INSERT INTO vector_user_baselines (
    tenant_id, user_id, computed_at,
    login_hours, login_countries, login_asns,
    known_devices, known_ips,
    avg_daily_events, avg_daily_logins, baseline_days,
    first_seen
)
SELECT
    u.tenant_id,
    u.user_id,
    NOW(),
    COALESCE(h.login_hours, '{}'::jsonb),
    u.login_countries,
    u.login_asns,
    u.known_devices,
    u.known_ips,
    u.avg_daily_events,
    u.avg_daily_logins,
    u.baseline_days,
    fs.first_seen
FROM per_user u
LEFT JOIN hourly h
    ON h.tenant_id = u.tenant_id
   AND h.user_id   = u.user_id
LEFT JOIN first_seen_all fs
    ON fs.tenant_id = u.tenant_id
   AND fs.user_id   = u.user_id
ON CONFLICT (tenant_id, user_id) DO UPDATE SET
    computed_at      = EXCLUDED.computed_at,
    login_hours      = EXCLUDED.login_hours,
    login_countries  = EXCLUDED.login_countries,
    login_asns       = EXCLUDED.login_asns,
    known_devices    = EXCLUDED.known_devices,
    known_ips        = EXCLUDED.known_ips,
    avg_daily_events = EXCLUDED.avg_daily_events,
    avg_daily_logins = EXCLUDED.avg_daily_logins,
    baseline_days    = EXCLUDED.baseline_days,
    first_seen       = LEAST(EXCLUDED.first_seen, vector_user_baselines.first_seen)
"""


class BaselineEngine:
    """Daily bulk baseline builder.

    ``poll_once()`` is the main-loop entrypoint. It runs one full
    bulk upsert when ``BASELINE_POLL_INTERVAL`` has elapsed since
    the last successful build, otherwise returns immediately. Any
    exception is logged, the DB transaction is rolled back, and
    the ``_last_run`` clock is still advanced so a persistently
    failing build doesn't spin us up on every 5-minute cycle.
    """

    # Structured-logging anchors for main.py's JSON log format so
    # every worker emits consistent tenant_id / client_name fields
    # on its log lines. The engine is tenant-global.
    tenant_id = "*"
    client_name = "global"

    def __init__(self, db: Database) -> None:
        self._db = db
        self._last_run: datetime | None = None

    def poll_once(self) -> None:
        now = datetime.now(timezone.utc)
        if (
            self._last_run is not None
            and now - self._last_run < BASELINE_POLL_INTERVAL
        ):
            return
        try:
            self._run_bulk_upsert()
        except Exception:
            logger.exception("[baseline] bulk build crashed")
            try:
                self._db.conn.rollback()
            except Exception:
                pass
        finally:
            # Always advance the clock, even on failure, so a broken
            # build doesn't re-trigger on every ingest cycle.
            self._last_run = datetime.now(timezone.utc)

    # ------------------------------------------------------------------

    def _run_bulk_upsert(self) -> None:
        start = time.monotonic()
        logger.info(
            "[baseline] bulk build starting lookback_days=%d",
            _LOOKBACK_DAYS,
        )

        with self._db.conn.cursor() as cur:
            cur.execute(_BULK_UPSERT_SQL)
            # Postgres' INSERT ... ON CONFLICT DO UPDATE reports a
            # rowcount equal to inserts + updates combined, which
            # is the right "users processed" number for this log.
            rows_written = max(0, cur.rowcount)
        self._db.conn.commit()

        duration_sec = time.monotonic() - start
        logger.info(
            "[baseline] bulk build complete users=%d duration_sec=%.2f",
            rows_written,
            duration_sec,
        )
