"""PostgreSQL access layer for the UAL ingest service.

Owns a single long-lived connection and exposes:
    - migrations runner
    - checkpoint read / write
    - bulk insert of normalized events with dedup

All writes use ON CONFLICT DO NOTHING against dedup_fingerprint so
re-running the poller against overlapping windows is safe.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)


INSERT_EVENT_SQL = """
INSERT INTO vector_events (
    tenant_id,
    client_name,
    user_id,
    entity_key,
    event_type,
    workload,
    result_status,
    client_ip,
    user_agent,
    timestamp,
    source,
    dedup_fingerprint,
    raw_json
) VALUES %s
ON CONFLICT (dedup_fingerprint) DO NOTHING
"""


INSERT_DEFENDER_ALERT_SQL = """
INSERT INTO vector_defender_alerts (
    id, tenant_id, client_name, incident_id, severity, status, category,
    threat_family, title, machine_id, computer_name, threat_name,
    logged_on_users, alert_creation_time, first_event_time, last_event_time,
    detection_source, investigation_state, mitre_techniques, raw_json
) VALUES (
    %(id)s, %(tenant_id)s, %(client_name)s, %(incident_id)s, %(severity)s,
    %(status)s, %(category)s, %(threat_family)s, %(title)s, %(machine_id)s,
    %(computer_name)s, %(threat_name)s, %(logged_on_users)s,
    %(alert_creation_time)s, %(first_event_time)s, %(last_event_time)s,
    %(detection_source)s, %(investigation_state)s, %(mitre_techniques)s,
    %(raw_json)s
)
ON CONFLICT (id) DO UPDATE SET
    status              = EXCLUDED.status,
    investigation_state = EXCLUDED.investigation_state,
    last_event_time     = EXCLUDED.last_event_time,
    raw_json            = EXCLUDED.raw_json
"""


INSERT_DEFENDER_HUNTING_SQL = """
INSERT INTO vector_defender_hunting (
    tenant_id, client_name, query_name, device_id, device_name,
    account_upn, action_type, timestamp, raw_json
) VALUES (
    %(tenant_id)s, %(client_name)s, %(query_name)s, %(device_id)s,
    %(device_name)s, %(account_upn)s, %(action_type)s, %(timestamp)s,
    %(raw_json)s
)
ON CONFLICT (tenant_id, query_name, device_id, timestamp) DO NOTHING
"""


INSERT_MESSAGE_TRACE_SQL = """
INSERT INTO vector_message_trace (
    tenant_id, client_name, message_id, sender_address, recipient_address,
    subject, received, status, size_bytes, direction, original_client_ip
) VALUES (
    %(tenant_id)s, %(client_name)s, %(message_id)s, %(sender_address)s,
    %(recipient_address)s, %(subject)s, %(received)s, %(status)s,
    %(size_bytes)s, %(direction)s, %(original_client_ip)s
)
ON CONFLICT (message_id) DO NOTHING
"""


class Database:
    def __init__(self) -> None:
        self._conn: psycopg2.extensions.connection | None = None

    # ------------------------------------------------------------------ connection
    def connect(self) -> None:
        host = os.environ.get("POSTGRES_HOST", "postgres")
        port = int(os.environ.get("POSTGRES_PORT", "5432"))
        dbname = os.environ.get("POSTGRES_DB", "nero_vector")
        user = os.environ.get("POSTGRES_USER", "nero_vector")
        password = os.environ.get("POSTGRES_PASSWORD", "")

        logger.info(
            "connecting to postgres",
            extra={"host": host, "port": port, "db": dbname, "user": user},
        )
        self._conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password,
            application_name="vector-ingest",
        )
        self._conn.autocommit = False
        # Pin the session timezone to UTC so TIMESTAMPTZ values round-trip
        # without any implicit local offset (the process may run in CEST, etc.).
        with self._conn.cursor() as cur:
            cur.execute("SET TIME ZONE 'UTC'")
        self._conn.commit()

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    @property
    def conn(self) -> psycopg2.extensions.connection:
        if self._conn is None:
            raise RuntimeError("Database.connect() has not been called")
        return self._conn

    # ------------------------------------------------------------------ migrations
    def run_migrations(self, migrations_dir: str | Path) -> None:
        migrations_path = Path(migrations_dir)
        if not migrations_path.is_dir():
            logger.warning(
                "migrations directory missing, skipping",
                extra={"path": str(migrations_path)},
            )
            return

        files = sorted(p for p in migrations_path.iterdir() if p.suffix == ".sql")
        for sql_file in files:
            logger.info("applying migration", extra={"file": sql_file.name})
            sql = sql_file.read_text(encoding="utf-8")
            with self.conn.cursor() as cur:
                cur.execute(sql)
            self.conn.commit()

    # ------------------------------------------------------------------ checkpoints
    @staticmethod
    def _to_naive_utc(ts: datetime) -> datetime:
        """Return ts as a naive datetime whose wall-clock is UTC.

        Accepts either an aware datetime (in any offset) or an already-naive
        datetime that the caller asserts is UTC. All checkpoint values must
        be naive UTC so they can be embedded directly in the O365 Management
        API startTime/endTime query params without a local-tz shift.
        """
        if ts.tzinfo is not None:
            ts = ts.astimezone(timezone.utc).replace(tzinfo=None)
        return ts

    def get_checkpoint(self, tenant_id: str, content_type: str) -> datetime | None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT last_ingested_at
                FROM vector_ingest_state
                WHERE tenant_id = %s AND content_type = %s
                """,
                (tenant_id, content_type),
            )
            row = cur.fetchone()
        if row and row[0] is not None:
            # psycopg2 returns TIMESTAMPTZ as an aware datetime in the session
            # timezone. We've pinned the session to UTC, but normalize defensively.
            return self._to_naive_utc(row[0])
        return None

    def update_checkpoint(
        self,
        tenant_id: str,
        client_name: str,
        content_type: str,
        last_ingested_at: datetime,
    ) -> None:
        checkpoint_utc = self._to_naive_utc(last_ingested_at)
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO vector_ingest_state (
                    tenant_id, client_name, content_type,
                    last_ingested_at, updated_at
                )
                VALUES (%s, %s, %s, %s, now())
                ON CONFLICT (tenant_id, content_type) DO UPDATE
                SET last_ingested_at = EXCLUDED.last_ingested_at,
                    client_name      = EXCLUDED.client_name,
                    updated_at       = now()
                """,
                (tenant_id, client_name, content_type, checkpoint_utc),
            )
        self.conn.commit()

    # ------------------------------------------------------------------ event insert
    def insert_events(self, events: Iterable[dict]) -> int:
        """Bulk insert normalized events. Returns the count actually written."""
        rows = []
        for ev in events:
            rows.append(
                (
                    ev["tenant_id"],
                    ev["client_name"],
                    ev.get("user_id"),
                    ev["entity_key"],
                    ev.get("event_type"),
                    ev.get("workload"),
                    ev.get("result_status"),
                    ev.get("client_ip"),
                    ev.get("user_agent"),
                    ev["timestamp"],
                    ev.get("source", "UAL"),
                    ev["dedup_fingerprint"],
                    json.dumps(ev["raw_json"]),
                )
            )

        if not rows:
            return 0

        with self.conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, INSERT_EVENT_SQL, rows)
            written = cur.rowcount
        self.conn.commit()
        return max(written, 0)

    # ------------------------------------------------------------------ defender
    def insert_defender_alert(self, row: dict) -> bool:
        """Upsert a single Defender alert. Returns True on any write."""
        payload = dict(row)
        payload["logged_on_users"] = json.dumps(payload.get("logged_on_users") or [])
        payload["mitre_techniques"] = json.dumps(payload.get("mitre_techniques") or [])
        payload["raw_json"] = json.dumps(payload["raw_json"])
        with self.conn.cursor() as cur:
            cur.execute(INSERT_DEFENDER_ALERT_SQL, payload)
            written = cur.rowcount
        self.conn.commit()
        return written > 0

    def insert_defender_hunting(self, row: dict) -> bool:
        """Insert a single Advanced Hunting result. De-dup via the table UNIQUE."""
        payload = dict(row)
        payload["raw_json"] = json.dumps(payload["raw_json"])
        with self.conn.cursor() as cur:
            cur.execute(INSERT_DEFENDER_HUNTING_SQL, payload)
            written = cur.rowcount
        self.conn.commit()
        return written > 0

    # ------------------------------------------------------------------ message trace
    def insert_message_trace(self, row: dict) -> bool:
        """Insert a single MessageTrace row, de-dup on message_id."""
        with self.conn.cursor() as cur:
            cur.execute(INSERT_MESSAGE_TRACE_SQL, row)
            written = cur.rowcount
        self.conn.commit()
        return written > 0
