from __future__ import annotations
import json
import logging
from datetime import datetime, timedelta, timezone
from vector_ingest.db import Database

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


