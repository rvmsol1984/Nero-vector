-- Migration 008 -- ThreatLocker ActionLog ingest storage.
--
-- One row per ThreatLocker ActionLog entry produced by the
-- ThreatLockerIngestor poller. Dedup is on the vendor-supplied
-- eActionLogId so re-polling overlapping windows is safe.
--
-- ThreatLocker action_id semantics (from the portal API):
--   1  = Permit
--   2  = Deny
--   3  = Ringfenced (policy-blocked)
--   6  = Elevated / admin escalation
-- The governance endpoint filters on {2, 3, 6} so we surface the
-- meaningful blocking events.

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS vector_threatlocker_events (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id      VARCHAR(128),
    client_name    VARCHAR(256),
    action_log_id  VARCHAR(128) UNIQUE,
    event_time     TIMESTAMPTZ,
    hostname       VARCHAR(256),
    username       VARCHAR(512),
    full_path      TEXT,
    process_path   TEXT,
    action_type    VARCHAR(128),
    action         VARCHAR(128),
    action_id      INTEGER,
    policy_name    VARCHAR(256),
    hash           VARCHAR(128),
    raw_json       JSONB        NOT NULL,
    ingested_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_threatlocker_tenant_time
    ON vector_threatlocker_events (tenant_id, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_threatlocker_action_id
    ON vector_threatlocker_events (action_id);
CREATE INDEX IF NOT EXISTS idx_threatlocker_hostname
    ON vector_threatlocker_events (hostname);
CREATE INDEX IF NOT EXISTS idx_threatlocker_username
    ON vector_threatlocker_events (username);

-- Best-effort self-grant so the current role keeps its own rights
-- after any future table recreate. Wrapped in a DO/EXCEPTION block so
-- a fixture role that can't GRANT doesn't break the migration.
DO $$
BEGIN
    EXECUTE 'GRANT ALL PRIVILEGES ON TABLE vector_threatlocker_events TO '
         || quote_ident(current_user);
EXCEPTION WHEN OTHERS THEN
    NULL;
END $$;
