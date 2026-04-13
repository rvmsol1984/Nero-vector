-- Migration 006 -- Datto EDR (Infocyte) webhook receiver storage.
--
-- One row per accepted Infocyte alert / observable. Dedup is handled
-- on a SHA-256 fingerprint bucketed to 5 minutes so webhook retries
-- (or the same alert delivered by multiple upstream hops) collapse
-- into a single row.

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS vector_edr_events (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id           VARCHAR(128),
    client_name         VARCHAR(256),
    event_type          VARCHAR(64)  NOT NULL,
    severity            VARCHAR(64),
    host_name           VARCHAR(256),
    host_ip             VARCHAR(128),
    user_account        VARCHAR(512),
    process_name        VARCHAR(256),
    process_path        TEXT,
    command_line        TEXT,
    threat_name         VARCHAR(256),
    threat_score        INTEGER,
    action_taken        VARCHAR(128),
    timestamp           TIMESTAMPTZ  NOT NULL,
    raw_json            JSONB        NOT NULL,
    ingested_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    dedup_fingerprint   VARCHAR(64)  UNIQUE
);

CREATE INDEX IF NOT EXISTS idx_edr_events_tenant_time
    ON vector_edr_events (tenant_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_edr_events_severity
    ON vector_edr_events (severity);
CREATE INDEX IF NOT EXISTS idx_edr_events_host
    ON vector_edr_events (host_name);
CREATE INDEX IF NOT EXISTS idx_edr_events_event_type
    ON vector_edr_events (event_type);

-- Best-effort self-grant so the current role keeps its own rights
-- after any future table recreate. Wrapped in a DO/EXCEPTION block so
-- a fixture role that can't GRANT doesn't break the migration.
DO $$
BEGIN
    EXECUTE 'GRANT ALL PRIVILEGES ON TABLE vector_edr_events TO '
         || quote_ident(current_user);
EXCEPTION WHEN OTHERS THEN
    NULL;
END $$;
