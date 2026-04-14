-- Migration 007 -- IOC enrichment match storage.
--
-- One row per (IOC value, triggering event) pair produced by the
-- vector-ingest IocEnricher worker. Values are extracted out of the
-- last 5 minutes of vector_events / vector_defender_hunting /
-- vector_edr_events / vector_message_trace and looked up in OpenCTI
-- via stixCyberObservables. A row is written whenever OpenCTI returns
-- a linked indicator with confidence >= 50.
--
-- The UNIQUE (ioc_value, matched_event_id) constraint collapses
-- repeated matches of the same indicator against the same triggering
-- event into a single row so the worker can re-run safely over any
-- overlapping 5-minute window.

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS vector_ioc_matches (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id          VARCHAR(128),
    client_name        VARCHAR(256),
    ioc_type           VARCHAR(32)  NOT NULL,
    ioc_value          VARCHAR(1024) NOT NULL,
    opencti_id         VARCHAR(256),
    indicator_name     VARCHAR(512),
    confidence         INTEGER,
    matched_event_id   UUID REFERENCES vector_events(id) ON DELETE SET NULL,
    matched_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    raw_json           JSONB,
    UNIQUE (ioc_value, matched_event_id)
);

CREATE INDEX IF NOT EXISTS idx_ioc_matches_matched_at
    ON vector_ioc_matches (matched_at DESC);
CREATE INDEX IF NOT EXISTS idx_ioc_matches_tenant_time
    ON vector_ioc_matches (tenant_id, matched_at DESC);
CREATE INDEX IF NOT EXISTS idx_ioc_matches_type
    ON vector_ioc_matches (ioc_type);
CREATE INDEX IF NOT EXISTS idx_ioc_matches_value
    ON vector_ioc_matches (ioc_value);
CREATE INDEX IF NOT EXISTS idx_ioc_matches_event
    ON vector_ioc_matches (matched_event_id);

-- Best-effort self-grant so the current role keeps its own rights
-- after any future table recreate. Wrapped in a DO/EXCEPTION block so
-- a fixture role that can't GRANT doesn't break the migration.
DO $$
BEGIN
    EXECUTE 'GRANT ALL PRIVILEGES ON TABLE vector_ioc_matches TO '
         || quote_ident(current_user);
EXCEPTION WHEN OTHERS THEN
    NULL;
END $$;
