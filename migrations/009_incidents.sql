-- Migration 009 -- Phase 2 incidents + baseline engine storage.
--
-- Three new tables:
--   * vector_incidents          -- one row per confirmed incident
--   * vector_incident_events    -- evidence link from an incident
--                                  back to the underlying source rows
--   * vector_user_baselines     -- rolled-up "what's normal for
--                                  this user" profile the scoring
--                                  engine diff's against

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS vector_incidents (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id           VARCHAR(128),
    client_name         VARCHAR(256),
    user_id             VARCHAR(512),
    entity_key          VARCHAR(1024),
    incident_type       VARCHAR(64),
    severity            VARCHAR(32),
    status              VARCHAR(32)   NOT NULL DEFAULT 'open',
    score               INTEGER,
    title               TEXT,
    summary             TEXT,
    patient_zero        VARCHAR(1024),
    dwell_time_minutes  INTEGER,
    first_seen          TIMESTAMPTZ,
    last_seen           TIMESTAMPTZ,
    confirmed_at        TIMESTAMPTZ   NOT NULL DEFAULT now(),
    contained_at        TIMESTAMPTZ,
    evidence            JSONB         NOT NULL DEFAULT '[]'::jsonb,
    watchlist_id        UUID          REFERENCES vector_watchlist(id)
                                        ON DELETE SET NULL,
    raw_signals         JSONB         NOT NULL DEFAULT '{}'::jsonb,
    created_at          TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_incidents_tenant_status
    ON vector_incidents (tenant_id, status, confirmed_at DESC);
CREATE INDEX IF NOT EXISTS idx_incidents_status
    ON vector_incidents (status);
CREATE INDEX IF NOT EXISTS idx_incidents_entity_key
    ON vector_incidents (entity_key);
CREATE INDEX IF NOT EXISTS idx_incidents_severity
    ON vector_incidents (severity);
CREATE INDEX IF NOT EXISTS idx_incidents_confirmed_at
    ON vector_incidents (confirmed_at DESC);


CREATE TABLE IF NOT EXISTS vector_incident_events (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    incident_id    UUID NOT NULL REFERENCES vector_incidents(id)
                                    ON DELETE CASCADE,
    event_source   VARCHAR(32) NOT NULL,
    event_id       UUID,
    event_type     VARCHAR(128),
    timestamp      TIMESTAMPTZ,
    significance   TEXT,
    raw_json       JSONB,
    added_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_incident_events_incident
    ON vector_incident_events (incident_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_incident_events_source
    ON vector_incident_events (event_source);


CREATE TABLE IF NOT EXISTS vector_user_baselines (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id         VARCHAR(128)   NOT NULL,
    user_id           VARCHAR(512)   NOT NULL,
    computed_at       TIMESTAMPTZ    NOT NULL DEFAULT now(),
    login_hours       JSONB          NOT NULL DEFAULT '{}'::jsonb,
    login_countries   JSONB          NOT NULL DEFAULT '{}'::jsonb,
    login_asns        JSONB          NOT NULL DEFAULT '{}'::jsonb,
    known_devices     JSONB          NOT NULL DEFAULT '[]'::jsonb,
    known_ips         JSONB          NOT NULL DEFAULT '[]'::jsonb,
    avg_daily_events  DOUBLE PRECISION,
    avg_daily_logins  DOUBLE PRECISION,
    baseline_days     INTEGER,
    UNIQUE (tenant_id, user_id)
);

CREATE INDEX IF NOT EXISTS idx_baselines_user
    ON vector_user_baselines (user_id);
CREATE INDEX IF NOT EXISTS idx_baselines_computed_at
    ON vector_user_baselines (computed_at DESC);


-- Best-effort self-grant so the current role keeps its own rights
-- after any future table recreate. Wrapped in a DO/EXCEPTION block so
-- a fixture role that can't GRANT doesn't break the migration.
DO $$
BEGIN
    EXECUTE 'GRANT ALL PRIVILEGES ON TABLE vector_incidents TO '
         || quote_ident(current_user);
    EXECUTE 'GRANT ALL PRIVILEGES ON TABLE vector_incident_events TO '
         || quote_ident(current_user);
    EXECUTE 'GRANT ALL PRIVILEGES ON TABLE vector_user_baselines TO '
         || quote_ident(current_user);
EXCEPTION WHEN OTHERS THEN
    NULL;
END $$;
