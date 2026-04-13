-- Migration 004 — Defender ATP alerts & hunting + INKY SIEM events + watchlist.
--
-- The vector_watchlist table is re-created with a new schema oriented
-- around the Phase-2 correlation engine: a pin carries a user_email,
-- a trigger_type, a JSONB trigger_details blob, and a lifecycle
-- (active/escalated/expired). Any data that lived in the old schema
-- (from migration 002) is disposable — it was only populated by the
-- v0.1 INKY receiver and never read from the UI.

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ---------------------------------------------------------------------------
-- Microsoft Defender ATP alerts
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS vector_defender_alerts (
    id                    VARCHAR(128) PRIMARY KEY,
    tenant_id             VARCHAR(128)  NOT NULL,
    client_name           VARCHAR(256)  NOT NULL,
    incident_id           BIGINT,
    severity              VARCHAR(32),
    status                VARCHAR(32),
    category              VARCHAR(128),
    threat_family         VARCHAR(256),
    title                 TEXT,
    machine_id            VARCHAR(256),
    computer_name         VARCHAR(256),
    threat_name           VARCHAR(256),
    logged_on_users       JSONB,
    alert_creation_time   TIMESTAMPTZ,
    first_event_time      TIMESTAMPTZ,
    last_event_time       TIMESTAMPTZ,
    detection_source      VARCHAR(128),
    investigation_state   VARCHAR(128),
    mitre_techniques      JSONB,
    raw_json              JSONB         NOT NULL,
    ingested_at           TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_defender_alerts_tenant_created
    ON vector_defender_alerts (tenant_id, alert_creation_time DESC);
CREATE INDEX IF NOT EXISTS idx_defender_alerts_severity
    ON vector_defender_alerts (severity);
CREATE INDEX IF NOT EXISTS idx_defender_alerts_status
    ON vector_defender_alerts (status);
CREATE INDEX IF NOT EXISTS idx_defender_alerts_machine
    ON vector_defender_alerts (machine_id);

-- ---------------------------------------------------------------------------
-- Defender Advanced Hunting results
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS vector_defender_hunting (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id      VARCHAR(128)  NOT NULL,
    client_name    VARCHAR(256)  NOT NULL,
    query_name     VARCHAR(128)  NOT NULL,
    device_id      VARCHAR(256),
    device_name    VARCHAR(256),
    account_upn    VARCHAR(512),
    action_type    VARCHAR(128),
    timestamp      TIMESTAMPTZ   NOT NULL,
    raw_json       JSONB         NOT NULL,
    ingested_at    TIMESTAMPTZ   NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, query_name, device_id, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_defender_hunting_tenant_time
    ON vector_defender_hunting (tenant_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_defender_hunting_query
    ON vector_defender_hunting (query_name);
CREATE INDEX IF NOT EXISTS idx_defender_hunting_upn
    ON vector_defender_hunting (account_upn);

-- ---------------------------------------------------------------------------
-- INKY SIEM events (webhook-delivered)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS vector_inky_events (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id      VARCHAR(128),
    client_name    VARCHAR(256),
    event_type     VARCHAR(64)   NOT NULL,
    recipient      VARCHAR(512),
    sender         VARCHAR(512),
    subject        TEXT,
    verdict        VARCHAR(64),
    url            TEXT,
    aitm_detected  BOOLEAN       NOT NULL DEFAULT false,
    threat_level   VARCHAR(64),
    policy         VARCHAR(256),
    timestamp      TIMESTAMPTZ   NOT NULL,
    raw_json       JSONB         NOT NULL,
    ingested_at    TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_inky_events_tenant_time
    ON vector_inky_events (tenant_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_inky_events_recipient
    ON vector_inky_events (recipient);
CREATE INDEX IF NOT EXISTS idx_inky_events_verdict
    ON vector_inky_events (verdict);
CREATE INDEX IF NOT EXISTS idx_inky_events_event_type
    ON vector_inky_events (event_type);

-- ---------------------------------------------------------------------------
-- Correlation watchlist (new schema -- replaces the v0.1 table from 002)
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS vector_watchlist CASCADE;

CREATE TABLE vector_watchlist (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id        VARCHAR(128),
    client_name      VARCHAR(256),
    user_email       VARCHAR(512),
    trigger_type     VARCHAR(64)   NOT NULL,
    trigger_details  JSONB         NOT NULL,
    created_at       TIMESTAMPTZ   NOT NULL DEFAULT now(),
    expires_at       TIMESTAMPTZ   NOT NULL,
    status           VARCHAR(32)   NOT NULL DEFAULT 'active',
    incident_id      UUID
);

CREATE INDEX IF NOT EXISTS idx_watchlist_active_expires
    ON vector_watchlist (expires_at)
    WHERE status = 'active';
CREATE INDEX IF NOT EXISTS idx_watchlist_user
    ON vector_watchlist (user_email);
CREATE INDEX IF NOT EXISTS idx_watchlist_tenant
    ON vector_watchlist (tenant_id);
CREATE INDEX IF NOT EXISTS idx_watchlist_trigger
    ON vector_watchlist (trigger_type);
