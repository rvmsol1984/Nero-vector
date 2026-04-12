-- NERO Vector: initial schema for the UAL ingest service.
-- Creates the normalized event store and the per-tenant/per-content-type
-- checkpoint table used to resume ingestion after a restart.

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ---------------------------------------------------------------------------
-- Normalized audit events
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS vector_events (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id          VARCHAR(128)  NOT NULL,
    client_name        VARCHAR(256)  NOT NULL,
    user_id            VARCHAR(512),
    entity_key         VARCHAR(768)  NOT NULL,
    event_type         VARCHAR(256),
    workload           VARCHAR(128),
    result_status      VARCHAR(128),
    client_ip          VARCHAR(128),
    user_agent         TEXT,
    timestamp          TIMESTAMPTZ   NOT NULL,
    source             VARCHAR(32)   NOT NULL DEFAULT 'UAL',
    dedup_fingerprint  VARCHAR(64)   NOT NULL UNIQUE,
    raw_json           JSONB         NOT NULL,
    ingested_at        TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_vector_events_tenant_ts
    ON vector_events (tenant_id, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_vector_events_entity_key
    ON vector_events (entity_key);

CREATE INDEX IF NOT EXISTS idx_vector_events_event_type
    ON vector_events (event_type);

CREATE INDEX IF NOT EXISTS idx_vector_events_workload
    ON vector_events (workload);

-- ---------------------------------------------------------------------------
-- Ingest checkpoints (per tenant, per content type)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS vector_ingest_state (
    tenant_id         VARCHAR(128)  NOT NULL,
    client_name       VARCHAR(256)  NOT NULL,
    content_type      VARCHAR(64)   NOT NULL,
    last_ingested_at  TIMESTAMPTZ,
    updated_at        TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, content_type)
);
