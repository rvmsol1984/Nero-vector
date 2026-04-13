-- Migration 005 — Office 365 MessageTrace metadata store.
--
-- Populated by vector_ingest.message_trace (polls every 15 min per
-- tenant via the legacy Reporting Web Service). Message body content
-- is never stored -- only the headers / envelope needed to correlate
-- sign-in anomalies with the emails that preceded them.

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS vector_message_trace (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id           VARCHAR(128) NOT NULL,
    client_name         VARCHAR(256) NOT NULL,
    message_id          VARCHAR(512) UNIQUE,
    sender_address      VARCHAR(512),
    recipient_address   VARCHAR(512),
    subject             TEXT,
    received            TIMESTAMPTZ,
    status              VARCHAR(64),
    size_bytes          BIGINT,
    direction           VARCHAR(32),
    original_client_ip  VARCHAR(128),
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_message_trace_tenant_received
    ON vector_message_trace (tenant_id, received DESC);
CREATE INDEX IF NOT EXISTS idx_message_trace_sender
    ON vector_message_trace (sender_address);
CREATE INDEX IF NOT EXISTS idx_message_trace_recipient
    ON vector_message_trace (recipient_address);
