-- Migration 007 -- IOC match cache from OpenCTI enrichment.
--
-- vector_ingest.ioc_enricher pulls unique IPs, email addresses and
-- file hashes from the last few minutes of events, queries OpenCTI
-- for each one, and caches any hit >= confidence 50 into this
-- table. Hits >= confidence 75 ALSO get a row in vector_watchlist
-- with trigger_type='ioc_match' / status='escalated' so the
-- correlation engine picks them up immediately.
--
-- The (ioc_value, matched_event_id) UNIQUE lets us re-run the
-- enricher on the same window without duplicating rows.

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS vector_ioc_matches (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id          VARCHAR(128),
    client_name        VARCHAR(256),
    ioc_type           VARCHAR(32)  NOT NULL,
    ioc_value          VARCHAR(512) NOT NULL,
    opencti_id         VARCHAR(256),
    indicator_name     VARCHAR(512),
    confidence         INTEGER,
    matched_event_id   UUID,
    matched_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    raw_json           JSONB,
    UNIQUE (ioc_value, matched_event_id)
);

CREATE INDEX IF NOT EXISTS idx_ioc_matches_tenant_time
    ON vector_ioc_matches (tenant_id, matched_at DESC);
CREATE INDEX IF NOT EXISTS idx_ioc_matches_type
    ON vector_ioc_matches (ioc_type);
CREATE INDEX IF NOT EXISTS idx_ioc_matches_value
    ON vector_ioc_matches (ioc_value);
CREATE INDEX IF NOT EXISTS idx_ioc_matches_confidence
    ON vector_ioc_matches (confidence DESC);

DO $$
BEGIN
    EXECUTE 'GRANT ALL PRIVILEGES ON TABLE vector_ioc_matches TO '
         || quote_ident(current_user);
EXCEPTION WHEN OTHERS THEN
    NULL;
END $$;
