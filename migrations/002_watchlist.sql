-- Provisional watchlist entries produced by the INKY MailShield webhook.
-- Each entry pins a suspicious inbound-email indicator for a correlation
-- window (default 60 min) so the engine can bind downstream UAL events to
-- the original INKY verdict while the pin is still live.

CREATE TABLE IF NOT EXISTS vector_watchlist (
    id                             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id                      VARCHAR(128) NOT NULL,
    source                         VARCHAR(32)  NOT NULL DEFAULT 'INKY',
    verdict                        VARCHAR(64)  NOT NULL,
    recipient                      VARCHAR(512),
    sender                         VARCHAR(512),
    url                            TEXT,
    event_type                     VARCHAR(64),
    timestamp                      TIMESTAMPTZ NOT NULL,
    correlation_window_expires_at  TIMESTAMPTZ NOT NULL,
    raw_json                       JSONB       NOT NULL,
    created_at                     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_vector_watchlist_tenant_expires
    ON vector_watchlist (tenant_id, correlation_window_expires_at DESC);

CREATE INDEX IF NOT EXISTS idx_vector_watchlist_recipient
    ON vector_watchlist (recipient);

CREATE INDEX IF NOT EXISTS idx_vector_watchlist_verdict
    ON vector_watchlist (verdict);

CREATE INDEX IF NOT EXISTS idx_vector_watchlist_active
    ON vector_watchlist (correlation_window_expires_at)
    WHERE correlation_window_expires_at > now();
