-- Datto RMM device inventory and open alerts.
CREATE TABLE IF NOT EXISTS vector_datto_devices (
    id                   SERIAL PRIMARY KEY,
    uid                  TEXT UNIQUE NOT NULL,
    site_uid             TEXT,
    client_name          TEXT,
    hostname             TEXT,
    operating_system     TEXT,
    online               BOOLEAN,
    last_seen            TIMESTAMPTZ,
    last_logged_in_user  TEXT,
    raw_json             JSONB,
    updated_at           TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_datto_devices_client_name
    ON vector_datto_devices (client_name);

CREATE INDEX IF NOT EXISTS idx_datto_devices_hostname
    ON vector_datto_devices (LOWER(hostname));

CREATE TABLE IF NOT EXISTS vector_datto_alerts (
    id               SERIAL PRIMARY KEY,
    uid              TEXT UNIQUE NOT NULL,
    device_uid       TEXT,
    client_name      TEXT,
    hostname         TEXT,
    alert_type       TEXT,
    message          TEXT,
    priority         TEXT,
    alert_timestamp  TIMESTAMPTZ,
    resolved         BOOLEAN DEFAULT FALSE,
    ingested_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_datto_alerts_client_name
    ON vector_datto_alerts (client_name);

CREATE INDEX IF NOT EXISTS idx_datto_alerts_resolved_priority
    ON vector_datto_alerts (resolved, priority, alert_timestamp DESC);
