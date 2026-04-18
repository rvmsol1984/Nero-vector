-- Migration 013 -- rule exception / allowlist table.
--
-- Lets operators suppress specific scoring-rule triggers per tenant
-- so known-legitimate patterns (e.g. a VPN exit in a high-risk
-- country, a service account that always authenticates off-hours)
-- don't generate incidents every cycle.

CREATE TABLE IF NOT EXISTS vector_rule_exceptions (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id        UUID NOT NULL,
    client_name      TEXT NOT NULL,
    rule_name        TEXT NOT NULL,
    exception_type   TEXT NOT NULL
                       CHECK (exception_type IN ('country','ip','user','domain','any')),
    exception_value  TEXT NOT NULL,
    note             TEXT,
    created_by       TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, rule_name, exception_type, exception_value)
);

CREATE INDEX IF NOT EXISTS idx_rule_exceptions_tenant_rule
    ON vector_rule_exceptions (tenant_id, rule_name);

DO $$
BEGIN
    EXECUTE 'GRANT ALL PRIVILEGES ON TABLE vector_rule_exceptions TO '
         || quote_ident(current_user);
EXCEPTION WHEN OTHERS THEN
    NULL;
END $$;
