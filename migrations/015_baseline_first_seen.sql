-- Migration 015 — Add first_seen to vector_user_baselines.
--
-- first_seen tracks the absolute earliest event timestamp ever seen
-- for a user across all tenants. Rules use it to suppress false
-- positives for users with fewer than MIN_BASELINE_DAYS of history.
-- The column is nullable so existing rows continue to work; the
-- baseline engine fills it on the next cycle.

ALTER TABLE vector_user_baselines
    ADD COLUMN IF NOT EXISTS first_seen TIMESTAMPTZ;

-- Keep the earliest value across cycles — never overwrite with a
-- later timestamp. This is enforced in the upsert ON CONFLICT clause
-- (LEAST(EXCLUDED.first_seen, vector_user_baselines.first_seen)).
