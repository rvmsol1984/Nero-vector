-- Migration 012 -- persist InternetMessageId on vector_message_trace.
--
-- The MessageTraceIngestor extracts InternetMessageId from
-- Defender's EmailEvents KQL but previously only held it in-memory
-- for the attachment-names backfill. Persisting it in a column lets
-- the backend /api/users/{key}/emails/{msg}/attachments endpoint
-- look up the Graph message resource reliably instead of falling
-- back to an unreliable subject search.

ALTER TABLE vector_message_trace
    ADD COLUMN IF NOT EXISTS internet_message_id TEXT;

CREATE INDEX IF NOT EXISTS idx_message_trace_internet_message_id
    ON vector_message_trace (internet_message_id)
    WHERE internet_message_id IS NOT NULL;
