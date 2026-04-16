-- Migration 010 -- attachment metadata on message trace.
--
-- Two new columns on vector_message_trace:
--
--   has_attachments  -- BOOLEAN. Set during hunting-path ingest from
--                       Defender EmailEvents.AttachmentCount > 0, so
--                       the UI can render a paperclip without a
--                       round-trip to Graph.
--
--   attachment_names -- TEXT[]. Filled in a best-effort second pass
--                       after the primary insert: for messages where
--                       has_attachments is true, the ingestor calls
--                       Graph /messages/{id}/attachments?$select=name
--                       up to a rate-limited budget per poll and
--                       UPDATEs this column. Default empty array so
--                       the column is safe to read unconditionally.
--
-- A GIN index on attachment_names backs the governance / user-detail
-- UI's "find messages with a named attachment" search path, which
-- uses `ILIKE ANY(unnest(attachment_names))` against a substring
-- pattern. GIN is the right structure for the ANY-membership shape
-- even though ILIKE itself doesn't benefit from the index; the
-- optimizer still uses the GIN lookup to narrow candidate rows.

ALTER TABLE vector_message_trace
    ADD COLUMN IF NOT EXISTS has_attachments  BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE vector_message_trace
    ADD COLUMN IF NOT EXISTS attachment_names TEXT[]  NOT NULL DEFAULT '{}';

CREATE INDEX IF NOT EXISTS idx_message_trace_attachment_names
    ON vector_message_trace USING GIN (attachment_names);

CREATE INDEX IF NOT EXISTS idx_message_trace_has_attachments
    ON vector_message_trace (has_attachments)
    WHERE has_attachments = TRUE;

-- Best-effort self-grant so the current role keeps its own rights
-- after any future table recreate. Wrapped in a DO/EXCEPTION block
-- so a fixture role that can't GRANT doesn't break the migration.
DO $$
BEGIN
    EXECUTE 'GRANT ALL PRIVILEGES ON TABLE vector_message_trace TO '
         || quote_ident(current_user);
EXCEPTION WHEN OTHERS THEN
    NULL;
END $$;
