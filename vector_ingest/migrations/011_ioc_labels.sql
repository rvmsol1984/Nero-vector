ALTER TABLE vector_ioc_matches
  ADD COLUMN IF NOT EXISTS labels TEXT[] NOT NULL DEFAULT '{}';

CREATE INDEX IF NOT EXISTS idx_ioc_matches_labels
  ON vector_ioc_matches USING GIN (labels);
