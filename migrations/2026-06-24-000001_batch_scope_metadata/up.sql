-- Batch scope / metadata (Tasker #601).
--
-- Adds business-level identity to a batch so it can be filtered and searched:
--   * `scope`    — a simple, indexable text label (exact-match filtering).
--   * `metadata` — arbitrary structured JSON (containment filtering, like task.metadata).
--
-- A `batch` row used to exist ONLY when an `on_batch_complete` webhook was registered.
-- From now on a row is also created when scope/metadata is provided (with
-- `on_complete = '[]'`), so `on_complete` keeps its NOT NULL constraint.
ALTER TABLE batch ADD COLUMN scope    TEXT  NULL;
ALTER TABLE batch ADD COLUMN metadata JSONB NOT NULL DEFAULT '{}'::jsonb;

-- Exact / prefix scope filtering.
CREATE INDEX idx_batch_scope ON batch (scope) WHERE scope IS NOT NULL;
-- Containment (`@>`) filtering on metadata.
CREATE INDEX idx_batch_metadata_gin ON batch USING GIN (metadata);
