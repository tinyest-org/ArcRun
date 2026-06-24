DROP INDEX IF EXISTS idx_batch_metadata_gin;
DROP INDEX IF EXISTS idx_batch_scope;
ALTER TABLE batch DROP COLUMN IF EXISTS metadata;
ALTER TABLE batch DROP COLUMN IF EXISTS scope;
