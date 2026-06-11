-- Reverse the batch-complete migration.
--
-- PostgreSQL cannot remove a value from an enum type without recreating it, so the
-- `batch_complete` value is intentionally left in place (harmless if unused), the
-- same way `exhausted` is left by the outbox migration's down.sql. We drop the
-- batch table, the batch_id column / constraint / index, and restore task_id NOT NULL.
--
DROP INDEX IF EXISTS idx_webhook_execution_batch_id;

-- Batch-level outbox rows (task_id IS NULL) would violate the restored NOT NULL
-- constraint below — remove them so the rollback always succeeds.
DELETE FROM webhook_execution WHERE task_id IS NULL;

ALTER TABLE webhook_execution
    DROP CONSTRAINT IF EXISTS webhook_execution_origin_chk,
    DROP COLUMN IF EXISTS batch_id;

ALTER TABLE webhook_execution
    ALTER COLUMN task_id SET NOT NULL;

DROP TABLE IF EXISTS batch;
