-- Reverse the webhook outbox migration.
--
-- PostgreSQL cannot remove a value from an enum type without recreating it, so the
-- `exhausted` value is intentionally left in place (harmless if unused). We drop the
-- columns and the partial index that this migration added.
DROP INDEX IF EXISTS idx_webhook_execution_pending_due;

ALTER TABLE webhook_execution
    DROP COLUMN IF EXISTS next_attempt_at,
    DROP COLUMN IF EXISTS last_error;
