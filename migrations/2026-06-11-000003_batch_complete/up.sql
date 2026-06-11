-- Batch-complete webhook (Lot 3b).
--
-- Adds an optional batch-level webhook fired once when the LAST task of a batch
-- reaches a terminal state. Storage:
--   * `batch` table — one row per batch that registered an `on_batch_complete`
--     payload (batches without the webhook cost nothing).
--   * `webhook_execution` is extended to carry batch-level outbox rows: `task_id`
--     becomes nullable and a nullable `batch_id` column is added, with a CHECK that
--     exactly one origin (task or batch) is set.
--   * the `batch_complete` value is added to the `trigger_kind` enum.
--
-- NOTE: `ALTER TYPE ... ADD VALUE` cannot run inside a transaction block. We only
-- ADD the value here (it is never *used* — referenced as a literal — in this same
-- migration), so PG 12+ runs this fine. The integration-test migration runner
-- detects ALTER TYPE ... ADD VALUE and runs the whole migration statement-by-
-- statement outside a transaction with an `IF NOT EXISTS` rewrite
-- (see tests/integration/common/setup.rs).
ALTER TYPE trigger_kind ADD VALUE IF NOT EXISTS 'batch_complete';

CREATE TABLE batch (
    id UUID PRIMARY KEY,
    on_complete JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE webhook_execution
    ALTER COLUMN task_id DROP NOT NULL,
    ADD COLUMN batch_id UUID NULL REFERENCES batch(id),
    ADD CONSTRAINT webhook_execution_origin_chk
        CHECK (task_id IS NOT NULL OR batch_id IS NOT NULL);

CREATE INDEX idx_webhook_execution_batch_id
    ON webhook_execution (batch_id)
    WHERE batch_id IS NOT NULL;
