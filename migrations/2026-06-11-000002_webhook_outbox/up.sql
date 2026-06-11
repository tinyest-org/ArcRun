-- Transactional outbox for webhook delivery (Lot 2).
--
-- Adds the `exhausted` terminal status, retry scheduling (`next_attempt_at`)
-- and diagnostics (`last_error`) to the existing `webhook_execution` table so it
-- can act as a transactional outbox: end/cancel webhooks are inserted as `pending`
-- rows inside the status-change transaction and delivered asynchronously by the
-- delivery loop (5th worker) with exponential backoff.
--
-- NOTE: `ALTER TYPE ... ADD VALUE` cannot run inside a transaction block when the
-- new value is *used* in the same transaction. We only add the value here (we never
-- reference 'exhausted' literally in this migration's other statements), so PG 12+
-- runs this fine. The integration-test migration runner additionally detects
-- ALTER TYPE ... ADD VALUE and runs the whole migration outside a transaction with
-- an `IF NOT EXISTS` rewrite (see tests/integration/common/setup.rs).
ALTER TYPE webhook_execution_status ADD VALUE IF NOT EXISTS 'exhausted';

ALTER TABLE webhook_execution
    ADD COLUMN next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN last_error TEXT;

-- Partial index serving the delivery-loop SELECT: only `pending` rows are ever
-- scanned for maturity, and they are ordered/filtered by `next_attempt_at`.
CREATE INDEX idx_webhook_execution_pending_due
    ON webhook_execution (next_attempt_at)
    WHERE status = 'pending';
