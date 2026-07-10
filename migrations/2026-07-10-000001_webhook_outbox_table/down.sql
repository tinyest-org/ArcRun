-- Reverse the webhook_outbox split (Audit 2, D3).
--
-- Reintegrate every queued row back into webhook_execution as `pending` (restoring the
-- pre-split single-table outbox), then drop the dedicated queue table and recreate the
-- partial pending-due index that up.sql dropped.
--
-- A given idempotency_key lives in at most one of the two tables at any time (the
-- backstop NOT EXISTS on enqueue + the DELETE-then-INSERT on success/exhausted keep
-- them disjoint), so `ON CONFLICT DO NOTHING` is a belt-and-braces no-op here.
INSERT INTO webhook_execution
    (id, task_id, batch_id, trigger, condition, idempotency_key,
     status, attempts, created_at, updated_at, next_attempt_at, last_error)
SELECT id, task_id, batch_id, trigger, condition, idempotency_key,
       'pending', attempts, created_at, updated_at, next_attempt_at, last_error
FROM webhook_outbox
ON CONFLICT (idempotency_key) DO NOTHING;

DROP TABLE webhook_outbox;

-- Restore the partial pending-due index (byte-for-byte from
-- migrations/2026-06-11-000002_webhook_outbox/up.sql).
CREATE INDEX idx_webhook_execution_pending_due
    ON webhook_execution (next_attempt_at)
    WHERE status = 'pending';
