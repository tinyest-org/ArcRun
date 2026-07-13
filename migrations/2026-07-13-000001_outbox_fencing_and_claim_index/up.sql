-- Fence delivery workers: every lease receives a fresh token and only its owner may
-- apply success/retry/exhausted. A worker that finishes after its lease was reclaimed
-- becomes a harmless no-op.
ALTER TABLE webhook_outbox ADD COLUMN lease_token UUID;

-- The claim is ordered by due time first, then FIFO within the same due time. Align the
-- index with both the maturity predicate and the LIMIT order.
DROP INDEX IF EXISTS idx_webhook_outbox_next_attempt_at;
CREATE INDEX idx_webhook_outbox_due_created
    ON webhook_outbox (next_attempt_at ASC, created_at ASC, id ASC);
