-- Audit 2, D3 — split the webhook outbox QUEUE into a dedicated `webhook_outbox`
-- table, leaving `webhook_execution` as the idempotency ledger (start gate) + the
-- observability log (delivery history).
--
-- Rationale: `webhook_execution` was overloaded with three roles — (1) the
-- at-least-once QUEUE of end/cancel/batch_complete notifications (rows claimed by
-- lease, hot + churny), (2) the idempotency LEDGER of `start` deliveries (start_loop
-- gate), and (3) the delivery LOG (`GET /webhook-deliveries`). The queue wants rows
-- DELETED on success (small, hot, vacuum-friendly); the ledger/log wants retention.
-- Splitting the queue out lets it stay tiny while the ledger keeps full history.
--
-- `webhook_outbox` is a PURE queue: every row present is awaiting delivery, so there
-- is NO `status` column. A row is removed (and historised into `webhook_execution`
-- as `success`/`exhausted`) the moment delivery terminates.

CREATE TABLE webhook_outbox (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    task_id UUID NULL REFERENCES task(id),
    batch_id UUID NULL REFERENCES batch(id),
    trigger trigger_kind NOT NULL,
    condition trigger_condition NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    attempts INT4 NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error TEXT NULL
);

-- The whole table is queue, so a plain (non-partial) index on next_attempt_at serves
-- the delivery-loop claim (`next_attempt_at <= now()` + ORDER BY created_at) and the
-- backlog stats. Replaces the old partial `idx_webhook_execution_pending_due`.
CREATE INDEX idx_webhook_outbox_next_attempt_at ON webhook_outbox (next_attempt_at);
-- Cleanup lookups by owner (retention deletes queue rows before their task/batch).
CREATE INDEX idx_webhook_outbox_task_id ON webhook_outbox (task_id) WHERE task_id IS NOT NULL;
CREATE INDEX idx_webhook_outbox_batch_id ON webhook_outbox (batch_id) WHERE batch_id IS NOT NULL;

-- Data migration: move the in-flight QUEUE rows (pending end/cancel/batch_complete)
-- out of webhook_execution into webhook_outbox, preserving id/key/attempts/timestamps/
-- last_error. Pending `start` rows stay in webhook_execution (that is the ledger the
-- start-before-end gate reads). Non-pending end/cancel/batch_complete rows
-- (success/failure/exhausted) also stay — they are the delivery history.
INSERT INTO webhook_outbox
    (id, task_id, batch_id, trigger, condition, idempotency_key,
     attempts, created_at, updated_at, next_attempt_at, last_error)
SELECT id, task_id, batch_id, trigger, condition, idempotency_key,
       attempts, created_at, updated_at, next_attempt_at, last_error
FROM webhook_execution
WHERE trigger IN ('end', 'cancel', 'batch_complete')
  AND status = 'pending';

DELETE FROM webhook_execution
WHERE trigger IN ('end', 'cancel', 'batch_complete')
  AND status = 'pending';

-- The partial pending-due index existed SOLELY to serve the queue drain, which now
-- lives in webhook_outbox. The remaining webhook_execution readers are the start gate
-- (`task_id + trigger='start' + status='pending'`, served by idx_webhook_execution_task_id),
-- the ledger claim (`idempotency_key`, served by the UNIQUE index), and the delivery
-- listing (order by updated_at). None of them use `(next_attempt_at) WHERE status='pending'`.
DROP INDEX IF EXISTS idx_webhook_execution_pending_due;
