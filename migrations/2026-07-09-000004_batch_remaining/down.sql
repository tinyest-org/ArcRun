-- Restore the B3 partial index dropped by up.sql (byte-for-byte identical to
-- migrations/2026-07-09-000001_task_batch_active_index/up.sql).
CREATE INDEX idx_task_batch_active
    ON task (batch_id)
    WHERE status NOT IN ('success', 'failure', 'canceled');

ALTER TABLE batch DROP COLUMN remaining;
