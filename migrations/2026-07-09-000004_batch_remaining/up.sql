-- Audit 2, D2 — replace the batch-complete detection (FOR UPDATE on the `batch`
-- row + a `NOT EXISTS (task active)` probe) with a denormalized `batch.remaining`
-- counter.
--
-- `remaining` = number of a batch's tasks that are NOT yet terminal. Each terminal
-- transition decrements it (in the same transaction) with
--   UPDATE batch SET remaining = GREATEST(remaining - N, 0) ... RETURNING remaining
-- and `remaining = 0` IS the completion signal — atomic, O(1) per transition,
-- naturally serialized on the `batch` row, and free progress reporting for
-- scope/metadata-only batches.
ALTER TABLE batch ADD COLUMN remaining integer NOT NULL DEFAULT 0;

-- Backfill existing rows: remaining = count of the batch's non-terminal tasks.
-- Batches whose tasks are all terminal keep the DEFAULT 0 (correct — already
-- complete). Uses the same terminal-status set as the retired probe.
UPDATE batch b
SET remaining = sub.cnt
FROM (
    SELECT batch_id, COUNT(*)::int AS cnt
    FROM task
    WHERE batch_id IS NOT NULL
      AND status NOT IN ('success', 'failure', 'canceled')
    GROUP BY batch_id
) sub
WHERE b.id = sub.batch_id;

-- The B3 partial index existed SOLELY to serve the retired `NOT EXISTS (task
-- active)` probe. D2 removes that probe, so the index is now dead — drop it.
-- (down.sql restores it byte-for-byte.)
DROP INDEX idx_task_batch_active;
