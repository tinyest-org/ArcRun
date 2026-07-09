-- Audit 2, B3 — make batch-complete detection O(1) per terminal transition.
--
-- `maybe_enqueue_batch_complete` (src/db/webhook_execution.rs) probes, on EVERY
-- terminal transition of a batched task:
--
--   SELECT NOT EXISTS (
--       SELECT 1 FROM task t
--       WHERE t.batch_id = $1
--         AND t.status NOT IN ('success', 'failure', 'canceled')
--   )
--
-- serialised per batch by the `batch` row lock. With only `idx_task_batch_id`
-- (all rows of the batch), near the end of a large batch's life the probe scans
-- almost every already-terminal row of the batch — O(N) per transition, O(N^2)
-- per batch (a 50k batch ≈ 1.25 billion cumulative row visits).
--
-- This PARTIAL index contains ONLY the still-active (non-terminal) rows, so the
-- `NOT EXISTS` probe becomes an index-only existence check whose cost is
-- independent of the number of terminal rows (O(1) as the batch drains).
--
-- The predicate is byte-for-byte the same expression as the probe's inner WHERE
-- (`status NOT IN ('success', 'failure', 'canceled')`). `status` is the
-- `status_kind` enum; the bare string literals coerce to `status_kind`
-- identically in both the index predicate and the query, so the planner proves
-- predicate implication and qualifies this partial index for the probe.
CREATE INDEX idx_task_batch_active
    ON task (batch_id)
    WHERE status NOT IN ('success', 'failure', 'canceled');
