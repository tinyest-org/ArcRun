-- Restore the original index (without the `id` tiebreaker) from
-- 2026-03-13-000001_add_priority.
DROP INDEX IF EXISTS idx_task_priority;
CREATE INDEX idx_task_priority ON task(status, priority DESC, created_at ASC);
