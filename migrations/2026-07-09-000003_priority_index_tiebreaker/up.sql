-- Audit 2, B7 — add the `id` tiebreaker column to idx_task_priority so it
-- matches the claim scan's ORDER BY exactly.
--
-- The start_loop keyset scan (list_pending_page) orders Pending tasks by
--     priority DESC, created_at ASC, id ASC
-- (the `id` tiebreaker was added in 6.6a to keep the keyset cursor stable when
-- rows are claimed between pages). The previous index only covered
--     (status, priority DESC, created_at ASC)
-- so the planner had to add an Incremental Sort on `id` for every page — a
-- per-page sort step over the backlog. Appending `id ASC` makes the index a
-- byte-for-byte prefix of the full ORDER BY, so the scan is a pure Index Scan
-- (no sort node) and each keyset page is served directly from the index.
DROP INDEX IF EXISTS idx_task_priority;
CREATE INDEX idx_task_priority ON task(status, priority DESC, created_at ASC, id ASC);
