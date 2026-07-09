-- Audit 2, B7 — drop dead / redundant indexes on the hottest tables.
--
-- Each of these three indexes carries no constraint and is not the sole server
-- of any real query, verified against every predicate in the code:
--
--   * idx_action_task_id ON action(task_id)
--       Redundant. It is a strict PREFIX of idx_action_task_id_trigger
--       ON action(task_id, trigger). Every `action` lookup filters by task_id
--       (Action::belonging_to → WHERE task_id = $1, optionally AND trigger = $2;
--       the cleanup DELETE filters task_id = ANY($1); find_detailed_task_by_id
--       LEFT JOINs on task_id). The composite index serves the task_id-only
--       predicates via its leading column, so this single-column index only
--       duplicates maintenance cost on a write-heavy table.
--
--   * idx_action_trigger ON action(trigger)
--       Dead. No query filters on `trigger` alone — every trigger predicate is
--       always conjoined with task_id (Action::belonging_to + .filter(trigger…)),
--       which is served by idx_action_task_id_trigger. A bare index on the
--       low-cardinality `trigger` enum column is never chosen.
--
--   * idx_task_kind ON task(kind)
--       Dead. Every `kind` predicate is either (a) conjoined with `status`
--       (dedupe count `kind = $ AND status = $`, concurrency `t.kind = r.kind
--       AND t.status = …`) — served by idx_task_status_kind ON task(status, kind);
--       (b) conjoined with `batch_id` (update_batch_rules) — served by
--       idx_task_batch_id; or (c) a substring `kind LIKE '%…%'` in the list
--       filter, which is not indexable by a b-tree at all. A bare index on
--       `kind` is therefore never the chosen path.
--
-- Dropping them removes write-amplification (index maintenance on every
-- action/task INSERT/UPDATE) with no read-path regression.

DROP INDEX IF EXISTS idx_action_task_id;
DROP INDEX IF EXISTS idx_action_trigger;
DROP INDEX IF EXISTS idx_task_kind;
