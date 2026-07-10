-- Audit 2, D6 (7.5b) — cold archive of terminal tasks.
--
-- The retention loop (cleanup_old_terminal_tasks) used to DELETE terminal tasks older
-- than RETENTION_DAYS: the hot `task` table stayed small but the history was lost
-- (`GET /task/{id}` -> 404). D6 turns that DELETE into an atomic MOVE: the task row is
-- archived here (in the same transaction as the actions/links/webhook deletions) so the
-- record survives while its "tooling" (actions, links, outbox/ledger rows) is still
-- reclaimed.
--
-- `task_archive` mirrors every `task` column verbatim (same types) plus `archived_at`.
-- It is a COLD, autonomous store:
--   * NO foreign keys. A task's `batch` row is deleted by the orphan-batch sweep once
--     all its tasks are archived, so an archived task's `batch_id` must be allowed to
--     dangle. Actions/links FKs are gone too (their rows are deleted on archive).
--   * Minimal indexes only: the PK serves the `GET /task/{id}` archive fallback; the
--     `archived_at` index serves the archive purge (RETENTION_ARCHIVE_DAYS). No listing
--     is ever served from here (hot queries — GET /tasks, DAG, batches — never touch it),
--     so no other index is warranted.
--
-- Range-partitioning by `created_at` is explicitly out of scope for D6 ("the next step").
CREATE TABLE task_archive (
    id                UUID PRIMARY KEY,
    "name"            TEXT NOT NULL,
    "kind"            TEXT NOT NULL,
    "status"          status_kind NOT NULL,
    "timeout"         INT4 NOT NULL,
    "created_at"      TIMESTAMPTZ NOT NULL,
    "started_at"      TIMESTAMPTZ,
    "last_updated"    TIMESTAMPTZ NOT NULL,
    "metadata"        JSONB NOT NULL,
    "ended_at"        TIMESTAMPTZ,
    "start_condition" JSONB NOT NULL,
    "wait_success"    INT4 NOT NULL,
    "wait_finished"   INT4 NOT NULL,
    "success"         INT4 NOT NULL,
    "failures"        INT4 NOT NULL,
    "failure_reason"  TEXT,
    "batch_id"        UUID,
    "expected_count"  INT4,
    "dead_end_barrier" BOOLEAN NOT NULL,
    "priority"        INTEGER NOT NULL,
    "claimed_slot_keys" TEXT[],
    "capacity_charge" INTEGER,
    -- When the retention loop moved this task out of the hot table.
    "archived_at"     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Serves the archive purge (DELETE ... WHERE archived_at <= cutoff).
CREATE INDEX idx_task_archive_archived_at ON task_archive (archived_at);
