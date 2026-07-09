-- Recreate the dropped indexes at their original definitions
-- (from 2025-12-01-000001_add_indexes).
CREATE INDEX idx_action_task_id ON action(task_id);
CREATE INDEX idx_action_trigger ON action(trigger);
CREATE INDEX idx_task_kind ON task(kind);
