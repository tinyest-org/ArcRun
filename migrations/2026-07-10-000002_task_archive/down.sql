-- Reverse the D6 (7.5b) task archive.
--
-- Rollback simply DROPs the archive table. It intentionally does NOT re-insert archived
-- rows back into `task`: an archived task's actions, links and webhook rows were deleted
-- when it was archived (the archive keeps the task record, not its tooling), and its
-- `batch` row was likely swept as an orphan afterwards. Reinstating the task rows would
-- recreate tasks that the actions/links FKs no longer reference and whose batch may be
-- gone — an inconsistent hot table. So rollback = the archive is lost and the pre-D6
-- behavior (terminal tasks are DELETED by retention, no history) is restored.
DROP TABLE task_archive;
