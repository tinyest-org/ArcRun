use crate::{Conn, models::StatusKind};
use chrono::Utc;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;

use super::{DbError, run_in_transaction};

/// Delete terminal tasks (Success/Failure/Canceled) with `ended_at` older than the
/// retention period. Deletes in FK order (actions → links → tasks) within a transaction.
/// Returns the number of tasks deleted.
pub async fn cleanup_old_terminal_tasks<'a>(
    conn: &mut Conn<'a>,
    retention_days: u32,
    batch_size: i64,
) -> Result<usize, DbError> {
    use crate::schema::task::dsl as task_dsl;

    let cutoff = Utc::now() - chrono::Duration::days(retention_days as i64);

    // Find task IDs eligible for cleanup
    let task_ids: Vec<uuid::Uuid> = task_dsl::task
        .filter(
            task_dsl::status
                .eq(StatusKind::Success)
                .or(task_dsl::status.eq(StatusKind::Failure))
                .or(task_dsl::status.eq(StatusKind::Canceled)),
        )
        .filter(task_dsl::ended_at.le(cutoff))
        .select(task_dsl::id)
        .limit(batch_size)
        .load::<uuid::Uuid>(conn)
        .await?;

    if task_ids.is_empty() {
        return Ok(0);
    }

    let count = task_ids.len();

    run_in_transaction(conn, |conn| {
        Box::pin(async move {
            use crate::schema::action::dsl as action_dsl;
            use crate::schema::link::dsl as link_dsl;
            use crate::schema::webhook_execution::dsl as we_dsl;
            use crate::schema::webhook_outbox::dsl as wo_dsl;

            // 1. Delete actions belonging to these tasks
            diesel::delete(action_dsl::action.filter(action_dsl::task_id.eq_any(&task_ids)))
                .execute(&mut *conn)
                .await?;

            // 2. Delete webhook rows (FK on task_id): the delivery QUEUE
            // (webhook_outbox — normally empty for terminal tasks this old, but
            // belt-and-braces) and the ledger/history (webhook_execution).
            diesel::delete(wo_dsl::webhook_outbox.filter(wo_dsl::task_id.eq_any(&task_ids)))
                .execute(&mut *conn)
                .await?;
            diesel::delete(we_dsl::webhook_execution.filter(we_dsl::task_id.eq_any(&task_ids)))
                .execute(&mut *conn)
                .await?;

            // 3. Delete links referencing these tasks (as parent or child)
            diesel::delete(
                link_dsl::link.filter(
                    link_dsl::parent_id
                        .eq_any(&task_ids)
                        .or(link_dsl::child_id.eq_any(&task_ids)),
                ),
            )
            .execute(&mut *conn)
            .await?;

            // 4. Delete the tasks themselves
            diesel::delete(task_dsl::task.filter(task_dsl::id.eq_any(&task_ids)))
                .execute(&mut *conn)
                .await?;

            // 5. Clean up orphaned `batch` rows (Lot 3b): a batch row whose tasks have
            // all been deleted is unreachable. Delete its batch-level webhook rows first
            // (FK on batch_id — both the queue and the history), then the batch rows
            // themselves. We scope to batches touched by the tasks just deleted to keep
            // this bounded.
            use crate::schema::batch::dsl as batch_dsl;
            let orphan_batch_ids: Vec<uuid::Uuid> = batch_dsl::batch
                .filter(diesel::dsl::not(diesel::dsl::exists(
                    task_dsl::task.filter(task_dsl::batch_id.eq(batch_dsl::id.nullable())),
                )))
                // Never sweep a batch whose batch_complete signal is still awaiting
                // delivery: an empty batch (all tasks dedupe-skipped) has no tasks at
                // all, so it is "orphaned" from birth — deleting it here would destroy
                // the queued outbox row and lose the at-least-once signal. A present
                // webhook_outbox row (D3) with this batch_id IS the pending signal.
                .filter(diesel::dsl::not(diesel::dsl::exists(
                    wo_dsl::webhook_outbox.filter(wo_dsl::batch_id.eq(batch_dsl::id.nullable())),
                )))
                .select(batch_dsl::id)
                .limit(batch_size)
                .load::<uuid::Uuid>(&mut *conn)
                .await?;

            if !orphan_batch_ids.is_empty() {
                diesel::delete(
                    wo_dsl::webhook_outbox.filter(wo_dsl::batch_id.eq_any(&orphan_batch_ids)),
                )
                .execute(&mut *conn)
                .await?;
                diesel::delete(
                    we_dsl::webhook_execution.filter(we_dsl::batch_id.eq_any(&orphan_batch_ids)),
                )
                .execute(&mut *conn)
                .await?;

                diesel::delete(batch_dsl::batch.filter(batch_dsl::id.eq_any(&orphan_batch_ids)))
                    .execute(&mut *conn)
                    .await?;
            }

            Ok(count)
        })
    })
    .await
}

/// Garbage-collect empty concurrency slot rows (Audit 2, D1). Slot keys are derived
/// from task metadata and are therefore unbounded, so without periodic GC the
/// `rule_slot` table grows forever. Deleting a `used = 0` row is safe under concurrency:
/// the DELETE takes a row lock, and Postgres re-checks `used = 0` after acquiring it —
/// so a row a concurrent claim just incremented (to >= 1) is NOT deleted. If a key is
/// needed again after deletion, the claim's `INSERT ... ON CONFLICT` recreates it.
/// Returns the number of rows deleted.
pub async fn gc_empty_rule_slots<'a>(conn: &mut Conn<'a>) -> Result<usize, DbError> {
    let deleted = diesel::sql_query("DELETE FROM rule_slot WHERE used = 0")
        .execute(conn)
        .await?;
    Ok(deleted)
}
