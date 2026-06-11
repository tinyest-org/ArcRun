use crate::{
    Conn,
    models::{StatusKind, WebhookExecutionStatus},
};
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

            // 1. Delete actions belonging to these tasks
            diesel::delete(action_dsl::action.filter(action_dsl::task_id.eq_any(&task_ids)))
                .execute(&mut *conn)
                .await?;

            // 2. Delete webhook_execution records (FK on task_id)
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
            // all been deleted is unreachable. Delete its batch-level webhook_execution
            // rows first (FK on batch_id), then the batch rows themselves. We scope to
            // batches touched by the tasks just deleted to keep this bounded.
            use crate::schema::batch::dsl as batch_dsl;
            let orphan_batch_ids: Vec<uuid::Uuid> = batch_dsl::batch
                .filter(diesel::dsl::not(diesel::dsl::exists(
                    task_dsl::task.filter(task_dsl::batch_id.eq(batch_dsl::id.nullable())),
                )))
                // Never sweep a batch whose batch_complete signal is still awaiting
                // delivery: an empty batch (all tasks dedupe-skipped) has no tasks at
                // all, so it is "orphaned" from birth — deleting it here would destroy
                // the pending outbox row and lose the at-least-once signal.
                .filter(diesel::dsl::not(diesel::dsl::exists(
                    we_dsl::webhook_execution.filter(
                        we_dsl::batch_id
                            .eq(batch_dsl::id.nullable())
                            .and(we_dsl::status.eq(WebhookExecutionStatus::Pending)),
                    ),
                )))
                .select(batch_dsl::id)
                .limit(batch_size)
                .load::<uuid::Uuid>(&mut *conn)
                .await?;

            if !orphan_batch_ids.is_empty() {
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
