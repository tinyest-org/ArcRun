mod batch_listing;
mod cleanup;
mod task_crud;
mod task_lifecycle;
mod task_query;
mod webhook_execution;

use crate::Conn;
use diesel_async::RunQueryDsl;
use std::future::Future;
use std::pin::Pin;

pub(crate) type DbError = crate::error::ArcRunError;

// Re-exports from task_crud
pub use crate::rule::concurrency_lock_key;
pub use task_crud::{
    ClaimResult, batch_claim_tasks, claim_task, claim_task_with_rules, mark_task_running,
};
pub(crate) use task_crud::{find_detailed_task_by_id, insert_new_task};

// Re-exports from task_lifecycle
pub use task_lifecycle::{UpdateTaskResult, update_running_task};
pub(crate) use task_lifecycle::{
    fail_task_and_propagate, pause_task, save_cancel_actions, stop_batch,
};

// Re-exports from task_query
pub(crate) use task_query::{
    PendingCursor, find_timed_out_tasks, get_dag_for_batch, list_pending_page,
    list_task_filtered_paged, requeue_stale_claimed_tasks, timeout_task_and_propagate,
};

// Re-exports from batch_listing
pub(crate) use batch_listing::{get_batch_stats, list_batches, update_batch_rules};

// Re-exports from cleanup
pub(crate) use cleanup::cleanup_old_terminal_tasks;

// Re-exports from webhook_execution
pub use webhook_execution::{complete_webhook_execution, try_claim_webhook_execution};

/// Execute a closure within a database transaction.
/// Automatically rolls back on error. Commits on success.
/// Callers must wrap their async block with `Box::pin(async move { ... })`.
pub async fn run_in_transaction<'a, T: Send>(
    conn: &mut Conn<'a>,
    f: impl for<'c> FnOnce(
        &'c mut Conn<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<T, DbError>> + Send + 'c>>,
) -> Result<T, DbError> {
    diesel::sql_query("BEGIN").execute(&mut *conn).await?;
    match f(conn).await {
        Ok(val) => {
            diesel::sql_query("COMMIT").execute(&mut *conn).await?;
            Ok(val)
        }
        Err(e) => {
            if let Err(rb_err) = diesel::sql_query("ROLLBACK").execute(&mut *conn).await {
                log::error!("Failed to rollback transaction: {}", rb_err);
            }
            Err(e)
        }
    }
}
