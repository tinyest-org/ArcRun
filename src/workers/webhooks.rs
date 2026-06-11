//! Outbox enqueue helpers for end/cancel webhook notifications (Lot 2).
//!
//! End and cancel webhooks are at-least-once notifications. Instead of firing
//! reqwest inline in the HTTP/worker call path (which held a DB connection for up
//! to the webhook timeout and lost events on crash), the status-change transaction
//! enqueues `pending` rows into the `webhook_execution` outbox. The delivery loop
//! (`delivery_loop.rs`) drains them asynchronously with retries.
//!
//! These helpers MUST be called inside the same transaction as the status change so
//! that "API response = durable state" holds: when the transaction commits, the
//! outbox row is durably persisted and will be delivered (after crash if needed).
//!
//! Exception (decision actée): `on_start` stays synchronous in the start_loop — it
//! is control-flow, not a notification (see `start_loop::start_task`).

use crate::{
    Conn,
    action::idempotency_key,
    db_operation::{self, DbError},
    models::{StatusKind, TriggerCondition, TriggerKind},
};

use super::delivery_loop::end_condition_for;
use super::propagation::CanceledAncestor;

/// Enqueue an `end` outbox row for a task that reached a terminal state.
pub(crate) async fn enqueue_end_outbox<'a>(
    task_id: &uuid::Uuid,
    result_status: StatusKind,
    conn: &mut Conn<'a>,
) -> Result<(), DbError> {
    let cond = end_condition_for(result_status);
    let key = idempotency_key(*task_id, &TriggerKind::End, &cond);
    db_operation::enqueue_outbox(conn, *task_id, TriggerKind::End, cond, &key).await
}

/// Enqueue a `cancel` outbox row for a task that was canceled while Running.
pub(crate) async fn enqueue_cancel_outbox<'a>(
    task_id: &uuid::Uuid,
    conn: &mut Conn<'a>,
) -> Result<(), DbError> {
    let key = idempotency_key(*task_id, &TriggerKind::Cancel, &TriggerCondition::Success);
    db_operation::enqueue_outbox(
        conn,
        *task_id,
        TriggerKind::Cancel,
        TriggerCondition::Success,
        &key,
    )
    .await
}

/// Enqueue the `end` outbox row for a task plus on_failure rows for every
/// cascade-failed child. Runs inside the transition transaction.
pub(crate) async fn enqueue_end_outbox_with_cascade<'a>(
    task_id: &uuid::Uuid,
    result_status: StatusKind,
    cascade_failed_ids: &[uuid::Uuid],
    conn: &mut Conn<'a>,
) -> Result<(), DbError> {
    enqueue_end_outbox(task_id, result_status, conn).await?;
    for child_id in cascade_failed_ids {
        enqueue_end_outbox(child_id, StatusKind::Failure, conn).await?;
    }
    Ok(())
}

/// Enqueue outbox rows for ancestors canceled by dead-end detection:
/// - a `cancel` row if the ancestor was Running,
/// - plus an on_failure `end` row in all cases.
pub(crate) async fn enqueue_outbox_for_canceled_ancestors<'a>(
    ancestors: &[CanceledAncestor],
    conn: &mut Conn<'a>,
) -> Result<(), DbError> {
    for ancestor in ancestors {
        if ancestor.was_running {
            enqueue_cancel_outbox(&ancestor.id, conn).await?;
        }
        enqueue_end_outbox(&ancestor.id, StatusKind::Failure, conn).await?;
    }
    Ok(())
}
