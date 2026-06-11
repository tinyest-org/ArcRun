//! Webhook delivery loop (Lot 2 — transactional outbox).
//!
//! The 5th background worker. End/cancel webhooks are no longer fired inline in the
//! HTTP/worker call path: the status-change transaction enqueues a `pending` row in
//! `webhook_execution` (the outbox), and this loop drains it asynchronously with
//! at-least-once delivery semantics and exponential backoff.
//!
//! Guarantees (see `docs/perf-correctness-plan.md`, "Contrat API cible"):
//! - **At-least-once**: a row survives crash/redeploy and is delivered on restart.
//! - **Per-task order**: an `end`/`cancel` row is only delivered once the task's
//!   `start` row is no longer `pending` (enforced in `claim_due_outbox`).
//! - **Idempotency**: the `Idempotency-Key` header (= `idempotency_key`) lets the
//!   consumer dedupe; a delivered row is marked `success` and never re-sent.

use std::sync::Arc;

use actix_web::rt;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use tokio::sync::watch;

use crate::{
    Conn, DbPool,
    action::{ActionExecutor, WebhookEnrichment},
    db_operation, metrics,
    models::{Action, StatusKind, Task, TriggerCondition, TriggerKind, WebhookExecution},
};

/// Tunables for the delivery loop, resolved from config once at startup.
#[derive(Clone, Copy)]
pub struct DeliveryConfig {
    pub batch_size: i64,
    pub max_attempts: i32,
    pub backoff_base_secs: i64,
    pub backoff_cap_secs: i64,
}

/// Background loop: drains the webhook outbox at a fixed interval until shutdown.
pub async fn delivery_loop(
    evaluator: Arc<ActionExecutor>,
    pool: DbPool,
    interval: std::time::Duration,
    cfg: DeliveryConfig,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        if let Ok(mut conn) = pool.get().await {
            match run_delivery_once(evaluator.as_ref(), &mut conn, cfg).await {
                Ok(0) => log::debug!("Delivery worker: no mature outbox rows"),
                Ok(n) => log::debug!("Delivery worker: processed {} outbox rows", n),
                Err(e) => log::error!("Delivery worker: error draining outbox: {:?}", e),
            }
        } else {
            log::error!("Delivery worker: failed to acquire DB connection");
        }

        tokio::select! {
            _ = shutdown.changed() => {
                log::info!("Delivery worker: shutdown signal received, exiting");
                return;
            }
            _ = rt::time::sleep(interval) => {}
        }
    }
}

/// Drain one batch of mature outbox rows. Returns the number of rows processed.
///
/// Exposed (and re-exported via `workers::run_delivery_once`) so integration tests
/// can drive delivery deterministically instead of waiting on the timer.
///
/// The claim SELECT (`FOR UPDATE SKIP LOCKED`) and all per-row state updates run in
/// a single transaction, so the row locks are held for the duration of delivery —
/// this prevents a concurrent delivery worker from also picking up the same rows.
pub async fn run_delivery_once<'a>(
    evaluator: &'a ActionExecutor,
    conn: &mut Conn<'a>,
    cfg: DeliveryConfig,
) -> Result<usize, db_operation::DbError> {
    let batch_size = cfg.batch_size;
    db_operation::run_in_transaction(conn, |conn| {
        Box::pin(async move {
            let due = db_operation::claim_due_outbox(conn, batch_size).await?;
            let mut processed = 0usize;
            for row in due {
                deliver_row(evaluator, conn, &row, cfg).await?;
                processed += 1;
            }
            Ok(processed)
        })
    })
    .await
}

/// Deliver a single outbox row: load the task + its actions for this trigger,
/// execute them, and transition the row to success / retry / exhausted.
async fn deliver_row<'a>(
    evaluator: &ActionExecutor,
    conn: &mut Conn<'a>,
    row: &WebhookExecution,
    cfg: DeliveryConfig,
) -> Result<(), db_operation::DbError> {
    use crate::schema::action::dsl::{condition as a_condition, trigger as a_trigger};
    use crate::schema::task::dsl::{id as task_id_col, task as task_tbl};

    let trigger_label = trigger_label(row.trigger, row.condition);
    let key = &row.idempotency_key;

    // Batch-level (batch_complete) rows take a dedicated path (no task, no handle).
    if row.trigger == TriggerKind::BatchComplete {
        return deliver_batch_complete_row(evaluator, conn, row, cfg).await;
    }

    // Task-level rows must have a task_id (DB CHECK guarantees one of task/batch).
    let Some(row_task_id) = row.task_id else {
        log::error!(
            "Delivery worker: task-level outbox key {} has NULL task_id; marking success",
            key
        );
        db_operation::mark_outbox_success(conn, key).await?;
        return Ok(());
    };

    // Load the task (it must still exist; if it was deleted by retention we treat
    // the delivery as vacuously done).
    let task: Option<Task> = task_tbl
        .filter(task_id_col.eq(row_task_id))
        .first::<Task>(conn)
        .await
        .optional()?;

    let Some(task) = task else {
        log::warn!(
            "Delivery worker: task {} for outbox key {} no longer exists; marking success",
            row_task_id,
            key
        );
        db_operation::mark_outbox_success(conn, key).await?;
        return Ok(());
    };

    let actions = Action::belonging_to(&task)
        .filter(a_trigger.eq(row.trigger))
        .filter(a_condition.eq(row.condition))
        .load::<Action>(conn)
        .await?;

    // No matching actions: nothing to deliver. Mark success immediately (documented
    // design choice — we enqueue unconditionally to keep the transition tx minimal).
    if actions.is_empty() {
        record_lag(row);
        db_operation::mark_outbox_success(conn, key).await?;
        return Ok(());
    }

    let enrichment = WebhookEnrichment {
        status: task.status,
        ended_at: task.ended_at,
        trigger: match row.trigger {
            TriggerKind::Cancel => "cancel".to_string(),
            _ => "end".to_string(),
        },
    };

    let mut errors: Vec<String> = Vec::new();
    for act in actions.iter() {
        match evaluator
            .execute_with_enrichment(act, &task, Some(key), Some(&enrichment))
            .await
        {
            Ok(_) => log::debug!("Delivery worker: action {} delivered", act.id),
            Err(e) => {
                log::warn!("Delivery worker: action {} delivery failed: {}", act.id, e);
                errors.push(e);
            }
        }
    }

    if errors.is_empty() {
        record_lag(row);
        db_operation::mark_outbox_success(conn, key).await?;
        return Ok(());
    }

    // At least one action failed. Either reschedule with backoff or give up.
    let error_msg = errors.join("; ");
    // attempts is the count of prior failed attempts; this delivery makes it +1.
    let attempts_after = row.attempts + 1;
    if attempts_after >= cfg.max_attempts {
        log::error!(
            "Delivery worker: outbox key {} exhausted after {} attempts: {}",
            key,
            attempts_after,
            error_msg
        );
        db_operation::mark_outbox_exhausted(conn, key, &error_msg).await?;
        metrics::record_webhook_delivery_exhausted(trigger_label);
    } else {
        let backoff = compute_backoff(row.attempts, cfg);
        db_operation::mark_outbox_retry(conn, key, &error_msg, backoff).await?;
        metrics::record_webhook_delivery_retry(trigger_label);
    }

    Ok(())
}

/// Deliver a batch-complete outbox row: load the batch's `on_complete` payload,
/// execute each action WITHOUT a `?handle=` (no task to drive), with an `arcrun`
/// enrichment of `{batch_id, counts, completed_at}` merged into the body.
async fn deliver_batch_complete_row<'a>(
    evaluator: &ActionExecutor,
    conn: &mut Conn<'a>,
    row: &WebhookExecution,
    cfg: DeliveryConfig,
) -> Result<(), db_operation::DbError> {
    let key = &row.idempotency_key;
    let trigger_label = "batch_complete";

    let Some(batch_id) = row.batch_id else {
        log::error!(
            "Delivery worker: batch_complete outbox key {} has NULL batch_id; marking success",
            key
        );
        db_operation::mark_outbox_success(conn, key).await?;
        return Ok(());
    };

    // Load the batch payload. If the batch row is gone (e.g. retention removed it),
    // treat delivery as vacuously done.
    let on_complete = db_operation::load_batch_on_complete(conn, batch_id).await?;
    let Some(on_complete) = on_complete else {
        log::warn!(
            "Delivery worker: batch {} for outbox key {} no longer exists; marking success",
            batch_id,
            key
        );
        db_operation::mark_outbox_success(conn, key).await?;
        return Ok(());
    };

    let actions: Vec<crate::dtos::NewActionDto> = match serde_json::from_value(on_complete) {
        Ok(a) => a,
        Err(e) => {
            // Malformed payload: this can never succeed, so exhaust immediately.
            log::error!(
                "Delivery worker: batch {} on_complete payload is malformed: {}",
                batch_id,
                e
            );
            db_operation::mark_outbox_exhausted(conn, key, &format!("malformed payload: {}", e))
                .await?;
            metrics::record_webhook_delivery_exhausted(trigger_label);
            return Ok(());
        }
    };

    if actions.is_empty() {
        record_lag(row);
        db_operation::mark_outbox_success(conn, key).await?;
        return Ok(());
    }

    // Compute counts + completed_at at delivery time.
    let stats = db_operation::batch_completion_stats(conn, batch_id).await?;
    let completed_at = stats.completed_at.unwrap_or(row.updated_at);
    let enrichment = serde_json::json!({
        "batch_id": batch_id,
        "counts": {
            "success": stats.success,
            "failure": stats.failure,
            "canceled": stats.canceled,
        },
        "completed_at": completed_at,
        "trigger": "batch_complete",
    });

    let mut errors: Vec<String> = Vec::new();
    for act in &actions {
        match evaluator
            .execute_batch_action(act, Some(key), enrichment.clone())
            .await
        {
            Ok(_) => log::debug!("Delivery worker: batch_complete action delivered"),
            Err(e) => {
                log::warn!("Delivery worker: batch_complete action failed: {}", e);
                errors.push(e);
            }
        }
    }

    if errors.is_empty() {
        record_lag(row);
        db_operation::mark_outbox_success(conn, key).await?;
        return Ok(());
    }

    let error_msg = errors.join("; ");
    let attempts_after = row.attempts + 1;
    if attempts_after >= cfg.max_attempts {
        log::error!(
            "Delivery worker: batch_complete outbox key {} exhausted after {} attempts: {}",
            key,
            attempts_after,
            error_msg
        );
        db_operation::mark_outbox_exhausted(conn, key, &error_msg).await?;
        metrics::record_webhook_delivery_exhausted(trigger_label);
    } else {
        let backoff = compute_backoff(row.attempts, cfg);
        db_operation::mark_outbox_retry(conn, key, &error_msg, backoff).await?;
        metrics::record_webhook_delivery_retry(trigger_label);
    }

    Ok(())
}

/// Exponential backoff: `base^(prior_attempts + 1)` seconds, capped.
/// First failure (prior_attempts = 0) => base^1; second => base^2; etc.
fn compute_backoff(prior_attempts: i32, cfg: DeliveryConfig) -> i64 {
    let exp = (prior_attempts as u32).saturating_add(1);
    let mut delay = cfg.backoff_base_secs;
    for _ in 1..exp {
        delay = delay.saturating_mul(cfg.backoff_base_secs);
        if delay >= cfg.backoff_cap_secs {
            return cfg.backoff_cap_secs;
        }
    }
    delay.min(cfg.backoff_cap_secs)
}

fn record_lag(row: &WebhookExecution) {
    let lag = (chrono::Utc::now() - row.created_at)
        .num_milliseconds()
        .max(0) as f64
        / 1000.0;
    metrics::record_webhook_delivery_lag(lag);
}

fn trigger_label(trigger: TriggerKind, condition: TriggerCondition) -> &'static str {
    match (trigger, condition) {
        (TriggerKind::End, TriggerCondition::Success) => "end_success",
        (TriggerKind::End, TriggerCondition::Failure) => "end_failure",
        (TriggerKind::Cancel, _) => "cancel",
        (TriggerKind::Start, _) => "start",
        (TriggerKind::BatchComplete, _) => "batch_complete",
    }
}

/// Map a task's final status to the outbox `end` condition. Mirrors the inline
/// `fire_end_webhooks` mapping (Success => Success, everything else => Failure).
pub(crate) fn end_condition_for(status: StatusKind) -> TriggerCondition {
    match status {
        StatusKind::Success => TriggerCondition::Success,
        _ => TriggerCondition::Failure,
    }
}
