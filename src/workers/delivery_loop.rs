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
//!   `start` row is no longer `pending` (enforced in `claim_due_outbox_leased`).
//! - **Idempotency**: the `Idempotency-Key` header (= `idempotency_key`) lets the
//!   consumer dedupe; a delivered row is marked `success` and never re-sent.
//!
//! ## Lease-based claim + parallel out-of-tx delivery
//!
//! `run_delivery_once` runs in four phases, NOT one long transaction:
//!
//! 1. **Claim (short tx).** Select the mature rows and push their `next_attempt_at`
//!    a *lease* into the future (`claim_due_outbox_leased`). The lease is a soft lock:
//!    while it holds, no other worker / iteration re-claims the row; if we crash
//!    mid-delivery, the lease expires and the row matures again (at-least-once). The
//!    lease does NOT bump `attempts`.
//! 2. **Prefetch (autocommit reads).** Load each row's delivery inputs (task + actions,
//!    or `batch.on_complete` + stats). Terminal states are immutable, so these reads
//!    are stable. The fast-paths (task/batch gone ⇒ success; malformed batch payload ⇒
//!    exhausted; zero actions ⇒ success) are resolved here, before any HTTP.
//! 3. **Deliver (parallel, no DB).** The HTTP executions run concurrently, bounded by
//!    `concurrency`, via `buffer_unordered`. No DB connection is held during HTTP.
//!    Actions of a *single* row stay sequential (unchanged behaviour).
//! 4. **Mark (short autocommit statements).** Each outcome is committed with the
//!    existing `mark_outbox_*` helpers. A failed mark is logged and skipped — it does
//!    NOT roll back already-posted marks (the old single-tx design's flaw); the lease
//!    re-delivers, which is fine under at-least-once.

use std::sync::Arc;

use actix_web::rt;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use futures_util::stream::{self, StreamExt};
use tokio::sync::watch;

use crate::{
    Conn, DbPool,
    action::{ActionExecutor, WebhookEnrichment},
    db_operation, metrics,
    models::{Action, StatusKind, Task, TriggerCondition, TriggerKind, WebhookExecution},
    workers::WorkerNudges,
};

/// Tunables for the delivery loop, resolved from config once at startup.
#[derive(Clone, Copy)]
pub struct DeliveryConfig {
    pub batch_size: i64,
    pub max_attempts: i32,
    pub backoff_base_secs: i64,
    pub backoff_cap_secs: i64,
    /// Lease (seconds) applied to a row at claim time: it stays "in-flight" (not
    /// re-claimable) until the lease expires. Must exceed the worst-case single-row
    /// delivery time.
    pub lease_secs: i64,
    /// Max concurrent HTTP deliveries within one `run_delivery_once` batch.
    pub concurrency: usize,
    /// Freshness bound (seconds) for the start-before-end gate (Audit 2, A2). An
    /// `end`/`cancel` row is held back only while the task's `start` row is pending
    /// AND that start row was updated within the last `start_stale_secs`. A start row
    /// that never completes (crash between `mark_task_running` and the start-row
    /// completion, or a Claimed task canceled mid-webhook) eventually goes stale and
    /// stops blocking delivery. Mirror of the `webhook_idempotency_timeout` /
    /// `WORKER_CLAIM_TIMEOUT_SECS` staleness used by `try_claim_webhook_execution`.
    pub start_stale_secs: i64,
}

/// Background loop: drains the webhook outbox at a fixed interval until shutdown.
pub async fn delivery_loop(
    evaluator: Arc<ActionExecutor>,
    pool: DbPool,
    interval: std::time::Duration,
    cfg: DeliveryConfig,
    mut shutdown: watch::Receiver<bool>,
    nudges: WorkerNudges,
) {
    loop {
        let loop_start = std::time::Instant::now();
        if let Ok(mut conn) = pool.get().await {
            match run_delivery_once(evaluator.as_ref(), &mut conn, cfg).await {
                Ok(0) => log::debug!("Delivery worker: no mature outbox rows"),
                Ok(n) => log::debug!("Delivery worker: processed {} outbox rows", n),
                Err(e) => log::error!("Delivery worker: error draining outbox: {:?}", e),
            }

            // Snapshot the outbox backlog (depth + oldest-mature age). A single
            // indexed scan per iteration — reveals a growing backlog or a stuck row,
            // neither of which the per-delivery lag histogram can show.
            match db_operation::outbox_backlog_stats(&mut conn).await {
                Ok(stats) => metrics::set_webhook_outbox_backlog(
                    stats.ready,
                    stats.leased,
                    stats.oldest_ready_age_secs,
                ),
                Err(e) => log::warn!("Delivery worker: outbox backlog stats failed: {:?}", e),
            }
        } else {
            log::error!("Delivery worker: failed to acquire DB connection");
        }
        metrics::record_worker_loop_iteration("delivery", loop_start.elapsed().as_secs_f64());

        // B4: a transition that enqueued an outbox row (end/failure/cancel/
        // batch_complete) nudges this loop so the delivery doesn't wait a full tick.
        // `notify_one` keeps a nudge fired mid-iteration from being lost. The poll
        // remains the correctness/fallback path (e.g. rows maturing off a backoff, or
        // a lease expiring after a crash).
        tokio::select! {
            _ = shutdown.changed() => {
                log::info!("Delivery worker: shutdown signal received, exiting");
                return;
            }
            _ = nudges.delivery.notified() => {}
            _ = rt::time::sleep(interval) => {}
        }
    }
}

/// Drain one batch of mature outbox rows. Returns the number of rows processed.
///
/// Exposed (and re-exported via `workers::run_delivery_once`) so integration tests
/// can drive delivery deterministically instead of waiting on the timer.
///
/// Four phases (see module docs): short claim tx (with lease), out-of-tx prefetch,
/// parallel HTTP delivery, then short mark statements. The single `conn` is used only
/// for the DB phases (claim / prefetch / marks); the HTTP phase holds no connection.
pub async fn run_delivery_once<'a>(
    evaluator: &'a ActionExecutor,
    conn: &mut Conn<'a>,
    cfg: DeliveryConfig,
) -> Result<usize, db_operation::DbError> {
    // Phase 1 — claim with lease (short transaction).
    let claimed = db_operation::run_in_transaction(conn, |conn| {
        let batch_size = cfg.batch_size;
        let lease = cfg.lease_secs;
        let start_stale = cfg.start_stale_secs;
        Box::pin(async move {
            db_operation::claim_due_outbox_leased(conn, batch_size, lease, start_stale).await
        })
    })
    .await?;

    let processed = claimed.len();
    if claimed.is_empty() {
        return Ok(0);
    }

    // Phase 2 — prefetch delivery inputs (autocommit reads). Fast-path marks are
    // collected separately so they don't need an HTTP round-trip.
    let mut plans: Vec<DeliveryPlan> = Vec::with_capacity(claimed.len());
    for row in claimed {
        match prepare_row(conn, row).await? {
            Prepared::Mark(mark) => apply_mark(conn, mark).await,
            Prepared::Deliver(plan) => plans.push(plan),
        }
    }

    // Phase 3 — deliver in parallel (no DB connection held).
    let outcomes: Vec<DeliveryOutcome> = stream::iter(plans.into_iter().map(|plan| {
        let evaluator = &*evaluator;
        async move { deliver_plan(evaluator, plan, cfg).await }
    }))
    .buffer_unordered(cfg.concurrency.max(1))
    .collect()
    .await;

    // Phase 4 — post results (short autocommit statements; failures don't cascade).
    for outcome in outcomes {
        apply_mark(conn, outcome.mark).await;
    }

    Ok(processed)
}

/// Outcome of preparing a row for delivery: either an immediate mark (fast-path) or a
/// plan that requires HTTP delivery.
enum Prepared {
    Mark(MarkAction),
    Deliver(DeliveryPlan),
}

/// What to do with an outbox row's `status` after preparing/delivering it. Owns its
/// data so it can be applied without any borrow of the original row.
enum MarkAction {
    Success {
        key: String,
        lag: Option<f64>,
        label: &'static str,
    },
    Retry {
        key: String,
        error: String,
        backoff: i64,
        label: &'static str,
    },
    Exhausted {
        key: String,
        error: String,
        label: &'static str,
    },
}

/// Everything needed to deliver one outbox row's HTTP, owned (no DB borrow).
struct DeliveryPlan {
    key: String,
    trigger_label: &'static str,
    /// Prior failed-attempt count (for backoff/exhaustion decisions).
    attempts: i32,
    /// `created_at`, for the delivery-lag metric on success.
    created_at: chrono::DateTime<chrono::Utc>,
    kind: DeliveryKind,
}

/// Task-level vs batch-level delivery payload.
enum DeliveryKind {
    Task {
        task: Box<Task>,
        actions: Vec<Action>,
        enrichment: WebhookEnrichment,
    },
    Batch {
        actions: Vec<crate::dtos::NewActionDto>,
        enrichment: serde_json::Value,
    },
}

/// Result of delivering a plan: which mark to post.
struct DeliveryOutcome {
    mark: MarkAction,
}

/// Phase 2 helper: prefetch a row's delivery inputs and resolve fast-paths.
async fn prepare_row<'a>(
    conn: &mut Conn<'a>,
    row: WebhookExecution,
) -> Result<Prepared, db_operation::DbError> {
    if row.trigger == TriggerKind::BatchComplete {
        return prepare_batch_complete_row(conn, row).await;
    }
    prepare_task_row(conn, row).await
}

async fn prepare_task_row<'a>(
    conn: &mut Conn<'a>,
    row: WebhookExecution,
) -> Result<Prepared, db_operation::DbError> {
    use crate::schema::action::dsl::{condition as a_condition, trigger as a_trigger};
    use crate::schema::task::dsl::{id as task_id_col, task as task_tbl};

    let key = row.idempotency_key.clone();
    let label = trigger_label(row.trigger, row.condition);

    let Some(row_task_id) = row.task_id else {
        log::error!(
            "Delivery worker: task-level outbox key {} has NULL task_id; marking success",
            key
        );
        return Ok(Prepared::Mark(MarkAction::Success {
            key,
            lag: None,
            label,
        }));
    };

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
        return Ok(Prepared::Mark(MarkAction::Success {
            key,
            lag: None,
            label,
        }));
    };

    let actions = Action::belonging_to(&task)
        .filter(a_trigger.eq(row.trigger))
        .filter(a_condition.eq(row.condition))
        .load::<Action>(conn)
        .await?;

    // No matching actions: nothing to deliver. Mark success immediately (documented
    // design choice — we enqueue unconditionally to keep the transition tx minimal).
    if actions.is_empty() {
        return Ok(Prepared::Mark(MarkAction::Success {
            key,
            lag: Some(lag_secs(row.created_at)),
            label,
        }));
    }

    let enrichment = WebhookEnrichment {
        status: task.status,
        ended_at: task.ended_at,
        trigger: match row.trigger {
            TriggerKind::Cancel => "cancel".to_string(),
            _ => "end".to_string(),
        },
    };

    Ok(Prepared::Deliver(DeliveryPlan {
        key,
        trigger_label: label,
        attempts: row.attempts,
        created_at: row.created_at,
        kind: DeliveryKind::Task {
            task: Box::new(task),
            actions,
            enrichment,
        },
    }))
}

async fn prepare_batch_complete_row<'a>(
    conn: &mut Conn<'a>,
    row: WebhookExecution,
) -> Result<Prepared, db_operation::DbError> {
    let key = row.idempotency_key.clone();
    let label = "batch_complete";

    let Some(batch_id) = row.batch_id else {
        log::error!(
            "Delivery worker: batch_complete outbox key {} has NULL batch_id; marking success",
            key
        );
        return Ok(Prepared::Mark(MarkAction::Success {
            key,
            lag: None,
            label,
        }));
    };

    let on_complete = db_operation::load_batch_on_complete(conn, batch_id).await?;
    let Some(on_complete) = on_complete else {
        log::warn!(
            "Delivery worker: batch {} for outbox key {} no longer exists; marking success",
            batch_id,
            key
        );
        return Ok(Prepared::Mark(MarkAction::Success {
            key,
            lag: None,
            label,
        }));
    };

    let actions: Vec<crate::dtos::NewActionDto> = match serde_json::from_value(on_complete) {
        Ok(a) => a,
        Err(e) => {
            // Malformed payload: this can never succeed, so exhaust immediately.
            // (The exhausted metric is emitted by apply_mark, the single emission point.)
            log::error!(
                "Delivery worker: batch {} on_complete payload is malformed: {}",
                batch_id,
                e
            );
            return Ok(Prepared::Mark(MarkAction::Exhausted {
                key,
                error: format!("malformed payload: {}", e),
                label,
            }));
        }
    };

    if actions.is_empty() {
        return Ok(Prepared::Mark(MarkAction::Success {
            key,
            lag: Some(lag_secs(row.created_at)),
            label,
        }));
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

    Ok(Prepared::Deliver(DeliveryPlan {
        key,
        trigger_label: label,
        attempts: row.attempts,
        created_at: row.created_at,
        kind: DeliveryKind::Batch {
            actions,
            enrichment,
        },
    }))
}

/// Phase 3 helper: run a plan's HTTP (no DB) and decide the resulting mark.
async fn deliver_plan(
    evaluator: &ActionExecutor,
    plan: DeliveryPlan,
    cfg: DeliveryConfig,
) -> DeliveryOutcome {
    let DeliveryPlan {
        key,
        trigger_label,
        attempts,
        created_at,
        kind,
    } = plan;

    // Track delivery-phase concurrency (separate from the start phase) so we can see
    // whether we plateau at WEBHOOK_DELIVERY_CONCURRENCY.
    let _in_flight = metrics::WebhooksInFlightGuard::new("delivery");

    let mut errors: Vec<String> = Vec::new();
    match kind {
        DeliveryKind::Task {
            task,
            actions,
            enrichment,
        } => {
            for act in actions.iter() {
                match evaluator
                    .execute_with_enrichment(act, &task, Some(&key), Some(&enrichment))
                    .await
                {
                    Ok(_) => log::debug!("Delivery worker: action {} delivered", act.id),
                    Err(e) => {
                        log::warn!("Delivery worker: action {} delivery failed: {}", act.id, e);
                        errors.push(e);
                    }
                }
            }
        }
        DeliveryKind::Batch {
            actions,
            enrichment,
        } => {
            for act in &actions {
                match evaluator
                    .execute_batch_action(act, Some(&key), enrichment.clone())
                    .await
                {
                    Ok(_) => log::debug!("Delivery worker: batch_complete action delivered"),
                    Err(e) => {
                        log::warn!("Delivery worker: batch_complete action failed: {}", e);
                        errors.push(e);
                    }
                }
            }
        }
    }

    if errors.is_empty() {
        return DeliveryOutcome {
            mark: MarkAction::Success {
                key,
                lag: Some(lag_secs(created_at)),
                label: trigger_label,
            },
        };
    }

    // At least one action failed. Either reschedule with backoff or give up.
    let error_msg = errors.join("; ");
    // attempts is the count of prior failed attempts; this delivery makes it +1.
    let attempts_after = attempts + 1;
    if attempts_after >= cfg.max_attempts {
        log::error!(
            "Delivery worker: outbox key {} exhausted after {} attempts: {}",
            key,
            attempts_after,
            error_msg
        );
        DeliveryOutcome {
            mark: MarkAction::Exhausted {
                key,
                error: error_msg,
                label: trigger_label,
            },
        }
    } else {
        let backoff = compute_backoff(attempts, cfg);
        DeliveryOutcome {
            mark: MarkAction::Retry {
                key,
                error: error_msg,
                backoff,
                label: trigger_label,
            },
        }
    }
}

/// Phase 4 helper: post one mark via the existing autocommit helpers. A DB error is
/// logged and swallowed (the lease will re-deliver; at-least-once). This never rolls
/// back marks already posted for other rows in the batch.
async fn apply_mark<'a>(conn: &mut Conn<'a>, mark: MarkAction) {
    let (result, mark_label) = match &mark {
        MarkAction::Success { key, lag, label } => {
            if let Some(lag) = lag {
                metrics::record_webhook_delivery_lag(*lag);
            }
            let r = db_operation::mark_outbox_success(conn, key).await;
            if r.is_ok() {
                metrics::record_webhook_delivery_success(label);
            }
            (r, "success")
        }
        MarkAction::Retry {
            key,
            error,
            backoff,
            label,
        } => {
            let r = db_operation::mark_outbox_retry(conn, key, error, *backoff).await;
            if r.is_ok() {
                metrics::record_webhook_delivery_retry(label);
            }
            (r, "retry")
        }
        MarkAction::Exhausted { key, error, label } => {
            let r = db_operation::mark_outbox_exhausted(conn, key, error).await;
            if r.is_ok() {
                metrics::record_webhook_delivery_exhausted(label);
            }
            (r, "exhausted")
        }
    };

    if let Err(e) = result {
        // Previously swallowed silently (M2): count it so a mark that fails in a loop
        // is visible without grepping logs. The lease re-delivers (at-least-once).
        metrics::record_webhook_mark_failure(mark_label);
        let key = match &mark {
            MarkAction::Success { key, .. }
            | MarkAction::Retry { key, .. }
            | MarkAction::Exhausted { key, .. } => key,
        };
        log::error!(
            "Delivery worker: failed to post outbox mark for key {} ({:?}); \
             lease will re-deliver",
            key,
            e
        );
    }
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

fn lag_secs(created_at: chrono::DateTime<chrono::Utc>) -> f64 {
    (chrono::Utc::now() - created_at).num_milliseconds().max(0) as f64 / 1000.0
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
