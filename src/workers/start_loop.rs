use crate::{
    Conn, DbPool,
    action::{ActionExecutor, idempotency_key},
    db_operation,
    dtos::NewActionDto,
    metrics,
    models::{Action, Task, TriggerCondition, TriggerKind},
    rule::Strategy,
};
use actix_web::rt;
use diesel::BelongingToDsl;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::watch;
use tokio::task::JoinSet;

/// In order to cache results and avoid too many db calls.
/// Uses lock keys (i64) instead of Strategy to ensure metadata-sensitive caching:
/// two tasks with the same rule but different metadata values get different lock keys.
struct EvaluationContext {
    ko: HashSet<i64>,
}

struct StartTaskResult {
    cancel_tasks: Vec<NewActionDto>,
    idempotency_key: String,
    claimed: bool,
}

pub async fn start_loop(
    evaluator: &ActionExecutor,
    pool: DbPool,
    interval: std::time::Duration,
    dead_end_enabled: bool,
    start_batch_size: i64,
    webhook_concurrency: usize,
    mut shutdown: watch::Receiver<bool>,
) {
    let semaphore = Arc::new(tokio::sync::Semaphore::new(webhook_concurrency));

    loop {
        let loop_start = std::time::Instant::now();

        // Phase 1: Claim (sequential, single connection)
        let claimed_tasks = claim_phase(&pool, start_batch_size).await;

        // Phase 2: Webhooks (parallel, JoinSet + Semaphore)
        let tasks_processed = webhook_phase(
            claimed_tasks,
            evaluator,
            &pool,
            &semaphore,
            dead_end_enabled,
        )
        .await;

        // Record worker loop metrics
        let loop_duration = loop_start.elapsed().as_secs_f64();
        metrics::record_worker_loop_iteration("start", loop_duration);
        metrics::record_tasks_processed_per_loop(tasks_processed);

        tokio::select! {
            _ = shutdown.changed() => {
                log::info!("Start worker: shutdown signal received, exiting");
                return;
            }
            _ = rt::time::sleep(interval) => {}
        }
    }
}

/// Internal page size for the paginated claim scan. Bounds the memory used per
/// iteration (a page of full Task rows, including JSONB) while keeping the entire
/// Pending backlog *visible* across pages — visibility is never limited by the cap,
/// only memory is. See `docs/perf-correctness-plan.md`, "Lot 1".
const CLAIM_PAGE_SIZE: i64 = 500;

/// Phase 1: Scan Pending tasks page-by-page (keyset pagination) and claim them.
/// `claim_cap` (= `WORKER_START_BATCH_SIZE`) bounds the number of claims per
/// iteration. Returns the list of successfully claimed tasks.
async fn claim_phase(pool: &DbPool, claim_cap: i64) -> Vec<Task> {
    let conn = pool.get();
    let Ok(mut conn) = conn.await else {
        log::error!("Start worker: failed to acquire DB connection for claim phase");
        return vec![];
    };

    run_claim_loop(&mut conn, claim_cap, CLAIM_PAGE_SIZE).await
}

/// Paginated claim loop, factored out of `claim_phase` so tests can drive it with a
/// small `page_size`. Scans Pending tasks ordered by `priority DESC, created_at ASC,
/// id ASC` via keyset pagination, claiming up to `claim_cap` tasks.
///
/// Within each page, tasks are processed strictly in order:
/// - Rule-free tasks (empty `start_condition`) are accumulated into a contiguous
///   batch and claimed with a single `batch_claim_tasks` UPDATE. The batch is
///   flushed (claimed) as soon as we hit a rule-bearing task, the end of the page,
///   or the claim cap — never *across* a rule-bearing task (which could otherwise
///   invert priority: a low-priority rule-free run gets counted by a higher-priority
///   rule-bearing task's concurrency rule).
/// - Rule-bearing tasks are claimed one at a time via `claim_task_with_rules`, with
///   the `ko` cache of blocked concurrency lock keys carried across pages.
///
/// Early stop is gated on `claimed >= claim_cap` (we stop because we have work, not
/// blindly). A short (incomplete) page means the backlog is exhausted -> stop.
pub async fn run_claim_loop<'a>(conn: &mut Conn<'a>, claim_cap: i64, page_size: i64) -> Vec<Task> {
    let mut ctx = EvaluationContext { ko: HashSet::new() };
    let mut claimed: Vec<Task> = Vec::new();
    let mut cursor: Option<db_operation::PendingCursor> = None;
    let mut pages_scanned: usize = 0;

    'pages: loop {
        let page = match db_operation::list_pending_page(conn, cursor.as_ref(), page_size).await {
            Ok(p) => p,
            Err(e) => {
                log::error!("Start worker: error fetching pending page: {:?}", e);
                break 'pages;
            }
        };
        pages_scanned += 1;

        let page_len = page.len();
        log::debug!("Start worker: fetched page of {} pending tasks", page_len);

        // Advance the cursor to the last row of this page before consuming it.
        if let Some(last) = page.last() {
            cursor = Some(db_operation::PendingCursor::from(last));
        }

        // Buffer of contiguous rule-free tasks awaiting a batch claim.
        let mut batch: Vec<Task> = Vec::new();

        for t in page {
            if t.start_condition.0.is_empty() {
                // Rule-free: accumulate for batch claim.
                batch.push(t);
                // Flush eagerly if the buffered batch alone could reach the cap,
                // so the cap is enforced precisely.
                if claim_cap > 0 && (claimed.len() + batch.len()) as i64 >= claim_cap {
                    flush_batch(conn, &mut batch, &mut claimed, claim_cap).await;
                    if claim_cap > 0 && claimed.len() as i64 >= claim_cap {
                        break 'pages;
                    }
                }
                continue;
            }

            // Rule-bearing task: flush any pending rule-free batch FIRST so ordering
            // (and priority) is respected — never batch across a rule-bearing task.
            flush_batch(conn, &mut batch, &mut claimed, claim_cap).await;
            if claim_cap > 0 && claimed.len() as i64 >= claim_cap {
                break 'pages;
            }

            // Pre-filter: if we already know a rule is blocked in this iteration,
            // skip the DB call entirely.
            if is_prefilter_blocked(&t, &ctx) {
                metrics::record_concurrency_ko_cache_hit();
                metrics::record_task_blocked_by_concurrency();
                log::debug!("Start worker: task {} blocked by cached rule", t.id);
                continue;
            }

            match db_operation::claim_task_with_rules(conn, &t).await {
                Ok(db_operation::ClaimResult::Claimed) => {
                    metrics::record_status_transition("Pending", "Claimed");
                    claimed.push(t);
                    if claim_cap > 0 && claimed.len() as i64 >= claim_cap {
                        break 'pages;
                    }
                }
                Ok(db_operation::ClaimResult::RuleBlocked) => {
                    // Cache the blocked lock keys for this iteration so subsequent
                    // tasks with the same rule+metadata combo are skipped without a DB call.
                    // Only cache Concurency keys; Capacity sums change with task progress
                    // and cannot be reliably cached within a loop iteration.
                    for strategy in &t.start_condition.0 {
                        match strategy {
                            Strategy::Concurency(rule) => {
                                let key = db_operation::concurrency_lock_key(rule, &t.metadata);
                                ctx.ko.insert(key);
                            }
                            Strategy::Capacity(_) => {
                                // Skip: capacity sum depends on live progress, can't cache
                            }
                        }
                    }
                    metrics::record_task_blocked_by_concurrency();
                    log::debug!("Start worker: task {} blocked by concurrency rule", t.id);
                }
                Ok(db_operation::ClaimResult::AlreadyClaimed) => {
                    log::debug!(
                        "Start worker: task {} already claimed by another worker",
                        t.id
                    );
                }
                Err(e) => {
                    log::error!("Start worker: failed to claim task {}: {:?}", t.id, e);
                }
            }
        }

        // Flush any trailing rule-free batch at end of page.
        flush_batch(conn, &mut batch, &mut claimed, claim_cap).await;
        if claim_cap > 0 && claimed.len() as i64 >= claim_cap {
            break 'pages;
        }

        // Incomplete page => backlog fully scanned => done. We never stop early
        // on a full page unless the claim cap was reached above (no head-of-line
        // blocking from blind truncation).
        if (page_len as i64) < page_size {
            break 'pages;
        }
    }

    metrics::record_claim_pages_scanned(pages_scanned);
    // Connection is dropped by the caller (returned to pool)
    claimed
}

/// Flush a buffered run of contiguous rule-free tasks via a single batch-claim UPDATE.
/// Respects `claim_cap`: at most `claim_cap - claimed.len()` tasks from the buffer are
/// claimed (the rest are left Pending for a later iteration). Emits one
/// `record_status_transition("Pending","Claimed")` per task actually claimed.
async fn flush_batch<'a>(
    conn: &mut Conn<'a>,
    batch: &mut Vec<Task>,
    claimed: &mut Vec<Task>,
    claim_cap: i64,
) {
    if batch.is_empty() {
        return;
    }

    // Bound the batch by the remaining claim budget.
    let mut to_claim = std::mem::take(batch);
    if claim_cap > 0 {
        let remaining = (claim_cap - claimed.len() as i64).max(0) as usize;
        if to_claim.len() > remaining {
            to_claim.truncate(remaining);
        }
    }
    if to_claim.is_empty() {
        return;
    }

    let ids: Vec<uuid::Uuid> = to_claim.iter().map(|t| t.id).collect();
    match db_operation::batch_claim_tasks(conn, &ids).await {
        Ok(claimed_ids) => {
            let claimed_set: HashSet<uuid::Uuid> = claimed_ids.into_iter().collect();
            for t in to_claim {
                if claimed_set.contains(&t.id) {
                    metrics::record_status_transition("Pending", "Claimed");
                    claimed.push(t);
                } else {
                    log::debug!(
                        "Start worker: task {} already claimed by another worker (batch)",
                        t.id
                    );
                }
            }
        }
        Err(e) => {
            log::error!(
                "Start worker: failed to batch-claim rule-free tasks: {:?}",
                e
            );
        }
    }
}

/// Phase 2: Execute on_start webhooks for all claimed tasks in parallel,
/// bounded by the semaphore.
async fn webhook_phase(
    claimed_tasks: Vec<Task>,
    evaluator: &ActionExecutor,
    pool: &DbPool,
    semaphore: &Arc<tokio::sync::Semaphore>,
    dead_end_enabled: bool,
) -> usize {
    if claimed_tasks.is_empty() {
        return 0;
    }

    let mut join_set = JoinSet::new();

    for t in claimed_tasks {
        // Acquire permit BEFORE spawning: bounds live spawned tasks to
        // semaphore capacity, preventing unbounded memory growth from
        // eagerly-spawned futures sitting in the JoinSet.
        let permit = Arc::clone(semaphore)
            .acquire_owned()
            .await
            .expect("semaphore should not be closed");

        let pool = pool.clone();
        let evaluator = evaluator.clone();

        join_set.spawn(async move {
            let _permit = permit; // released on drop
            let _guard = metrics::WebhooksInFlightGuard::new("start");
            execute_webhook_for_task(&evaluator, t, &pool, dead_end_enabled).await
        });
    }

    let mut tasks_processed = 0usize;
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(true) => tasks_processed += 1,
            Ok(false) => {}
            Err(e) => {
                log::error!("Start worker: webhook task panicked: {:?}", e);
            }
        }
    }

    tasks_processed
}

/// Execute the full webhook lifecycle for a single claimed task:
/// 1. start_task (on_start webhooks)
/// 2. mark_task_running
/// 3. save_cancel_actions
/// On failure: fail_task_and_propagate
///
/// Returns true if the task was successfully started.
async fn execute_webhook_for_task(
    evaluator: &ActionExecutor,
    task: Task,
    pool: &DbPool,
    dead_end_enabled: bool,
) -> bool {
    let Ok(mut conn) = pool.get().await else {
        log::error!(
            "Start worker: failed to acquire DB connection for task {}",
            task.id
        );
        return false;
    };

    match start_task(evaluator, &task, &mut conn).await {
        Ok(start_result) => {
            let StartTaskResult {
                cancel_tasks,
                idempotency_key,
                claimed,
            } = start_result;

            match db_operation::mark_task_running(&mut conn, &task.id).await {
                Ok(true) => {
                    metrics::record_status_transition("Claimed", "Running");
                    // Scheduler latency: time from task creation to actually running.
                    let wait_secs = (chrono::Utc::now() - task.created_at)
                        .num_milliseconds()
                        .max(0) as f64
                        / 1000.0;
                    metrics::record_task_wait(&task.kind, wait_secs);
                    log::debug!("Start worker: task {} started", task.id);

                    // Save cancel actions returned by the webhook
                    if let Err(e) =
                        db_operation::save_cancel_actions(&mut conn, &task, &cancel_tasks).await
                    {
                        log::error!(
                            "Start worker: failed to save cancel actions for task {}: {:?}",
                            task.id,
                            e
                        );
                    }
                }
                Ok(false) => {
                    log::warn!(
                        "Start worker: task {} no longer claimed; skipping running transition",
                        task.id
                    );
                }
                Err(e) => {
                    log::error!(
                        "Start worker: failed to mark task {} as running: {:?}",
                        task.id,
                        e
                    );
                }
            }

            if claimed {
                if let Err(e) =
                    db_operation::complete_webhook_execution(&mut conn, &idempotency_key, true)
                        .await
                {
                    log::error!(
                        "Failed to complete webhook execution record for key {}: {}",
                        idempotency_key,
                        e
                    );
                }
            }

            true
        }
        Err(e) => {
            // Webhook failed after claim -> mark task as failed,
            // propagate to children, and fire on_failure webhooks
            log::error!(
                "Start worker: on_start webhook failed for task {}: {:?}",
                task.id,
                e
            );
            if let Err(e2) = db_operation::fail_task_and_propagate(
                &mut conn,
                &task.id,
                "on_start webhook failed",
                dead_end_enabled,
            )
            .await
            {
                log::error!(
                    "Start worker: failed to mark task {} as failed and propagate: {:?}",
                    task.id,
                    e2
                );
            }
            false
        }
    }
}

/// Pre-filter check: if any of the task's rule+metadata lock keys were already
/// blocked in this iteration, skip the expensive DB call. Within a single loop
/// iteration, counts can only increase (we only claim tasks), so a blocked key
/// stays blocked.
fn is_prefilter_blocked(task: &Task, ctx: &EvaluationContext) -> bool {
    let conditions = &task.start_condition.0;
    if conditions.is_empty() {
        return false;
    }
    conditions.iter().any(|cond| match cond {
        Strategy::Concurency(rule) => {
            let key = db_operation::concurrency_lock_key(rule, &task.metadata);
            ctx.ko.contains(&key)
        }
        Strategy::Capacity(_) => false, // Can't prefilter: sum depends on live progress
    })
}

async fn start_task<'a>(
    evaluator: &ActionExecutor,
    task: &Task,
    conn: &mut Conn<'a>,
) -> Result<StartTaskResult, String> {
    use crate::schema::action::dsl::*;

    // Idempotency guard: claim the start trigger slot
    let key = idempotency_key(task.id, &TriggerKind::Start, &TriggerCondition::Success);
    let claimed = db_operation::try_claim_webhook_execution(
        conn,
        task.id,
        TriggerKind::Start,
        TriggerCondition::Success,
        &key,
        Some(evaluator.ctx.webhook_idempotency_timeout),
    )
    .await
    .map_err(|e| format!("Failed to claim webhook execution: {}", e))?;

    if !claimed {
        log::info!(
            "Start worker: skipping on_start webhooks for task {} — already executed (key={})",
            task.id,
            key
        );
        metrics::record_webhook_idempotent_skip("start");
        metrics::record_webhook_idempotent_conflict();
        return Ok(StartTaskResult {
            cancel_tasks: vec![],
            idempotency_key: key,
            claimed: false,
        });
    }

    let actions = Action::belonging_to(&task)
        .filter(trigger.eq(TriggerKind::Start))
        .load::<Action>(conn)
        .await
        .map_err(|e| e.to_string())?;
    let mut tasks = vec![];
    let mut errors: Vec<String> = Vec::new();
    for act in actions.iter() {
        let res = evaluator.execute(act, task, Some(&key)).await;
        match res {
            Ok(r) => {
                if let Some(t) = r {
                    tasks.push(t);
                };
                log::debug!("Action {} executed successfully", act.id);
            }
            Err(e) => {
                log::error!("Action {} failed: {}", act.id, e);
                errors.push(e);
            }
        }
    }

    let succeeded = errors.is_empty();
    if !succeeded {
        if let Err(e) = db_operation::complete_webhook_execution(conn, &key, false).await {
            log::error!(
                "Failed to complete webhook execution record for key {}: {}",
                key,
                e
            );
        }
        return Err(format!(
            "one or more on_start actions failed for task {}: {}",
            task.id,
            errors.join("; ")
        ));
    }
    Ok(StartTaskResult {
        cancel_tasks: tasks,
        idempotency_key: key,
        claimed: true,
    })
}
