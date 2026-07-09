use crate::{
    Conn, DbPool,
    action::{ActionExecutor, idempotency_key},
    db_operation,
    dtos::NewActionDto,
    metrics,
    models::{Action, Task, TriggerCondition, TriggerKind},
    rule::Strategy,
    workers::WorkerNudges,
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

/// Outcome of `start_task_phase_a` — the connection-holding preamble of on_start
/// (B1). Carries just enough state for phase B (HTTP) and phase C (the running
/// transition) to proceed without re-reading the DB.
enum StartPhaseA {
    /// No on_start HTTP should run: either the `start` slot was already claimed
    /// (idempotent conflict) or the A4 re-check found the task no longer `Claimed`.
    /// In both cases the start row is already completed; phase C only needs to run
    /// `mark_task_running` (the `claimed == false` path of the transition tx).
    Skip { idempotency_key: String },
    /// The slot was freshly claimed and the task is still `Claimed`; `actions` must
    /// be executed via HTTP in phase B (no connection held), then phase C completes
    /// the transition + start row + cancel actions in one transaction.
    Proceed {
        idempotency_key: String,
        actions: Vec<Action>,
    },
}

pub async fn start_loop(
    evaluator: &ActionExecutor,
    pool: DbPool,
    interval: std::time::Duration,
    dead_end_enabled: bool,
    start_batch_size: i64,
    webhook_concurrency: usize,
    mut shutdown: watch::Receiver<bool>,
    nudges: WorkerNudges,
    claim_timeout: std::time::Duration,
) {
    let semaphore = Arc::new(tokio::sync::Semaphore::new(webhook_concurrency));
    // B2: heartbeat interval = claim_timeout / 3, so a task waiting for a permit
    // has its `last_updated` refreshed at least twice before requeue-stale would
    // consider it stale.
    let heartbeat_interval = claim_timeout / 3;

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
            &nudges,
            heartbeat_interval,
        )
        .await;

        // Record worker loop metrics
        let loop_duration = loop_start.elapsed().as_secs_f64();
        metrics::record_worker_loop_iteration("start", loop_duration);
        metrics::record_tasks_processed_per_loop(tasks_processed);

        // B4: wake immediately on an in-process nudge (a POST /task, an unblocked
        // child, or a resume) instead of always waiting a full tick. `notify_one`
        // stores a permit, so a nudge that arrived while this iteration was running
        // still fires the next one — see `WorkerNudges`. The poll remains the
        // correctness/fallback path.
        tokio::select! {
            _ = shutdown.changed() => {
                log::info!("Start worker: shutdown signal received, exiting");
                return;
            }
            _ = nudges.start.notified() => {}
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
    nudges: &WorkerNudges,
    heartbeat_interval: std::time::Duration,
) -> usize {
    if claimed_tasks.is_empty() {
        return 0;
    }

    let mut join_set = JoinSet::new();

    for t in claimed_tasks {
        // B2: heartbeat `last_updated` while waiting for a permit, so
        // requeue-stale does not reclaim the task during long semaphore waits.
        let permit = acquire_permit_with_heartbeat(semaphore, pool, t.id, heartbeat_interval).await;

        let pool = pool.clone();
        let evaluator = evaluator.clone();
        let nudges = nudges.clone();

        join_set.spawn(async move {
            let _permit = permit; // released on drop
            let _guard = metrics::WebhooksInFlightGuard::new("start");
            execute_webhook_for_task(&evaluator, t, &pool, dead_end_enabled, &nudges).await
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

/// Acquire a semaphore permit while heartbeating `last_updated` on the task so
/// requeue-stale does not reclaim it during long waits (B2).
///
/// Every `heartbeat_interval` the task's `last_updated` is bumped via a cheap
/// autocommit UPDATE. On DB error the heartbeat is logged and skipped — it is a
/// best-effort optimization, not correctness (requeue-stale would reclaim and the
/// next iteration retries).
async fn acquire_permit_with_heartbeat(
    semaphore: &Arc<tokio::sync::Semaphore>,
    pool: &DbPool,
    task_id: uuid::Uuid,
    heartbeat_interval: std::time::Duration,
) -> tokio::sync::OwnedSemaphorePermit {
    use crate::schema::task::dsl;

    let acquire_fut = Arc::clone(semaphore).acquire_owned();
    tokio::pin!(acquire_fut);

    loop {
        tokio::select! {
            permit = &mut acquire_fut => {
                return permit.expect("semaphore should not be closed");
            }
            _ = rt::time::sleep(heartbeat_interval) => {
                if let Ok(mut conn) = pool.get().await {
                    let res = diesel::update(
                        dsl::task.filter(
                            dsl::id.eq(task_id)
                                .and(dsl::status.eq(crate::models::StatusKind::Claimed)),
                        ),
                    )
                    .set(dsl::last_updated.eq(diesel::dsl::now))
                    .execute(&mut conn)
                    .await;
                    if let Err(e) = res {
                        log::warn!(
                            "B2 heartbeat: failed to bump last_updated for task {}: {:?}",
                            task_id, e
                        );
                    }
                }
            }
        }
    }
}

/// Execute the full on_start lifecycle for a single claimed task, split into three
/// phases so a slow on_start webhook never holds a pool connection (B1 — the same
/// discipline the delivery loop already uses for end/cancel deliveries):
///
/// * **Phase A — `start_task_phase_a` (holds a connection, then drops it):** claim
///   the `start` outbox slot (idempotency guard), re-check the task is still
///   `Claimed` (A4 permit-wait window), and load the `Start` actions. The connection
///   is returned to the pool at the end of this phase — BEFORE any HTTP runs. This
///   drop is the core of the B1 fix.
/// * **Phase B — HTTP (holds NO connection):** execute the on_start webhooks
///   (sequential per task; up to `WORKER_WEBHOOK_CONCURRENCY` tasks run in parallel).
///   Because no pool connection is pinned here, a burst of slow downstreams can no
///   longer starve HTTP handlers and the other worker loops.
/// * **Phase C (re-acquires a connection):** the A2/A4 transaction —
///   `mark_task_running` + (when the webhook actually ran) `save_cancel_actions` +
///   `complete_webhook_execution` — in ONE transaction.
///
/// On on_start failure the task is marked Failed and propagated via
/// `fail_started_task` (also off the HTTP call path).
///
/// Returns true if the on_start path completed without a webhook failure.
async fn execute_webhook_for_task(
    evaluator: &ActionExecutor,
    task: Task,
    pool: &DbPool,
    dead_end_enabled: bool,
    nudges: &WorkerNudges,
) -> bool {
    // ---- Phase A: connection-holding preamble. The connection is dropped at the
    // end of this block, BEFORE the phase-B HTTP — the core of the B1 fix. ----
    let phase_a = {
        let Ok(mut conn) = pool.get().await else {
            log::error!(
                "Start worker: failed to acquire DB connection for task {} (phase A)",
                task.id
            );
            return false;
        };
        start_task_phase_a(evaluator, &task, &mut conn).await
        // `conn` is dropped here (returned to the pool): no connection is held
        // across the phase-B HTTP below.
    };

    let (idempotency_key, actions, claimed) = match phase_a {
        Ok(StartPhaseA::Skip { idempotency_key }) => (idempotency_key, Vec::new(), false),
        Ok(StartPhaseA::Proceed {
            idempotency_key,
            actions,
        }) => (idempotency_key, actions, true),
        Err(e) => {
            // Genuine DB error in the preamble (claim / re-check / load / skip-completion).
            // Same handling as the pre-split code: mark the task Failed and propagate. The
            // start row is left untouched here (never completed `false`) — matching the
            // pre-split behaviour, which only completed `false` after the HTTP actually ran.
            log::error!(
                "Start worker: on_start preamble failed for task {}: {}",
                task.id,
                e
            );
            fail_started_task(
                pool,
                &task,
                dead_end_enabled,
                "on_start webhook failed",
                None,
                nudges,
            )
            .await;
            return false;
        }
    };

    // ---- Phase B: HTTP execution of the on_start actions. NO pool connection is
    // held here (B1). The per-task loop stays sequential, as before. ----
    let mut cancel_tasks: Vec<NewActionDto> = Vec::new();
    if claimed {
        let mut errors: Vec<String> = Vec::new();
        for act in &actions {
            match evaluator.execute(act, &task, Some(&idempotency_key)).await {
                Ok(res) => {
                    if let Some(t) = res {
                        cancel_tasks.push(t);
                    }
                    log::debug!("Action {} executed successfully", act.id);
                }
                Err(e) => {
                    log::error!("Action {} failed: {}", act.id, e);
                    errors.push(e);
                }
            }
        }
        if !errors.is_empty() {
            // Webhook failed after claim -> complete the start row `false` and mark the
            // task Failed + propagate. Both run on a freshly re-acquired connection
            // (fail_started_task), never on the HTTP call path.
            log::error!(
                "Start worker: on_start webhook failed for task {}: {}",
                task.id,
                errors.join("; ")
            );
            fail_started_task(
                pool,
                &task,
                dead_end_enabled,
                "on_start webhook failed",
                Some(&idempotency_key),
                nudges,
            )
            .await;
            return false;
        }
    }

    // ---- Phase C: re-acquire a connection for the A2/A4 transaction. ----
    let Ok(mut conn) = pool.get().await else {
        // Safety net (same failure mode as a process crash right here): we could not
        // re-acquire a connection to commit the running transition. The task stays
        // `Claimed` with its `start` row still `pending`. The requeue-stale path
        // re-picks it, and the A2 start-row freshness bound keeps the delivery loop's
        // start-before-end gate from blocking end/cancel forever. So the task is never
        // silently lost — it is simply re-processed on a later iteration.
        log::error!(
            "Start worker: failed to re-acquire DB connection for task {} (phase C); \
             task stays Claimed with pending start row, recovered by requeue-stale",
            task.id
        );
        return false;
    };

    // A2 fix: transition Claimed -> Running AND complete the `start` outbox row
    // in ONE transaction. Previously these were two separate autocommit
    // statements; a crash (or a swallowed UPDATE error) between them left the
    // task Running with its `start` row stuck `pending` forever, which the
    // delivery loop's start-before-end gate then read as "hold end/cancel
    // forever". Committing them together closes that window on the nominal
    // path. The start row is completed whenever the on_start webhook was
    // actually executed (`claimed == true`), even if the running transition
    // itself is a no-op (task no longer Claimed) — so the row never lingers.
    //
    // A4 fix: `save_cancel_actions` now runs INSIDE this same tx, BEFORE the
    // start-row completion. The delivery loop's start-before-end gate holds a
    // task's cancel outbox row while its start row is `pending`; the start row
    // goes non-pending only when THIS tx commits, and that commit now also
    // contains the cancel actions. So a cancel row concurrently enqueued by a
    // DELETE/stop_batch that fired while on_start was in flight (the task left
    // `Claimed`, giving `Ok(false)` below) can never be prefetched by the
    // delivery loop before its actions exist — the consumer, which received
    // on_start and started work, will receive the cancel WITH those actions.
    // `save_cancel_actions` is best-effort on validation (invalid actions are
    // logged + skipped, never rolling back the transition — the 4.2 decision),
    // so the tx rolls back only on a genuine DB error.
    let task_id = task.id;
    let tx_result: Result<bool, _> = db_operation::run_in_transaction(&mut conn, move |conn| {
        let key = idempotency_key;
        let cancel_tasks = cancel_tasks;
        Box::pin(async move {
            let ran = db_operation::mark_task_running(conn, &task_id).await?;
            if claimed {
                db_operation::save_cancel_actions(conn, task_id, &cancel_tasks).await?;
                db_operation::complete_webhook_execution(conn, &key, true).await?;
            }
            Ok(ran)
        })
    })
    .await;

    match tx_result {
        Ok(true) => {
            metrics::record_status_transition("Claimed", "Running");
            // Scheduler latency: time from task creation to actually running.
            let wait_secs = (chrono::Utc::now() - task.created_at)
                .num_milliseconds()
                .max(0) as f64
                / 1000.0;
            metrics::record_task_wait(&task.kind, wait_secs);
            log::debug!("Start worker: task {} started", task.id);
        }
        Ok(false) => {
            // Task left `Claimed` during the on_start webhook (canceled, stopped,
            // failed, or paused by a concurrent request). The cancel actions were
            // still persisted inside the tx above (A4), atomically with the
            // start-row completion, so a cancel notification already enqueued by
            // the concurrent transition will be delivered WITH them.
            log::warn!(
                "Start worker: task {} no longer claimed; skipping running transition",
                task.id
            );
        }
        Err(e) => {
            // Genuine DB error: the tx rolled back, so the running transition, the
            // start-row completion AND the cancel-action save were all undone.
            // The task stays `Claimed` with its start row `pending`; the
            // requeue-stale path re-picks it, and the start-row freshness bound
            // (A2) keeps the gate from blocking end/cancel forever.
            log::error!(
                "Start worker: failed to commit running transition / start-row \
                 completion for task {}: {:?}",
                task.id,
                e
            );
        }
    }

    true
}

/// Run the on_start-failure path off the HTTP call path (B1): re-acquire a
/// connection, optionally complete the task's `start` outbox row as failed (only
/// when the HTTP actions actually ran — matching the pre-split behaviour, which
/// left the row untouched on a preamble/DB error), then mark the task Failed and
/// propagate (which also enqueues its on_failure outbox rows).
async fn fail_started_task(
    pool: &DbPool,
    task: &Task,
    dead_end_enabled: bool,
    reason: &str,
    complete_start_false: Option<&str>,
    nudges: &WorkerNudges,
) {
    let Ok(mut conn) = pool.get().await else {
        log::error!(
            "Start worker: failed to acquire DB connection to fail task {}",
            task.id
        );
        return;
    };
    if let Some(key) = complete_start_false {
        if let Err(e) = db_operation::complete_webhook_execution(&mut conn, key, false).await {
            log::error!(
                "Failed to complete webhook execution record for key {}: {}",
                key,
                e
            );
        }
    }
    if let Err(e2) =
        db_operation::fail_task_and_propagate(&mut conn, &task.id, reason, dead_end_enabled).await
    {
        log::error!(
            "Start worker: failed to mark task {} as failed and propagate: {:?}",
            task.id,
            e2
        );
    } else {
        // B4: the failure + cascade enqueued on_failure outbox rows in the tx above;
        // wake the delivery loop so they don't wait a full delivery tick.
        nudges.nudge_delivery();
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

/// Phase A of on_start processing: the connection-holding preamble (B1).
///
/// Claims the `start` outbox slot (idempotency guard), performs the A4 re-check
/// (the task may have left `Claimed` while waiting for a webhook permit), and loads
/// the `Start` actions. Runs NO HTTP — the caller drops the connection immediately
/// after this returns and executes the webhooks (phase B) without a connection held.
///
/// Returns `Skip` when no HTTP should run (idempotent conflict, or the A4 re-check
/// found the task no longer `Claimed`); in both cases the start row is already
/// completed and phase C only runs `mark_task_running`. Returns `Proceed` with the
/// actions to execute otherwise. `Err` is a genuine DB error → the caller runs the
/// on_start-failure path, matching the pre-split behaviour.
async fn start_task_phase_a<'a>(
    evaluator: &ActionExecutor,
    task: &Task,
    conn: &mut Conn<'a>,
) -> Result<StartPhaseA, String> {
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
        return Ok(StartPhaseA::Skip {
            idempotency_key: key,
        });
    }

    // A4: the task may have left `Claimed` while waiting for a webhook permit — a
    // cancel enqueued in that window was NOT gated (the start row above did not exist
    // yet) and may already have been delivered as a zero-action fast-path. Firing
    // on_start now would hand the consumer zombie work whose cancel is already
    // consumed. Re-check the status now that the start row exists: if the task is no
    // longer Claimed, skip on_start entirely and complete the start row (releases the
    // gate; nothing was executed, so a zero-action cancel is correct). Any transition
    // landing AFTER this check enqueues a cancel row that the gate holds (start row
    // pending + fresh) until the completion tx commits the cancel actions.
    {
        use crate::models::StatusKind;
        use crate::schema::task::dsl as task_dsl;
        let current_status: StatusKind = task_dsl::task
            .filter(task_dsl::id.eq(task.id))
            .select(task_dsl::status)
            .get_result(conn)
            .await
            .map_err(|e| format!("Failed to re-check task status: {}", e))?;
        if current_status != StatusKind::Claimed {
            log::info!(
                "Start worker: task {} left Claimed ({:?}) before its on_start fired; \
                 skipping on_start",
                task.id,
                current_status
            );
            db_operation::complete_webhook_execution(conn, &key, true)
                .await
                .map_err(|e| format!("Failed to complete skipped start row: {}", e))?;
            return Ok(StartPhaseA::Skip {
                idempotency_key: key,
            });
        }
    }

    let actions = Action::belonging_to(&task)
        .filter(trigger.eq(TriggerKind::Start))
        .load::<Action>(conn)
        .await
        .map_err(|e| e.to_string())?;

    Ok(StartPhaseA::Proceed {
        idempotency_key: key,
        actions,
    })
}
