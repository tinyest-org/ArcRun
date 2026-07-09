use crate::Conn;
use crate::models::{TriggerCondition, TriggerKind, WebhookExecution, WebhookExecutionStatus};
use diesel::ExpressionMethods;
use diesel::OptionalExtension;
use diesel::QueryDsl;
use diesel_async::RunQueryDsl;

use super::DbError;

/// Attempt to claim a webhook execution slot for idempotency.
///
/// Uses INSERT ... ON CONFLICT to atomically claim or skip:
/// - If no row exists: inserts a new `pending` row → returns `Ok(true)` (proceed)
/// - If a row exists with `status = 'success'`: no update → returns `Ok(false)` (skip)
/// - If a row exists with `status = 'failure'`: resets to pending → returns `Ok(true)` (retry)
/// - If a row exists with `status = 'pending'`: retries only when `stale_after` elapsed
/// Note: for Start/Cancel triggers, `condition` is stored as `Success` sentinel.
pub async fn try_claim_webhook_execution<'a>(
    conn: &mut Conn<'a>,
    task_id: uuid::Uuid,
    trigger_kind: TriggerKind,
    trigger_condition: TriggerCondition,
    key: &str,
    stale_after: Option<std::time::Duration>,
) -> Result<bool, DbError> {
    use crate::schema::sql_types as st;

    let stale_after_micros = stale_after.map(|d| i64::try_from(d.as_micros()).unwrap_or(i64::MAX));

    // INSERT ... ON CONFLICT with a WHERE filter on the DO UPDATE.
    // - If status = 'success' → no update (0 rows affected)
    // - If status = 'failure' → update, reset to pending (retry allowed)
    // - If status = 'pending' → update only if stale_after is provided and elapsed
    let result: usize = diesel::sql_query(
        "INSERT INTO webhook_execution (task_id, trigger, condition, idempotency_key, status, attempts)
         VALUES ($1, $2, $3, $4, 'pending', 1)
         ON CONFLICT (idempotency_key) DO UPDATE
         SET attempts = webhook_execution.attempts + 1,
             status = 'pending',
             updated_at = now()
         WHERE webhook_execution.status = 'failure'
            OR (
                webhook_execution.status = 'pending'
                AND $5 IS NOT NULL
                AND webhook_execution.updated_at < (now() - ($5::bigint * interval '1 microsecond'))
            )"
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .bind::<st::TriggerKind, _>(trigger_kind)
    .bind::<st::TriggerCondition, _>(trigger_condition)
    .bind::<diesel::sql_types::Text, _>(key)
    .bind::<diesel::sql_types::Nullable<diesel::sql_types::BigInt>, _>(stale_after_micros)
    .execute(conn)
    .await?;

    Ok(result > 0)
}

/// Mark a webhook execution as success or failure after execution completes.
pub async fn complete_webhook_execution<'a>(
    conn: &mut Conn<'a>,
    key: &str,
    succeeded: bool,
) -> Result<(), DbError> {
    use crate::schema::webhook_execution::dsl;

    let new_status = if succeeded {
        WebhookExecutionStatus::Success
    } else {
        WebhookExecutionStatus::Failure
    };

    diesel::update(dsl::webhook_execution.filter(dsl::idempotency_key.eq(key)))
        .set((
            dsl::status.eq(new_status),
            dsl::updated_at.eq(diesel::dsl::now),
        ))
        .execute(conn)
        .await?;

    Ok(())
}

// =============================================================================
// Transactional outbox (Lot 2)
// =============================================================================

/// Enqueue a `pending` outbox row for an end/cancel webhook event, inside the
/// caller's transaction (the same tx that changed the task status).
///
/// Uses `INSERT ... ON CONFLICT (idempotency_key) DO NOTHING`: the existing
/// idempotency guarantee (one row per task+trigger+condition) means a re-run of
/// the same transition never enqueues a duplicate. Once a row exists in any state
/// (pending/success/failure/exhausted) the event is considered already accounted
/// for and we never re-enqueue it.
///
/// The row matures immediately (`next_attempt_at` defaults to `now()`), so the
/// delivery loop can pick it up on its next iteration.
///
/// Design note: we insert unconditionally (without checking whether the task has
/// matching actions). The delivery loop marks rows with zero matching actions as
/// `success` immediately — this keeps the status-change transaction minimal (one
/// INSERT, no extra action lookup) and is the simplest correct option.
pub async fn enqueue_outbox<'a>(
    conn: &mut Conn<'a>,
    task_id: uuid::Uuid,
    trigger_kind: TriggerKind,
    trigger_condition: TriggerCondition,
    key: &str,
) -> Result<(), DbError> {
    use crate::schema::sql_types as st;

    diesel::sql_query(
        "INSERT INTO webhook_execution
            (task_id, trigger, condition, idempotency_key, status, attempts, next_attempt_at)
         VALUES ($1, $2, $3, $4, 'pending', 0, now())
         ON CONFLICT (idempotency_key) DO NOTHING",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .bind::<st::TriggerKind, _>(trigger_kind)
    .bind::<st::TriggerCondition, _>(trigger_condition)
    .bind::<diesel::sql_types::Text, _>(key)
    .execute(conn)
    .await?;

    Ok(())
}

/// Enqueue a `batch_complete` outbox row for a batch whose last task just became
/// terminal, inside the caller's transaction. `task_id` is left NULL; `batch_id` is
/// set. The `condition` column is the `Success` sentinel (NOT NULL column; meaning-
/// less for batch-complete). `ON CONFLICT (idempotency_key) DO NOTHING` makes
/// concurrent detection (two tasks finishing "at once") enqueue at most one row.
pub async fn enqueue_batch_complete_outbox<'a>(
    conn: &mut Conn<'a>,
    batch_id: uuid::Uuid,
    key: &str,
) -> Result<(), DbError> {
    let rows_inserted = diesel::sql_query(
        "INSERT INTO webhook_execution
            (batch_id, trigger, condition, idempotency_key, status, attempts, next_attempt_at)
         VALUES ($1, 'batch_complete', 'success', $2, 'pending', 0, now())
         ON CONFLICT (idempotency_key) DO NOTHING",
    )
    .bind::<diesel::sql_types::Uuid, _>(batch_id)
    .bind::<diesel::sql_types::Text, _>(key)
    .execute(conn)
    .await?;

    // Count only a real insert (ON CONFLICT DO NOTHING ⇒ 0 rows on a concurrent
    // double-detection), so the counter tracks distinct batch completions.
    if rows_inserted > 0 {
        crate::metrics::record_batch_completed();
    }

    Ok(())
}

/// Check whether `batch_id` is fully terminal (no task with a non-terminal status)
/// and, if so, enqueue a `batch_complete` outbox row — but ONLY if the batch has a
/// non-empty `on_complete` (a registered `on_batch_complete` webhook payload). A
/// batch with no `batch` row, or a `batch` row whose `on_complete` is `'[]'`
/// (scope/metadata-only batch, #601), carries no completion signal and is skipped
/// without taking the batch lock (Audit 2, B3).
///
/// Idempotent and concurrency-safe: the unique idempotency key + `ON CONFLICT DO
/// NOTHING` mean repeated/concurrent calls enqueue at most one row. Call inside the
/// transaction that made the last task terminal so the signal commits atomically.
///
/// `caller` is a short label for logs (e.g. "update_running_task").
pub async fn maybe_enqueue_batch_complete<'a>(
    conn: &mut Conn<'a>,
    batch_id: uuid::Uuid,
    caller: &str,
) -> Result<(), DbError> {
    use crate::schema::batch::dsl;

    // Serialize concurrent detection per batch by locking the `batch` row FIRST, in
    // its own statement. Under READ COMMITTED, two transactions each finishing one
    // of the batch's last two tasks could otherwise BOTH see the other's task as
    // still non-terminal (write skew) and neither would enqueue the signal — losing
    // it forever. With the row lock, the second transaction waits for the first to
    // commit, and its terminality check below then runs on a fresh statement
    // snapshot that includes the first one's update. (Folding the terminality
    // subquery into this locking statement would NOT work: the subquery would be
    // evaluated on the pre-wait snapshot.)
    //
    // Non-vacuity is pushed INTO the locking statement (Audit 2, B3). A batch with an
    // EMPTY `on_complete` (scope/metadata-only batch, #601, or a batch with no
    // `on_batch_complete` at all) carries no completion signal, so it must NOT be
    // locked: otherwise EVERY terminal transition of such a batch would serialise on
    // this lock for nothing. The `on_complete <> '[]'::jsonb` predicate lets the
    // SELECT match (and lock) only webhook batches; a no-webhook batch — or a batch
    // with no `batch` row — returns no row and early-returns without ever taking the
    // lock. (`on_complete` is a JSONB array by construction: the `'[]'` column default
    // or a JSON array of `NewActionDto`. The literal `<> '[]'::jsonb` comparison is
    // therefore robust and never errors, unlike `jsonb_array_length` on a non-array.)
    // The lock still serialises concurrent detection for webhook batches, preserving
    // the write-skew protection above.
    let locked: Option<uuid::Uuid> = dsl::batch
        .filter(dsl::id.eq(batch_id))
        .filter(diesel::dsl::sql::<diesel::sql_types::Bool>(
            "on_complete <> '[]'::jsonb",
        ))
        .select(dsl::id)
        .for_update()
        .first::<uuid::Uuid>(conn)
        .await
        .optional()?;

    if locked.is_none() {
        return Ok(());
    }

    #[derive(diesel::QueryableByName)]
    struct ReadyRow {
        #[diesel(sql_type = diesel::sql_types::Bool)]
        ready: bool,
    }

    let row: ReadyRow = diesel::sql_query(
        "SELECT NOT EXISTS (
            SELECT 1 FROM task t
            WHERE t.batch_id = $1
              AND t.status NOT IN ('success', 'failure', 'canceled')
         ) AS ready",
    )
    .bind::<diesel::sql_types::Uuid, _>(batch_id)
    .get_result(conn)
    .await?;

    if row.ready {
        let key = crate::action::batch_complete_idempotency_key(batch_id);
        enqueue_batch_complete_outbox(conn, batch_id, &key).await?;
        log::debug!(
            "[{}] batch {} fully terminal — enqueued batch_complete outbox row",
            caller,
            batch_id
        );
    }

    Ok(())
}

/// Convenience wrapper: resolve a task's `batch_id` and, if set, run
/// [`maybe_enqueue_batch_complete`]. A task without a batch is a no-op.
pub async fn maybe_enqueue_batch_complete_for_task<'a>(
    conn: &mut Conn<'a>,
    task_id: uuid::Uuid,
    caller: &str,
) -> Result<(), DbError> {
    use crate::schema::task::dsl;
    let batch_id: Option<uuid::Uuid> = dsl::task
        .filter(dsl::id.eq(task_id))
        .select(dsl::batch_id)
        .first::<Option<uuid::Uuid>>(conn)
        .await
        .optional()?
        .flatten();

    if let Some(bid) = batch_id {
        maybe_enqueue_batch_complete(conn, bid, caller).await?;
    }
    Ok(())
}

/// Insert the `batch` row, inside the caller's transaction (the same tx as
/// `add_task`). `on_complete` is the JSON array of `NewActionDto` (empty `[]` for a
/// scope/metadata-only batch); `scope`/`metadata` are the batch's business-level
/// identity used for filtering and search.
pub async fn insert_batch<'a>(
    conn: &mut Conn<'a>,
    batch_id: uuid::Uuid,
    on_complete: serde_json::Value,
    scope: Option<String>,
    metadata: serde_json::Value,
) -> Result<(), DbError> {
    use crate::schema::batch::dsl;
    diesel::insert_into(dsl::batch)
        .values(crate::models::NewBatch {
            id: batch_id,
            on_complete,
            scope,
            metadata,
        })
        .execute(conn)
        .await?;
    Ok(())
}

/// Load a batch's `on_complete` payload, if the batch row exists.
pub async fn load_batch_on_complete<'a>(
    conn: &mut Conn<'a>,
    batch_id: uuid::Uuid,
) -> Result<Option<serde_json::Value>, DbError> {
    use crate::schema::batch::dsl;
    let row: Option<serde_json::Value> = dsl::batch
        .filter(dsl::id.eq(batch_id))
        .select(dsl::on_complete)
        .first::<serde_json::Value>(conn)
        .await
        .optional()?;
    Ok(row)
}

/// Per-status terminal counts for a batch, plus the latest `ended_at`, used to build
/// the `arcrun` enrichment of a batch_complete webhook payload at delivery time.
#[derive(diesel::QueryableByName, Debug)]
pub struct BatchCompletionStats {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    pub success: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    pub failure: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    pub canceled: i64,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>)]
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// Compute per-status terminal counts + max(ended_at) for a batch (at delivery time).
pub async fn batch_completion_stats<'a>(
    conn: &mut Conn<'a>,
    batch_id: uuid::Uuid,
) -> Result<BatchCompletionStats, DbError> {
    let stats: BatchCompletionStats = diesel::sql_query(
        "SELECT
            COUNT(*) FILTER (WHERE status = 'success')  AS success,
            COUNT(*) FILTER (WHERE status = 'failure')  AS failure,
            COUNT(*) FILTER (WHERE status = 'canceled') AS canceled,
            MAX(ended_at) AS completed_at
         FROM task WHERE batch_id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(batch_id)
    .get_result(conn)
    .await?;
    Ok(stats)
}

#[derive(diesel::QueryableByName, Debug)]
pub struct OutboxBacklogStats {
    /// `pending` rows whose `next_attempt_at <= now()` (mature, awaiting delivery).
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    pub ready: i64,
    /// `pending` rows whose `next_attempt_at > now()` (leased in-flight or backing off).
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    pub leased: i64,
    /// Age in seconds of the oldest mature `pending` row (0.0 if none).
    #[diesel(sql_type = diesel::sql_types::Double)]
    pub oldest_ready_age_secs: f64,
}

/// Snapshot the outbox backlog for observability: how many pending rows are mature
/// (`ready`) vs not-yet-due (`leased`), and the age of the oldest mature row. A
/// single indexed scan over `pending` rows (served by `idx_webhook_execution_pending_due`),
/// run once per delivery-loop iteration — never in an HTTP path.
pub async fn outbox_backlog_stats<'a>(conn: &mut Conn<'a>) -> Result<OutboxBacklogStats, DbError> {
    let stats: OutboxBacklogStats = diesel::sql_query(
        "SELECT
            COUNT(*) FILTER (WHERE next_attempt_at <= now()) AS ready,
            COUNT(*) FILTER (WHERE next_attempt_at >  now()) AS leased,
            COALESCE(
                EXTRACT(EPOCH FROM (now() - MIN(created_at) FILTER (WHERE next_attempt_at <= now()))),
                0
            )::double precision AS oldest_ready_age_secs
         FROM webhook_execution WHERE status = 'pending'",
    )
    .get_result(conn)
    .await?;
    Ok(stats)
}

/// Select up to `limit` mature `pending` outbox rows for delivery, locking them
/// with `FOR UPDATE SKIP LOCKED` so concurrent delivery workers don't double-deliver.
///
/// Maturity: `next_attempt_at <= now()`.
///
/// Per-task ordering guarantee (start-before-end): an `end`/`cancel` row is only
/// returned if there is NO **fresh** `start` row for the same task still in `pending`
/// state. `start` rows are delivered synchronously by the start_loop, but a start row
/// can linger as `pending` while the on_start webhook is in flight or has failed — in
/// that window we must not deliver the task's end/cancel notification.
///
/// Freshness bound (Audit 2, A2): the gate only holds while the start row's
/// `updated_at > now() - start_stale_secs`. A start row that is never completed
/// (crash between `mark_task_running` and its completion, or a Claimed task canceled
/// mid-webhook) would otherwise block the task's end/cancel delivery **forever**. The
/// bound is a deliberate relaxation: once the start row goes stale, end/cancel are
/// delivered anyway (better than an eternal block; start-before-end still holds for
/// healthy starts). Mirror of the `try_claim_webhook_execution` staleness.
///
/// NOTE: this non-leased variant is currently unused (the delivery loop uses
/// [`claim_due_outbox_leased`]); the freshness bound is mirrored here to keep the two
/// gates from diverging.
///
/// Rows are returned oldest-first (`created_at ASC`) for fair, FIFO-ish delivery.
pub async fn claim_due_outbox<'a>(
    conn: &mut Conn<'a>,
    limit: i64,
    start_stale_secs: i64,
) -> Result<Vec<WebhookExecution>, DbError> {
    // Includes batch_complete rows (Lot 3b). The start-before-end gate below only
    // affects task-level rows: a batch_complete row has NULL task_id, so the
    // correlated `s.task_id = we.task_id` is never true and the gate never blocks it
    // (the *state* is the source of truth — by the time a batch_complete row exists,
    // every task of the batch is terminal anyway).
    let rows = diesel::sql_query(
        "SELECT we.* FROM webhook_execution we
         WHERE we.status = 'pending'
           AND we.trigger IN ('end', 'cancel', 'batch_complete')
           AND we.next_attempt_at <= now()
           AND NOT EXISTS (
                 SELECT 1 FROM webhook_execution s
                 WHERE s.task_id = we.task_id
                   AND s.trigger = 'start'
                   AND s.status = 'pending'
                   AND s.updated_at > now() - ($2::bigint * interval '1 second')
           )
         ORDER BY we.created_at ASC
         LIMIT $1
         FOR UPDATE OF we SKIP LOCKED",
    )
    .bind::<diesel::sql_types::BigInt, _>(limit)
    .bind::<diesel::sql_types::BigInt, _>(start_stale_secs)
    .load::<WebhookExecution>(conn)
    .await?;

    Ok(rows)
}

/// Claim up to `limit` mature `pending` outbox rows for delivery AND push their
/// `next_attempt_at` `lease_secs` into the future, in one statement.
///
/// This is the lease-based variant of [`claim_due_outbox`]: instead of holding the
/// `FOR UPDATE` locks for the whole delivery (HTTP included), it commits a short
/// claim that sets a *lease* on each row. While the lease is in effect the row is no
/// longer mature, so a concurrent worker / next iteration won't re-claim it. If the
/// process crashes mid-delivery, the lease expires and the row becomes mature again
/// (at-least-once preserved). The lease does **not** bump `attempts` — `attempts`
/// counts observed delivery failures, not interrupted (leased) attempts.
///
/// Same selection predicates as [`claim_due_outbox`]: `status = 'pending'`, triggers
/// in `('end','cancel','batch_complete')`, `next_attempt_at <= now()`, the per-task
/// start-before-end gate (bounded by `start_stale_secs` freshness — see below),
/// `ORDER BY created_at ASC`, `LIMIT`, `FOR UPDATE SKIP LOCKED`.
///
/// `lease_secs` must be >= 1 and should exceed the worst-case delivery time of a single
/// row (otherwise a slow delivery could be double-claimed while still in flight).
///
/// `start_stale_secs` (Audit 2, A2) bounds the start-before-end gate by freshness: an
/// `end`/`cancel` row is held back only while the task's `start` row is pending AND
/// that start row's `updated_at > now() - start_stale_secs`. A start row that never
/// completes (crash between `mark_task_running` and the start-row completion, or a
/// Claimed task canceled/stopped mid-webhook) would otherwise block the task's
/// end/cancel delivery forever; once it goes stale the end/cancel delivers anyway.
/// This is a deliberate relaxation — start-before-end is still guaranteed for healthy
/// (fresh) starts. Mirror of the `try_claim_webhook_execution` staleness.
pub async fn claim_due_outbox_leased<'a>(
    conn: &mut Conn<'a>,
    limit: i64,
    lease_secs: i64,
    start_stale_secs: i64,
) -> Result<Vec<WebhookExecution>, DbError> {
    let rows = diesel::sql_query(
        "UPDATE webhook_execution we
         SET next_attempt_at = now() + ($2::bigint * interval '1 second')
         FROM (
             SELECT c.id FROM webhook_execution c
             WHERE c.status = 'pending'
               AND c.trigger IN ('end', 'cancel', 'batch_complete')
               AND c.next_attempt_at <= now()
               AND NOT EXISTS (
                     SELECT 1 FROM webhook_execution s
                     WHERE s.task_id = c.task_id
                       AND s.trigger = 'start'
                       AND s.status = 'pending'
                       AND s.updated_at > now() - ($3::bigint * interval '1 second')
               )
             ORDER BY c.created_at ASC
             LIMIT $1
             FOR UPDATE SKIP LOCKED
         ) AS claimed
         WHERE we.id = claimed.id
         RETURNING we.*",
    )
    .bind::<diesel::sql_types::BigInt, _>(limit)
    .bind::<diesel::sql_types::BigInt, _>(lease_secs)
    .bind::<diesel::sql_types::BigInt, _>(start_stale_secs)
    .load::<WebhookExecution>(conn)
    .await?;

    Ok(rows)
}

/// Mark an outbox row delivered (terminal success).
pub async fn mark_outbox_success<'a>(conn: &mut Conn<'a>, key: &str) -> Result<(), DbError> {
    use crate::schema::webhook_execution::dsl;
    diesel::update(dsl::webhook_execution.filter(dsl::idempotency_key.eq(key)))
        .set((
            dsl::status.eq(WebhookExecutionStatus::Success),
            dsl::updated_at.eq(diesel::dsl::now),
            dsl::last_error.eq::<Option<String>>(None),
        ))
        .execute(conn)
        .await?;
    Ok(())
}

/// Record a failed delivery attempt: increment `attempts`, store `last_error`,
/// and schedule the next attempt `backoff_secs` in the future. Stays `pending`.
pub async fn mark_outbox_retry<'a>(
    conn: &mut Conn<'a>,
    key: &str,
    error: &str,
    backoff_secs: i64,
) -> Result<(), DbError> {
    // Truncate overly-long error bodies to keep the row bounded.
    let trimmed: String = error.chars().take(1000).collect();
    diesel::sql_query(
        "UPDATE webhook_execution
         SET attempts = attempts + 1,
             status = 'pending',
             last_error = $2,
             next_attempt_at = now() + ($3::bigint * interval '1 second'),
             updated_at = now()
         WHERE idempotency_key = $1",
    )
    .bind::<diesel::sql_types::Text, _>(key)
    .bind::<diesel::sql_types::Text, _>(trimmed)
    .bind::<diesel::sql_types::BigInt, _>(backoff_secs)
    .execute(conn)
    .await?;
    Ok(())
}

/// Mark an outbox row permanently failed after exhausting retries.
pub async fn mark_outbox_exhausted<'a>(
    conn: &mut Conn<'a>,
    key: &str,
    error: &str,
) -> Result<(), DbError> {
    use crate::schema::webhook_execution::dsl;
    let trimmed: String = error.chars().take(1000).collect();
    diesel::update(dsl::webhook_execution.filter(dsl::idempotency_key.eq(key)))
        .set((
            dsl::status.eq(WebhookExecutionStatus::Exhausted),
            dsl::attempts.eq(dsl::attempts + 1),
            dsl::last_error.eq(Some(trimmed)),
            dsl::updated_at.eq(diesel::dsl::now),
        ))
        .execute(conn)
        .await?;
    Ok(())
}

/// List webhook delivery (outbox) rows, optionally filtered by status, ordered by
/// most recently updated. Used by `GET /webhook-deliveries` for observability.
pub async fn list_webhook_deliveries<'a>(
    conn: &mut Conn<'a>,
    status: Option<WebhookExecutionStatus>,
    limit: i64,
    offset: i64,
) -> Result<Vec<WebhookExecution>, DbError> {
    use crate::schema::webhook_execution::dsl;

    let mut query = dsl::webhook_execution.into_boxed();
    if let Some(s) = status {
        query = query.filter(dsl::status.eq(s));
    }
    let rows = query
        .order((dsl::updated_at.desc(), dsl::id.desc()))
        .limit(limit)
        .offset(offset)
        .load::<WebhookExecution>(conn)
        .await?;
    Ok(rows)
}
