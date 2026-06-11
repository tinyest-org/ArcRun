use crate::Conn;
use crate::models::{TriggerCondition, TriggerKind, WebhookExecution, WebhookExecutionStatus};
use diesel::ExpressionMethods;
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

/// Select up to `limit` mature `pending` outbox rows for delivery, locking them
/// with `FOR UPDATE SKIP LOCKED` so concurrent delivery workers don't double-deliver.
///
/// Maturity: `next_attempt_at <= now()`.
///
/// Per-task ordering guarantee (start-before-end): an `end`/`cancel` row is only
/// returned if there is NO `start` row for the same task still in `pending` state.
/// `start` rows are delivered synchronously by the start_loop, but a start row can
/// linger as `pending` while the on_start webhook is in flight or has failed — in
/// that window we must not deliver the task's end/cancel notification.
///
/// Rows are returned oldest-first (`created_at ASC`) for fair, FIFO-ish delivery.
pub async fn claim_due_outbox<'a>(
    conn: &mut Conn<'a>,
    limit: i64,
) -> Result<Vec<WebhookExecution>, DbError> {
    let rows = diesel::sql_query(
        "SELECT we.* FROM webhook_execution we
         WHERE we.status = 'pending'
           AND we.trigger IN ('end', 'cancel')
           AND we.next_attempt_at <= now()
           AND NOT EXISTS (
                 SELECT 1 FROM webhook_execution s
                 WHERE s.task_id = we.task_id
                   AND s.trigger = 'start'
                   AND s.status = 'pending'
           )
         ORDER BY we.created_at ASC
         LIMIT $1
         FOR UPDATE OF we SKIP LOCKED",
    )
    .bind::<diesel::sql_types::BigInt, _>(limit)
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
        .order(dsl::updated_at.desc())
        .limit(limit)
        .offset(offset)
        .load::<WebhookExecution>(conn)
        .await?;
    Ok(rows)
}
