//! Audit 2, A1 — `run_in_transaction` cancel-safety regression tests.
//!
//! # Original bug
//! `db::run_in_transaction` used to emit raw `sql_query("BEGIN")` / `"COMMIT"` /
//! `"ROLLBACK"`, bypassing diesel-async's `AnsiTransactionManager`. bb8 inspects
//! only that manager state in its `has_broken` check, which the raw SQL never
//! updated. If actix dropped a handler future between `BEGIN` and `COMMIT`
//! (client disconnect, request timeout, any cancellation) the physical
//! connection returned to the pool with an **open transaction still holding row
//! locks** (e.g. the batch `FOR UPDATE`). The next borrower then ran its
//! "autocommit" statements *inside* that leaked transaction, and a later
//! `COMMIT` could make a half-propagated transition durable — violating
//! "API response = durable state" in the other direction.
//!
//! # Fix
//! `run_in_transaction` now delegates to `AsyncConnection::transaction`, which
//! drives the transaction manager. On mid-transaction cancellation the manager
//! is left reporting an open (non-test) transaction, so bb8's `has_broken`
//! returns true and the connection is discarded instead of reused.
//!
//! # What these tests assert
//! * `test_audit2_a1_cancelled_midtx_does_not_leak_transaction` — cancelling a
//!   transaction mid-flight (a `tokio::time::timeout` around a `pg_sleep` inside
//!   the closure) leaves the pool clean: the aborted write is not committed, and
//!   the *next* borrower runs in autocommit (its write is durably visible from an
//!   INDEPENDENT connection with no explicit COMMIT). With the old raw-SQL
//!   implementation the leaked-transaction connection would be reused, so either
//!   the follow-up statement errors on a desynced protocol or the independent
//!   observer sees no committed rows — the test fails.
//! * `test_audit2_a1_commits_on_ok` / `test_audit2_a1_rolls_back_on_err` —
//!   baseline semantics are preserved: `Ok` commits, `Err` rolls back and
//!   propagates the original error, and the connection stays usable afterwards.
//!
//! A dedicated `max_size = 1` pool is used so the (potentially leaked) physical
//! connection is deterministically handed back to the next borrower, and a
//! separate observer pool provides a genuinely independent view of committed
//! state.

use crate::common::*;

use arcrun::action::idempotency_key;
use arcrun::error::ArcRunError;
use arcrun::models::{StatusKind, TriggerCondition, TriggerKind};
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

// NOTE: `diesel_async::RunQueryDsl` is intentionally NOT imported at module scope.
// It is a blanket-implemented trait, so bringing it into scope shadows the inherent
// `AtomicUsize::load` on `Arc<AtomicUsize>` (our `hits` counters), turning every
// `hits.load(...)` into an ambiguous call. Fully-qualify `RunQueryDsl::…` in the SQL
// helpers instead (same convention as `tests/integration/test_outbox.rs`).

#[derive(diesel::QueryableByName)]
struct ProbeId {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    id: i32,
}

/// Create the scratch table used to observe writes.
async fn create_probe_table(pool: &arcrun::DbPool) {
    use diesel_async::RunQueryDsl;
    let mut conn = pool.get().await.unwrap();
    diesel::sql_query("CREATE TABLE tx_probe (id INT PRIMARY KEY)")
        .execute(&mut conn)
        .await
        .expect("create probe table");
}

/// Return the committed `tx_probe` ids as seen through `pool` (sorted).
async fn probe_ids(pool: &arcrun::DbPool) -> Vec<i32> {
    use diesel_async::RunQueryDsl;
    let mut conn = pool.get().await.unwrap();
    let rows: Vec<ProbeId> = diesel::sql_query("SELECT id FROM tx_probe ORDER BY id")
        .get_results(&mut conn)
        .await
        .expect("select probe ids");
    rows.into_iter().map(|r| r.id).collect()
}

/// Build an independent single-connection pool to the same database — a view of
/// committed state that is unaffected by whatever the pool-under-test is doing.
async fn build_observer(url: &str) -> arcrun::DbPool {
    use diesel_async::pooled_connection::AsyncDieselConnectionManager;
    use diesel_async::pooled_connection::bb8::Pool;
    let config =
        AsyncDieselConnectionManager::<diesel_async::AsyncPgConnection>::new(url.to_string());
    Pool::builder()
        .max_size(1)
        .build(config)
        .await
        .expect("observer pool")
}

#[tokio::test]
async fn test_audit2_a1_cancelled_midtx_does_not_leak_transaction() {
    use diesel_async::RunQueryDsl;
    let app = setup_test_db_with_pool_size(1).await;
    create_probe_table(&app.pool).await;
    let observer = build_observer(&app.url).await;

    // 1) Open a transaction that inserts id=1, then blocks on a long server-side
    //    sleep. Cancel it mid-flight by dropping the future via a short timeout.
    {
        let mut conn = app.pool.get().await.unwrap();
        let fut = arcrun::db::run_in_transaction(&mut conn, |conn| {
            Box::pin(async move {
                diesel::sql_query("INSERT INTO tx_probe (id) VALUES (1)")
                    .execute(conn)
                    .await?;
                // The future is dropped while awaiting this — i.e. mid-transaction.
                diesel::sql_query("SELECT pg_sleep(30)")
                    .execute(conn)
                    .await?;
                Ok::<(), ArcRunError>(())
            })
        });
        let res = tokio::time::timeout(Duration::from_millis(500), fut).await;
        assert!(
            res.is_err(),
            "the pg_sleep transaction future should have been cancelled by the timeout"
        );
        // `conn` drops here → returned to the pool. If the transaction manager
        // reports the connection broken (open tx), bb8 discards it rather than
        // handing it — with the leaked transaction — to the next borrower.
    }

    // 2) The next borrower must be in a clean autocommit state: this INSERT is
    //    NOT wrapped in an explicit transaction, so it must commit on its own.
    {
        let mut conn = app.pool.get().await.unwrap();
        diesel::sql_query("INSERT INTO tx_probe (id) VALUES (2)")
            .execute(&mut conn)
            .await
            .expect("autocommit insert on the reused/fresh connection must succeed");
    }

    // 3) Observed from an INDEPENDENT connection: id=1 (cancelled) must have
    //    rolled back, and id=2 must be durably committed. If the connection had
    //    leaked its transaction, id=2 would be trapped uncommitted (observer sees
    //    nothing) — proving real durability, not same-session visibility.
    let ids = probe_ids(&observer).await;
    assert_eq!(
        ids,
        vec![2],
        "observer must see only the autocommit write (id=2); the cancelled tx (id=1) \
         must have rolled back and id=2 must be durably committed — got {ids:?}"
    );
}

#[tokio::test]
async fn test_audit2_a1_commits_on_ok() {
    use diesel_async::RunQueryDsl;
    let app = setup_test_db_with_pool_size(1).await;
    create_probe_table(&app.pool).await;
    let observer = build_observer(&app.url).await;

    {
        let mut conn = app.pool.get().await.unwrap();
        arcrun::db::run_in_transaction(&mut conn, |conn| {
            Box::pin(async move {
                diesel::sql_query("INSERT INTO tx_probe (id) VALUES (10)")
                    .execute(conn)
                    .await?;
                Ok::<(), ArcRunError>(())
            })
        })
        .await
        .expect("Ok closure should commit");
    }

    assert_eq!(
        probe_ids(&observer).await,
        vec![10],
        "an Ok closure must commit its writes"
    );
}

#[tokio::test]
async fn test_audit2_a1_rolls_back_on_err() {
    use diesel_async::RunQueryDsl;
    let app = setup_test_db_with_pool_size(1).await;
    create_probe_table(&app.pool).await;
    let observer = build_observer(&app.url).await;

    {
        let mut conn = app.pool.get().await.unwrap();
        let res: Result<(), ArcRunError> = arcrun::db::run_in_transaction(&mut conn, |conn| {
            Box::pin(async move {
                diesel::sql_query("INSERT INTO tx_probe (id) VALUES (20)")
                    .execute(conn)
                    .await?;
                Err(ArcRunError::Internal("intentional rollback".into()))
            })
        })
        .await;
        assert!(
            matches!(res, Err(ArcRunError::Internal(_))),
            "the closure's error must propagate unchanged"
        );
    }

    assert_eq!(
        probe_ids(&observer).await,
        Vec::<i32>::new(),
        "an Err closure must roll back its writes"
    );

    // The connection must remain usable (clean rollback, not broken): a following
    // autocommit write commits normally.
    {
        let mut conn = app.pool.get().await.unwrap();
        diesel::sql_query("INSERT INTO tx_probe (id) VALUES (21)")
            .execute(&mut conn)
            .await
            .expect("connection must be reusable after a rolled-back transaction");
    }

    assert_eq!(
        probe_ids(&observer).await,
        vec![21],
        "post-rollback autocommit write must commit"
    );
}

// =============================================================================
// Audit 2, A2 — start-before-end gate: no permanent block on end/cancel delivery
// =============================================================================
//
// # Original bug
// The delivery loop's start-before-end gate (`claim_due_outbox_leased`) held an
// `end`/`cancel` outbox row back while ANY `start` row for the same task was still
// `pending`, with no time bound. Two paths could leave a start row `pending`
// forever, so the gate blocked the task's end/cancel delivery indefinitely:
//   1. A crash (or a swallowed UPDATE error) between `mark_task_running` and the
//      start-row completion — the task is Running, but nothing ever re-runs its
//      start (the stale-claim requeue only touches Claimed tasks).
//   2. A Claimed task canceled / stop_batched / dead-end-canceled after a crash
//      mid-webhook, never re-claimed.
// Side effect: those mature-but-unplayable rows inflated `outbox_backlog_stats`
// `oldest_ready_age_secs` without bound (a permanent false alert).
//
// # Fix
//   * `execute_webhook_for_task` now commits `mark_task_running` AND the start-row
//     completion in ONE transaction, closing the crash window on the nominal path.
//   * The gate is bounded by freshness: a pending start row only blocks while its
//     `updated_at > now() - start_stale_secs` (mirror of the existing
//     `webhook_idempotency_timeout` / `WORKER_CLAIM_TIMEOUT_SECS` staleness). A
//     start row that never completes eventually goes stale and stops blocking, so
//     end/cancel are delivered anyway — a deliberate relaxation that keeps
//     start-before-end for healthy starts while forbidding an eternal block.

#[derive(diesel::QueryableByName)]
struct TextStatus {
    #[diesel(sql_type = diesel::sql_types::Text)]
    status: String,
}

/// Status of the (single) `start` outbox row for a task, or `None` if absent.
async fn start_row_status(pool: &arcrun::DbPool, task_id: uuid::Uuid) -> Option<String> {
    let mut conn = pool.get().await.unwrap();
    let rows: Vec<TextStatus> = diesel_async::RunQueryDsl::get_results(
        diesel::sql_query(
            "SELECT status::text AS status FROM webhook_execution \
             WHERE task_id = $1 AND trigger = 'start'",
        )
        .bind::<diesel::sql_types::Uuid, _>(task_id),
        &mut *conn,
    )
    .await
    .unwrap();
    rows.into_iter().next().map(|r| r.status)
}

/// Count outbox rows for a task with the given status (raw SQL).
async fn outbox_count(pool: &arcrun::DbPool, task_id: uuid::Uuid, status: &str) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Cnt {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        c: i64,
    }
    let mut conn = pool.get().await.unwrap();
    let r: Cnt = diesel_async::RunQueryDsl::get_result(
        diesel::sql_query(
            "SELECT count(*) AS c FROM webhook_execution \
             WHERE task_id = $1 AND status = $2::webhook_execution_status",
        )
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .bind::<diesel::sql_types::Text, _>(status),
        &mut *conn,
    )
    .await
    .unwrap();
    r.c
}

/// Age the task's pending `start` outbox row so its `updated_at` is `secs` in the
/// past — simulating a start row that has been stuck `pending` (crash between
/// `mark_task_running` and its completion) beyond the freshness bound.
async fn age_start_row(pool: &arcrun::DbPool, task_id: uuid::Uuid, secs: i64) {
    let mut conn = pool.get().await.unwrap();
    diesel_async::RunQueryDsl::execute(
        diesel::sql_query(
            "UPDATE webhook_execution \
             SET updated_at = now() - ($2::bigint * interval '1 second') \
             WHERE task_id = $1 AND trigger = 'start'",
        )
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .bind::<diesel::sql_types::BigInt, _>(secs),
        &mut *conn,
    )
    .await
    .unwrap();
}

/// Insert a `pending` `start` outbox row for `task_id` (claims the idempotency slot
/// but never completes it) — the pathological state left by a crash mid-start.
async fn insert_pending_start_row(pool: &arcrun::DbPool, task_id: uuid::Uuid) {
    let mut conn = pool.get().await.unwrap();
    let key = idempotency_key(task_id, &TriggerKind::Start, &TriggerCondition::Success);
    let claimed = arcrun::db_operation::try_claim_webhook_execution(
        &mut conn,
        task_id,
        TriggerKind::Start,
        TriggerCondition::Success,
        &key,
        None,
    )
    .await
    .unwrap();
    assert!(claimed, "start row should be claimed (pending)");
}

/// A2 key test — a **stale** pending `start` row must NOT block end delivery.
///
/// Builds the pathological state: a task with a `start` row stuck `pending` whose
/// `updated_at` is aged well past the freshness bound, plus a mature `end:success`
/// row. Driving the delivery loop must deliver the end row anyway (the gate relaxes
/// on the stale start). With the unbounded gate (fix reverted) the end row would be
/// held back forever and this test fails (no delivery).
#[tokio::test]
async fn test_audit2_a2_stale_start_row_unblocks_end_delivery() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let hits = Arc::new(AtomicUsize::new(0));
    let (webhook_url, shutdown_server) = spawn_webhook_server(hits.clone());

    let payload = json!({
        "id": "a2-stale",
        "name": "A2 Stale Start",
        "kind": "a2-test",
        "timeout": 60,
        "metadata": {"test": true},
        "on_start": webhook_action(),
        "on_success": [{"kind": "Webhook", "params": {"url": webhook_url, "verb": "Post"}}]
    });
    let created = create_tasks_ok(&app, &[payload]).await;
    let task_id = created[0].id;

    // Pathological state: a start row stuck pending, then aged past the freshness bound.
    insert_pending_start_row(&state.pool, task_id).await;

    // Terminal transition -> enqueue the end:success outbox row.
    succeed_task(&state, task_id).await;

    // Age the stuck start row to 1h old (>> the 30s freshness bound of default cfg).
    age_start_row(&state.pool, task_id, 3600).await;
    assert_eq!(
        start_row_status(&state.pool, task_id).await.as_deref(),
        Some("pending"),
        "the start row is still pending (never completed) — the pathological state"
    );

    // default_delivery_cfg() uses start_stale_secs = 30, so the 1h-old start row is
    // stale and the gate must relax.
    let processed = drain_outbox(&state).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert!(
        processed >= 1,
        "the end row must be claimed/delivered despite the stale pending start row"
    );
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "end webhook delivered exactly once despite a stale pending start row"
    );
    assert_eq!(
        outbox_count(&state.pool, task_id, "success").await,
        1,
        "the end row should be marked success"
    );

    let _ = shutdown_server.send(());
}

/// A2 order-preserved test — a **fresh** pending `start` row STILL blocks end
/// delivery (the gate holds for healthy in-flight starts). Once the same row is
/// aged past the freshness bound, the end row delivers — proving the block is
/// time-bounded, not permanent.
#[tokio::test]
async fn test_audit2_a2_fresh_start_row_still_blocks_end() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let hits = Arc::new(AtomicUsize::new(0));
    let (webhook_url, shutdown_server) = spawn_webhook_server(hits.clone());

    let payload = json!({
        "id": "a2-fresh",
        "name": "A2 Fresh Start",
        "kind": "a2-test",
        "timeout": 60,
        "metadata": {"test": true},
        "on_start": webhook_action(),
        "on_success": [{"kind": "Webhook", "params": {"url": webhook_url, "verb": "Post"}}]
    });
    let created = create_tasks_ok(&app, &[payload]).await;
    let task_id = created[0].id;

    // A fresh pending start row (updated_at = now()) — a healthy in-flight start.
    insert_pending_start_row(&state.pool, task_id).await;

    // Terminal transition -> enqueue the end:success outbox row.
    succeed_task(&state, task_id).await;

    // With a fresh start row, the gate holds: end must NOT be delivered.
    let processed = drain_outbox(&state).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        processed, 0,
        "end row must be held while a FRESH start row is pending (start-before-end)"
    );
    assert_eq!(hits.load(Ordering::SeqCst), 0, "no end webhook fired yet");

    // Now age the start row past the freshness bound: the gate relaxes and the end
    // row becomes deliverable (the block is bounded, never permanent).
    age_start_row(&state.pool, task_id, 3600).await;
    drain_outbox(&state).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "end webhook delivered once the start row went stale"
    );

    let _ = shutdown_server.send(());
}

/// A2 same-tx (nominal path) test — driving a task through the real `start_loop`,
/// the on_start webhook succeeds and the running transition + start-row completion
/// commit together, so the `start` row ends up `success` (never a lingering
/// `pending`). The task's end row then delivers normally through the healthy gate.
#[tokio::test]
async fn test_audit2_a2_start_row_completed_atomically_with_running() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let start_hits = Arc::new(AtomicUsize::new(0));
    let (start_url, start_shutdown) = spawn_webhook_server(start_hits.clone());
    let end_hits = Arc::new(AtomicUsize::new(0));
    let (end_url, end_shutdown) = spawn_webhook_server(end_hits.clone());

    let payload = json!({
        "id": "a2-nominal",
        "name": "A2 Nominal Start",
        "kind": "a2-test",
        "timeout": 60,
        "metadata": {"test": true},
        "on_start": {"kind": "Webhook", "params": {"url": start_url, "verb": "Post"}},
        "on_success": [{"kind": "Webhook", "params": {"url": end_url, "verb": "Post"}}]
    });
    let created = create_tasks_ok(&app, &[payload]).await;
    let task_id = created[0].id;
    assert_eq!(created[0].status, StatusKind::Pending);

    // Drive the real start_loop: claim -> on_start webhook -> mark_task_running +
    // start-row completion (one tx).
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let evaluator = state.action_executor.clone();
    let pool = state.pool.clone();
    let handle = tokio::spawn(async move {
        arcrun::workers::start_loop(
            &evaluator,
            pool,
            Duration::from_millis(50),
            true,
            50,
            10,
            shutdown_rx,
        )
        .await;
    });

    tokio::time::sleep(Duration::from_millis(500)).await;
    let _ = shutdown_tx.send(true);
    let _ = handle.await;

    assert_eq!(
        start_hits.load(Ordering::SeqCst),
        1,
        "on_start webhook should have fired once"
    );
    assert_task_status(
        &app,
        task_id,
        StatusKind::Running,
        "task should be Running after start_loop",
    )
    .await;

    // The A2 fix: the start row is `success` (committed atomically with the running
    // transition), NOT a lingering `pending` that would block end/cancel forever.
    assert_eq!(
        start_row_status(&state.pool, task_id).await.as_deref(),
        Some("success"),
        "start row must be completed (success) atomically with the running transition"
    );

    // Transition Running -> Success, enqueuing the end row. The (success) start row
    // does not block it — the end webhook delivers normally.
    {
        let mut conn = state.pool.get().await.unwrap();
        let dto = arcrun::dtos::UpdateTaskDto {
            status: Some(StatusKind::Success),
            metadata: None,
            new_success: None,
            new_failures: None,
            failure_reason: None,
            expected_count: None,
            priority: None,
        };
        let result = arcrun::db_operation::update_running_task(&mut conn, task_id, dto, true)
            .await
            .unwrap();
        assert_eq!(result, arcrun::db_operation::UpdateTaskResult::Updated);
    }

    drain_outbox(&state).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        end_hits.load(Ordering::SeqCst),
        1,
        "end webhook delivered normally after a healthy (success) start row"
    );

    let _ = start_shutdown.send(());
    let _ = end_shutdown.send(());
}

// =============================================================================
// Audit 2, A4 — Claimed + on_start in flight: no zombie work at the consumer
// =============================================================================
//
// # Original bug
// The `Claimed` state spans the ENTIRE on_start webhook-in-flight window (the
// worker's permit queue plus up to the webhook timeout per action), not just the
// instant before on_start. Two gaps let a consumer receive on_start, start work,
// and then never get a cancel:
//   1. `execute_webhook_for_task` only called `save_cancel_actions` in the
//      `mark_task_running == true` branch. If the task left `Claimed` while on_start
//      was in flight (a concurrent DELETE / stop_batch / requeue), the transition
//      returned false and the cancel action the consumer returned was silently
//      dropped — so even if a cancel was later delivered, it carried no action.
//   2. `cancel_task` enqueued a cancel outbox row only for `Running`, and
//      `stop_batch` treated `Claimed` as "on_start not yet called" (no cancel), and
//      the dead-end path gated the cancel on `was_running`. So a Claimed task that
//      had received on_start was never sent a cancel at all.
//
// # Fix
//   * `save_cancel_actions` now runs INSIDE the running-transition/start-row-
//     completion transaction, unconditionally when on_start actually executed
//     (`claimed == true`) — regardless of whether `mark_task_running` transitioned
//     the row. Committing the actions atomically with the start-row completion also
//     closes the race with the delivery loop's start-before-end gate: the cancel row
//     (enqueued by the concurrent transition) is held while the start row is pending,
//     and by the time the gate releases (start row completed in this tx) the cancel
//     actions are already committed. Validation is best-effort (invalid actions are
//     skipped, never rolling back the transition — the 4.2 decision).
//   * `cancel_task`, `stop_batch`, and the dead-end path now enqueue a cancel outbox
//     row for `Claimed` as well as `Running`. A Claimed task that never returned a
//     cancel action prefetches zero cancel actions ⇒ fast-path success (no HTTP), so
//     the broadened enqueue is innocuous.

/// Count `cancel`-trigger action rows registered for a task.
async fn cancel_action_count(pool: &arcrun::DbPool, task_id: uuid::Uuid) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Cnt {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        c: i64,
    }
    let mut conn = pool.get().await.unwrap();
    let r: Cnt = diesel_async::RunQueryDsl::get_result(
        diesel::sql_query(
            "SELECT count(*) AS c FROM action WHERE task_id = $1 AND trigger = 'cancel'",
        )
        .bind::<diesel::sql_types::Uuid, _>(task_id),
        &mut *conn,
    )
    .await
    .unwrap();
    r.c
}

/// Count `cancel`-trigger outbox rows for a task with the given status.
async fn cancel_outbox_count(pool: &arcrun::DbPool, task_id: uuid::Uuid, status: &str) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Cnt {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        c: i64,
    }
    let mut conn = pool.get().await.unwrap();
    let r: Cnt = diesel_async::RunQueryDsl::get_result(
        diesel::sql_query(
            "SELECT count(*) AS c FROM webhook_execution \
             WHERE task_id = $1 AND trigger = 'cancel' \
               AND status = $2::webhook_execution_status",
        )
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .bind::<diesel::sql_types::Text, _>(status),
        &mut *conn,
    )
    .await
    .unwrap();
    r.c
}

/// Spawn a mock on_start server that responds — after `delay` — with a `NewActionDto`
/// (a cancel action pointing at `cancel_url`). The delay keeps the task `Claimed`
/// (on_start in flight) long enough for a concurrent cancel to fire.
fn spawn_slow_server_returning_cancel_action(
    cancel_url: &str,
    delay: Duration,
) -> (String, tokio::sync::oneshot::Sender<()>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    listener.set_nonblocking(true).unwrap();
    let listener = tokio::net::TcpListener::from_std(listener).unwrap();

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let cancel_url = cancel_url.to_string();

    tokio::spawn(async move {
        loop {
            tokio::select! {
                result = listener.accept() => {
                    if let Ok((mut stream, _)) = result {
                        let cancel_url = cancel_url.clone();
                        tokio::spawn(async move {
                            let mut buf = [0u8; 4096];
                            let _ = stream.read(&mut buf).await;
                            // Hold the response so the task stays Claimed (in flight).
                            tokio::time::sleep(delay).await;
                            let body = serde_json::json!({
                                "kind": "Webhook",
                                "params": {"url": cancel_url, "verb": "Post"}
                            });
                            let body_str = serde_json::to_string(&body).unwrap();
                            let response = format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                                body_str.len(),
                                body_str
                            );
                            let _ = stream.write_all(response.as_bytes()).await;
                        });
                    }
                }
                _ = &mut shutdown_rx => break,
            }
        }
    });

    (format!("http://{}/start", addr), shutdown_tx)
}

/// A4 core test — cancelling a task WHILE its on_start is in flight must NOT drop the
/// cancel action the consumer returned, and the consumer must still receive the cancel.
///
/// The task is claimed and its (slow) on_start webhook begins; while it is in flight a
/// DELETE cancels the task (it leaves `Claimed`), so when on_start returns
/// `mark_task_running` == false. The A4 fix persists the returned cancel action inside
/// the same tx anyway, and the cancel outbox row (enqueued by the DELETE, now allowed
/// for `Claimed`) is delivered WITH that action after draining. With the fix reverted,
/// the action is dropped (and/or no cancel row is enqueued) → 0 deliveries.
#[tokio::test]
async fn test_audit2_a4_cancel_action_saved_when_task_left_claimed_during_on_start() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    // Cancel webhook target — counts deliveries to the consumer.
    let cancel_hits = Arc::new(AtomicUsize::new(0));
    let (cancel_url, cancel_shutdown) = spawn_webhook_server(cancel_hits.clone());

    // Slow on_start that returns a cancel action once it finally responds.
    let (start_url, start_shutdown) =
        spawn_slow_server_returning_cancel_action(&cancel_url, Duration::from_millis(1500));

    let payload = json!({
        "id": "a4-inflight",
        "name": "A4 In-flight Cancel",
        "kind": "a4-test",
        "timeout": 60,
        "metadata": {"test": true},
        "on_start": {"kind": "Webhook", "params": {"url": start_url, "verb": "Post"}}
    });
    let created = create_tasks_ok(&app, &[payload]).await;
    let task_id = created[0].id;

    // Drive the real start_loop.
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let evaluator = state.action_executor.clone();
    let pool = state.pool.clone();
    let handle = tokio::spawn(async move {
        arcrun::workers::start_loop(
            &evaluator,
            pool,
            Duration::from_millis(50),
            true,
            50,
            10,
            shutdown_rx,
        )
        .await;
    });

    // Wait for start_loop to claim the task and begin the slow on_start webhook.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_task_status(
        &app,
        task_id,
        StatusKind::Claimed,
        "task should be Claimed with on_start in flight",
    )
    .await;

    // Cancel it WHILE on_start is in flight — the task leaves Claimed for Canceled and
    // (A4) a cancel outbox row is enqueued.
    let cancel_req = actix_web::test::TestRequest::delete()
        .uri(&format!("/task/{}", task_id))
        .to_request();
    let cancel_resp = actix_web::test::call_service(&app, cancel_req).await;
    assert!(cancel_resp.status().is_success(), "cancel should succeed");
    assert_task_status(
        &app,
        task_id,
        StatusKind::Canceled,
        "task should be Canceled",
    )
    .await;

    // Let the slow on_start finish; the start_loop tx then runs mark_task_running
    // (== false) but STILL saves the cancel action + completes the start row (A4).
    // Shutting down waits for the in-flight webhook + its tx to complete.
    let _ = shutdown_tx.send(true);
    let _ = handle.await;

    assert_eq!(
        cancel_action_count(&state.pool, task_id).await,
        1,
        "cancel action returned by on_start must be saved even though mark_task_running == false"
    );

    // Deliver the outbox: the cancel row was held by the start-before-end gate until
    // the start row completed (same tx as the save), so it now delivers WITH the action.
    drain_outbox(&state).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        cancel_hits.load(Ordering::SeqCst),
        1,
        "the consumer must receive the cancel webhook exactly once (no zombie work)"
    );

    let _ = cancel_shutdown.send(());
    let _ = start_shutdown.send(());
}

/// A4 — cancelling a `Claimed` task (via DELETE /task) must enqueue a cancel outbox
/// row. Before the fix, `cancel_task` only enqueued for `Running`.
#[tokio::test]
async fn test_audit2_a4_cancel_task_on_claimed_enqueues_cancel_outbox() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let payload = json!({
        "id": "a4-claimed",
        "name": "A4 Claimed Cancel",
        "kind": "a4-test",
        "timeout": 60,
        "metadata": {"test": true},
        "on_start": webhook_action()
    });
    let created = create_tasks_ok(&app, &[payload]).await;
    let task_id = created[0].id;

    // Drive the task to Claimed directly — the window the audit flags.
    {
        let mut conn = state.pool.get().await.unwrap();
        let claimed = arcrun::db_operation::claim_task(&mut conn, &task_id)
            .await
            .unwrap();
        assert!(claimed, "task should be claimed");
    }
    assert_task_status(&app, task_id, StatusKind::Claimed, "task should be Claimed").await;

    let cancel_req = actix_web::test::TestRequest::delete()
        .uri(&format!("/task/{}", task_id))
        .to_request();
    let resp = actix_web::test::call_service(&app, cancel_req).await;
    assert!(resp.status().is_success(), "cancel should succeed");
    assert_task_status(
        &app,
        task_id,
        StatusKind::Canceled,
        "task should be Canceled",
    )
    .await;

    assert_eq!(
        cancel_outbox_count(&state.pool, task_id, "pending").await,
        1,
        "canceling a Claimed task must enqueue a pending cancel outbox row"
    );
}

/// A4 — stopping a batch that contains a `Claimed` task must enqueue a cancel outbox
/// row for that task. Before the fix, `stop_batch` treated Claimed as "on_start not
/// yet called" and enqueued nothing.
#[tokio::test]
async fn test_audit2_a4_stop_batch_on_claimed_enqueues_cancel_outbox() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let payload = json!({
        "id": "a4-batch-claimed",
        "name": "A4 Batch Claimed",
        "kind": "a4-test",
        "timeout": 60,
        "metadata": {"test": true},
        "on_start": webhook_action()
    });
    let created = create_tasks_ok(&app, &[payload]).await;
    let task_id = created[0].id;
    let batch_id = created[0].batch_id.expect("task should have a batch_id");

    {
        let mut conn = state.pool.get().await.unwrap();
        assert!(
            arcrun::db_operation::claim_task(&mut conn, &task_id)
                .await
                .unwrap(),
            "task should be claimed"
        );
    }
    assert_task_status(&app, task_id, StatusKind::Claimed, "task should be Claimed").await;

    let req = actix_web::test::TestRequest::delete()
        .uri(&format!("/batch/{}", batch_id))
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert!(resp.status().is_success(), "stop_batch should succeed");
    assert_task_status(
        &app,
        task_id,
        StatusKind::Canceled,
        "task should be Canceled",
    )
    .await;

    assert_eq!(
        cancel_outbox_count(&state.pool, task_id, "pending").await,
        1,
        "stopping a batch with a Claimed task must enqueue a pending cancel outbox row"
    );
}

/// A4 innocuousness — a `Claimed` task that never started (no cancel action ever
/// registered) canceled and drained: the cancel row exists but delivers via the
/// zero-action fast-path (marked success) with ZERO HTTP hits.
#[tokio::test]
async fn test_audit2_a4_claimed_cancel_without_action_delivers_no_http() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    // A server we assert is never hit.
    let hits = Arc::new(AtomicUsize::new(0));
    let (_url, shutdown) = spawn_webhook_server(hits.clone());

    let payload = json!({
        "id": "a4-noop",
        "name": "A4 No-op Cancel",
        "kind": "a4-test",
        "timeout": 60,
        "metadata": {"test": true},
        "on_start": webhook_action()
    });
    let created = create_tasks_ok(&app, &[payload]).await;
    let task_id = created[0].id;

    {
        let mut conn = state.pool.get().await.unwrap();
        assert!(
            arcrun::db_operation::claim_task(&mut conn, &task_id)
                .await
                .unwrap(),
            "task should be claimed"
        );
    }

    let req = actix_web::test::TestRequest::delete()
        .uri(&format!("/task/{}", task_id))
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert!(resp.status().is_success(), "cancel should succeed");
    assert_task_status(
        &app,
        task_id,
        StatusKind::Canceled,
        "task should be Canceled",
    )
    .await;

    assert_eq!(
        cancel_outbox_count(&state.pool, task_id, "pending").await,
        1,
        "cancel outbox row enqueued for the Claimed task"
    );

    drain_outbox(&state).await;
    tokio::time::sleep(Duration::from_millis(150)).await;

    assert_eq!(
        hits.load(Ordering::SeqCst),
        0,
        "no cancel action registered -> zero HTTP deliveries (innocuous)"
    );
    assert_eq!(
        cancel_outbox_count(&state.pool, task_id, "success").await,
        1,
        "the zero-action cancel row must be marked success via the fast-path"
    );

    let _ = shutdown.send(());
}

/// A4 permit-wait window — a task canceled while queued for a webhook permit (claimed,
/// but its `start` outbox row not yet created) must NOT receive on_start at all.
///
/// In that window the cancel outbox row is not held by the start-before-end gate
/// (there is no start row yet), so it can be drained as a zero-action fast-path
/// BEFORE on_start fires. If on_start then fires anyway, the consumer starts zombie
/// work whose cancel has already been consumed — unfixable after the fact. The fix
/// re-checks the task's status in `start_task` right after creating the start row
/// (post-permit): no longer `Claimed` ⇒ skip on_start and complete the start row
/// (nothing executed, so the zero-action cancel was correct). Any transition landing
/// after that check is gated normally (start row pending + fresh) until the
/// completion tx commits the cancel actions.
///
/// Deterministic setup: `webhook_concurrency = 1`; T1's slow on_start (2 s) holds the
/// only permit while T2 — claimed in the same iteration — waits. T2 is canceled and
/// the outbox drained during that wait. With the fix, T2's on_start never fires and
/// its start row ends `success`; with the fix reverted, T2's mock receives on_start.
#[tokio::test]
async fn test_audit2_a4_cancel_during_permit_wait_skips_on_start() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    // T1's slow on_start: holds the single permit for 2 s.
    let t1_cancel_hits = Arc::new(AtomicUsize::new(0));
    let (t1_cancel_url, t1_cancel_shutdown) = spawn_webhook_server(t1_cancel_hits.clone());
    let (t1_start_url, t1_start_shutdown) =
        spawn_slow_server_returning_cancel_action(&t1_cancel_url, Duration::from_millis(2000));

    // T2's on_start: must NEVER be hit.
    let t2_start_hits = Arc::new(AtomicUsize::new(0));
    let (t2_start_url, t2_start_shutdown) = spawn_webhook_server(t2_start_hits.clone());

    // Higher priority on T1 so it is claimed/processed first and takes the permit.
    let t1 = json!({
        "id": "a4-permit-t1",
        "name": "A4 Permit Holder",
        "kind": "a4-test",
        "timeout": 60,
        "priority": 10,
        "metadata": {"test": true},
        "on_start": {"kind": "Webhook", "params": {"url": t1_start_url, "verb": "Post"}}
    });
    let t2 = json!({
        "id": "a4-permit-t2",
        "name": "A4 Permit Waiter",
        "kind": "a4-test",
        "timeout": 60,
        "priority": 0,
        "metadata": {"test": true},
        "on_start": {"kind": "Webhook", "params": {"url": t2_start_url, "verb": "Post"}}
    });
    let created = create_tasks_ok(&app, &[t1, t2]).await;
    let t2_id = created[1].id;

    // Drive the real start_loop with webhook_concurrency = 1.
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let evaluator = state.action_executor.clone();
    let pool = state.pool.clone();
    let handle = tokio::spawn(async move {
        arcrun::workers::start_loop(
            &evaluator,
            pool,
            Duration::from_millis(50),
            true,
            50,
            1,
            shutdown_rx,
        )
        .await;
    });

    // Both tasks are claimed in the first iteration; T1's webhook is in flight, T2 is
    // Claimed, queued for the permit, with NO start row yet.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_task_status(
        &app,
        t2_id,
        StatusKind::Claimed,
        "T2 should be Claimed, waiting for the permit",
    )
    .await;
    assert_eq!(
        start_row_status(&state.pool, t2_id).await,
        None,
        "T2's start row must not exist yet (permit-wait window)"
    );

    // Cancel T2 during the permit wait and drain: the cancel row is not gated (no
    // start row) and is consumed as a zero-action fast-path — as in production.
    let req = actix_web::test::TestRequest::delete()
        .uri(&format!("/task/{}", t2_id))
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert!(resp.status().is_success(), "cancel should succeed");
    drain_outbox(&state).await;
    assert_eq!(
        cancel_outbox_count(&state.pool, t2_id, "success").await,
        1,
        "T2's cancel row was drained (zero-action) during the permit wait"
    );

    // Let T1's slow webhook finish; the loop then processes T2 with the permit.
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let _ = shutdown_tx.send(true);
    let _ = handle.await;

    assert_eq!(
        t2_start_hits.load(Ordering::SeqCst),
        0,
        "T2's on_start must NOT fire for a task that left Claimed during the permit wait"
    );
    assert_eq!(
        start_row_status(&state.pool, t2_id).await.as_deref(),
        Some("success"),
        "T2's start row must be completed by the skip path (gate released)"
    );
    assert_task_status(&app, t2_id, StatusKind::Canceled, "T2 stays Canceled").await;

    let _ = t1_cancel_shutdown.send(());
    let _ = t1_start_shutdown.send(());
    let _ = t2_start_shutdown.send(());
}

// ---------------------------------------------------------------------------
// A7 — batch-updater counter pipeline: terminal guard, i32 clamp, anti-poison.
// ---------------------------------------------------------------------------

// NOTE: `RunQueryDsl` is intentionally not in module scope (see top-of-file
// comment), so `execute` is invoked in fully-qualified form here.

/// Force a task's status (and, for `running`, a known-old `last_updated`) directly.
async fn force_status_running_old(pool: &arcrun::DbPool, task_id: uuid::Uuid) {
    use diesel::sql_types::{Timestamptz, Uuid as SqlUuid};
    let mut conn = pool.get().await.unwrap();
    let past = chrono::Utc::now() - chrono::Duration::seconds(3600);
    let q = diesel::sql_query(
        "UPDATE task SET status = 'running', started_at = $1, last_updated = $1 WHERE id = $2",
    )
    .bind::<Timestamptz, _>(past)
    .bind::<SqlUuid, _>(task_id);
    diesel_async::RunQueryDsl::execute(q, &mut conn)
        .await
        .unwrap();
}

/// Force a task to terminal `success`, stamping `last_updated = now()`.
async fn force_status_terminal_success(pool: &arcrun::DbPool, task_id: uuid::Uuid) {
    use diesel::sql_types::Uuid as SqlUuid;
    let mut conn = pool.get().await.unwrap();
    let q = diesel::sql_query(
        "UPDATE task SET status = 'success', ended_at = now(), last_updated = now() WHERE id = $1",
    )
    .bind::<SqlUuid, _>(task_id);
    diesel_async::RunQueryDsl::execute(q, &mut conn)
        .await
        .unwrap();
}

/// Set a task's raw success counter (used to place it near i32::MAX).
async fn set_success_counter(pool: &arcrun::DbPool, task_id: uuid::Uuid, value: i32) {
    use diesel::sql_types::{Integer, Uuid as SqlUuid};
    let mut conn = pool.get().await.unwrap();
    let q = diesel::sql_query("UPDATE task SET success = $1 WHERE id = $2")
        .bind::<Integer, _>(value)
        .bind::<SqlUuid, _>(task_id);
    diesel_async::RunQueryDsl::execute(q, &mut conn)
        .await
        .unwrap();
}

async fn put_counts(
    app: &impl actix_web::dev::Service<
        actix_http::Request,
        Response = actix_web::dev::ServiceResponse<impl actix_web::body::MessageBody>,
        Error = actix_web::Error,
    >,
    task_id: uuid::Uuid,
    success: i32,
    failures: i32,
) {
    let req = actix_web::test::TestRequest::put()
        .uri(&format!("/task/{}", task_id))
        .set_json(&json!({"new_success": success, "new_failures": failures}))
        .to_request();
    let resp = actix_web::test::call_service(app, req).await;
    assert_eq!(
        resp.status(),
        actix_web::http::StatusCode::ACCEPTED,
        "PUT counter update should be accepted"
    );
}

/// # Original bug (A7)
/// The batch-updater flush UNNEST statement had **no status guard**. A counter
/// flush (from a buffered `PUT /task/{id}`) that landed *after* the task became
/// terminal mutated `success`/`failures`/`last_updated` of a terminal task —
/// violating "terminal states are immutable" and diverging from the counts that
/// were already delivered with the task's end notification.
///
/// # Fix
/// The flush now carries `AND task.status NOT IN ('success','failure','canceled')`.
/// A row whose task is terminal no longer matches, so its buffered counts are
/// **dropped** (a terminal task's counters are frozen — dropping, not re-queuing,
/// is correct). A non-match is not an error, so nothing is re-queued.
///
/// # What this test asserts
/// * Control: while the task is `running`, a flush DOES apply the counts and bumps
///   `last_updated` (keepalive preserved).
/// * After the task is terminal, a later flush leaves `success`, `failures` AND
///   `last_updated` **unchanged**. Reverting the guard makes the terminal counters
///   move and this test go red.
#[tokio::test]
async fn test_audit2_a7_terminal_guard_drops_late_flush() {
    let (_g, test_state) = setup_test_app_with_batch_updater().await;
    let state = test_state.state;
    let app = test_service!(state);

    let created = create_tasks_ok(&app, &[task_json("a7-guard", "A7 terminal guard", "a7")]).await;
    let task_id = created[0].id;

    // Control: while running, a flush must apply counts + bump last_updated.
    force_status_running_old(&state.pool, task_id).await;
    let running_before = get_task_ok(&app, task_id).await;
    put_counts(&app, task_id, 2, 0).await;
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    let running_after = get_task_ok(&app, task_id).await;
    assert_eq!(
        running_after.success, 2,
        "running task must receive counter updates"
    );
    assert!(
        running_after.last_updated > running_before.last_updated,
        "running task flush must bump last_updated (keepalive)"
    );

    // Transition to terminal; snapshot the frozen counters + last_updated.
    force_status_terminal_success(&state.pool, task_id).await;
    let terminal = get_task_ok(&app, task_id).await;

    // A late flush for the now-terminal task must be dropped entirely.
    put_counts(&app, task_id, 5, 3).await;
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;

    let after = get_task_ok(&app, task_id).await;
    assert_eq!(
        after.success, terminal.success,
        "success must be frozen on a terminal task (late flush dropped)"
    );
    assert_eq!(
        after.failures, terminal.failures,
        "failures must be frozen on a terminal task (late flush dropped)"
    );
    assert_eq!(
        after.last_updated, terminal.last_updated,
        "last_updated must NOT move on a terminal task (late flush dropped)"
    );
}

/// # Original bug (A7 — poison pill)
/// The flush summed counters as `task.success + batch.s` in `int4`. A task whose
/// counter approached `i32::MAX` (realistic for a long-lived high-throughput
/// batch) would overflow on the next delta, raising `integer out of range`. That
/// error failed the whole multi-row flush, which re-queued **every** count with
/// no backoff or cap — the same overflowing row erroring on each iteration, so the
/// entire counter pipeline (all tasks) was wedged until process restart (which
/// loses the in-memory DashMap).
///
/// # Fix
/// The sum is computed in `bigint` and clamped:
/// `LEAST(task.success::bigint + batch.s, 2147483647)`. The `::bigint` cast makes
/// the addition overflow-proof; `LEAST(..)` saturates at `i32::MAX`. No error is
/// ever raised, so the flush commits and the pipeline keeps flowing.
///
/// # What this test asserts
/// A task set to `i32::MAX - 100` that receives a `+1000` delta saturates to
/// exactly `2147483647` with no error, and the updater loop remains alive
/// (a second, independent task's counters are still flushed afterwards). Reverting
/// the clamp makes the flush raise an overflow error: the delta is never applied
/// (counter stays at `i32::MAX - 100`) and this test goes red.
#[tokio::test]
async fn test_audit2_a7_clamp_prevents_i32_overflow() {
    let (_g, test_state) = setup_test_app_with_batch_updater().await;
    let state = test_state.state;
    let app = test_service!(state);

    let created = create_tasks_ok(&app, &[task_json("a7-clamp", "A7 clamp", "a7")]).await;
    let task_id = created[0].id;

    // Place the counter just below i32::MAX while the task is active.
    force_status_running_old(&state.pool, task_id).await;
    let near_max = i32::MAX - 100; // 2_147_483_547
    set_success_counter(&state.pool, task_id, near_max).await;

    // A +1000 delta would overflow int4 without the bigint/LEAST clamp.
    put_counts(&app, task_id, 1000, 0).await;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let after = get_task_ok(&app, task_id).await;
    assert_eq!(
        after.success,
        i32::MAX,
        "counter must saturate at i32::MAX, not overflow (got {})",
        after.success
    );

    // Pipeline liveness: an independent task's counters still flush afterwards,
    // proving the near-overflow row did not wedge the updater loop.
    let created2 = create_tasks_ok(
        &app,
        &[task_json("a7-clamp-live", "A7 clamp liveness", "a7")],
    )
    .await;
    let live_id = created2[0].id;
    force_status_running_old(&state.pool, live_id).await;
    put_counts(&app, live_id, 7, 0).await;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let live = get_task_ok(&app, live_id).await;
    assert_eq!(
        live.success, 7,
        "pipeline must stay alive: a later task's counters are still persisted"
    );
}

// ---------------------------------------------------------------------------
// A3 — `Paused` is no longer an escape-hatch / state-trap.
// ---------------------------------------------------------------------------
//
// # Original bug
// `Paused` combined three defects: (1) no resume path existed (the endpoint doc
// promised "PATCH to Pending" but `validate_update_task` only accepted
// Success/Failure and `update_running_task` only matched Running/Claimed, so the
// only way out of Paused was cancel); (2) all propagation filtered `status =
// 'waiting'`, so a task paused while `Waiting` never received the `wait_*`
// decrements nor the cascade-fail from parents that finished during the pause —
// counters were stranded forever, the batch never went terminal and
// `on_batch_complete` never fired; (3) a `Running` task could be paused, escaping
// the timeout loop while the external worker's PATCH 404'd — a wedged task.
//
// # Fix
// * Pause is restricted to `Pending`/`Waiting` (atomic guarded UPDATE); Running
//   (suggest cancel), Claimed and terminal states are refused with a 400.
// * A new `PATCH /task/resume/{id}` moves a `Paused` task back to `Waiting` (if any
//   dependency is still outstanding) or `Pending` (all met), in one atomic guarded
//   UPDATE (`CASE` on the counters — no SELECT-then-UPDATE race). Only `Paused` is
//   resumable.
// * Propagation now targets `status IN ('waiting','paused')` for the `wait_*`
//   decrements AND the cascade-fail, so a paused task is kept consistent and is not
//   shielded from a required parent's failure. The transition-to-Pending stays
//   `Waiting`-only: a Paused task whose counters reach 0 stays Paused until resume.

use actix_http::Request as ActixRequest;
use actix_web::body::MessageBody as ActixMessageBody;
use actix_web::dev::{Service as ActixService, ServiceResponse as ActixServiceResponse};
use actix_web::http::StatusCode;
use arcrun::handlers::AppState;

/// PATCH /task/pause/{id}, returning the HTTP status.
async fn pause_task_http<S, B>(app: &S, id: uuid::Uuid) -> StatusCode
where
    S: ActixService<ActixRequest, Response = ActixServiceResponse<B>, Error = actix_web::Error>,
    B: ActixMessageBody,
{
    let req = actix_web::test::TestRequest::patch()
        .uri(&format!("/task/pause/{}", id))
        .to_request();
    actix_web::test::call_service(app, req).await.status()
}

/// PATCH /task/resume/{id}, returning the HTTP status.
async fn resume_task_http<S, B>(app: &S, id: uuid::Uuid) -> StatusCode
where
    S: ActixService<ActixRequest, Response = ActixServiceResponse<B>, Error = actix_web::Error>,
    B: ActixMessageBody,
{
    let req = actix_web::test::TestRequest::patch()
        .uri(&format!("/task/resume/{}", id))
        .to_request();
    actix_web::test::call_service(app, req).await.status()
}

/// Drive a Pending task straight to `Claimed` (no on_start), for the refusal tests.
async fn force_claimed(state: &AppState, task_id: uuid::Uuid) {
    let mut conn = state.pool.get().await.unwrap();
    assert!(
        arcrun::db_operation::claim_task(&mut conn, &task_id)
            .await
            .unwrap(),
        "task should be claimed"
    );
}

/// A3 test 1 — a `Paused` task still receives `wait_*` decrements when a parent
/// finishes, but does NOT auto-transition to Pending; resume then unblocks it.
///
/// DAG: parent -> child (requires_success). Child is Waiting, then paused. When the
/// parent succeeds, the (Paused) child's counters must be decremented to 0 while it
/// stays Paused. Resuming it (counters 0) moves it to Pending. With the fix reverted
/// the decrement is filtered out (`status = 'waiting'` only) and the counters stay
/// frozen at 1/1 — the child could never run even after resume.
#[tokio::test]
async fn test_audit2_a3_paused_receives_decrements() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let created = create_tasks_ok(
        &app,
        &[
            task_json("parent", "Parent", "a3"),
            task_with_deps("child", "Child", "a3", vec![("parent", true)]),
        ],
    )
    .await;
    let parent = created[0].id;
    let child = created[1].id;
    assert_eq!(created[1].status, StatusKind::Waiting);

    // Pause the Waiting child.
    assert_eq!(pause_task_http(&app, child).await, StatusCode::OK);
    assert_task_status(&app, child, StatusKind::Paused, "child should be Paused").await;
    assert_eq!(
        read_wait_counters(&state.pool, child).await,
        (1, 1),
        "child starts with one unmet dependency"
    );

    // Parent succeeds -> the Paused child must be decremented, but stay Paused.
    succeed_task(&state, parent).await;
    assert_eq!(
        read_wait_counters(&state.pool, child).await,
        (0, 0),
        "Paused child must receive the wait_* decrements from its parent"
    );
    assert_task_status(
        &app,
        child,
        StatusKind::Paused,
        "child must stay Paused (no auto-transition to Pending)",
    )
    .await;

    // Resume -> counters are 0, so it goes straight to Pending.
    assert_eq!(resume_task_http(&app, child).await, StatusCode::OK);
    assert_task_status(
        &app,
        child,
        StatusKind::Pending,
        "resumed child with met deps should be Pending",
    )
    .await;
}

/// A3 test 2 — cascade-fail reaches a `Paused` task and propagates recursively.
///
/// DAG: parent -> child -> grandchild (all requires_success). Child is paused, then
/// the parent fails. The Paused child must be marked Failure (a pause does not shield
/// it), and the failure must cascade recursively to the grandchild. With the fix
/// reverted the cascade skips the Paused child (and thus the grandchild), leaving both
/// stuck Waiting/Paused forever.
#[tokio::test]
async fn test_audit2_a3_cascade_fail_reaches_paused() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let created = create_tasks_ok(
        &app,
        &[
            task_json("parent", "Parent", "a3"),
            task_with_deps("child", "Child", "a3", vec![("parent", true)]),
            task_with_deps("grandchild", "Grandchild", "a3", vec![("child", true)]),
        ],
    )
    .await;
    let parent = created[0].id;
    let child = created[1].id;
    let grandchild = created[2].id;

    assert_eq!(pause_task_http(&app, child).await, StatusCode::OK);
    assert_task_status(&app, child, StatusKind::Paused, "child should be Paused").await;

    // Parent fails -> cascade must reach the Paused child AND recurse to grandchild.
    fail_task(&state, parent, "boom").await;
    assert_task_status(
        &app,
        child,
        StatusKind::Failure,
        "Paused child must be cascade-failed by its required parent",
    )
    .await;
    assert_task_status(
        &app,
        grandchild,
        StatusKind::Failure,
        "failure must propagate recursively through the (formerly Paused) child",
    )
    .await;
}

/// A3 test 3 — resume is contextual: a task with outstanding deps resumes to Waiting,
/// a task with all deps met resumes to Pending.
#[tokio::test]
async fn test_audit2_a3_resume_is_contextual() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    // Two-parent child.
    let created = create_tasks_ok(
        &app,
        &[
            task_json("p1", "P1", "a3"),
            task_json("p2", "P2", "a3"),
            task_with_deps("child", "Child", "a3", vec![("p1", true), ("p2", true)]),
        ],
    )
    .await;
    let p1 = created[0].id;
    let p2 = created[1].id;
    let child = created[2].id;

    assert_eq!(pause_task_http(&app, child).await, StatusCode::OK);

    // Only ONE parent succeeds -> child still has an outstanding dependency.
    succeed_task(&state, p1).await;
    assert_eq!(read_wait_counters(&state.pool, child).await, (1, 1));

    // Resume -> Waiting (deps still outstanding), NOT Pending.
    assert_eq!(resume_task_http(&app, child).await, StatusCode::OK);
    assert_task_status(
        &app,
        child,
        StatusKind::Waiting,
        "resume with outstanding deps must return to Waiting",
    )
    .await;

    // The remaining parent succeeds -> child (now Waiting) transitions to Pending.
    succeed_task(&state, p2).await;
    assert_task_status(
        &app,
        child,
        StatusKind::Pending,
        "child becomes Pending once all deps are met",
    )
    .await;

    // Simple case: pause a Pending task, resume -> Pending.
    let standalone = create_tasks_ok(&app, &[task_json("solo", "Solo", "a3")]).await;
    let solo = standalone[0].id;
    assert_eq!(solo_status(&app, solo).await, StatusKind::Pending);
    assert_eq!(pause_task_http(&app, solo).await, StatusCode::OK);
    assert_task_status(&app, solo, StatusKind::Paused, "solo should be Paused").await;
    assert_eq!(resume_task_http(&app, solo).await, StatusCode::OK);
    assert_task_status(
        &app,
        solo,
        StatusKind::Pending,
        "resumed Pending task returns to Pending",
    )
    .await;
}

/// Read a task's status via GET /task/{id}.
async fn solo_status<S, B>(app: &S, id: uuid::Uuid) -> StatusKind
where
    S: ActixService<ActixRequest, Response = ActixServiceResponse<B>, Error = actix_web::Error>,
    B: ActixMessageBody,
{
    get_task_ok(app, id).await.status
}

/// A3 test 4 — pause/resume refusals: pausing a Running/Claimed/terminal task and
/// resuming a non-Paused task all return 400.
#[tokio::test]
async fn test_audit2_a3_pause_resume_refusals() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let created = create_tasks_ok(
        &app,
        &[
            task_json("running", "Running", "a3"),
            task_json("claimed", "Claimed", "a3"),
            task_json("terminal", "Terminal", "a3"),
            task_json("pending", "Pending", "a3"),
        ],
    )
    .await;
    let running = created[0].id;
    let claimed = created[1].id;
    let terminal = created[2].id;
    let pending = created[3].id;

    // Running -> pause refused.
    {
        let mut conn = state.pool.get().await.unwrap();
        assert!(
            arcrun::db_operation::claim_task(&mut conn, &running)
                .await
                .unwrap()
        );
        assert!(
            arcrun::db_operation::mark_task_running(&mut conn, &running)
                .await
                .unwrap()
        );
    }
    assert_task_status(&app, running, StatusKind::Running, "should be Running").await;
    assert_eq!(
        pause_task_http(&app, running).await,
        StatusCode::BAD_REQUEST,
        "pausing a Running task must be refused (cancel instead)"
    );

    // Claimed -> pause refused.
    force_claimed(&state, claimed).await;
    assert_task_status(&app, claimed, StatusKind::Claimed, "should be Claimed").await;
    assert_eq!(
        pause_task_http(&app, claimed).await,
        StatusCode::BAD_REQUEST,
        "pausing a Claimed task must be refused"
    );

    // Terminal -> pause refused.
    succeed_task(&state, terminal).await;
    assert_task_status(&app, terminal, StatusKind::Success, "should be Success").await;
    assert_eq!(
        pause_task_http(&app, terminal).await,
        StatusCode::BAD_REQUEST,
        "pausing a terminal task must be refused"
    );

    // Resume of a non-Paused (Pending) task -> refused.
    assert_eq!(
        resume_task_http(&app, pending).await,
        StatusCode::BAD_REQUEST,
        "resuming a non-Paused task must be refused"
    );
}

/// A3 test 5 — the `on_batch_complete` signal still fires when the batch's last
/// non-terminal task is a `Paused` task that gets cascade-failed.
///
/// Batch (object form) with `on_batch_complete`: parent -> child (requires_success).
/// The child is paused, then the parent fails, cascade-failing the Paused child. Both
/// tasks are now terminal, so the batch-complete signal (enqueued via the centralized
/// helper on the parent's terminal transition) must be delivered exactly once. With
/// the cascade-fail-reaches-Paused fix reverted, the child stays Paused (non-terminal)
/// and the signal never fires.
#[tokio::test]
async fn test_audit2_a3_batch_complete_after_cascade_fail_of_paused() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let hits = Arc::new(AtomicUsize::new(0));
    let (webhook_url, shutdown_server) = spawn_webhook_server(hits.clone());

    let body = json!({
        "tasks": [
            task_json("parent", "Parent", "a3bc"),
            task_with_deps("child", "Child", "a3bc", vec![("parent", true)])
        ],
        "on_batch_complete": [
            {"kind": "Webhook", "params": {"url": webhook_url, "verb": "Post"}}
        ]
    });
    let req = actix_web::test::TestRequest::post()
        .uri("/task")
        .insert_header(("requester", "test"))
        .set_json(&body)
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::CREATED);
    let batch_id: uuid::Uuid = resp
        .headers()
        .get("X-Batch-ID")
        .unwrap()
        .to_str()
        .unwrap()
        .parse()
        .unwrap();

    // Identify parent (Pending) and child (Waiting) via the DAG.
    let dag_req = actix_web::test::TestRequest::get()
        .uri(&format!("/dag/{}", batch_id))
        .to_request();
    let dag_resp = actix_web::test::call_service(&app, dag_req).await;
    let dag: serde_json::Value = actix_web::test::read_body_json(dag_resp).await;
    let mut parent = None;
    let mut child = None;
    for t in dag["tasks"].as_array().unwrap() {
        let id = uuid::Uuid::parse_str(t["id"].as_str().unwrap()).unwrap();
        match t["name"].as_str().unwrap() {
            "Parent" => parent = Some(id),
            "Child" => child = Some(id),
            _ => {}
        }
    }
    let parent = parent.unwrap();
    let child = child.unwrap();

    // Pause the Waiting child -> the batch now has a Pending parent + a Paused child
    // (no task terminal yet, no signal).
    assert_eq!(pause_task_http(&app, child).await, StatusCode::OK);
    assert_task_status(&app, child, StatusKind::Paused, "child should be Paused").await;

    // Parent fails -> cascade-fails the Paused child; both terminal -> signal enqueued.
    fail_task(&state, parent, "boom").await;
    assert_task_status(
        &app,
        child,
        StatusKind::Failure,
        "Paused child cascade-failed",
    )
    .await;

    drain_outbox(&state).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "on_batch_complete must fire once after the last non-terminal (Paused) task is cascade-failed"
    );

    let _ = shutdown_server.send(());
}

// =============================================================================
// Audit 2, A8 — inter-request dedupe: check-then-act now guarded by an advisory lock
// =============================================================================
//
// # Original bug
// `handle_dedupe` (`src/db/task_crud.rs`) evaluates `dedupe_strategy` with a
// `COUNT(*)` over the committed snapshot, then the caller INSERTs the task later in
// the SAME transaction. There was no lock spanning that check-then-act window, so two
// concurrent `POST /task` requests carrying the same dedupe key both saw `count == 0`
// and both inserted — producing duplicates despite `dedupe_strategy`.
//
// # Fix
// Before running the counts, `handle_dedupe` now takes a `pg_advisory_xact_lock` on a
// stable hash of each applicable matcher (`rule::dedupe_lock_key` = kind + status +
// the matcher's metadata field values). Because it runs inside the `insert_task_batch`
// transaction, the lock is held until COMMIT/ROLLBACK, so a second concurrent request
// with the same key parks on the lock until the first commits, then observes the
// just-inserted row (`count > 0`) and correctly dedupes. Keys are acquired
// sorted+deduped in one round-trip to keep a consistent global lock order.
//
// # What these tests assert
// * `test_audit2_a8_concurrent_same_key_creates_exactly_one` — N concurrent identical
//   dedupe requests create exactly ONE task total. With the fix reverted this races
//   and usually inserts several duplicates.
// * `test_audit2_a8_concurrent_distinct_keys_all_created` — N concurrent requests with
//   DISTINCT dedupe keys all succeed (the lock serializes only same-key requests, it
//   does not over-block distinct keys).
// * The pre-existing `test_dedupe.rs` guards (bug #7) stay green — the guard branches
//   take no lock and preserve the "allow creation" semantics.

/// Count `task` rows of the given kind (the ground truth for "how many were created").
async fn count_tasks_of_kind(pool: &arcrun::DbPool, kind: &str) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Cnt {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        c: i64,
    }
    let mut conn = pool.get().await.unwrap();
    let r: Cnt = diesel_async::RunQueryDsl::get_result(
        diesel::sql_query("SELECT count(*) AS c FROM task WHERE kind = $1")
            .bind::<diesel::sql_types::Text, _>(kind),
        &mut *conn,
    )
    .await
    .unwrap();
    r.c
}

/// Build a single-task `POST /task` body (bare array) carrying a `dedupe_strategy`
/// that matches on `kind` + `status=Pending` + the `unique_key` metadata field.
fn dedupe_task_body(kind: &str, unique_key: &str) -> serde_json::Value {
    json!([{
        "id": "dedupe-race",
        "name": "A8 Dedupe Race",
        "kind": kind,
        "timeout": 60,
        "metadata": {"unique_key": unique_key},
        "on_start": webhook_action(),
        "dedupe_strategy": [{
            "kind": kind,
            "status": "Pending",
            "fields": ["unique_key"]
        }]
    }])
}

/// A8 race test — N concurrent `POST /task` with the SAME dedupe key create exactly
/// one task. The requests are fired concurrently via `join_all` on the shared test
/// service; a pool larger than N ensures each request holds its own connection (so the
/// advisory lock genuinely serializes them rather than the pool doing it by accident).
#[tokio::test]
async fn test_audit2_a8_concurrent_same_key_creates_exactly_one() {
    // Pool comfortably larger than the concurrency so every in-flight request holds
    // its own connection and blocks on the advisory lock, not on connection checkout.
    const N: usize = 16;
    let test_app = setup_test_db_with_pool_size((N as u32) + 8).await;
    let state = create_test_state(test_app.pool.clone());
    let app = test_service!(state);

    let kind = "a8-same-key";
    let futs = (0..N).map(|_| {
        let body = dedupe_task_body(kind, "same-key");
        let app = &app;
        async move {
            let req = actix_web::test::TestRequest::post()
                .uri("/task")
                .insert_header(("requester", "test"))
                .set_json(&body)
                .to_request();
            actix_web::test::call_service(app, req).await.status()
        }
    });
    let statuses = futures_util::future::join_all(futs).await;

    // Every request returned a well-formed response (201 for the single winner, 204
    // No Content for the deduped losers). No 5xx / deadlock error.
    for s in &statuses {
        assert!(
            *s == StatusCode::CREATED || *s == StatusCode::NO_CONTENT,
            "each concurrent request must return 201 or 204, got {s}"
        );
    }

    // Ground truth: exactly one task of this kind exists in the DB.
    assert_eq!(
        count_tasks_of_kind(&state.pool, kind).await,
        1,
        "exactly one task must be created despite {N} concurrent same-key requests"
    );
}

/// A8 no-over-blocking test — N concurrent `POST /task` with DISTINCT dedupe keys all
/// create their task. The advisory lock keyed on (kind,status,field-values) must not
/// serialize away legitimately-distinct tasks (a hash collision may serialize two of
/// them, but every request must still succeed and produce a row).
#[tokio::test]
async fn test_audit2_a8_concurrent_distinct_keys_all_created() {
    const N: usize = 16;
    let test_app = setup_test_db_with_pool_size((N as u32) + 8).await;
    let state = create_test_state(test_app.pool.clone());
    let app = test_service!(state);

    let kind = "a8-distinct-keys";
    let futs = (0..N).map(|i| {
        let body = dedupe_task_body(kind, &format!("key-{i}"));
        let app = &app;
        async move {
            let req = actix_web::test::TestRequest::post()
                .uri("/task")
                .insert_header(("requester", "test"))
                .set_json(&body)
                .to_request();
            actix_web::test::call_service(app, req).await.status()
        }
    });
    let statuses = futures_util::future::join_all(futs).await;

    // Distinct keys never dedupe -> every request must be a 201 Created.
    for s in &statuses {
        assert_eq!(
            *s,
            StatusCode::CREATED,
            "distinct-key requests must all create a task (got {s})"
        );
    }

    assert_eq!(
        count_tasks_of_kind(&state.pool, kind).await,
        N as i64,
        "all {N} distinct-key tasks must be created (lock must not over-serialize)"
    );
}
