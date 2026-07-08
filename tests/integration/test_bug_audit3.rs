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
