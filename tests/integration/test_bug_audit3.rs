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

use arcrun::error::ArcRunError;
use diesel_async::RunQueryDsl;
use std::time::Duration;

#[derive(diesel::QueryableByName)]
struct ProbeId {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    id: i32,
}

/// Create the scratch table used to observe writes.
async fn create_probe_table(pool: &arcrun::DbPool) {
    let mut conn = pool.get().await.unwrap();
    diesel::sql_query("CREATE TABLE tx_probe (id INT PRIMARY KEY)")
        .execute(&mut conn)
        .await
        .expect("create probe table");
}

/// Return the committed `tx_probe` ids as seen through `pool` (sorted).
async fn probe_ids(pool: &arcrun::DbPool) -> Vec<i32> {
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
