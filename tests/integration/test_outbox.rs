//! Lot 2 — transactional webhook outbox tests.
//!
//! End/cancel webhooks are enqueued into the `webhook_execution` outbox inside the
//! status-change transaction and delivered asynchronously by the delivery loop.
//! These tests drive delivery deterministically via `run_delivery_once`
//! (exposed as `drain_outbox*` helpers) instead of waiting on the worker timer.

use crate::common::*;

use arcrun::models::{TriggerCondition, TriggerKind};
use arcrun::workers::DeliveryConfig;
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

/// Helper: count outbox rows for a task with a given status (raw SQL).
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

/// Crash window: a `pending` outbox row enqueued in-tx survives (we DON'T run the
/// delivery loop right away), then is delivered when the loop finally runs.
#[tokio::test]
async fn test_outbox_crash_window_delivers_on_restart() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let hits = Arc::new(AtomicUsize::new(0));
    let (webhook_url, shutdown_server) = spawn_webhook_server(hits.clone());

    let task_payload = json!({
        "id": "crash-window",
        "name": "Crash Window",
        "kind": "outbox-test",
        "timeout": 60,
        "metadata": {"test": true},
        "on_start": webhook_action(),
        "on_success": [{"kind": "Webhook", "params": {"url": webhook_url, "verb": "Post"}}]
    });

    let created = create_tasks_ok(&app, &[task_payload]).await;
    let task_id = created[0].id;

    // Transition Success: enqueues the end:success outbox row. Simulate a crash by
    // NOT running the delivery loop yet.
    succeed_task(&state, task_id).await;

    assert_eq!(
        outbox_count(&state.pool, task_id, "pending").await,
        1,
        "an end:success outbox row should be pending after the transition"
    );
    assert_eq!(
        hits.load(Ordering::SeqCst),
        0,
        "no delivery yet (crash window)"
    );

    // Restart: run the delivery loop. The pending row is delivered.
    drain_outbox(&state).await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "webhook delivered after restart"
    );
    assert_eq!(
        outbox_count(&state.pool, task_id, "success").await,
        1,
        "outbox row should be marked success"
    );

    let _ = shutdown_server.send(());
}

/// Retry after a downstream failure: a mock that fails once then succeeds is
/// eventually delivered, with attempts > 1.
#[tokio::test]
async fn test_outbox_retry_then_success() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let hits = Arc::new(AtomicUsize::new(0));
    // Fail the first delivery attempt, succeed on the second.
    let (webhook_url, shutdown_server) = spawn_flaky_webhook_server(hits.clone(), 1);

    let task_payload = json!({
        "id": "retry-success",
        "name": "Retry Success",
        "kind": "outbox-test",
        "timeout": 60,
        "metadata": {"test": true},
        "on_start": webhook_action(),
        "on_success": [{"kind": "Webhook", "params": {"url": webhook_url, "verb": "Post"}}]
    });

    let created = create_tasks_ok(&app, &[task_payload]).await;
    let task_id = created[0].id;

    succeed_task(&state, task_id).await;

    // Backoff base 0 so the retry is immediately mature again (next_attempt_at = now()).
    let cfg = DeliveryConfig {
        batch_size: 100,
        max_attempts: 10,
        backoff_base_secs: 1,
        backoff_cap_secs: 1,
    };

    // First pass: delivery fails (500), row stays pending with attempts incremented.
    drain_outbox_with(&state, cfg, 1).await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert_eq!(
        outbox_count(&state.pool, task_id, "pending").await,
        1,
        "row should still be pending after first (failed) delivery"
    );

    // The retry was scheduled 1s out; wait for it to mature, then deliver again.
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    drain_outbox_with(&state, cfg, 5).await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    assert!(
        hits.load(Ordering::SeqCst) >= 2,
        "webhook should have been attempted at least twice, got {}",
        hits.load(Ordering::SeqCst)
    );
    assert_eq!(
        outbox_count(&state.pool, task_id, "success").await,
        1,
        "row should be delivered (success) after retry"
    );

    let _ = shutdown_server.send(());
}

/// No double delivery: running the delivery loop twice delivers the webhook exactly
/// once (the row is marked success and never re-sent).
#[tokio::test]
async fn test_outbox_no_double_delivery() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let hits = Arc::new(AtomicUsize::new(0));
    let (webhook_url, shutdown_server) = spawn_webhook_server(hits.clone());

    let task_payload = json!({
        "id": "no-double",
        "name": "No Double Delivery",
        "kind": "outbox-test",
        "timeout": 60,
        "metadata": {"test": true},
        "on_start": webhook_action(),
        "on_success": [{"kind": "Webhook", "params": {"url": webhook_url, "verb": "Post"}}]
    });

    let created = create_tasks_ok(&app, &[task_payload]).await;
    let task_id = created[0].id;

    succeed_task(&state, task_id).await;

    // Two full drains.
    drain_outbox(&state).await;
    drain_outbox(&state).await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "webhook should be delivered exactly once across two delivery passes"
    );

    let _ = shutdown_server.send(());
}

/// Per-task ordering: an `end` outbox row is NOT delivered while a `start` row for
/// the same task is still `pending`. Once the start row leaves pending, the end row
/// is delivered.
#[tokio::test]
async fn test_outbox_start_before_end_ordering() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let hits = Arc::new(AtomicUsize::new(0));
    let (webhook_url, shutdown_server) = spawn_webhook_server(hits.clone());

    let task_payload = json!({
        "id": "order-task",
        "name": "Ordering Task",
        "kind": "outbox-test",
        "timeout": 60,
        "metadata": {"test": true},
        "on_start": webhook_action(),
        "on_success": [{"kind": "Webhook", "params": {"url": webhook_url, "verb": "Post"}}]
    });

    let created = create_tasks_ok(&app, &[task_payload]).await;
    let task_id = created[0].id;

    let mut conn = state.pool.get().await.unwrap();

    // Leave a `start` row in `pending` (claimed but never completed — simulates the
    // on_start webhook being in flight / failed).
    let start_key =
        arcrun::action::idempotency_key(task_id, &TriggerKind::Start, &TriggerCondition::Success);
    let claimed = arcrun::db_operation::try_claim_webhook_execution(
        &mut conn,
        task_id,
        TriggerKind::Start,
        TriggerCondition::Success,
        &start_key,
        None,
    )
    .await
    .unwrap();
    assert!(claimed, "start row should be claimed (pending)");
    drop(conn);

    // Transition Success -> enqueues the end:success outbox row.
    succeed_task(&state, task_id).await;

    // Delivery loop must NOT deliver the end row while the start row is pending.
    let processed = drain_outbox(&state).await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(
        processed, 0,
        "end row must not be delivered while start row is pending"
    );
    assert_eq!(hits.load(Ordering::SeqCst), 0, "no webhook fired yet");

    // Complete the start row (success). Now the end row is deliverable.
    let mut conn = state.pool.get().await.unwrap();
    arcrun::db_operation::complete_webhook_execution(&mut conn, &start_key, true)
        .await
        .unwrap();
    drop(conn);

    drain_outbox(&state).await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "end webhook delivered once start row is no longer pending"
    );

    let _ = shutdown_server.send(());
}

/// Exhausted: a mock that always fails, with a low max_attempts, ends up `exhausted`
/// and is visible via GET /webhook-deliveries?status=exhausted.
#[tokio::test]
async fn test_outbox_exhausted_visible_via_endpoint() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let (webhook_url, shutdown_server) = spawn_500_webhook_server();

    let task_payload = json!({
        "id": "exhaust-task",
        "name": "Exhaust Task",
        "kind": "outbox-test",
        "timeout": 60,
        "metadata": {"test": true},
        "on_start": webhook_action(),
        "on_success": [{"kind": "Webhook", "params": {"url": webhook_url, "verb": "Post"}}]
    });

    let created = create_tasks_ok(&app, &[task_payload]).await;
    let task_id = created[0].id;

    succeed_task(&state, task_id).await;

    // max_attempts = 2, backoff = 1s. Deliver, wait, deliver -> exhausted.
    let cfg = DeliveryConfig {
        batch_size: 100,
        max_attempts: 2,
        backoff_base_secs: 1,
        backoff_cap_secs: 1,
    };

    drain_outbox_with(&state, cfg, 1).await; // attempt 1 -> fail, retry scheduled
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    drain_outbox_with(&state, cfg, 1).await; // attempt 2 -> exhausted
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    assert_eq!(
        outbox_count(&state.pool, task_id, "exhausted").await,
        1,
        "row should be exhausted after max_attempts failures"
    );

    // Visible via the endpoint (case-insensitive status filter).
    let req = actix_web::test::TestRequest::get()
        .uri("/webhook-deliveries?status=exhausted")
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::OK);
    let list: Vec<serde_json::Value> = actix_web::test::read_body_json(resp).await;
    assert_eq!(list.len(), 1, "exactly one exhausted delivery expected");
    assert_eq!(list[0]["task_id"], task_id.to_string());
    assert_eq!(list[0]["status"], "Exhausted");
    assert!(
        list[0]["last_error"].is_string(),
        "exhausted row should carry last_error"
    );

    let _ = shutdown_server.send(());
}

/// PATCH responds fast even when the downstream consumer is slow: the on_success
/// webhook points at a server that sleeps 2s, but the PATCH (status transition)
/// returns in well under 2s because delivery is async (outbox), not inline.
#[tokio::test]
async fn test_patch_fast_even_when_downstream_slow() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let hits = Arc::new(AtomicUsize::new(0));
    let (webhook_url, shutdown_server) =
        spawn_slow_200_webhook_server(hits.clone(), std::time::Duration::from_secs(2));

    let task_payload = json!({
        "id": "fast-patch",
        "name": "Fast Patch",
        "kind": "outbox-test",
        "timeout": 60,
        "metadata": {"test": true},
        "on_start": webhook_action(),
        "on_success": [{"kind": "Webhook", "params": {"url": webhook_url, "verb": "Post"}}]
    });

    let created = create_tasks_ok(&app, &[task_payload]).await;
    let task_id = created[0].id;

    // Move to Running first.
    let mut conn = state.pool.get().await.unwrap();
    arcrun::db_operation::claim_task(&mut conn, &task_id)
        .await
        .unwrap();
    arcrun::db_operation::mark_task_running(&mut conn, &task_id)
        .await
        .unwrap();
    drop(conn);

    // PATCH the task to Success and time it.
    let start = std::time::Instant::now();
    let req = actix_web::test::TestRequest::patch()
        .uri(&format!("/task/{}", task_id))
        .set_json(&json!({"status": "Success"}))
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    let elapsed = start.elapsed();
    assert!(resp.status().is_success(), "PATCH should succeed");

    assert!(
        elapsed < std::time::Duration::from_millis(1000),
        "PATCH should return fast (< 1s) despite a 2s downstream webhook; took {:?}",
        elapsed
    );
    assert_eq!(
        hits.load(Ordering::SeqCst),
        0,
        "webhook should NOT have been called synchronously during PATCH"
    );

    // The notification is in the outbox and deliverable.
    assert_eq!(outbox_count(&state.pool, task_id, "pending").await, 1);

    let _ = shutdown_server.send(());
}
