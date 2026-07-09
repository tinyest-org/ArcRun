use crate::common::*;

use arcrun::dtos::BasicTaskDto;
use arcrun::models::StatusKind;
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[tokio::test]
async fn test_start_loop_marks_task_failed_on_start_webhook_error() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let task_payload = json!({
        "id": "start-fail",
        "name": "Start Fail Task",
        "kind": "start-fail-kind",
        "timeout": 60,
        "metadata": {"test": true},
        "on_start": {
            "kind": "Webhook",
            "params": {
                "url": "http://127.0.0.1:1/fail",
                "verb": "Post"
            }
        }
    });

    let created = create_tasks_ok(&app, &[task_payload]).await;
    let task_id = created[0].id;

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let evaluator = state.action_executor.clone();
    let pool = state.pool.clone();
    let handle = tokio::spawn(async move {
        arcrun::workers::start_loop(
            &evaluator,
            pool,
            std::time::Duration::from_millis(50),
            true,
            50,
            10,
            shutdown_rx,
            arcrun::workers::WorkerNudges::new(),
        )
        .await;
    });

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let _ = shutdown_tx.send(true);
    let _ = handle.await;

    let updated = get_task_ok(&app, task_id).await;
    assert_eq!(
        updated.status,
        StatusKind::Failure,
        "on_start failure should mark task as Failure"
    );
    assert!(
        updated
            .failure_reason
            .as_deref()
            .unwrap_or("")
            .contains("on_start"),
        "failure_reason should mention on_start"
    );
}

#[tokio::test]
async fn test_timeout_loop_fires_on_failure_webhook() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let hits = Arc::new(AtomicUsize::new(0));
    let (webhook_url, shutdown_server) = spawn_webhook_server(hits.clone());

    let on_failure = json!({
        "kind": "Webhook",
        "params": {
            "url": webhook_url,
            "verb": "Post"
        }
    });

    let task_payload = json!({
        "id": "timeout-fail",
        "name": "Timeout Failure Task",
        "kind": "timeout-kind",
        "timeout": 1,
        "metadata": {"test": true},
        "on_start": webhook_action(),
        "on_failure": [on_failure]
    });

    let created = create_tasks_ok(&app, &[task_payload]).await;
    let task_id = created[0].id;

    let mut conn = state.pool.get().await.unwrap();
    use diesel::sql_query;
    use diesel::sql_types::{Timestamptz, Uuid as SqlUuid};

    let past = chrono::Utc::now() - chrono::Duration::seconds(120);
    {
        use diesel_async::RunQueryDsl;
        sql_query(
            "UPDATE task \
             SET status = 'running', started_at = $1, last_updated = $2 \
             WHERE id = $3",
        )
        .bind::<Timestamptz, _>(past)
        .bind::<Timestamptz, _>(past)
        .bind::<SqlUuid, _>(task_id)
        .execute(&mut conn)
        .await
        .unwrap();
    }

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let pool = state.pool.clone();
    let handle = tokio::spawn(async move {
        arcrun::workers::timeout_loop(
            pool,
            std::time::Duration::from_millis(50),
            std::time::Duration::from_secs(30),
            true,
            shutdown_rx,
            arcrun::workers::WorkerNudges::new(),
        )
        .await;
    });

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let _ = shutdown_tx.send(true);
    let _ = handle.await;

    let updated = get_task_ok(&app, task_id).await;
    assert_eq!(updated.status, StatusKind::Failure, "task should be failed");

    // The timeout loop enqueued the on_failure notification into the outbox;
    // drive the delivery loop to send it.
    drain_outbox(&state).await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let hit_count = hits.load(Ordering::SeqCst);
    assert_eq!(
        hit_count, 1,
        "on_failure webhook should be called exactly once"
    );

    let _ = shutdown_server.send(());
}

#[tokio::test]
async fn test_pagination_large_page_does_not_overflow() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let tasks = vec![task_json("page-overflow", "Page Overflow", "page-test")];
    create_tasks_ok(&app, &tasks).await;

    let req = actix_web::test::TestRequest::get()
        .uri(&format!("/task?page={}&page_size=100", i64::MAX))
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::OK);

    let list: Vec<BasicTaskDto> = actix_web::test::read_body_json(resp).await;
    assert!(
        list.is_empty(),
        "Huge page should return an empty list (no overflow)"
    );
}

/// Regression test: batch counter updates (failures/successes via PUT) must
/// refresh `last_updated`, which the timeout loop uses. If the timeout query
/// checks `started_at` instead of `last_updated`, a task that is actively
/// receiving updates will be incorrectly timed out.
///
/// Setup: task with timeout=2s, started 120s ago (would timeout based on started_at),
/// but last_updated is recent (just got a failure increment).
/// Expected: timeout loop does NOT mark it as failed.
#[tokio::test]
async fn test_recent_batch_update_prevents_timeout() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let task_payload = json!({
        "id": "batch-keepalive",
        "name": "Batch Keepalive Task",
        "kind": "batch-kind",
        "timeout": 2,
        "metadata": {"test": true},
        "on_start": webhook_action()
    });

    let created = create_tasks_ok(&app, &[task_payload]).await;
    let task_id = created[0].id;

    // Force task to Running with started_at far in the past but last_updated = now
    let mut conn = state.pool.get().await.unwrap();
    use diesel::sql_query;
    use diesel::sql_types::{Timestamptz, Uuid as SqlUuid};
    {
        let past = chrono::Utc::now() - chrono::Duration::seconds(120);
        use diesel_async::RunQueryDsl;
        sql_query(
            "UPDATE task \
             SET status = 'running', started_at = $1, last_updated = now() \
             WHERE id = $2",
        )
        .bind::<Timestamptz, _>(past)
        .bind::<SqlUuid, _>(task_id)
        .execute(&mut conn)
        .await
        .unwrap();
    }

    // Run timeout loop — it should NOT timeout this task because last_updated is fresh
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let pool = state.pool.clone();
    let handle = tokio::spawn(async move {
        arcrun::workers::timeout_loop(
            pool,
            std::time::Duration::from_millis(50),
            std::time::Duration::from_secs(30),
            true,
            shutdown_rx,
            arcrun::workers::WorkerNudges::new(),
        )
        .await;
    });

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let _ = shutdown_tx.send(true);
    let _ = handle.await;

    let updated = get_task_ok(&app, task_id).await;
    assert_eq!(
        updated.status,
        StatusKind::Running,
        "Task with recent last_updated should NOT be timed out (bug: timeout used started_at instead of last_updated)"
    );
}

/// Timeout propagation: when a parent times out, its children with
/// requires_success=true should be marked as Failure recursively.
#[tokio::test]
async fn test_timeout_propagates_failure_to_children() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let tasks = vec![
        task_json("timeout-parent", "Timeout Parent", "timeout-prop"),
        task_with_deps(
            "timeout-child",
            "Timeout Child",
            "timeout-prop",
            vec![("timeout-parent", true)],
        ),
    ];

    let created = create_tasks_ok(&app, &tasks).await;
    let parent_id = created[0].id;
    let child_id = created[1].id;

    assert_eq!(created[0].status, StatusKind::Pending);
    assert_eq!(created[1].status, StatusKind::Waiting);

    // Force parent to Running with last_updated far in the past (simulates timeout)
    let mut conn = state.pool.get().await.unwrap();
    {
        use diesel::sql_query;
        use diesel::sql_types::{Timestamptz, Uuid as SqlUuid};
        use diesel_async::RunQueryDsl;

        let past = chrono::Utc::now() - chrono::Duration::seconds(120);
        sql_query(
            "UPDATE task \
             SET status = 'running', started_at = $1, last_updated = $2 \
             WHERE id = $3",
        )
        .bind::<Timestamptz, _>(past)
        .bind::<Timestamptz, _>(past)
        .bind::<SqlUuid, _>(parent_id)
        .execute(&mut conn)
        .await
        .unwrap();
    }

    // Run timeout loop
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let pool = state.pool.clone();
    let handle = tokio::spawn(async move {
        arcrun::workers::timeout_loop(
            pool,
            std::time::Duration::from_millis(50),
            std::time::Duration::from_secs(30),
            true,
            shutdown_rx,
            arcrun::workers::WorkerNudges::new(),
        )
        .await;
    });

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let _ = shutdown_tx.send(true);
    let _ = handle.await;

    assert_task_status(
        &app,
        parent_id,
        StatusKind::Failure,
        "Parent should be failed by timeout",
    )
    .await;

    let child = get_task_ok(&app, child_id).await;
    assert_eq!(
        child.status,
        StatusKind::Failure,
        "Child with requires_success=true should fail when parent times out"
    );
    assert!(
        child.failure_reason.is_some(),
        "Child should have a failure reason"
    );
}
