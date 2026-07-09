use crate::common::*;

use arcrun::models::StatusKind;
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Spawn a mock TCP server that responds 200 OK after a configurable delay.
/// Counts incoming requests via `hits`.
fn spawn_delayed_webhook_server(
    hits: Arc<AtomicUsize>,
    delay: std::time::Duration,
) -> (String, tokio::sync::oneshot::Sender<()>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    listener.set_nonblocking(true).unwrap();
    let listener = tokio::net::TcpListener::from_std(listener).unwrap();

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    tokio::spawn(async move {
        loop {
            tokio::select! {
                result = listener.accept() => {
                    if let Ok((mut stream, _)) = result {
                        let hits = hits.clone();
                        tokio::spawn(async move {
                            let mut buf = [0u8; 4096];
                            let _ = stream.read(&mut buf).await;
                            hits.fetch_add(1, Ordering::SeqCst);
                            tokio::time::sleep(delay).await;
                            let response = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
                            let _ = stream.write_all(response.as_bytes()).await;
                        });
                    }
                }
                _ = &mut shutdown_rx => break,
            }
        }
    });

    (format!("http://{}/webhook", addr), shutdown_tx)
}

/// Test that multiple tasks in one batch are all started by the parallel webhook phase.
///
/// Creates 3 independent Pending tasks with a working webhook, runs start_loop once,
/// and verifies all 3 reach Running.
#[tokio::test]
async fn test_parallel_webhooks_all_tasks_started() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let hits = Arc::new(AtomicUsize::new(0));
    let (webhook_url, shutdown_server) = spawn_webhook_server(hits.clone());

    let tasks: Vec<serde_json::Value> = (0..3)
        .map(|i| {
            json!({
                "id": format!("par-{}", i),
                "name": format!("Parallel Task {}", i),
                "kind": "parallel-test",
                "timeout": 60,
                "metadata": {"test": true},
                "on_start": {
                    "kind": "Webhook",
                    "params": { "url": &webhook_url, "verb": "Post" }
                }
            })
        })
        .collect();

    let created = create_tasks_ok(&app, &tasks).await;
    assert_eq!(created.len(), 3);
    let ids: Vec<uuid::Uuid> = created.iter().map(|t| t.id).collect();

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

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let _ = shutdown_tx.send(true);
    let _ = handle.await;
    let _ = shutdown_server.send(());

    // All 3 tasks should be Running
    for (i, id) in ids.iter().enumerate() {
        assert_task_status(
            &app,
            *id,
            StatusKind::Running,
            &format!("task {} should be Running after parallel start_loop", i),
        )
        .await;
    }

    assert_eq!(
        hits.load(Ordering::SeqCst),
        3,
        "on_start webhook should be called once per task"
    );
}

/// Test that a failing webhook for one task does not block or affect other tasks.
///
/// Creates 2 tasks: one pointing to a 500 server, one to a 200 server.
/// After start_loop, the first should be Failure, the second Running.
#[tokio::test]
async fn test_parallel_webhooks_failure_isolation() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let good_hits = Arc::new(AtomicUsize::new(0));
    let (good_url, good_shutdown) = spawn_webhook_server(good_hits.clone());
    let (bad_url, bad_shutdown) = spawn_500_webhook_server();

    let tasks = vec![
        json!({
            "id": "fail-iso-bad",
            "name": "Bad Webhook Task",
            "kind": "iso-test",
            "timeout": 60,
            "metadata": {"test": true},
            "on_start": {
                "kind": "Webhook",
                "params": { "url": &bad_url, "verb": "Post" }
            }
        }),
        json!({
            "id": "fail-iso-good",
            "name": "Good Webhook Task",
            "kind": "iso-test",
            "timeout": 60,
            "metadata": {"test": true},
            "on_start": {
                "kind": "Webhook",
                "params": { "url": &good_url, "verb": "Post" }
            }
        }),
    ];

    let created = create_tasks_ok(&app, &tasks).await;
    let bad_id = created[0].id;
    let good_id = created[1].id;

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

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let _ = shutdown_tx.send(true);
    let _ = handle.await;
    let _ = good_shutdown.send(());
    let _ = bad_shutdown.send(());

    let bad_task = get_task_ok(&app, bad_id).await;
    assert_eq!(
        bad_task.status,
        StatusKind::Failure,
        "task with 500 webhook should be Failed"
    );

    let good_task = get_task_ok(&app, good_id).await;
    assert_eq!(
        good_task.status,
        StatusKind::Running,
        "task with 200 webhook should be Running despite sibling failure"
    );
}

/// Test that a slow webhook for one task does not delay other tasks.
///
/// Creates 2 tasks: one with a 2s delay webhook, one with instant webhook.
/// With webhook_concurrency=10, the fast task should reach Running well before
/// the slow webhook completes.
#[tokio::test]
async fn test_parallel_webhooks_slow_does_not_block_fast() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let fast_hits = Arc::new(AtomicUsize::new(0));
    let (fast_url, fast_shutdown) = spawn_webhook_server(fast_hits.clone());

    let slow_hits = Arc::new(AtomicUsize::new(0));
    let (slow_url, slow_shutdown) =
        spawn_delayed_webhook_server(slow_hits.clone(), std::time::Duration::from_secs(2));

    let tasks = vec![
        json!({
            "id": "slow-first",
            "name": "Slow Webhook Task",
            "kind": "slow-test",
            "timeout": 60,
            "metadata": {"test": true},
            "on_start": {
                "kind": "Webhook",
                "params": { "url": &slow_url, "verb": "Post" }
            }
        }),
        json!({
            "id": "fast-second",
            "name": "Fast Webhook Task",
            "kind": "slow-test",
            "timeout": 60,
            "metadata": {"test": true},
            "on_start": {
                "kind": "Webhook",
                "params": { "url": &fast_url, "verb": "Post" }
            }
        }),
    ];

    let created = create_tasks_ok(&app, &tasks).await;
    let slow_id = created[0].id;
    let fast_id = created[1].id;

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

    // Wait less than the slow webhook's delay but enough for the fast one
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // The fast task should already be Running even though slow hasn't finished
    let fast_task = get_task_ok(&app, fast_id).await;
    assert_eq!(
        fast_task.status,
        StatusKind::Running,
        "fast task should be Running while slow webhook is still in flight"
    );

    // Wait for the slow webhook to complete
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let _ = shutdown_tx.send(true);
    let _ = handle.await;
    let _ = fast_shutdown.send(());
    let _ = slow_shutdown.send(());

    // Now both should be Running
    assert_task_status(
        &app,
        slow_id,
        StatusKind::Running,
        "slow task should also reach Running eventually",
    )
    .await;
}

/// Test that webhook_concurrency=1 still processes all tasks (sequentially).
///
/// Creates 2 tasks, runs start_loop with concurrency=1. Both should reach Running.
#[tokio::test]
async fn test_webhook_concurrency_one_still_works() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let hits = Arc::new(AtomicUsize::new(0));
    let (webhook_url, shutdown_server) = spawn_webhook_server(hits.clone());

    let tasks = vec![
        json!({
            "id": "seq-a",
            "name": "Sequential A",
            "kind": "seq-test",
            "timeout": 60,
            "metadata": {"test": true},
            "on_start": {
                "kind": "Webhook",
                "params": { "url": &webhook_url, "verb": "Post" }
            }
        }),
        json!({
            "id": "seq-b",
            "name": "Sequential B",
            "kind": "seq-test",
            "timeout": 60,
            "metadata": {"test": true},
            "on_start": {
                "kind": "Webhook",
                "params": { "url": &webhook_url, "verb": "Post" }
            }
        }),
    ];

    let created = create_tasks_ok(&app, &tasks).await;
    let id_a = created[0].id;
    let id_b = created[1].id;

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
            1, // concurrency = 1
            shutdown_rx,
            arcrun::workers::WorkerNudges::new(),
        )
        .await;
    });

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let _ = shutdown_tx.send(true);
    let _ = handle.await;
    let _ = shutdown_server.send(());

    assert_task_status(
        &app,
        id_a,
        StatusKind::Running,
        "task A should be Running with concurrency=1",
    )
    .await;

    assert_task_status(
        &app,
        id_b,
        StatusKind::Running,
        "task B should be Running with concurrency=1",
    )
    .await;

    assert_eq!(
        hits.load(Ordering::SeqCst),
        2,
        "both webhooks should have been called"
    );
}

/// Test that parallel failure propagation works correctly.
///
/// Creates 2 independent parent tasks (both with failing webhooks) that each have
/// a child with requires_success=true. After start_loop, both parents should be
/// Failed AND both children should be cascade-Failed — proving that parallel
/// fail_task_and_propagate calls don't interfere with each other.
#[tokio::test]
async fn test_parallel_failure_propagation_isolation() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let (bad_url, bad_shutdown) = spawn_500_webhook_server();

    let tasks = vec![
        json!({
            "id": "pfp-parent-a",
            "name": "Fail Parent A",
            "kind": "pfp-test",
            "timeout": 60,
            "metadata": {"test": true},
            "on_start": {
                "kind": "Webhook",
                "params": { "url": &bad_url, "verb": "Post" }
            }
        }),
        json!({
            "id": "pfp-child-a",
            "name": "Child A",
            "kind": "pfp-test",
            "timeout": 60,
            "metadata": {"test": true},
            "on_start": webhook_action(),
            "dependencies": [{ "id": "pfp-parent-a", "requires_success": true }]
        }),
        json!({
            "id": "pfp-parent-b",
            "name": "Fail Parent B",
            "kind": "pfp-test",
            "timeout": 60,
            "metadata": {"test": true},
            "on_start": {
                "kind": "Webhook",
                "params": { "url": &bad_url, "verb": "Post" }
            }
        }),
        json!({
            "id": "pfp-child-b",
            "name": "Child B",
            "kind": "pfp-test",
            "timeout": 60,
            "metadata": {"test": true},
            "on_start": webhook_action(),
            "dependencies": [{ "id": "pfp-parent-b", "requires_success": true }]
        }),
    ];

    let created = create_tasks_ok(&app, &tasks).await;
    let parent_a = created[0].id;
    let child_a = created[1].id;
    let parent_b = created[2].id;
    let child_b = created[3].id;

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

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let _ = shutdown_tx.send(true);
    let _ = handle.await;
    let _ = bad_shutdown.send(());

    // Both parents should be Failed
    assert_task_status(&app, parent_a, StatusKind::Failure, "parent A should fail").await;
    assert_task_status(&app, parent_b, StatusKind::Failure, "parent B should fail").await;

    // Both children should be cascade-Failed
    let ca = get_task_ok(&app, child_a).await;
    assert_eq!(
        ca.status,
        StatusKind::Failure,
        "child A should be cascade-Failed from parent A"
    );

    let cb = get_task_ok(&app, child_b).await;
    assert_eq!(
        cb.status,
        StatusKind::Failure,
        "child B should be cascade-Failed from parent B"
    );
}
