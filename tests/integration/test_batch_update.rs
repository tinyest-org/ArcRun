use crate::common::*;

use arcrun::dtos::TaskDto;
use serde_json::json;

#[tokio::test]
async fn test_batch_update_increments_counters() {
    let (_g, test_state) = setup_test_app_with_batch_updater().await;
    let state = test_state.state;
    let app = test_service!(state);

    let tasks = vec![task_json("batch-test", "Batch Update Test", "batch-test")];
    let created = create_tasks_ok(&app, &tasks).await;
    let task_id = created[0].id;

    assert_eq!(created[0].success, 0);
    assert_eq!(created[0].failures, 0);

    // Send batch update via PUT endpoint
    let update_req = actix_web::test::TestRequest::put()
        .uri(&format!("/task/{}", task_id))
        .set_json(&json!({"new_success": 5, "new_failures": 2}))
        .to_request();
    let update_resp = actix_web::test::call_service(&app, update_req).await;
    assert_eq!(
        update_resp.status(),
        actix_web::http::StatusCode::ACCEPTED,
        "Batch update should be accepted"
    );

    // Wait for batch updater to process (runs every 100ms)
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;

    let updated: TaskDto = get_task_ok(&app, task_id).await;
    assert_eq!(
        updated.success, 5,
        "Success counter should be incremented to 5"
    );
    assert_eq!(
        updated.failures, 2,
        "Failures counter should be incremented to 2"
    );
}

#[tokio::test]
async fn test_batch_update_accumulates_multiple_updates() {
    let (_g, test_state) = setup_test_app_with_batch_updater().await;
    let state = test_state.state;
    let app = test_service!(state);

    let tasks = vec![task_json("batch-multi", "Batch Multi Test", "batch-test")];
    let created = create_tasks_ok(&app, &tasks).await;
    let task_id = created[0].id;

    // Send multiple rapid updates
    for _ in 0..10 {
        let update_req = actix_web::test::TestRequest::put()
            .uri(&format!("/task/{}", task_id))
            .set_json(&json!({"new_success": 1, "new_failures": 0}))
            .to_request();
        actix_web::test::call_service(&app, update_req).await;
    }

    // Wait for batch updater to process
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;

    let updated: TaskDto = get_task_ok(&app, task_id).await;
    assert_eq!(
        updated.success, 10,
        "Success counter should accumulate to 10 from 10 updates of +1 each"
    );
}

#[tokio::test]
async fn test_batch_update_rejects_zero_counters() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let tasks = vec![task_json("batch-reject", "Batch Reject Test", "batch-test")];
    let created = create_tasks_ok(&app, &tasks).await;
    let task_id = created[0].id;

    // Try to send update with zero counters
    let update_req = actix_web::test::TestRequest::put()
        .uri(&format!("/task/{}", task_id))
        .set_json(&json!({"new_success": 0, "new_failures": 0}))
        .to_request();
    let update_resp = actix_web::test::call_service(&app, update_req).await;

    assert_eq!(
        update_resp.status(),
        actix_web::http::StatusCode::BAD_REQUEST,
        "Should reject updates with all zero counters"
    );
}

/// Terminal tasks have FROZEN counters — a late batch flush must NOT mutate them.
///
/// Contract change (audit 2, A7): the batch-updater flush is now gated by
/// `AND task.status NOT IN ('success','failure','canceled')`. A PUT whose counts
/// land after the task became terminal (Success/Canceled) is silently dropped,
/// because a terminal task's counters are immutable and were already delivered
/// with its end notification — re-applying them would diverge from what consumers
/// observed. This intentionally reverses commit 2e50620 ("ensure we don't skip
/// the last updates"), which had removed the guard so racing counts still landed;
/// audit A7 re-decided the trade-off in favor of terminal immutability. The PUT
/// is still accepted (202) so callers racing the transition are not surprised by
/// an error; the counts simply have no effect once the task is terminal.
#[tokio::test]
async fn test_batch_update_dropped_on_terminal_tasks() {
    let (_g, test_state) = setup_test_app_with_batch_updater().await;
    let state = test_state.state;
    let app = test_service!(state);

    // Success case: counters must NOT change after the task is terminal.
    let tasks = vec![task_json(
        "terminal-success",
        "Terminal Success",
        "batch-test",
    )];
    let created = create_tasks_ok(&app, &tasks).await;
    let task_id = created[0].id;

    succeed_task(&state, task_id).await;
    let before: TaskDto = get_task_ok(&app, task_id).await;

    let update_req = actix_web::test::TestRequest::put()
        .uri(&format!("/task/{}", task_id))
        .set_json(&json!({"new_success": 3, "new_failures": 1}))
        .to_request();
    let update_resp = actix_web::test::call_service(&app, update_req).await;
    assert_eq!(
        update_resp.status(),
        actix_web::http::StatusCode::ACCEPTED,
        "Batch update is still accepted even if task is terminal"
    );

    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    let updated: TaskDto = get_task_ok(&app, task_id).await;
    assert_eq!(
        updated.success, before.success,
        "Success counter must NOT change on a terminal (Success) task"
    );
    assert_eq!(
        updated.failures, before.failures,
        "Failure counter must NOT change on a terminal (Success) task"
    );

    // Canceled case: counters must NOT change after cancellation.
    let tasks = vec![task_json(
        "terminal-canceled",
        "Terminal Canceled",
        "batch-test",
    )];
    let created = create_tasks_ok(&app, &tasks).await;
    let cancel_id = created[0].id;

    let cancel_req = actix_web::test::TestRequest::delete()
        .uri(&format!("/task/{}", cancel_id))
        .to_request();
    let cancel_resp = actix_web::test::call_service(&app, cancel_req).await;
    assert!(cancel_resp.status().is_success());
    let before: TaskDto = get_task_ok(&app, cancel_id).await;

    let update_req = actix_web::test::TestRequest::put()
        .uri(&format!("/task/{}", cancel_id))
        .set_json(&json!({"new_success": 2, "new_failures": 2}))
        .to_request();
    let update_resp = actix_web::test::call_service(&app, update_req).await;
    assert_eq!(
        update_resp.status(),
        actix_web::http::StatusCode::ACCEPTED,
        "Batch update is still accepted even if task is canceled"
    );

    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    let updated: TaskDto = get_task_ok(&app, cancel_id).await;
    assert_eq!(
        updated.success, before.success,
        "Success counter must NOT change on a terminal (Canceled) task"
    );
    assert_eq!(
        updated.failures, before.failures,
        "Failure counter must NOT change on a terminal (Canceled) task"
    );
}
