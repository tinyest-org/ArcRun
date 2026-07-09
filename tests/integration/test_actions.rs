use crate::common::*;

use serde_json::json;

#[tokio::test]
async fn test_task_with_success_and_failure_actions() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let task = json!({
        "id": "with-actions",
        "name": "Task with Actions",
        "kind": "action-test",
        "timeout": 60,
        "metadata": {"test": true},
        "on_start": webhook_action(),
        "on_success": [webhook_action()],
        "on_failure": [webhook_action()]
    });

    let body = create_tasks_ok(&app, &[task]).await;
    // Should have 3 actions: on_start, on_success, on_failure
    let full = get_task_ok(&app, body[0].id).await;
    assert_eq!(full.actions.len(), 3);
}
