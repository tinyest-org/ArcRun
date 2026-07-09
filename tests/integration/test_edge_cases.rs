use crate::common::*;

use serde_json::json;

#[tokio::test]
async fn test_task_with_large_metadata() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let large_array: Vec<i32> = (0..1000).collect();
    let task = json!({
        "id": "large-meta",
        "name": "Large Metadata Task",
        "kind": "large",
        "timeout": 60,
        "metadata": {
            "array": large_array,
            "nested": {
                "level1": {
                    "level2": {
                        "level3": "deep value"
                    }
                }
            }
        },
        "on_start": webhook_action()
    });

    let body = create_tasks_ok(&app, &[task]).await;
    let full = get_task_ok(&app, body[0].id).await;
    assert!(full.metadata.get("array").is_some());
}

#[tokio::test]
async fn test_task_with_special_characters_in_name() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let task = json!({
        "id": "special",
        "name": "Task with 'quotes' and \"double quotes\" and unicode: 日本語",
        "kind": "special-chars",
        "timeout": 60,
        "metadata": {"test": true},
        "on_start": webhook_action()
    });

    let body = create_tasks_ok(&app, &[task]).await;
    assert!(body[0].name.contains("quotes"));
    assert!(body[0].name.contains("日本語"));
}
