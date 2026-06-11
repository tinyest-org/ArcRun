//! Integration tests for the paginated claim loop (Lot 1).
//!
//! These tests drive `arcrun::workers::run_claim_loop` directly with a small
//! `page_size` / `claim_cap` so the behaviour can be asserted deterministically
//! without spinning the real `start_loop` timer.
//!
//! Covered:
//! 1. Head-of-line regression: a low-priority eligible task is claimed in the same
//!    iteration even when higher-priority tasks are blocked by a concurrency rule.
//! 2. Claim cap: exactly `claim_cap` tasks are claimed, the rest stay Pending.
//! 3. Batch-claim ordering: a rule-bearing task interleaved between rule-free tasks
//!    is evaluated at its position; the batch is never claimed across it.
//! 4. Keyset pagination: tasks beyond the first internal page are still claimed.

use crate::common::*;

use arcrun::models::StatusKind;
use serde_json::json;

/// Concurrency rule JSON with `max_concurency` on a given kind, matching on the
/// shared `{"test": true}` metadata that `webhook_action` builders use.
fn conc_rule(kind: &str, max: i32) -> serde_json::Value {
    json!([{
        "type": "Concurency",
        "matcher": { "kind": kind, "status": "Running", "fields": [] },
        "max_concurency": max
    }])
}

/// Build a Pending task JSON with explicit priority and optional rules.
fn task_with_priority(
    id: &str,
    kind: &str,
    priority: i32,
    rules: Option<serde_json::Value>,
) -> serde_json::Value {
    let mut t = json!({
        "id": id,
        "name": id,
        "kind": kind,
        "timeout": 60,
        "metadata": {"test": true},
        "on_start": webhook_action(),
        "priority": priority
    });
    if let Some(r) = rules {
        t["rules"] = r;
    }
    t
}

/// Move a freshly-created task to Running so it counts against concurrency rules.
async fn make_running(state: &arcrun::handlers::AppState, id: uuid::Uuid) {
    let mut conn = state.pool.get().await.unwrap();
    arcrun::db_operation::claim_task(&mut conn, &id)
        .await
        .unwrap();
    arcrun::db_operation::mark_task_running(&mut conn, &id)
        .await
        .unwrap();
}

// =============================================================================
// 1. Head-of-line blocking regression
// =============================================================================

/// N high-priority tasks blocked by a saturated concurrency rule must NOT prevent
/// a lower-priority eligible task (created after, no rules) from being claimed in
/// the same iteration. This is the core anti-famine guarantee of Lot 1.
#[actix_web::test]
async fn test_head_of_line_eligible_low_priority_claimed() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let kind = "hol-kind";

    // Blocker: a Running task of the same kind/metadata that saturates max_concurency=1.
    let blocker = task_with_priority("blocker", kind, 0, None);
    let created = create_tasks_ok(&app, &[blocker]).await;
    make_running(&state, created[0].id).await;

    // 5 high-priority Pending tasks, all blocked by the (now saturated) concurrency rule.
    let mut high: Vec<serde_json::Value> = (0..5)
        .map(|i| task_with_priority(&format!("hi-{i}"), kind, 900, Some(conc_rule(kind, 1))))
        .collect();
    // 1 eligible low-priority task with a DIFFERENT kind (no rule, not counted).
    let eligible = task_with_priority("eligible", "free-kind", -900, None);
    high.push(eligible);

    let created = create_tasks_ok(&app, &high).await;
    let eligible_id = created.last().unwrap().id;

    // Drive one claim iteration with a generous cap and tiny page size.
    let mut conn = state.pool.get().await.unwrap();
    let claimed = arcrun::workers::run_claim_loop(&mut conn, 50, 500).await;
    drop(conn);

    // The eligible low-priority task must have been claimed despite being last in order.
    assert!(
        claimed.iter().any(|t| t.id == eligible_id),
        "low-priority eligible task should be claimed in the same iteration"
    );
    assert_task_status(
        &app,
        eligible_id,
        StatusKind::Claimed,
        "eligible task should be Claimed",
    )
    .await;

    // The blocked high-priority tasks remain Pending.
    for c in created.iter().filter(|c| c.id != eligible_id) {
        assert_task_status(
            &app,
            c.id,
            StatusKind::Pending,
            "blocked task stays Pending",
        )
        .await;
    }
}

// =============================================================================
// 2. Claim cap respected
// =============================================================================

/// More eligible (rule-free) tasks than `claim_cap` => exactly `claim_cap` claimed,
/// remainder stays Pending; a subsequent iteration picks up the rest.
#[actix_web::test]
async fn test_claim_cap_limits_claims_per_iteration() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let tasks: Vec<serde_json::Value> = (0..7)
        .map(|i| task_with_priority(&format!("c-{i}"), "cap-kind", 0, None))
        .collect();
    let created = create_tasks_ok(&app, &tasks).await;

    // claim_cap = 3 (page_size larger so pagination isn't the limiter).
    let mut conn = state.pool.get().await.unwrap();
    let claimed = arcrun::workers::run_claim_loop(&mut conn, 3, 500).await;
    assert_eq!(claimed.len(), 3, "exactly claim_cap=3 tasks claimed");

    // Count statuses: 3 Claimed, 4 Pending.
    let claimed_count = created
        .iter()
        .filter(|c| claimed.iter().any(|t| t.id == c.id))
        .count();
    assert_eq!(claimed_count, 3);

    // Next iteration claims the remaining 4.
    let claimed2 = arcrun::workers::run_claim_loop(&mut conn, 3, 500).await;
    assert_eq!(claimed2.len(), 3, "second iteration claims up to cap again");
    let claimed3 = arcrun::workers::run_claim_loop(&mut conn, 3, 500).await;
    assert_eq!(claimed3.len(), 1, "third iteration claims the last one");
    drop(conn);

    for c in &created {
        assert_task_status(&app, c.id, StatusKind::Claimed, "all eventually Claimed").await;
    }
}

// =============================================================================
// 3. Batch-claim ordering: never batch across a rule-bearing task
// =============================================================================

/// Ordering: rule-free tasks before AND after a (blocked) rule-bearing task at a
/// middle priority. The interleaved rule-bearing task must be evaluated at its
/// position (and blocked), and the rule-free tasks on both sides must be claimed —
/// the batch must NOT skip past the rule-bearing task.
#[actix_web::test]
async fn test_batch_claim_respects_order_around_rule_task() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let kind = "ord-kind";

    // Saturate the concurrency rule with a Running blocker.
    let blocker = task_with_priority("ord-blocker", kind, 0, None);
    let created = create_tasks_ok(&app, &[blocker]).await;
    make_running(&state, created[0].id).await;

    // Priority ordering (claim order = priority DESC):
    //   free-hi (prio 300, no rule)   <- claimed
    //   ruled   (prio 200, blocked)   <- evaluated here, RuleBlocked
    //   free-lo (prio 100, no rule)   <- claimed (batch must resume after ruled)
    let tasks = vec![
        task_with_priority("free-hi", "free-a", 300, None),
        task_with_priority("ruled", kind, 200, Some(conc_rule(kind, 1))),
        task_with_priority("free-lo", "free-b", 100, None),
    ];
    let created = create_tasks_ok(&app, &tasks).await;
    let free_hi = created[0].id;
    let ruled = created[1].id;
    let free_lo = created[2].id;

    let mut conn = state.pool.get().await.unwrap();
    let claimed = arcrun::workers::run_claim_loop(&mut conn, 50, 500).await;
    drop(conn);

    assert!(claimed.iter().any(|t| t.id == free_hi), "free-hi claimed");
    assert!(claimed.iter().any(|t| t.id == free_lo), "free-lo claimed");
    assert!(
        !claimed.iter().any(|t| t.id == ruled),
        "ruled task must be blocked, not claimed"
    );

    assert_task_status(&app, free_hi, StatusKind::Claimed, "free-hi Claimed").await;
    assert_task_status(&app, free_lo, StatusKind::Claimed, "free-lo Claimed").await;
    assert_task_status(&app, ruled, StatusKind::Pending, "ruled stays Pending").await;
}

// =============================================================================
// 4. Keyset pagination across pages
// =============================================================================

/// With more Pending tasks than the (test-injected) page_size, tasks beyond the
/// first page must still be claimed — pagination must not blind the scan.
#[actix_web::test]
async fn test_keyset_pagination_claims_beyond_first_page() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    // 12 rule-free tasks, page_size=5 => at least 3 pages.
    let tasks: Vec<serde_json::Value> = (0..12)
        .map(|i| task_with_priority(&format!("pg-{i:02}"), "page-kind", 0, None))
        .collect();
    let created = create_tasks_ok(&app, &tasks).await;

    let mut conn = state.pool.get().await.unwrap();
    let claimed = arcrun::workers::run_claim_loop(&mut conn, 100, 5).await;
    drop(conn);

    assert_eq!(claimed.len(), 12, "all 12 tasks across pages claimed");
    for c in &created {
        assert_task_status(&app, c.id, StatusKind::Claimed, "task across pages Claimed").await;
    }
}

/// Keyset pagination with rule-bearing tasks spread across pages: a blocked
/// rule-bearing task at the head must not stop later pages from being scanned.
#[actix_web::test]
async fn test_pagination_with_rules_does_not_block_later_pages() {
    let (_g, state) = setup_test_app().await;
    let app = test_service!(state);

    let kind = "pgr-kind";
    let blocker = task_with_priority("pgr-blocker", kind, 0, None);
    let created = create_tasks_ok(&app, &[blocker]).await;
    make_running(&state, created[0].id).await;

    // 6 blocked rule-bearing high-priority tasks (fill the first page of size 5+),
    // then a rule-free eligible task at the lowest priority.
    let mut tasks: Vec<serde_json::Value> = (0..6)
        .map(|i| task_with_priority(&format!("pgr-hi-{i}"), kind, 900, Some(conc_rule(kind, 1))))
        .collect();
    tasks.push(task_with_priority("pgr-eligible", "pgr-free", -900, None));
    let created = create_tasks_ok(&app, &tasks).await;
    let eligible_id = created.last().unwrap().id;

    let mut conn = state.pool.get().await.unwrap();
    // page_size=5 forces the eligible task onto a later page.
    let claimed = arcrun::workers::run_claim_loop(&mut conn, 50, 5).await;
    drop(conn);

    assert!(
        claimed.iter().any(|t| t.id == eligible_id),
        "eligible task on a later page should be claimed despite blocked head-of-file rules"
    );
    assert_task_status(&app, eligible_id, StatusKind::Claimed, "eligible Claimed").await;
}
