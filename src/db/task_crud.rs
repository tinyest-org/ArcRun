use crate::{
    Conn,
    dtos::{self, TaskDto},
    metrics,
    models::{self, Action, Link, NewAction, StatusKind, Task},
    rule::{self, Matcher, Strategy},
};
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use std::collections::HashMap;
use uuid::Uuid;

use super::{DbError, run_in_transaction};

/// ensure we avoid creating duplicate tasks
async fn handle_dedupe<'a>(
    conn: &mut Conn<'a>,
    rules: Vec<Matcher>,
    _metadata: &Option<serde_json::Value>,
) -> Result<bool, DbError> {
    let empty_metadata = serde_json::Value::Null;
    for matcher in rules.iter() {
        use crate::schema::task::dsl::*;
        use diesel::PgJsonbExpressionMethods;

        let meta_ref = _metadata.as_ref().unwrap_or(&empty_metadata);

        if !matcher.fields.is_empty() && _metadata.is_none() {
            // Metadata is None but the matcher requires field comparisons.
            // We can't evaluate this rule without metadata, so skip it
            // (allow creation). Without this guard, m stays as {} and
            // metadata.contains({}) would match ALL existing tasks with
            // non-null metadata, causing over-aggressive deduplication.
            log::warn!(
                "Metadata is None but dedupe matcher requires fields {:?}, skipping rule",
                matcher.fields
            );
            continue;
        }

        let m = match matcher.extract_metadata_fields(meta_ref) {
            Ok(m) => m,
            Err(field) => {
                log::warn!(
                    "Metadata missing field '{}' required by dedupe matcher, skipping rule",
                    field
                );
                continue;
            }
        };

        let count = task
            .filter(
                kind.eq(&matcher.kind)
                    .and(status.eq(&matcher.status))
                    .and(metadata.contains(m)),
            )
            .count()
            .get_result::<i64>(conn)
            .await?;
        if count > 0 {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(crate) async fn insert_actions<'a>(
    task_id: Uuid,
    actions: &[dtos::NewActionDto],
    trigger: &models::TriggerKind,
    condition: &models::TriggerCondition,
    conn: &mut Conn<'a>,
) -> Result<Vec<Action>, DbError> {
    use crate::schema::action::dsl::action;
    if actions.is_empty() {
        return Ok(vec![]);
    }
    let items = actions
        .iter()
        .map(|a| NewAction {
            task_id,
            kind: &a.kind,
            params: a.params.clone(),
            trigger,
            condition,
        })
        .collect::<Vec<_>>();

    let r = diesel::insert_into(action)
        .values(items)
        .returning(Action::as_returning())
        .get_results(conn)
        .await?;
    Ok(r)
}

/// A task DTO that has been resolved against the current `id_mapping` into the
/// concrete rows to insert (task row + links + action specs), with its
/// app-generated UUID already assigned. Built outside the DB so a contiguous run
/// of these can be flushed in a few multi-row INSERTs (Lot 3a).
struct PreparedTask {
    id: Uuid,
    new_task: models::NewTask,
    /// Resolved parent links; `child_id` is already set to `id`.
    links: Vec<Link>,
    on_start: dtos::NewActionDto,
    on_failure: Vec<dtos::NewActionDto>,
    on_success: Vec<dtos::NewActionDto>,
    wait_finished: i32,
}

/// Insert a whole batch of tasks, grouping contiguous runs of tasks WITHOUT a
/// `dedupe_strategy` into multi-row INSERTs (one per `task` / `link` / `action`),
/// while still evaluating dedupe tasks one-at-a-time so they can match tasks
/// inserted earlier in the same batch.
///
/// UUIDs are generated app-side (`Uuid::new_v4()`) so the full `id_mapping` is
/// known before any insert, letting links be built without intermediate RETURNINGs.
///
/// Semantics preserved exactly from the old per-task `insert_new_task`:
/// - dedupe-skipped tasks are absent from `id_mapping` (so a child depending on a
///   skipped parent has that dependency ignored with a warn),
/// - `wait_*` counters, initial Waiting/Pending status, and per-task metrics are
///   identical,
/// - the returned `TaskDto`s preserve input order.
pub(crate) async fn insert_task_batch<'a>(
    conn: &mut Conn<'a>,
    dtos_in: Vec<dtos::NewTaskDto>,
    batch_id: Option<Uuid>,
) -> Result<Vec<TaskDto>, DbError> {
    let mut id_mapping: HashMap<String, Uuid> = HashMap::new();
    let mut results: Vec<TaskDto> = Vec::with_capacity(dtos_in.len());

    // Buffer of prepared, dedupe-free tasks in the current contiguous run.
    let mut run: Vec<PreparedTask> = Vec::new();

    for dto in dtos_in.into_iter() {
        let has_dedupe = dto.dedupe_strategy.is_some();

        if has_dedupe {
            // Flush the pending run FIRST so its tasks are visible to the dedupe
            // check (a dedupe task may match a task inserted earlier in this batch).
            flush_run(conn, &mut run, &mut results).await?;

            let local_id = dto.id.clone();
            let dedupe_rules = dto.dedupe_strategy.clone().unwrap();
            let should_write = handle_dedupe(conn, dedupe_rules, &dto.metadata).await?;
            if !should_write {
                continue;
            }

            let task_id = Uuid::new_v4();
            let prepared = resolve_task(dto, task_id, batch_id, &id_mapping);
            id_mapping.insert(local_id, task_id);
            // A dedupe task is its own one-element run (it may not be batched with
            // others past it, but flushing it alone is fine and keeps the code simple).
            run.push(prepared);
            flush_run(conn, &mut run, &mut results).await?;
        } else {
            let local_id = dto.id.clone();
            let task_id = Uuid::new_v4();
            let prepared = resolve_task(dto, task_id, batch_id, &id_mapping);
            id_mapping.insert(local_id, task_id);
            run.push(prepared);
        }
    }

    // Flush any trailing run.
    flush_run(conn, &mut run, &mut results).await?;

    Ok(results)
}

/// Resolve a DTO into a [`PreparedTask`] against the provided `id_mapping`.
/// (Standalone version that does not use the thread-local indirection.)
fn resolve_task(
    dto: dtos::NewTaskDto,
    task_id: Uuid,
    batch_id: Option<Uuid>,
    id_mapping: &HashMap<String, Uuid>,
) -> PreparedTask {
    let (wait_success, wait_finished, links) = if let Some(ref deps) = dto.dependencies {
        let mut ws = 0i32;
        let mut wf = 0i32;
        let mut resolved_links = Vec::new();
        for dep in deps {
            if let Some(&parent_id) = id_mapping.get(&dep.id) {
                wf += 1;
                if dep.requires_success {
                    ws += 1;
                }
                resolved_links.push(Link {
                    parent_id,
                    child_id: task_id,
                    requires_success: dep.requires_success,
                });
            } else {
                log::warn!("Dependency with local id '{}' not found in mapping", dep.id);
            }
        }
        (ws, wf, resolved_links)
    } else {
        (0, 0, Vec::new())
    };

    let initial_status = if wait_finished > 0 {
        models::StatusKind::Waiting
    } else {
        models::StatusKind::Pending
    };

    let new_task = models::NewTask {
        id: task_id,
        name: dto.name,
        kind: dto.kind,
        status: initial_status,
        timeout: dto.timeout.unwrap_or(60),
        metadata: dto.metadata.unwrap_or(serde_json::Value::Null),
        start_condition: dto.rules.unwrap_or_default(),
        wait_success,
        wait_finished,
        batch_id,
        expected_count: dto.expected_count,
        dead_end_barrier: dto.dead_end_barrier.unwrap_or(false),
        priority: dto.priority.unwrap_or(0),
    };

    PreparedTask {
        id: task_id,
        new_task,
        links,
        on_start: dto.on_start,
        on_failure: dto.on_failure.unwrap_or_default(),
        on_success: dto.on_success.unwrap_or_default(),
        wait_finished,
    }
}

/// Flush a run of prepared tasks into the DB with up to three multi-row INSERTs
/// (task, link, action), then append their `TaskDto`s to `results` (preserving
/// run order) and record per-task metrics. Clears `run`.
async fn flush_run<'a>(
    conn: &mut Conn<'a>,
    run: &mut Vec<PreparedTask>,
    results: &mut Vec<TaskDto>,
) -> Result<(), DbError> {
    use crate::schema::action::dsl::action as action_tbl;
    use crate::schema::link::dsl::link as link_tbl;
    use crate::schema::task::dsl::task as task_tbl;

    if run.is_empty() {
        return Ok(());
    }

    let prepared: Vec<PreparedTask> = std::mem::take(run);

    // 1. Multi-row INSERT of task rows.
    let new_tasks: Vec<&models::NewTask> = prepared.iter().map(|p| &p.new_task).collect();
    let inserted_tasks: Vec<Task> = diesel::insert_into(task_tbl)
        .values(new_tasks)
        .returning(Task::as_returning())
        .get_results(conn)
        .await?;

    // Map id -> inserted Task so we can return them in input order regardless of the
    // order the DB returns rows.
    let mut task_by_id: HashMap<Uuid, Task> =
        inserted_tasks.into_iter().map(|t| (t.id, t)).collect();

    // 2. Multi-row INSERT of all links across the run.
    let all_links: Vec<Link> = prepared.iter().flat_map(|p| p.links.clone()).collect();
    if !all_links.is_empty() {
        diesel::insert_into(link_tbl)
            .values(&all_links)
            .execute(conn)
            .await?;
    }

    // 3. Multi-row INSERT of all actions across the run (start + failure + success).
    let mut all_new_actions: Vec<NewAction> = Vec::new();
    for p in &prepared {
        all_new_actions.push(NewAction {
            task_id: p.id,
            kind: &p.on_start.kind,
            params: p.on_start.params.clone(),
            trigger: &models::TriggerKind::Start,
            condition: &models::TriggerCondition::Success,
        });
        for a in &p.on_failure {
            all_new_actions.push(NewAction {
                task_id: p.id,
                kind: &a.kind,
                params: a.params.clone(),
                trigger: &models::TriggerKind::End,
                condition: &models::TriggerCondition::Failure,
            });
        }
        for a in &p.on_success {
            all_new_actions.push(NewAction {
                task_id: p.id,
                kind: &a.kind,
                params: a.params.clone(),
                trigger: &models::TriggerKind::End,
                condition: &models::TriggerCondition::Success,
            });
        }
    }

    let inserted_actions: Vec<Action> = diesel::insert_into(action_tbl)
        .values(all_new_actions)
        .returning(Action::as_returning())
        .get_results(conn)
        .await?;

    // Group inserted actions by task_id to assemble per-task TaskDtos.
    let mut actions_by_task: HashMap<Uuid, Vec<Action>> = HashMap::new();
    for a in inserted_actions {
        actions_by_task.entry(a.task_id).or_default().push(a);
    }

    // Assemble results in input (run) order + record metrics.
    for p in &prepared {
        let task = task_by_id
            .remove(&p.id)
            .expect("inserted task row missing for prepared id");
        let actions = actions_by_task.remove(&p.id).unwrap_or_default();

        metrics::record_task_created();
        if p.wait_finished > 0 {
            metrics::record_task_with_dependencies();
        }

        results.push(TaskDto::new(task, actions));
    }

    Ok(())
}

/// Find a task by ID with all its actions using a single LEFT JOIN query.
/// Returns None if the task doesn't exist.
pub(crate) async fn find_detailed_task_by_id<'a>(
    conn: &mut Conn<'a>,
    task_id: Uuid,
) -> Result<Option<dtos::TaskDto>, DbError> {
    use crate::schema::action::dsl as action_dsl;
    use crate::schema::task::dsl::*;

    // Use LEFT JOIN to fetch task and actions in a single query
    let results: Vec<(models::Task, Option<Action>)> = task
        .left_join(action_dsl::action)
        .filter(id.eq(task_id))
        .load::<(models::Task, Option<Action>)>(conn)
        .await?;

    if results.is_empty() {
        return Ok(None);
    }

    // All rows have the same task; take the first one by value, collect actions from the rest
    let mut iter = results.into_iter();
    let (base_task, first_action) = iter.next().unwrap();
    let actions: Vec<Action> = first_action
        .into_iter()
        .chain(iter.filter_map(|(_, a)| a))
        .collect();

    Ok(Some(TaskDto::new(base_task, actions)))
}

/// Atomically claim a Pending task by transitioning it to Claimed.
/// Returns true if this caller successfully claimed the task, false if another worker got it first.
pub async fn claim_task<'a>(conn: &mut Conn<'a>, task_id: &uuid::Uuid) -> Result<bool, DbError> {
    use crate::schema::task::dsl::*;
    use diesel::dsl::now;

    let updated_count =
        diesel::update(task.filter(id.eq(task_id).and(status.eq(StatusKind::Pending))))
            .set((status.eq(StatusKind::Claimed), last_updated.eq(now)))
            .execute(conn)
            .await?;

    Ok(updated_count == 1)
}

/// Atomically claim a contiguous run of rule-free Pending tasks in a single
/// `UPDATE ... WHERE id = ANY($ids) AND status='pending' RETURNING id`.
///
/// Only valid for tasks with an empty `start_condition` (no concurrency/capacity
/// rules), because a single UPDATE cannot evaluate per-task rules. Returns the set
/// of IDs that were actually transitioned Pending -> Claimed (some may already have
/// been claimed by another worker, in which case they are absent from the result).
pub async fn batch_claim_tasks<'a>(
    conn: &mut Conn<'a>,
    ids: &[uuid::Uuid],
) -> Result<Vec<uuid::Uuid>, DbError> {
    use crate::schema::task::dsl::*;
    use diesel::dsl::now;

    if ids.is_empty() {
        return Ok(vec![]);
    }

    let claimed_ids =
        diesel::update(task.filter(id.eq_any(ids).and(status.eq(StatusKind::Pending))))
            .set((status.eq(StatusKind::Claimed), last_updated.eq(now)))
            .returning(id)
            .get_results::<uuid::Uuid>(conn)
            .await?;

    Ok(claimed_ids)
}

/// Transition a Claimed task to Running and set started_at.
/// Returns true if the task was updated, false if it was no longer Claimed.
pub async fn mark_task_running<'a>(
    conn: &mut Conn<'a>,
    task_id: &uuid::Uuid,
) -> Result<bool, DbError> {
    use crate::schema::task::dsl::*;
    use diesel::dsl::now;

    let updated_count =
        diesel::update(task.filter(id.eq(task_id).and(status.eq(StatusKind::Claimed))))
            .set((
                status.eq(StatusKind::Running),
                started_at.eq(now),
                last_updated.eq(now),
            ))
            .execute(conn)
            .await?;

    Ok(updated_count == 1)
}

/// Result of attempting to atomically check concurrency rules and claim a task.
#[derive(Debug, PartialEq)]
pub enum ClaimResult {
    /// Task was successfully claimed (Pending -> Claimed).
    Claimed,
    /// A concurrency rule blocked this task from being claimed.
    RuleBlocked,
    /// Task was already claimed by another worker (UPDATE touched 0 rows).
    AlreadyClaimed,
}

pub(crate) use rule::capacity_lock_key;
/// Re-export lock key functions from rule module for use by other crates/modules.
pub(crate) use rule::concurrency_lock_key;

/// Pre-computed parameters for the rule-check-and-claim SQL query.
/// Built from a task's rules and metadata before entering the transaction,
/// so no references to the Task are needed inside the closure.
struct RuleQueryParams {
    lock_keys: Vec<i64>,
    // Concurrency rule arrays (parallel arrays, one entry per rule)
    conc_kinds: Vec<String>,
    conc_meta_texts: Vec<String>,
    conc_statuses: Vec<StatusKind>,
    conc_include_claimed: Vec<bool>,
    conc_thresholds: Vec<i64>,
    // Capacity rule arrays (parallel arrays, one entry per rule)
    cap_kinds: Vec<String>,
    cap_meta_texts: Vec<String>,
    cap_max_capacities: Vec<i64>,
}

impl RuleQueryParams {
    /// Build query parameters from a task's rules and metadata.
    /// Returns `Err(ClaimResult::RuleBlocked)` if a rule cannot be evaluated
    /// (missing metadata field or missing expected_count for capacity).
    fn from_task(t: &Task) -> Result<Self, ClaimResult> {
        let rules = &t.start_condition.0;
        let task_id = t.id;

        let mut params = RuleQueryParams {
            lock_keys: Vec::new(),
            conc_kinds: Vec::new(),
            conc_meta_texts: Vec::new(),
            conc_statuses: Vec::new(),
            conc_include_claimed: Vec::new(),
            conc_thresholds: Vec::new(),
            cap_kinds: Vec::new(),
            cap_meta_texts: Vec::new(),
            cap_max_capacities: Vec::new(),
        };

        for strategy in rules {
            match strategy {
                Strategy::Concurency(concurrency_rule) => {
                    let m = match concurrency_rule
                        .matcher
                        .extract_metadata_fields(&t.metadata)
                    {
                        Ok(m) => m,
                        Err(field) => {
                            log::warn!(
                                "Task {} missing metadata field '{}' required by concurrency rule, blocking",
                                task_id,
                                field
                            );
                            return Err(ClaimResult::RuleBlocked);
                        }
                    };

                    let lock_key = concurrency_lock_key(concurrency_rule, &t.metadata);
                    let is_same_kind = concurrency_rule.matcher.kind == t.kind;
                    let include_claimed = concurrency_rule.matcher.status == StatusKind::Running;

                    // Pre-compute threshold for the SQL check (`count >= threshold` means blocked):
                    // is_same_kind  → count < max  → blocked when count >= max
                    // !is_same_kind → count <= max → blocked when count >= max + 1
                    let threshold = if is_same_kind {
                        concurrency_rule.max_concurency as i64
                    } else {
                        (concurrency_rule.max_concurency + 1) as i64
                    };

                    params.lock_keys.push(lock_key);
                    params
                        .conc_kinds
                        .push(concurrency_rule.matcher.kind.clone());
                    params.conc_meta_texts.push(m.to_string());
                    params.conc_statuses.push(concurrency_rule.matcher.status);
                    params.conc_include_claimed.push(include_claimed);
                    params.conc_thresholds.push(threshold);
                }
                Strategy::Capacity(capacity_rule) => {
                    let m = match capacity_rule.matcher.extract_metadata_fields(&t.metadata) {
                        Ok(m) => m,
                        Err(field) => {
                            log::warn!(
                                "Task {} missing metadata field '{}' required by capacity rule, blocking",
                                task_id,
                                field
                            );
                            return Err(ClaimResult::RuleBlocked);
                        }
                    };

                    // Candidate must have expected_count set
                    if t.expected_count.is_none() {
                        log::warn!(
                            "Task {} has a Capacity rule but no expected_count, blocking",
                            task_id,
                        );
                        return Err(ClaimResult::RuleBlocked);
                    }

                    let lock_key = capacity_lock_key(capacity_rule, &t.metadata);
                    params.lock_keys.push(lock_key);
                    params.cap_kinds.push(capacity_rule.matcher.kind.clone());
                    params.cap_meta_texts.push(m.to_string());
                    params
                        .cap_max_capacities
                        .push(capacity_rule.max_capacity as i64);
                }
            }
        }

        // Sort and deduplicate lock keys to acquire them in consistent order (prevents deadlocks)
        params.lock_keys.sort();
        params.lock_keys.dedup();

        Ok(params)
    }
}

/// Atomically check concurrency rules and claim a task within a single transaction,
/// using `pg_advisory_xact_lock` to serialize workers checking the same rule/metadata combo.
///
/// This prevents the TOCTOU race where two workers both see count < max and both claim,
/// exceeding the concurrency limit.
///
/// Uses a two-query approach within the transaction:
/// 1. Acquire all advisory locks in one round-trip (via unnest)
/// 2. Check all concurrency + capacity rules and conditionally claim in a single CTE
///
/// This reduces the number of SQL round-trips from N+M+K+1 (N locks + M concurrency
/// checks + K capacity checks + 1 claim) to exactly 2, minimizing time spent holding
/// advisory locks and reducing contention between workers.
pub async fn claim_task_with_rules<'a>(
    conn: &mut Conn<'a>,
    t: &Task,
) -> Result<ClaimResult, DbError> {
    // No rules — just do a plain claim (no advisory lock needed)
    if t.start_condition.0.is_empty() {
        return match claim_task(conn, &t.id).await? {
            true => Ok(ClaimResult::Claimed),
            false => Ok(ClaimResult::AlreadyClaimed),
        };
    }

    // Pre-compute everything we need before entering the transaction closure.
    let task_id = t.id;
    let params = match RuleQueryParams::from_task(t) {
        Ok(p) => p,
        Err(result) => return Ok(result),
    };

    let RuleQueryParams {
        lock_keys,
        conc_kinds,
        conc_meta_texts,
        conc_statuses,
        conc_include_claimed,
        conc_thresholds,
        cap_kinds,
        cap_meta_texts,
        cap_max_capacities,
    } = params;

    run_in_transaction(conn, |conn| {
        Box::pin(async move {
            // Query 1: Acquire all advisory locks in one round-trip.
            // Locks are released automatically on COMMIT/ROLLBACK.
            diesel::sql_query(
                "SELECT pg_advisory_xact_lock(k) FROM unnest($1::bigint[]) AS k",
            )
            .bind::<diesel::sql_types::Array<diesel::sql_types::BigInt>, _>(&lock_keys)
            .execute(&mut *conn)
            .await?;

            // Query 2: Check all concurrency + capacity rules and conditionally claim
            // the task, all in a single CTE. Rule parameters are passed as parallel
            // arrays and unpacked via unnest.
            //
            // - conc_rules: one row per concurrency rule
            // - conc_blocked: rows where the concurrency count >= threshold
            // - cap_rules: one row per capacity rule
            // - cap_blocked: rows where the capacity sum >= max
            // - rules_check: single boolean — true iff no rule is blocked
            // - claim_result: conditional UPDATE, only executes if rules_check.ok is true
            #[derive(diesel::QueryableByName)]
            struct ClaimCheckRow {
                #[diesel(sql_type = diesel::sql_types::Bool)]
                rules_passed: bool,
                #[diesel(sql_type = diesel::sql_types::Bool)]
                claimed: bool,
            }

            // Note: meta_text values are produced by serde_json::Value::to_string(), which
            // always emits valid JSON. The SQL casts them back via `::jsonb`. This is safe but
            // less type-safe than the old code which passed metadata as Diesel's Jsonb type
            // directly — Diesel's sql_query bind API does not support binding Jsonb arrays, so
            // we pass them as text and cast in SQL.
            let row: ClaimCheckRow = diesel::sql_query(
                r#"
                WITH conc_rules AS (
                    SELECT ord, kind, meta_text, status_val, include_claimed, threshold
                    FROM unnest($1::text[], $2::text[], $3::status_kind[], $4::bool[], $5::bigint[])
                    WITH ORDINALITY AS r(kind, meta_text, status_val, include_claimed, threshold, ord)
                ),
                conc_blocked AS (
                    SELECT r.ord
                    FROM conc_rules r
                    WHERE (
                        SELECT COUNT(*)
                        FROM task t
                        WHERE t.kind = r.kind
                          AND t.metadata @> r.meta_text::jsonb
                          AND (
                              t.status = r.status_val
                              OR (r.include_claimed AND t.status = 'claimed')
                          )
                    ) >= r.threshold
                ),
                cap_rules AS (
                    SELECT ord, kind, meta_text, max_cap
                    FROM unnest($6::text[], $7::text[], $8::bigint[])
                    WITH ORDINALITY AS r(kind, meta_text, max_cap, ord)
                ),
                cap_blocked AS (
                    SELECT r.ord
                    FROM cap_rules r
                    WHERE (
                        SELECT COALESCE(SUM(GREATEST(COALESCE(t.expected_count, 0) - t.success - t.failures, 0)), 0)
                        FROM task t
                        WHERE t.kind = r.kind
                          AND (t.status = 'running' OR t.status = 'claimed')
                          AND t.metadata @> r.meta_text::jsonb
                    ) >= r.max_cap
                ),
                rules_check AS (
                    SELECT
                        NOT EXISTS (SELECT 1 FROM conc_blocked)
                        AND NOT EXISTS (SELECT 1 FROM cap_blocked) AS ok
                ),
                claim_result AS (
                    UPDATE task SET status = 'claimed', last_updated = now()
                    WHERE id = $9 AND status = 'pending'
                      AND (SELECT ok FROM rules_check)
                    RETURNING id
                )
                SELECT
                    (SELECT ok FROM rules_check) AS rules_passed,
                    EXISTS (SELECT 1 FROM claim_result) AS claimed
                "#,
            )
            .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(&conc_kinds)
            .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(&conc_meta_texts)
            .bind::<diesel::sql_types::Array<crate::schema::sql_types::StatusKind>, _>(&conc_statuses)
            .bind::<diesel::sql_types::Array<diesel::sql_types::Bool>, _>(&conc_include_claimed)
            .bind::<diesel::sql_types::Array<diesel::sql_types::BigInt>, _>(&conc_thresholds)
            .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(&cap_kinds)
            .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(&cap_meta_texts)
            .bind::<diesel::sql_types::Array<diesel::sql_types::BigInt>, _>(&cap_max_capacities)
            .bind::<diesel::sql_types::Uuid, _>(task_id)
            // INVARIANT: The final SELECT (no FROM/WHERE) always produces exactly 1 row,
            // so get_result is safe. If this query is ever modified to add filtering on the
            // outer SELECT, it must switch to get_results + handle the empty case.
            .get_result(&mut *conn)
            .await?;

            if row.claimed {
                Ok(ClaimResult::Claimed)
            } else if !row.rules_passed {
                Ok(ClaimResult::RuleBlocked)
            } else {
                Ok(ClaimResult::AlreadyClaimed)
            }
        })
    })
    .await
}
