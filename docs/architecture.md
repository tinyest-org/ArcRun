# Architecture

Internal engineering reference: task lifecycle, propagation, insertion, code map, DB schema.
See also: [workers.md](workers.md) (worker loops, rules, batch updater), [webhooks.md](webhooks.md) (outbox delivery, batch-complete).

## Task States

- `Pending` - Ready to run, waiting for worker to pick up
- `Running` - Currently executing (on_start webhook called)
- `Waiting` - Has unmet dependencies
- `Success` - Completed successfully
- `Failure` - Failed (timeout, explicit failure, or parent failure)
- `Canceled` - Manually canceled
- `Paused` - Manually paused (Audit 2, A3). Only a **`Pending` or `Waiting`** task can be paused (`PATCH /task/pause/{id}`); pausing a `Running`/`Claimed` task is refused with 400 (cancel it instead), as are terminal tasks. A `Paused` task is not scheduled by the worker loop, but it is **not shielded from its dependencies**: it still receives `wait_*` decrements and is still cascade-failed when a required parent fails (see Dependency Propagation). It never auto-transitions to `Pending` — only an explicit resume (`PATCH /task/resume/{id}`) moves it out of `Paused`, back to `Waiting` (if dependencies remain outstanding) or `Pending` (if all met). Both transitions are single atomic guarded UPDATEs. A `Paused` task can still be canceled/stop_batched.

(`Claimed` is an internal transient state between `Pending` and `Running`, used by the start_loop claim.)

## Action Model

Actions are webhook calls triggered by task lifecycle events:
- **TriggerKind**: `Start`, `End`, `Cancel`, `BatchComplete`
- **TriggerCondition**: `Success`, `Failure` (only meaningful for `End` triggers — determines which end action fires). For `Start`, `Cancel`, and `BatchComplete` triggers, the condition column is always stored as `Success` (sentinel value, since the column is NOT NULL).
- **ActionKindEnum**: `Webhook` (only kind currently)

The `on_start` action can return a `NewActionDto` in the response body to register a cancel action for the task.

## POST /task body shapes (Lot 3)

`POST /task` accepts two JSON shapes (serde `untagged`, fully backwards compatible):
- The legacy bare array `[NewTaskDto, …]`.
- An object `{ "tasks": [NewTaskDto, …], "on_batch_complete": [NewActionDto, …], "scope": "…", "metadata": {…} }`.

`on_batch_complete` registers a batch-level webhook fired exactly once (at-least-once via the outbox) when the **last** task of the batch becomes terminal (see [webhooks.md](webhooks.md)). `scope` (text label) and `metadata` (arbitrary JSON) are optional batch-level identity used for filtering/search via `GET /batches` (Tasker #601). A `batch` row is created when **any** of `on_batch_complete` / `scope` / `metadata` is provided (batches with none of them cost nothing); a scope/metadata-only batch stores `on_complete = '[]'`. Scope/metadata are validated (`validation::validate_batch_meta`: scope non-empty + ≤255 chars, metadata ≤64KB). SSRF/param validation runs on the batch-level actions like any other action.

Tasks are inserted in **grouped multi-row INSERTs** (Lot 3a): contiguous runs of tasks *without* a `dedupe_strategy` are flushed in one `task` / `link` / `action` INSERT each; UUIDs are app-generated (`Uuid::new_v4()`) so the full `id_mapping` is known before any insert. Each of the three grouped INSERTs is **chunked** (A10) so `rows × binds_per_row` stays under a conservative budget (`BIND_BUDGET = 60000`, below Postgres's 65535 bind-parameter ceiling) — the chunks share the same transaction, so atomicity is unchanged. A task carrying a `dedupe_strategy` ends the current run (the run is flushed first so the dedupe check can match tasks inserted earlier in the same batch) — same guard philosophy as the Lot 1 batch-claim.

## Dependency Propagation

When a task completes, `propagate_to_children` in `src/workers.rs` handles:
1. **Success**: Decrements `wait_finished` and `wait_success` counters on children
2. **Failure/Canceled**: Children with `requires_success=true` are marked as `Failure`, and the failure cascades to their descendants
3. **Transition to Pending**: When `wait_finished=0` and `wait_success=0`, child becomes `Pending`

**Failure cascade is level-by-level (Audit 2, B5)**: after the direct level, the failure cascade is walked as a **frontier BFS** (`cascade_failure_frontier` in `src/workers/propagation.rs`), not by recursing once per failed child. Each DAG level below the origin resolves with a constant number of statements (one links `SELECT` over the whole frontier + the A9 ordered pre-lock + one batched cascade-fail `UPDATE … RETURNING` whose result is the next frontier + one `wait_finished` decrement + one Pending unblock), so a root failure over N descendants costs **O(depth)** round-trips in the PATCH transaction instead of O(N). Every frontier node is `Failure`, so `wait_success` is never decremented in the cascade; a child required by one failing parent and optional to another (diamond in the cascade) is deduped and failed once (fail-before-decrement ordering). The `dependency_propagation` metric is still recorded once per propagated node (per-node semantics preserved).

**Paused children (Audit 2, A3)**: steps 1 and 2 apply to `Paused` children as well as `Waiting` ones — the counter decrements and the cascade-fail both target `status IN ('waiting','paused')`. A pause never strands `wait_*` counters and never shields a task from a required parent's failure (Paused → Failure, propagated through the frontier cascade, on_failure outbox enqueued as for a Waiting task). Step 3 stays **`Waiting`-only on purpose**: a `Paused` child whose counters reach 0 stays `Paused` (it does NOT auto-transition to `Pending` — resume decides).

## Code Map

### Entry Points
- `src/main.rs` - HTTP server startup, migration, worker spawning
- `src/test_server.rs` - Test server binary for integration tests
- `src/cache_helper.rs` - Cache utility binary

### HTTP Handlers (`src/handlers.rs`)
All HTTP handler functions and route configuration:
- `configure_routes` - Registers all routes on the Actix `ServiceConfig`
- `health_check` / `readiness_check` - Health and readiness probes. Both acquire a connection under a short 2s bound (Audit 2, B7 — never the pool's 30s `connection_timeout`, which would hang the kubelet under pool exhaustion). **`/health` (liveness) always returns 200** — a healthy body when the DB is reachable, a `degraded` body when the pool is saturated/unreachable (a restart cannot fix that, so liveness must not kill the pod). **`/ready` (readiness) returns 503** on acquire failure/timeout — the correct signal to remove the pod from the load balancer without restarting it.
- `add_task` - POST /task (batch create)
- `get_task` - GET /task/{task_id}
- `list_task` - GET /task (filtered, paginated). A malformed `?metadata=` filter is a **400** (A10) — not silently ignored (which used to return every task).
- `update_task` - PATCH /task/{task_id}. **Idempotent & precise status codes (A10):** a real transition → 200; re-PATCHing the status the task already holds → **200 no-op** (no duplicate propagation/outbox); the task exists but is not Running/Claimed (and not already the requested status) → **409** with `current_status` in the body; unknown id → 404. `metadata` is a **full replace, not a merge** — send the complete object (a partial update drops omitted keys, including any used by dedupe/concurrency matchers).
- `batch_task_updater` - PUT /task/{task_id} (high-throughput counter updates)
- `cancel_task` - DELETE /task/{task_id}. Cancelable from `Pending`, **`Waiting`** (A10 — lets an operator prune a not-yet-eligible DAG branch), `Paused`, `Claimed`, or `Running`; terminal tasks are refused. Error mapping (A10): unknown id → **404**, non-cancelable state → **400** (message names the state), DB failure → **500**. A canceled `Waiting`/`Pending` task never ran on_start, so no `cancel` outbox row is enqueued for it — but its children still cascade (Canceled == Failed).
- `pause_task` - PATCH /task/pause/{task_id} (Pending/Waiting only)
- `resume_task` - PATCH /task/resume/{task_id} (Paused only; → Waiting or Pending by remaining deps)
- `list_webhook_deliveries` - GET /webhook-deliveries (outbox observability; `?status=exhausted`, paginated)
- `get_dag` - GET /dag/{batch_id}
- `view_dag_page` - GET /view (serves static HTML)

### Database Models (`src/models.rs`)
- `Task` - Main task entity with status, metadata, counters
- `Action` - Webhook actions with kind, trigger, condition, params
- `Link` - Parent-child dependency relationships
- `NewTask` / `NewAction` - Insertable structs
- Enums: `StatusKind`, `ActionKindEnum`, `TriggerKind`, `TriggerCondition`

### DTOs (`src/dtos.rs`)
- `NewTaskDto` - Input for creating tasks (includes local `id` for dependency resolution, optional priority)
- `CreateTaskBody` - Untagged `POST /task` body: bare `Vec<NewTaskDto>` OR `CreateTaskBatchDto`
- `CreateTaskBatchDto` - Object form: `{ tasks, on_batch_complete }`
- `TaskDto` - Full task response with actions and priority
- `BasicTaskDto` - Lightweight task for listings (includes priority)
- `DagDto` - Tasks + links for visualization
- `UpdateTaskDto` - Task update payload (includes optional priority)
- `NewActionDto` - Action input (kind + params, trigger determined by context)
- `ActionDto` - Action output (includes trigger)
- `PaginationDto` / `FilterDto` - Query parameters

### Key Functions

**`src/db_operation.rs`**:
- `insert_task_batch` - Creates a whole batch of tasks (grouped multi-row INSERTs, dedupe-aware, Lot 3a)
- `decrement_batch_remaining_for_tasks[_for_task]` / `zero_batch_remaining_and_complete` / `init_batch_remaining` / `insert_batch` / `load_batch_on_complete` / `batch_completion_stats` - Batch-complete webhook support (Lot 3b, counter-based since D2)
- `claim_due_outbox_leased` - Lease-based outbox claim for the delivery loop (selects mature rows + pushes `next_attempt_at` a lease into the future in one statement, so HTTP delivery runs out-of-tx and in parallel)
- `update_running_task` - Updates status, calls `end_task` and `propagate_to_children`
- `find_detailed_task_by_id` - Single query with LEFT JOIN for task + actions
- `list_task_filtered_paged` - Filtered listing with pagination
- `get_dag_for_batch` - Fetches tasks + links for DAG visualization
- `pause_task` / `resume_task` - Atomic guarded Pending/Waiting → Paused, and Paused → Waiting/Pending (A3)
- `set_started_task` - Atomically transitions Pending -> Running

**`src/workers.rs`**:
- `propagate_to_children` - Handles dependency propagation (recursive for failures)
- `cancel_task` - Cancels task and propagates to children
- `claim_task_with_rules` (`src/db/task_crud.rs`) - Rule-checking claim: Concurrency AND Capacity via `rule_slot` conditional upserts (D1, unified `(key, increment, threshold)` slot list — 7.3b); all-or-nothing rollback via the `ClaimTxAbort` sentinel
- `release_slots_for_tasks` (`src/db/task_crud.rs`) - Releases the concurrency + capacity slots of just-transitioned tasks (A9 ordered pre-lock, read-keys-before-NULL CTE; `conc:` keys release 1, `cap:` keys release the stored `capacity_charge`, then both columns are NULLed); called in-tx by all 7 exit sites
- `run_counter_flush_once` (`src/workers/batch_updater.rs`) - Public test entry: apply one batch of counter deltas through the real flush path (counter UPDATE + 7.3b capacity-slot delta) deterministically
- `start_task_phase_a` - Connection-holding preamble of on_start (claim slot + A4 re-check + load actions); the on_start HTTP then runs with no connection held (B1)
- `end_task` - Executes on_success/on_failure webhooks
- `batch_updater` - Batches success/failure count updates to database (see [workers.md](workers.md))

**`src/action.rs`**:
- `ActionExecutor` - Executes webhook actions, passes `?handle=<host>/task/<id>` query param
- `WebhookParams` - URL, HTTP verb, optional body and headers
- Delivery-time SSRF resolver (Audit 2, A5) — see [webhooks.md](webhooks.md#ssrf-protection)

**`src/circuit_breaker.rs`**:
- `CircuitBreaker` - State machine (Closed -> Open -> HalfOpen) for DB pool resilience
- Records successes/failures and trips when threshold exceeded

**`src/validation.rs`**:
- `validate_task_batch` - Validates entire batch before insertion. Enforces the A10 structural limits (`MAX_TASKS_PER_BATCH`/`MAX_DEPS_PER_TASK`/`MAX_ACTIONS_PER_TASK`, read from the `LIMITS_CONFIG` `OnceLock` — same pattern as `SECURITY_CONFIG`, default fallback for tests). **No cycle-detection pass**: the forward-reference rule ("a dependency must appear before the task in the batch") already makes cycles impossible by construction, so the old recursive DFS (a stack-overflow DoS vector) was removed.
- SSRF protection on webhook URLs (creation-time) — see [webhooks.md](webhooks.md#ssrf-protection)

**`src/rule.rs`**:
- `Strategy::Concurency` - Concurrency rule with matcher and max count. **DB-enforced via `rule_slot` since D1** — only tasks that claimed through the rule occupy the slot (see [workers.md](workers.md))
- `Strategy::Capacity` - Capacity rule (sum of remaining work below a threshold). **DB-enforced via `rule_slot` since 7.3b** — the slot holds the sum of the *charges* of tasks that claimed through the rule; progress flushed by the batch_updater shrinks the charge
- `Matcher` - Matches on status, kind, and metadata fields
- `concurrency_slot_key` / `capacity_slot_key` - Canonical textual `rule_slot` keys (collision-free, JSON-encoded sorted fields; `conc:` vs `cap:` prefixes so the two rule kinds never share a counter row); the i64 hash keys remain for dedupe advisory locks and the start_loop prefilter cache

## Database Schema

```sql
-- Core tables
task (id, name, kind, status, metadata, timeout, batch_id, start_condition,
      wait_success, wait_finished, success, failures, failure_reason,
      created_at, started_at, ended_at, last_updated, priority, claimed_slot_keys,
      capacity_charge)
  -- claimed_slot_keys (Audit 2, D1) = TEXT[] of the slot keys (conc: AND cap:) consumed at
  --   claim time; released (decrement + NULLed) on every exit from Claimed/Running.
  --   Never recomputed from metadata (mutable while Running). NULL = holds no slots.
  -- capacity_charge (Audit 2, D1 / 7.3b) = INTEGER, the task's OUTSTANDING capacity charge:
  --   the amount currently counted in each of its cap: slot keys. Set at claim time to
  --   GREATEST(expected_count - success - failures, 0), shrunk (monotonically) by the
  --   batch_updater flush as progress lands, read back at release (never recomputed —
  --   expected_count is mutable while Running), NULLed on release. NULL = holds no charge.
action (id, task_id, kind, trigger, condition, params, success)
link (parent_id, child_id, requires_success)
rule_slot (lock_key, used)
  -- DB-enforced Concurrency AND Capacity rule counters (Audit 2, D1 — 7.3a/7.3b).
  --   lock_key = canonical textual key (rule::concurrency_slot_key `conc:…` /
  --   rule::capacity_slot_key `cap:…`); used = number of live claims through the rule
  --   (conc:) or the sum of the live claimants' outstanding charges (cap:).
  --   Claim: conditional upsert (used + inc, gated used < threshold) in the claim tx,
  --   sorted keys across both prefixes (A9). cap: slots also decremented mid-run by the
  --   batch_updater flush (progress frees capacity).
  --   used = 0 rows are GC'd by the retention loop (always runs, even RETENTION_ENABLED=0).
batch (id, on_complete, created_at, scope, metadata, remaining)
  -- one row per batch that registered on_batch_complete (Lot 3b) AND/OR scope/metadata (#601);
  -- on_complete = JSONB array of NewActionDto ('[]' for a scope/metadata-only batch).
  -- remaining = denormalized count of not-yet-terminal tasks (Audit 2, D2): initialized to the
  --   inserted-task count (dedupe-skips excluded), decremented in-tx by every terminal
  --   transition; 0 IS the batch-complete signal. Exposed via GET /batches (progress).
  -- scope = nullable TEXT label, metadata = JSONB (default '{}') — both filterable/searchable
  --   via GET /batches (?scope= exact, ?metadata= JSONB containment @>, ?search= substring).
  -- Batches with none of on_complete/scope/metadata have no row (tracked only via task.batch_id).
webhook_execution (id, task_id, trigger, condition, idempotency_key,
                   status, attempts, created_at, updated_at,
                   next_attempt_at, last_error, batch_id)
  -- status: pending | success | failure | exhausted
  -- trigger: start | end | cancel | batch_complete
  -- Since Audit 2 D3 this is the idempotency LEDGER + delivery HISTORY only (NOT the
  -- live queue — that moved to webhook_outbox):
  --   * on_start idempotency ledger: `pending`/`success`/`failure` `start` rows drive the
  --     start_loop gate (try_claim_webhook_execution) and the start-before-end gate.
  --   * delivery history: terminated end/cancel/batch_complete deliveries are written here
  --     as `success`/`exhausted` (moved out of the queue on completion). Retained for
  --     GET /webhook-deliveries.
  --   task_id  is NULL for batch_complete rows; batch_id is set instead
  --   CHECK (task_id IS NOT NULL OR batch_id IS NOT NULL)
webhook_outbox (id, task_id, batch_id, trigger, condition, idempotency_key,
                attempts, created_at, updated_at, next_attempt_at, last_error)
  -- The dedicated at-least-once delivery QUEUE for end/cancel/batch_complete webhooks
  --   (Audit 2, D3). PURE queue: every row present is awaiting delivery, so there is NO
  --   `status` column. A row is enqueued in the status-change transaction and DELETED —
  --   historised into webhook_execution as success/exhausted — the moment delivery ends.
  --   task_id XOR batch_id (batch_complete rows carry batch_id, task rows carry task_id);
  --     `condition` is the Success sentinel for cancel/batch_complete.
  --   next_attempt_at = when the row is eligible for (re)delivery (DEFAULT now(); pushed
  --     forward by the claim lease and each failed attempt); last_error = diagnostics.
  --   Kept disjoint from the ledger per idempotency_key by the enqueue backstop
  --     (NOT EXISTS webhook_execution) + the DELETE-then-INSERT on terminal.

-- FK constraints (relevant for cleanup ordering)
action.task_id -> task.id
webhook_execution.task_id -> task.id
webhook_execution.batch_id -> batch.id
webhook_outbox.task_id -> task.id
webhook_outbox.batch_id -> batch.id
link.parent_id -> task.id
link.child_id -> task.id

-- Key indexes
task_status_kind_idx ON task(status, kind)
task_batch_id_idx ON task(batch_id)
link_parent_id_idx ON link(parent_id)
link_child_id_idx ON link(child_id)
idx_webhook_execution_task_id ON webhook_execution(task_id)
idx_webhook_execution_status ON webhook_execution(status)
idx_webhook_execution_batch_id ON webhook_execution(batch_id) WHERE batch_id IS NOT NULL
  -- NB: the old partial idx_webhook_execution_pending_due (next_attempt_at WHERE
  --   status='pending') served ONLY the queue drain and was dropped by D3 — the queue's
  --   maturity scan is now served by idx_webhook_outbox_next_attempt_at below.
idx_webhook_outbox_next_attempt_at ON webhook_outbox(next_attempt_at)
idx_webhook_outbox_task_id ON webhook_outbox(task_id) WHERE task_id IS NOT NULL
idx_webhook_outbox_batch_id ON webhook_outbox(batch_id) WHERE batch_id IS NOT NULL
idx_task_priority ON task(status, priority DESC, created_at ASC, id ASC)
  -- the trailing `id ASC` (Audit 2, B7) matches the start_loop keyset ORDER BY exactly
  --   (priority DESC, created_at ASC, id ASC), so the Pending claim scan is a pure Index
  --   Scan with no Incremental Sort node per page.
idx_action_task_id_trigger ON action(task_id, trigger)
  -- serves both `WHERE task_id = $ AND trigger = $` and task_id-only lookups (leading column)
idx_batch_scope ON batch(scope) WHERE scope IS NOT NULL
idx_batch_metadata_gin ON batch USING GIN(metadata)
-- Dropped as dead/redundant (Audit 2, B7): idx_action_task_id (prefix of
--   idx_action_task_id_trigger), idx_action_trigger (no query filters trigger alone),
--   idx_task_kind (every kind predicate is paired with status → idx_task_status_kind,
--   or is a substring LIKE that no b-tree can serve).
-- Dropped by D2: idx_task_batch_active (partial index whose sole consumer was the
--   retired batch-complete NOT EXISTS probe — the batch.remaining counter replaces it).
```
