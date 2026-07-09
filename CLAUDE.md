# Claude Code Context for ArcRun

This file provides context for Claude Code when working on this project.

## Important Rules

- **Never read or write `static/dag.html`** — it is a build artifact generated from the SolidJS UI in `ui/`. Always edit source files in `ui/src/` instead.

## Project Overview

ArcRun is a Rust-based task orchestration service that manages task execution with:
- DAG (Directed Acyclic Graph) dependencies between tasks
- Concurrency control via configurable rules
- Webhook-based action execution
- PostgreSQL persistence with async operations
- Circuit breaker for connection pool resilience
- Distributed tracing via OpenTelemetry
- SSRF protection on webhook URLs

## Key Concepts

### Task States
- `Pending` - Ready to run, waiting for worker to pick up
- `Running` - Currently executing (on_start webhook called)
- `Waiting` - Has unmet dependencies
- `Success` - Completed successfully
- `Failure` - Failed (timeout, explicit failure, or parent failure)
- `Canceled` - Manually canceled
- `Paused` - Manually paused (Audit 2, A3). Only a **`Pending` or `Waiting`** task can be paused (`PATCH /task/pause/{id}`); pausing a `Running`/`Claimed` task is refused with 400 (cancel it instead), as are terminal tasks. A `Paused` task is not scheduled by the worker loop, but it is **not shielded from its dependencies**: it still receives `wait_*` decrements and is still cascade-failed when a required parent fails (see Dependency Propagation). It never auto-transitions to `Pending` — only an explicit resume (`PATCH /task/resume/{id}`) moves it out of `Paused`, back to `Waiting` (if dependencies remain outstanding) or `Pending` (if all met). Both transitions are single atomic guarded UPDATEs. A `Paused` task can still be canceled/stop_batched.

### Action Model
Actions are webhook calls triggered by task lifecycle events:
- **TriggerKind**: `Start`, `End`, `Cancel`, `BatchComplete`
- **TriggerCondition**: `Success`, `Failure` (only meaningful for `End` triggers — determines which end action fires). For `Start`, `Cancel`, and `BatchComplete` triggers, the condition column is always stored as `Success` (sentinel value, since the column is NOT NULL).
- **ActionKindEnum**: `Webhook` (only kind currently)

The `on_start` action can return a `NewActionDto` in the response body to register a cancel action for the task.

### POST /task body shapes (Lot 3)
`POST /task` accepts two JSON shapes (serde `untagged`, fully backwards compatible):
- The legacy bare array `[NewTaskDto, …]`.
- An object `{ "tasks": [NewTaskDto, …], "on_batch_complete": [NewActionDto, …], "scope": "…", "metadata": {…} }`.

`on_batch_complete` registers a batch-level webhook fired exactly once (at-least-once via the outbox) when the **last** task of the batch becomes terminal. `scope` (text label) and `metadata` (arbitrary JSON) are optional batch-level identity used for filtering/search via `GET /batches` (Tasker #601). A `batch` row is created when **any** of `on_batch_complete` / `scope` / `metadata` is provided (batches with none of them cost nothing); a scope/metadata-only batch stores `on_complete = '[]'`. Scope/metadata are validated (`validation::validate_batch_meta`: scope non-empty + ≤255 chars, metadata ≤64KB). SSRF/param validation runs on the batch-level actions like any other action. Tasks are inserted in **grouped multi-row INSERTs** (Lot 3a): contiguous runs of tasks *without* a `dedupe_strategy` are flushed in one `task` / `link` / `action` INSERT each; UUIDs are app-generated (`Uuid::new_v4()`) so the full `id_mapping` is known before any insert. Each of the three grouped INSERTs is **chunked** (A10) so `rows × binds_per_row` stays under a conservative budget (`BIND_BUDGET = 60000`, below Postgres's 65535 bind-parameter ceiling) — the chunks share the same transaction, so atomicity is unchanged. A task carrying a `dedupe_strategy` ends the current run (the run is flushed first so the dedupe check can match tasks inserted earlier in the same batch) — same guard philosophy as the Lot 1 batch-claim.

### Batch-complete detection (`BatchComplete` trigger — Lot 3b)
When any task of a batch reaches a terminal state, the transition transaction calls `maybe_enqueue_batch_complete[_for_task]` (`src/db/webhook_execution.rs`): if a `batch` row exists **with a non-empty `on_complete`** AND `NOT EXISTS (task WHERE batch_id = $1 AND status NOT IN (terminal))`, it enqueues one outbox row keyed `batch:<batch_id>:complete`. (The `on_complete` non-empty gate matters since #601: scope/metadata-only batches have a `batch` row but an empty `on_complete`, so they never signal completion.) The unique idempotency key + `ON CONFLICT DO NOTHING` make concurrent detection inoffensive (a single row even if two tasks finish "at once"). The check is centralised in ONE helper, called from every terminal site: `update_running_task`, `fail_task_and_propagate`, `stop_batch` (`task_lifecycle.rs`), `timeout_task_and_propagate` (`task_query.rs`), `cancel_task` (`propagation.rs`), and `add_task` (for the vacuously-complete empty / all-dedupe-skipped batch). The delivery loop (`delivery_loop.rs`: `prepare_batch_complete_row` prefetch + `deliver_plan`) loads `batch.on_complete`, executes each action **without** a `?handle=`, with an `arcrun` body enrichment `{batch_id, counts:{success,failure,canceled}, completed_at}` (counts / `completed_at = max(ended_at)` computed at delivery time). Retry/backoff/exhausted are identical to task-level rows. Retention (`src/db/cleanup.rs`) also deletes orphaned `batch` rows (and their batch-level `webhook_execution` rows) once their tasks are gone.

### Webhook Delivery Contract (transactional outbox — Lot 2)

End and cancel webhooks are **at-least-once notifications** delivered via a transactional outbox (`webhook_execution`), not fired inline in the request/worker call path:

1. **API response = durable state.** When `PATCH /task/{id}` (or cancel/timeout/stop_batch) responds, the status transition, all propagation, AND the outbox rows for the end/cancel notifications are committed in one transaction. No reqwest runs in the call path, so the connection is released immediately (no pool starvation from slow consumers).
2. **At-least-once delivery.** Every lifecycle notification (end, cancel) is delivered at least once, surviving crash/redeploy (the `pending` outbox row is durable). Consumers dedupe via the `Idempotency-Key` header (= `idempotency_key(task_id, trigger, condition)`).
3. **Ordering.** No order guaranteed *between* tasks (a parent's `on_success` may arrive after a child's `on_start`). Order guaranteed *per task*: `start` is delivered before `end` (the delivery loop holds an `end`/`cancel` row until the task's `start` row is no longer `pending`). This gate is **bounded by freshness** (Audit 2, A2): it only holds while the pending `start` row's `updated_at > now() - WORKER_CLAIM_TIMEOUT_SECS`. A start row that never completes (crash between `mark_task_running` and its completion, or a Claimed task canceled mid-webhook) eventually goes stale and stops blocking, so end/cancel deliver anyway — a deliberate relaxation (better than an eternal block; start-before-end still holds for healthy starts). The nominal path closes the crash window by committing `mark_task_running` and the start-row completion in one transaction (`execute_webhook_for_task`).
4. **`on_start` is control-flow, NOT a notification.** It stays synchronous in `start_loop` (its response can register a cancel action; its failure marks the task Failed). It does **not** go through the outbox. The webhook-supplied cancel action is persisted **inside** the same transaction that completes the `start` outbox row and transitions Claimed→Running (`execute_webhook_for_task`, Audit 2 A4) — even when the task already left `Claimed` while on_start was in flight (`mark_task_running` returns false). Committing it atomically with the start-row completion guarantees it is visible before the start-before-end gate can release the task's cancel row (validation is best-effort: invalid actions are logged + skipped, never rolling back the transition).
5. **Cancel notifications cover the whole webhook-in-flight window.** `cancel_task`, `stop_batch`, and dead-end cancellation enqueue a `cancel` outbox row for a task that is `Running` **OR `Claimed`** (A4). `Claimed` is not "on_start never called" — it spans the entire on_start-in-flight window, so a consumer that received on_start and started work always gets a cancel. A Claimed task that never returned a cancel action prefetches zero cancel actions ⇒ fast-path `success` (no HTTP), so the broadened enqueue is innocuous. The permit-wait sub-window (task claimed but its `start` row not yet created, so a cancel row is not gated and can drain as zero-action before on_start fires) is closed too: `start_task` re-checks the task's status right after creating the start row and, if it left `Claimed`, skips on_start entirely and completes the start row (nothing executed ⇒ the zero-action cancel was correct).

The delivery loop (`src/workers/delivery_loop.rs`, the 5th worker) drains mature `pending` outbox rows with `FOR UPDATE SKIP LOCKED`, executes the actions, and marks `success` / retries with exponential backoff / `exhausted` after `WEBHOOK_MAX_ATTEMPTS`. End/cancel webhook bodies are enriched with the task's final status + `ended_at` + trigger under a reserved `arcrun` key (merged non-destructively into any custom body). Inspect deliveries via `GET /webhook-deliveries?status=exhausted`.

**Outbox enqueue is unconditional** (a `pending` row is inserted even if the task has no matching action); the delivery loop marks zero-action rows `success` immediately. This keeps the transition transaction minimal (one INSERT, no action lookup). `INSERT ... ON CONFLICT (idempotency_key) DO NOTHING` makes re-runs of a transition idempotent.

### Dependency Propagation
When a task completes, `propagate_to_children` in `src/workers.rs` handles:
1. **Success**: Decrements `wait_finished` and `wait_success` counters on children
2. **Failure/Canceled**: Children with `requires_success=true` are marked as `Failure` (recursively)
3. **Transition to Pending**: When `wait_finished=0` and `wait_success=0`, child becomes `Pending`

**Paused children (Audit 2, A3)**: steps 1 and 2 apply to `Paused` children as well as `Waiting` ones — the counter decrements and the cascade-fail both target `status IN ('waiting','paused')`. A pause never strands `wait_*` counters and never shields a task from a required parent's failure (Paused → Failure, propagated recursively, on_failure outbox enqueued as for a Waiting task). Step 3 stays **`Waiting`-only on purpose**: a `Paused` child whose counters reach 0 stays `Paused` (it does NOT auto-transition to `Pending` — resume decides).

### Worker Loops
There are five background workers (spawned in `src/main.rs::spawn_workers`): `start_loop`, `timeout_loop`, `batch_updater`, `retention_cleanup_loop`, and `delivery_loop`. All share the same `watch::Receiver<bool>` shutdown channel.

**Start Loop** (`start_loop`, `src/workers/start_loop.rs`):
1. Finds `Pending` tasks (ordered by priority DESC, then created_at ASC) via paginated keyset scan
2. Checks concurrency rules against running tasks
3. Claims eligible tasks atomically (Pending → Claimed → Running)
4. Executes on_start webhooks **synchronously** (control-flow — its response can register a cancel action; its failure marks the task Failed). **The DB connection is NOT held during the HTTP call (B1):** `execute_webhook_for_task` is split into phases like the delivery loop — phase A borrows a connection to claim the `start` outbox slot + A4 re-check + load the Start actions then **drops it**, phase B runs the on_start HTTP with **no connection held**, phase C re-acquires a connection for the A2/A4 running-transition transaction. This stops a burst of slow on_start webhooks from starving the pool (handlers + the other loops). A failed phase-C re-acquire leaves the task `Claimed` with a `pending` start row — recovered by requeue-stale + the A2 freshness bound, exactly as a process crash would be.
5. On webhook failure: marks task as Failed, propagates to children, enqueues on_failure outbox rows (in-tx)

**Timeout Loop** (`timeout_loop`, `src/workers/timeout_loop.rs`):
1. Finds `Running` tasks where `last_updated < now - timeout` (in seconds)
2. Marks them as `Failure` with reason "Timeout"
3. Propagates failure to dependent children
4. Enqueues on_failure outbox rows (in the same transaction)

**Delivery Loop** (`delivery_loop`, `src/workers/delivery_loop.rs`) — the webhook outbox drainer. `run_delivery_once` runs in **four phases** instead of one long transaction (so HTTP never holds a lock or a connection, and deliveries within a batch run in parallel):
1. **Claim (short tx, lease).** `claim_due_outbox_leased` selects mature `pending` end/cancel/batch_complete rows (`next_attempt_at <= now()`, `FOR UPDATE SKIP LOCKED`, gated so an `end`/`cancel` row waits until the task's `start` row is no longer `pending` — per-task ordering, **bounded by freshness**: the gate only holds while the pending `start` row's `updated_at > now() - WORKER_CLAIM_TIMEOUT_SECS` (passed as `start_stale_secs`), so a start row that never completes eventually stops blocking end/cancel — Audit 2, A2) AND pushes their `next_attempt_at = now() + WEBHOOK_DELIVERY_LEASE_SECS` in one `UPDATE … FROM (SELECT … FOR UPDATE SKIP LOCKED) … RETURNING` statement, then commits. The **lease** is a soft lock: a concurrent worker / next iteration won't re-claim a leased row; on crash mid-delivery the lease expires and the row matures again (at-least-once). The lease does **not** bump `attempts`.
2. **Prefetch (autocommit reads).** For each row, load its delivery inputs (task + actions, or `batch.on_complete` + stats — terminal state is immutable, so the reads are stable). Fast-paths resolved here: task/batch gone ⇒ mark `success`; malformed batch payload ⇒ `exhausted`; zero actions ⇒ `success`.
3. **Deliver (parallel, no DB).** HTTP executions run concurrently via `futures_util::stream::buffer_unordered(WEBHOOK_DELIVERY_CONCURRENCY)`; no connection is held during HTTP. Actions of a *single* row stay sequential.
4. **Mark (short autocommit statements).** Each outcome is posted with the existing `mark_outbox_*` helpers: success ⇒ `status='success'`; failure ⇒ `attempts+1`, `last_error`, `next_attempt_at = now() + backoff` (overwrites the lease); after `WEBHOOK_MAX_ATTEMPTS` ⇒ `status='exhausted'` + metric. A failed mark is logged and skipped (it does **not** roll back marks already posted for other rows; the lease re-delivers — at-least-once).

Exposed as `run_delivery_once` for deterministic test driving (signature unchanged: `(evaluator, conn, cfg)` → number of rows processed).

**Important**: The timeout is based on `last_updated`, NOT `started_at`. This means batch counter updates (via `PUT /task/{id}`) reset the timeout clock, preventing active tasks from being incorrectly timed out.

## Code Architecture

### Entry Points
- `src/main.rs` - HTTP server startup, migration, worker spawning
- `src/test_server.rs` - Test server binary for integration tests
- `src/cache_helper.rs` - Cache utility binary

### HTTP Handlers (`src/handlers.rs`)
All HTTP handler functions and route configuration:
- `configure_routes` - Registers all routes on the Actix `ServiceConfig`
- `health_check` / `readiness_check` - Health and readiness probes
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
- `maybe_enqueue_batch_complete[_for_task]` / `insert_batch` / `load_batch_on_complete` / `batch_completion_stats` - Batch-complete webhook support (Lot 3b)
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
- `check_concurrency` - Evaluates concurrency rules
- `start_task_phase_a` - Connection-holding preamble of on_start (claim slot + A4 re-check + load actions); the on_start HTTP then runs with no connection held (B1)
- `end_task` - Executes on_success/on_failure webhooks
- `batch_updater` - Batches success/failure count updates to database (see below)

**`src/action.rs`**:
- `ActionExecutor` - Executes webhook actions, passes `?handle=<host>/task/<id>` query param
- `WebhookParams` - URL, HTTP verb, optional body and headers
- **Delivery-time SSRF resolver (Audit 2, A5)**: when SSRF validation is active (release / `SKIP_SSRF_VALIDATION=0`), `ActionExecutor` installs a custom reqwest DNS resolver (`SsrfGuardResolver`) that re-checks every resolved IP at request time and **refuses to connect if any is internal/reserved** (`is_internal_ip`, incl. IPv4-mapped v6) — closing the DNS-rebinding window (creation-time validation only saw the name; reqwest re-resolves at delivery, possibly across retries). A blocked resolution fails the delivery, which the outbox retries (at-least-once). **IP-literal URLs bypass this resolver** (reqwest connects to literals without DNS) — they are covered by the creation-time check. **Blocked hostnames/suffixes stay a creation-time concern** (the resolver filters only on the resolved IP). When SSRF is skipped (debug/tests), the stock resolver is used — unchanged behaviour, so tests webhooking to `127.0.0.1` keep working. Built via `ActionExecutor::with_security_config` (also the test seam for a strict executor without touching the global config).

**`src/circuit_breaker.rs`**:
- `CircuitBreaker` - State machine (Closed -> Open -> HalfOpen) for DB pool resilience
- Records successes/failures and trips when threshold exceeded

**`src/validation.rs`**:
- `validate_task_batch` - Validates entire batch before insertion. Enforces the A10 structural limits (`MAX_TASKS_PER_BATCH`/`MAX_DEPS_PER_TASK`/`MAX_ACTIONS_PER_TASK`, read from the `LIMITS_CONFIG` `OnceLock` — same pattern as `SECURITY_CONFIG`, default fallback for tests). **No cycle-detection pass**: the forward-reference rule ("a dependency must appear before the task in the batch") already makes cycles impossible by construction, so the old recursive DFS (a stack-overflow DoS vector) was removed.
- SSRF protection on webhook URLs (creation-time). Matches on `url.host()` so **IPv6 literals** (`[::1]`, `[fd00::1]`, `[::ffff:10.0.0.1]`) are actually inspected — the old `host_str().parse::<IpAddr>()` failed on the bracketed form and let them through in release (Audit 2, A5). `is_internal_ip` unwraps IPv4-mapped v6. `validate_webhook_url_with_config` is `pub` (config-injected entry point; used by tests to exercise the strict path without the global `OnceLock`).
- Cycle exclusion via the forward-reference rule (no separate cycle-detection pass — see `validate_task_batch` above)

**`src/rule.rs`**:
- `Strategy::Concurency` - Concurrency rule with matcher and max count
- `Matcher` - Matches on status, kind, and metadata fields

### Batch Updater Architecture

The batch updater (`src/workers.rs`) efficiently handles high-throughput success/failure counter updates:

```
+----------------+     channel      +-------------------------------------+
|   Handlers     | ---------------> |          Receiver Task               |
| (HTTP reqs)    |   UpdateEvent    |  - Accumulates counts in DashMap     |
+----------------+                  |  - No blocking, per-shard locks      |
                                    +-------------------------------------+
                                                   |
                                                   | DashMap (concurrent)
                                                   v
                                    +-------------------------------------+
                                    |         Updater Loop                 |
                                    |  - Swaps counts atomically           |
                                    |  - Persists to DB                    |
                                    |  - Re-queues on failure              |
                                    +-------------------------------------+
```

Key design decisions:
- **DashMap**: Lock-free concurrent HashMap with per-shard locking
- **Atomic counters**: `AtomicI32` for success/failures within each entry
- **No data loss (transient only)**: only *transient* DB failures re-add counts for retry. Two classes of failure are **dropped instead of re-queued** (contract change, audit A7): (a) a flush landing on a task that has become **terminal** — the flush SQL is gated by `AND task.status NOT IN ('success','failure','canceled')`, so a terminal task's counters stay frozen (a terminal task's counters were already delivered with its end notification; re-applying would diverge forever, and a re-queue would loop forever); (b) a **poison row** — on a batch-flush error the updater falls back to per-row; if some rows succeed while others keep failing, the connection is demonstrably alive, so the failing rows are deterministically faulty and are dropped + logged (`record_batch_update_failure`) rather than wedging the whole pipeline. Only an *all-rows-fail* per-row pass (DB/connection down) re-queues.
- **Overflow-safe**: the flush computes `LEAST(task.<c>::bigint + delta, 2147483647)` — the sum is done in `bigint` (no `int4` overflow) and clamped to `i32::MAX`, so a long-lived high-throughput counter can never poison the flush with `integer out of range`.
- **Shutdown drain**: on shutdown the receiver task drains all still-buffered channel events (`try_recv`) and is joined **before** the final flush snapshots the map, so in-flight events are never lost at shutdown.
- **Cleanup**: Zero-count entries removed periodically

## Testing

### Unit Tests
Located in test modules within source files:
- **`src/workers.rs`**: Batch updater tests (Entry accumulation, swap/requeue, DashMap concurrency, cleanup)
- **`src/validation.rs`**: Input validation, SSRF protection, circular dependency detection
- **`src/config.rs`**: Configuration defaults
- **`src/error.rs`**: Error handling

### Integration Tests (`tests/`)
Uses testcontainers for PostgreSQL. Split into focused test files with shared helpers:

**Shared helpers** (`tests/common/`):
- `setup.rs` — `TestApp`, `setup_test_db()`, DB migrations, `test_service!` macro
- `state.rs` — `test_config()`, `create_test_state()`, `TestStateWithBatchUpdater`
- `builders.rs` — `task_json()`, `task_with_deps()`, `webhook_action()`
- `assertions.rs` — `setup_test_app()`, `create_tasks_ok()`, `get_task_ok()`, `assert_task_status()`, `succeed_task()`, `fail_task()`, `read_wait_counters()`

**Test files**:
- `test_health.rs` — Health check endpoint (1 test)
- `test_crud.rs` — Task CRUD operations (4 tests)
- `test_dag.rs` — DAG dependency creation patterns (7 tests)
- `test_filtering.rs` — Listing, filtering, pagination (6 tests)
- `test_status.rs` — Status transitions: pause, cancel, update (5 tests)
- `test_propagation.rs` — Dependency propagation on success/failure (6 tests)
- `test_dedupe.rs` — Deduplication logic + bug #7 regressions (3 tests)
- `test_concurrency.rs` — Concurrency rules storage (3 tests)
- `test_actions.rs` — Action configuration (1 test)
- `test_edge_cases.rs` — Large metadata, special characters (2 tests)
- `test_batch_update.rs` — Batch counter updates (3 tests)
- `test_bug_audit1.rs` — Bug regressions: bugs #1-4, #8 (9 tests)
- `test_bug_audit2.rs` — Bug regressions: bugs #9-11, #16, #18-19 (8 tests)
- `test_regressions.rs` — Regression tests: on_start failure, timeout+webhook, batch keepalive timeout, pagination overflow (5 tests)
- `test_priority.rs` — Priority scheduling (6 tests)
- `test_outbox.rs` — Webhook transactional outbox (Lot 2): crash-window delivery, retry-then-success, no double delivery, start-before-end ordering, exhausted + `/webhook-deliveries`, fast PATCH under slow downstream (6 tests)
- `test_batch_complete.rs` — Lot 3: grouped insertion (mixed batch state, intra-batch dedupe match, dedupe-skipped-parent child Pending) + `on_batch_complete` webhook (both body shapes, fires-once-on-last-terminal, via stop_batch/timeout, single row under concurrent detection, empty-batch immediate signal, payload contents, retry+exhausted, SSRF validation) (12 tests)

(Test files live in `tests/integration/`, declared in `tests/integration/main.rs`; shared helpers in `tests/integration/common/`. The list above is indicative, not exhaustive — also present: `test_claim_loop`, `test_cancel_webhook`, `test_idempotency`, `test_parallel_webhooks`, `test_stop_batch`, `test_dead_end_cancel`, `test_batch_rules`, `test_batch_stats`, `test_propagation_edge`, `test_requeue_stale`, `test_validation_e2e`, `test_webhook_flows`.)

### Manual Testing (`test/test.ts`)
Bun script for manual API testing:
```bash
bun test.ts dag      # Create CI/CD pipeline DAG
bun test.ts single   # Create single task
bun test.ts list     # List tasks
bun test.ts update <id> Success|Failure
bun test.ts view <batch_id>
```

## Common Tasks

### Adding a New Endpoint
1. Add handler function in `src/handlers.rs`
2. Register route in `configure_routes`
3. Add any new DTOs in `src/dtos.rs`
4. Add database operations in `src/db_operation.rs`

### Adding a New Task Status
1. Add variant to `StatusKind` enum in `src/models.rs`
2. Add migration for the enum value
3. Update `propagate_to_children` if it affects propagation
4. Update `cancel_task` if it should be cancelable from this state

### Fixing a Bug
When fixing a bug, always add an integration test in the appropriate `tests/test_bug_audit*.rs` file that reproduces the bug scenario and verifies the fix. The test should:
1. Be named `test_bug<N>_<short_description>` (e.g. `test_bug7_dedupe_not_over_aggressive_when_metadata_is_none`)
2. Include a doc comment explaining the original bug, the fix, and what the test asserts
3. Assert the **correct** behavior (test passes when the fix is in place, fails if reverted)
4. Use shared helpers from `tests/common/` (`setup_test_app`, `create_tasks_ok`, `succeed_task`, etc.)

Existing bug regression tests are in `tests/test_bug_audit1.rs` (bugs #1-4, #8) and `tests/test_bug_audit2.rs` (bugs #9-19).

### Modifying Propagation Logic
Key file: `src/workers.rs`, function `propagate_to_children`
- `parent_succeeded` - Check if parent was successful
- `parent_failed` - Check if parent failed OR was canceled
- Recursive call for cascading failures

## Important Invariants

1. **Propagation is recursive**: When a child is marked as failed due to parent, it must propagate to its own children
2. **Canceled = Failed for propagation**: `Canceled` status is treated like `Failure` when propagating to children
3. **wait_finished vs wait_success**:
   - `wait_finished` counts ALL dependencies
   - `wait_success` counts only `requires_success=true` dependencies
4. **Actions are per-task**: Each task has its own action records, not shared
5. **Route configuration is centralized**: All routes in `handlers::configure_routes`, shared by main server and test server
6. **Timeout uses `last_updated`, not `started_at`**: The timeout loop must compare `last_updated` against the timeout duration. Using `started_at` would cause tasks that are actively receiving batch updates (failures/successes via PUT) to be incorrectly timed out. See `test_recent_batch_update_prevents_timeout` regression test. Note: the timeout transition intentionally does NOT update `last_updated` — it preserves when the task last showed real activity (`ended_at` captures when timeout occurred).
7. **Dependencies are intra-batch only**: `Dependency.id` references local IDs within the same `POST /task` batch. Cross-batch links are not possible via the API. This means `stop_batch` safely cancels all tasks without needing per-task propagation — both sides of every link are in the same batch.

## Database Schema

```sql
-- Core tables
task (id, name, kind, status, metadata, timeout, batch_id, start_condition,
      wait_success, wait_finished, success, failures, failure_reason,
      created_at, started_at, ended_at, last_updated, priority)
action (id, task_id, kind, trigger, condition, params, success)
link (parent_id, child_id, requires_success)
batch (id, on_complete, created_at, scope, metadata)
  -- one row per batch that registered on_batch_complete (Lot 3b) AND/OR scope/metadata (#601);
  -- on_complete = JSONB array of NewActionDto ('[]' for a scope/metadata-only batch).
  -- scope = nullable TEXT label, metadata = JSONB (default '{}') — both filterable/searchable
  --   via GET /batches (?scope= exact, ?metadata= JSONB containment @>, ?search= substring).
  -- Batches with none of on_complete/scope/metadata have no row (tracked only via task.batch_id).
webhook_execution (id, task_id, trigger, condition, idempotency_key,
                   status, attempts, created_at, updated_at,
                   next_attempt_at, last_error, batch_id)
  -- status: pending | success | failure | exhausted
  -- trigger: start | end | cancel | batch_complete
  -- doubles as the transactional outbox for end/cancel webhooks (Lot 2) and the
  -- batch_complete webhook (Lot 3b):
  --   task_id  is NULL for batch_complete rows; batch_id is set instead
  --   CHECK (task_id IS NOT NULL OR batch_id IS NOT NULL)
  --   next_attempt_at = when the row is eligible for (re)delivery (DEFAULT now())
  --   last_error      = last delivery error (diagnostics)

-- FK constraints (relevant for cleanup ordering)
action.task_id -> task.id
webhook_execution.task_id -> task.id
webhook_execution.batch_id -> batch.id
link.parent_id -> task.id
link.child_id -> task.id

-- Key indexes
task_status_kind_idx ON task(status, kind)
task_batch_id_idx ON task(batch_id)
link_parent_id_idx ON link(parent_id)
link_child_id_idx ON link(child_id)
idx_webhook_execution_task_id ON webhook_execution(task_id)
idx_webhook_execution_status ON webhook_execution(status)
idx_webhook_execution_pending_due ON webhook_execution(next_attempt_at) WHERE status = 'pending'
idx_webhook_execution_batch_id ON webhook_execution(batch_id) WHERE batch_id IS NOT NULL
idx_task_priority ON task(status, priority DESC, created_at ASC)
idx_batch_scope ON batch(scope) WHERE scope IS NOT NULL
idx_batch_metadata_gin ON batch USING GIN(metadata)
```

## Metrics

Prometheus metrics in `src/metrics.rs`:
- Task lifecycle: created, completed, cancelled, timed out, failed by dependency
- Status transitions and current status gauges
- Concurrency blocks
- Worker loop duration, iterations, tasks processed per loop
- Webhook executions and duration
- Webhook outbox delivery: retries, exhausted, delivery lag (now - created_at at delivery time)
- Task execution duration and wait time
- Dependency propagations, unblocked tasks
- Batch update failures, DB save failures
- Database query duration and slow query detection
- Circuit breaker state transitions and rejections

## Configuration

All configuration is via environment variables (loaded in `src/config.rs`):

**Required:**
- `DATABASE_URL` - PostgreSQL connection string
- `HOST_URL` - Public URL for webhook callbacks

**Optional:**
- `PORT` (default: 8085) - Server port
- `POOL_MAX_SIZE` (default: 10) - Max pool connections
- `POOL_MIN_IDLE` (default: 5) - Min idle connections
- `POOL_ACQUIRE_RETRIES` (default: 3) - Connection acquire retries
- `POOL_TIMEOUT_SECS` (default: 30) - Connection timeout
- `PAGINATION_DEFAULT` (default: 50) - Default items per page
- `PAGINATION_MAX` (default: 100) - Max items per page
- `WORKER_LOOP_INTERVAL_MS` (default: 1000) - Worker loop interval
- `WORKER_START_BATCH_SIZE` (default: 50) - Max claims per start_loop iteration (claim cap). The Pending backlog is scanned page-by-page via keyset pagination (internal page size ~500) so the full backlog stays visible; only the number of claims per iteration is capped, never visibility. Early stop only fires once this cap is reached.
- `WORKER_WEBHOOK_CONCURRENCY` (default: 10) - Max concurrent on_start webhook executions (should not exceed `POOL_MAX_SIZE`)
- `WEBHOOK_DELIVERY_INTERVAL_MS` (default: 1000) - Interval between webhook delivery-loop iterations (outbox drain)
- `WEBHOOK_DELIVERY_BATCH_SIZE` (default: 50) - Max outbox rows claimed per delivery-loop iteration
- `WEBHOOK_DELIVERY_LEASE_SECS` (default: 120, must be >= 1) - Lease applied to an outbox row at claim time; the row is not re-claimable until the lease expires. Must exceed the worst-case single-row delivery time so an in-flight delivery is never double-claimed.
- `WEBHOOK_DELIVERY_CONCURRENCY` (default: 10, must be >= 1) - Max concurrent HTTP deliveries within one delivery-loop batch (`buffer_unordered` bound)
- `WEBHOOK_MAX_ATTEMPTS` (default: 10) - Delivery attempts before an outbox row is marked `exhausted`
- `WEBHOOK_RETRY_BACKOFF_BASE_SECS` (default: 2) - Base of the exponential retry backoff (delay = base^attempt, capped)
- `WEBHOOK_RETRY_BACKOFF_CAP_SECS` (default: 300) - Cap on the retry backoff delay
- `BATCH_CHANNEL_CAPACITY` (default: 100) - Batch update channel size
- `CIRCUIT_BREAKER_ENABLED` (default: 1) - Enable circuit breaker
- `CIRCUIT_BREAKER_FAILURE_THRESHOLD` (default: 5) - Failures before opening
- `CIRCUIT_BREAKER_FAILURE_WINDOW_SECS` (default: 10) - Failure counting window
- `CIRCUIT_BREAKER_RECOVERY_TIMEOUT_SECS` (default: 30) - Time before half-open
- `CIRCUIT_BREAKER_SUCCESS_THRESHOLD` (default: 2) - Successes to close
- `SLOW_QUERY_THRESHOLD_MS` (default: 100) - Slow query warning threshold
- `TRACING_ENABLED` (default: 0) - Enable distributed tracing
- `OTEL_EXPORTER_OTLP_ENDPOINT` - OTLP endpoint URL
- `OTEL_SERVICE_NAME` (default: arcrun) - Service name for traces
- `OTEL_SAMPLING_RATIO` (default: 1.0) - Sampling ratio
- `SKIP_SSRF_VALIDATION` (default: 1 in debug, 0 in release) - Skip SSRF checks
- `BLOCKED_HOSTNAMES` - Comma-separated blocked hostnames
- `BLOCKED_HOSTNAME_SUFFIXES` - Comma-separated blocked hostname suffixes
- `MAX_TASKS_PER_BATCH` (default: 1000) - Structural limit (Audit 2, A10): max tasks accepted in one `POST /task` batch. Over the limit ⇒ 400.
- `MAX_DEPS_PER_TASK` (default: 100) - Structural limit (A10): max dependencies a single task may declare. Over ⇒ 400.
- `MAX_ACTIONS_PER_TASK` (default: 20) - Structural limit (A10): max actions per task (on_start + on_failure + on_success). Over ⇒ 400.
- `PAYLOAD_MAX_BYTES` (default: 2 MiB) - Explicit `web::JsonConfig` body-size cap (A10); larger request bodies ⇒ 413. Matches the historical implicit actix default, so non-breaking.
- `AUTH_TOKEN` (default: unset ⇒ auth disabled) - Optional static bearer token (Audit 2, A6). When set, an actix `from_fn` middleware (`src/auth.rs`) requires `Authorization: Bearer <token>` on **every** endpoint (including `/metrics`, Swagger UI, `/view`) **except** `/health` and `/ready` (k8s probes). Comparison is constant-time (manual byte XOR — `subtle` is only a transitive dep). Unset/blank ⇒ total pass-through (historical open behavior), with a loud release-build warning at startup. Token is header-only (never a query string), so `/view` needs a reverse proxy injecting the header. The `?handle=` capability URL is NOT gated here (deferred to a later breaking lot).
- `RUST_LOG` (default: info) - Log level

## Project Structure

```
src/
+-- main.rs           # HTTP server, routes, and startup
+-- test_server.rs    # Test server binary
+-- cache_helper.rs   # Cache utility binary
+-- lib.rs            # Module declarations, DB pool initialization
+-- handlers.rs       # HTTP handlers and route configuration
+-- models.rs         # Database models (Task, Action, Link, enums)
+-- dtos.rs           # API DTOs and query parameters
+-- schema.rs         # Diesel schema (auto-generated)
+-- db_operation.rs   # Database operations
+-- workers.rs        # Background worker loop, propagation, batch updater
+-- action.rs         # Webhook action execution
+-- rule.rs           # Concurrency rules and matchers
+-- config.rs         # Configuration loading from env vars
+-- metrics.rs        # Prometheus metrics
+-- validation.rs     # Input validation and SSRF protection
+-- error.rs          # Typed error definitions
+-- circuit_breaker.rs # Circuit breaker for DB pool resilience
+-- tracing.rs        # OpenTelemetry distributed tracing
+-- helper.rs         # Internal helpers
static/
+-- dag.html          # DAG visualization UI
test/
+-- test.ts           # Manual testing script (Bun)
migrations/           # Diesel migrations
tests/
+-- common/               # Shared test helpers
|   +-- mod.rs            # Re-exports + test_service! macro
|   +-- setup.rs          # TestApp, DB setup, migrations
|   +-- state.rs          # Test config/state factories
|   +-- builders.rs       # Task JSON builders
|   +-- assertions.rs     # HTTP + DB assertion helpers
+-- test_health.rs        # Health check (1 test)
+-- test_crud.rs          # CRUD operations (4 tests)
+-- test_dag.rs           # DAG patterns (7 tests)
+-- test_filtering.rs     # Filtering + pagination (6 tests)
+-- test_status.rs        # Status transitions (5 tests)
+-- test_propagation.rs   # Dependency propagation (6 tests)
+-- test_dedupe.rs        # Deduplication (3 tests)
+-- test_concurrency.rs   # Concurrency rules (3 tests)
+-- test_actions.rs       # Action config (1 test)
+-- test_edge_cases.rs    # Edge cases (2 tests)
+-- test_batch_update.rs  # Batch counters (3 tests)
+-- test_bug_audit1.rs    # Bug regressions #1-4, #8 (9 tests)
+-- test_bug_audit2.rs    # Bug regressions #9-19 (8 tests)
+-- test_priority.rs      # Priority scheduling (6 tests)
```
