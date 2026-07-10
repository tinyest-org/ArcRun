# Worker Loops & Rules

There are five background workers (spawned in `src/main.rs::spawn_workers`): `start_loop`, `timeout_loop`, `batch_updater`, `retention_cleanup_loop`, and `delivery_loop`. All share the same `watch::Receiver<bool>` shutdown channel.

See also: [architecture.md](architecture.md) (lifecycle, code map, schema), [webhooks.md](webhooks.md) (delivery loop detail, outbox contract).

## In-process nudges (Audit 2, B4)

To avoid every DAG edge paying a full poll tick, handlers and workers wake the `start`/`delivery` loops immediately via a shared `WorkerNudges` (two `tokio::sync::Notify`, held in `AppState` and passed to `spawn_workers`). After a committing transition, producers `notify_one` the relevant loop (`add_task`/`resume_task` → start; `update_task`/`cancel_task`/`stop_batch`/timeout+on_start failures → delivery; a real `update_task`/`cancel` transition → both) and the loop runs one extra iteration instead of waiting the interval. `notify_one` stores a permit, so a nudge fired mid-iteration is never lost. The nudge is **best-effort — the poll (`WORKER_LOOP_INTERVAL_MS` / `WEBHOOK_DELIVERY_INTERVAL_MS`) remains the correctness/fallback** (a missed or extra nudge only costs, at worst, one empty iteration). In-process only; a multi-replica deployment would use LISTEN/NOTIFY as the same kind of optimization, never as correctness.

## Start Loop (`start_loop` / `start_loop_leased`, `src/workers/start_loop.rs`)

0. **Leader lease (Audit 2, D7)**: production uses `start_loop_leased` — each iteration is gated by a session `pg_try_advisory_lock` (`START_LEADER_LOCK_KEY`) held on a **dedicated non-pooled connection** (`establish_direct_connection`), so in a multi-replica deployment exactly one replica schedules at a time; a standby just re-contends each tick and takes over automatically when the leader's connection drops. Single-replica is unchanged (the sole loop always wins). `start_loop` (no lease, always leader) remains the test entry point. Gauge: `start_loop_is_leader`.
1. Finds `Pending` tasks (ordered by priority DESC, then created_at ASC) via paginated keyset scan
2. Checks concurrency rules — **DB-enforced via `rule_slot` counters** (see "Concurrency & Capacity rules" below)
3. Claims eligible tasks atomically (Pending → Claimed → Running). **While a Claimed task waits for the concurrency semaphore permit (B2)**, `acquire_permit_with_heartbeat` bumps its `last_updated` every `claim_timeout / 3` via `tokio::select!`, preventing `requeue_stale_claimed_tasks` from reclaiming it.
4. Executes on_start webhooks **synchronously** (control-flow — its response can register a cancel action; its failure marks the task Failed). **The DB connection is NOT held during the HTTP call (B1):** `execute_webhook_for_task` is split into phases like the delivery loop — phase A borrows a connection to claim the `start` outbox slot + A4 re-check + load the Start actions then **drops it**, phase B runs the on_start HTTP with **no connection held**, phase C re-acquires a connection for the A2/A4 running-transition transaction. This stops a burst of slow on_start webhooks from starving the pool (handlers + the other loops). A failed phase-C re-acquire leaves the task `Claimed` with a `pending` start row — recovered by requeue-stale + the A2 freshness bound, exactly as a process crash would be.
5. On webhook failure: marks task as Failed, propagates to children, enqueues on_failure outbox rows (in-tx)

## Concurrency & Capacity rules — DB-enforced via `rule_slot` (Audit 2, D1 — 7.3a/7.3b)

Each rule of the candidate maps to a canonical textual key (`rule::concurrency_slot_key` → `conc:…`, `rule::capacity_slot_key` → `cap:…`); the claim transaction increments each slot with a conditional upsert (`ON CONFLICT DO UPDATE SET used = used + $inc WHERE used < $threshold RETURNING used`, keys processed sorted + deduped across both prefixes — A9 discipline) and a blocked slot rolls the whole claim back (`RuleBlocked`).

- **Concurrency** increments by `1` against `max_concurency`.
- **Capacity** increments by the candidate's **charge** = `GREATEST(expected_count - success - failures, 0)` against `max_capacity` (the admission check is on *others'* current sum — the candidate's own charge is not counted, so overshoot is allowed, matching the old probe; a missing `expected_count` or a `max_capacity <= 0` ⇒ `RuleBlocked` Rust-side — the fresh-INSERT upsert arm has no `used < threshold` check, so the `<= 0` guard preserves the old always-block behavior).

O(1) per claim (no COUNT/SUM over `task`), replica-safe by row locking; the concurrency AND capacity advisory-lock + CTE-SUM layers are gone. **Semantic change (D1, assumed)**: a slot counts only tasks that claimed *through* the rule — a Running task that merely matches the matcher but carries no rule no longer blocks candidates (holds for both rule kinds).

The consumed keys are persisted in `task.claimed_slot_keys` and the charge in `task.capacity_charge` by the claim UPDATE (charge set NULL when no Capacity rule — paranoia against staleness), and **released** (decrement — by 1 per `conc:` key, by the *stored* `capacity_charge` per `cap:` key — + keys and charge NULLed, `release_slots_for_tasks`) in the same transaction as EVERY exit from Claimed/Running: success/failure PATCH, on_start failure, timeout, cancel of a Claimed/Running task, stop_batch, dead-end-canceled ancestors, and requeue-stale (Claimed → Pending). Keys and charge are never recomputed at release (metadata/expected_count are mutable while Running).

**Capacity deltas are pushed by the batch_updater flush** (`handle_batch_with_counts`): in the same flush transaction (cap-slot pre-lock sorted FIRST, then the ordered task pre-lock — A9 slot-before-task discipline; the no-capacity common case costs one empty SELECT), each flushed task's `capacity_charge` shrinks to `GREATEST(LEAST(old, expected - success - failures), 0)` (monotonically non-increasing — a raised expected_count never raises the charge) and the shrink is decremented from its `cap:` slots, freeing capacity as Running tasks report progress via PUT. Divergence (accepted, user decision): a direct PATCH (counter-only `update_running_task`, or metadata/expected_count) does NOT push capacity deltas — only the flush does; the slot may read higher than true remaining (conservative — blocks more, never leaks) and is fully reconciled at release since release uses the stored charge.

Empty (`used = 0`) slot rows are GC'd by the retention loop (which now always runs — the task retention stays gated by `RETENTION_ENABLED`, the slot GC does not).

## Retention Loop (`retention_cleanup_loop`, `src/workers/retention.rs`)

The loop ALWAYS runs (its `rule_slot` GC must happen even when task retention is off). When `RETENTION_ENABLED=1`, each pass:

1. **Moves** terminal tasks (Success/Failure/Canceled) with `ended_at` older than `RETENTION_DAYS` into the cold `task_archive` table (Audit 2, D6 / 7.5b) — `cleanup_old_terminal_tasks`. This is an atomic `WITH moved AS (DELETE FROM task ... RETURNING <cols>) INSERT INTO task_archive (<cols>, archived_at) SELECT <cols>, now() FROM moved` (explicit column lists both sides), in the SAME transaction as the deletion of the tasks' actions → webhook rows → links (FK order). The task record survives (still served by `GET /task/{id}`); its tooling does not. The orphan-`batch` sweep is unchanged — a batch whose tasks are all archived has no `task` rows left, so it is swept (its `batch_id` lives on in `task_archive` without an FK), unless a `batch_complete` signal is still queued.
2. **Purges** the archive when `RETENTION_ARCHIVE_DAYS > 0` — `purge_old_archived_tasks` bounded-DELETEs `task_archive` rows with `archived_at` older than that window (`0` = keep forever, the default). This is the only thing that bounds archive growth.

Both steps are batched by `RETENTION_BATCH_SIZE` and gated by `RETENTION_ENABLED`; the `rule_slot` GC runs regardless.

## Timeout Loop (`timeout_loop`, `src/workers/timeout_loop.rs`)

0. **Requeues stale `Claimed` tasks first**, once per iteration, before the timeout drain — so a mass-timeout can never starve the requeue.
1. Finds up to `WORKER_TIMEOUT_BATCH_SIZE` (default 100) `Running` tasks where `last_updated < now - timeout` (in seconds), oldest-first (**bounded per pass**, Audit 2, B7)
2. Marks them as `Failure` with reason "Timeout" (one tx per task)
3. Propagates failure to dependent children
4. Enqueues on_failure outbox rows (in the same transaction)
5. **Bounded drain**: if a pass returned a full batch there may be more, so it re-fetches immediately (no tick wait) until a short pass or a safety cap of `MAX_TIMEOUT_DRAIN_PASSES` (50) is hit — a mass-timeout of thousands of tasks no longer pins the loop for minutes and delays the stale-Claimed requeue.

**Important**: The timeout is based on `last_updated`, NOT `started_at`. This means batch counter updates (via `PUT /task/{id}`) reset the timeout clock, preventing active tasks from being incorrectly timed out.

## Delivery Loop

The webhook outbox drainer — see [webhooks.md](webhooks.md#delivery-loop).

## Batch Updater (`src/workers.rs` / `src/workers/batch_updater.rs`)

The batch updater efficiently handles high-throughput success/failure counter updates:

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
- **Capacity-slot deltas (D1 / 7.3b)**: the flush transaction also shrinks each flushed capacity-holding task's `capacity_charge` to its new remaining work and decrements the shrink from its `cap:` `rule_slot` rows (A9: cap-slot pre-lock sorted FIRST, before the task pre-lock). This is the ONLY mid-run path that frees capacity; the common no-capacity flush pays one empty SELECT. `run_counter_flush_once` drives one flush deterministically for tests.
- **Shutdown drain**: on shutdown the receiver task drains all still-buffered channel events (`try_recv`) and is joined **before** the final flush snapshots the map, so in-flight events are never lost at shutdown.
- **Cleanup**: Zero-count entries removed periodically
