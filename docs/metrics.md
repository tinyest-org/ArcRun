# Prometheus Metrics

All metrics are exposed at `GET /metrics` in Prometheus format.

## Task Counters

| Metric | Labels | Description |
|--------|--------|-------------|
| `tasks_created_total` | - | Total tasks created |
| `tasks_completed_total` | `outcome`, `kind` | Tasks completed by outcome and kind |
| `tasks_cancelled_total` | - | Tasks cancelled |
| `tasks_timed_out_total` | - | Tasks timed out |
| `task_status_transitions_total` | `from_status`, `to_status` | Status transitions |

## Task Gauges

| Metric | Labels | Description |
|--------|--------|-------------|
| `tasks_by_status` | `status` | Current tasks by status (sampled every `METRICS_SAMPLER_INTERVAL_SECS`; `status` is the lowercase DB enum label) |
| `running_tasks_by_kind` | `kind` | Running tasks by kind (sampled) |

## Dependencies

| Metric | Labels | Description |
|--------|--------|-------------|
| `tasks_with_dependencies_total` | - | Tasks created with dependencies |
| `dependency_propagations_total` | `parent_outcome` | Dependency propagations |
| `tasks_unblocked_total` | - | Tasks unblocked after dependencies completed |
| `tasks_failed_by_dependency_total` | - | Tasks failed due to parent failure |

## Webhooks

| Metric | Labels | Description |
|--------|--------|-------------|
| `webhook_executions_total` | `trigger`, `outcome` | Webhook calls |
| `webhook_attempts_total` | `trigger`, `outcome` | Webhook attempts (includes failures) |
| `webhook_duration_seconds` | `trigger` | Webhook duration histogram |
| `webhook_idempotent_skips_total` | `trigger` | Webhook executions skipped due to idempotency |
| `webhook_idempotent_conflicts_total` | - | Idempotency conflicts when claiming executions |
| `webhook_delivery_retries_total` | `trigger` | Outbox deliveries that failed and were rescheduled |
| `webhook_delivery_exhausted_total` | `trigger` | Outbox deliveries that exhausted all retries |
| `webhook_delivery_success_total` | `trigger` | Outbox deliveries that succeeded |
| `webhook_delivery_lag_seconds` | - | Lag from outbox row creation to successful delivery |
| `webhook_outbox_pending` | `state` | Outbox backlog depth (`ready`: mature; `leased`: not yet due) |
| `webhook_outbox_oldest_pending_age_seconds` | - | Age of the oldest mature pending outbox row (stuck-row signal) |
| `webhook_mark_failures_total` | `mark` | Outbox mark writes that failed (`success`/`retry`/`exhausted`) |
| `webhooks_in_flight` | `phase` | Webhook executions in progress (`start`, `delivery`) |

## Concurrency

| Metric | Labels | Description |
|--------|--------|-------------|
| `tasks_blocked_by_concurrency_total` | - | Tasks blocked by rules |
| `concurrency_ko_cache_hits_total` | - | Claim-loop blocked-rule cache hits (skipped DB checks) |

## Duration

| Metric | Labels | Description |
|--------|--------|-------------|
| `task_duration_seconds` | `kind`, `outcome` | Task execution duration |
| `task_wait_seconds` | `kind` | Time from task creation to Running (scheduler latency) |

## Worker

| Metric | Labels | Description |
|--------|--------|-------------|
| `worker_loop_iterations_total` | `loop` | Worker loop iterations (per loop: `start`, `timeout`, `batch_updater`, `retention`, `delivery`, `metrics_sampler`) |
| `worker_loop_duration_seconds` | `loop` | Worker loop duration (per loop) |
| `worker_loop_last_iteration_timestamp_seconds` | `loop` | Unix ts of each loop's last iteration (liveness heartbeat) |
| `tasks_processed_per_loop` | - | Tasks processed per start-loop iteration |

## Database

| Metric | Labels | Description |
|--------|--------|-------------|
| `db_query_duration_seconds` | `query` | Query duration |
| `slow_queries_total` | `query` | Queries exceeding threshold |
| `db_pool_acquire_failures_total` | - | Failures to acquire a pool connection after all retries |
| `db_pool_acquire_wait_seconds` | - | Time to acquire a pool connection (HTTP path) |
| `db_pool_connections` | `state` | Pool connections by state (`in_use`, `idle`); sampled |
| `tasks_db_save_failures_total` | - | DB save failures after retries |
| `batch_update_failures_total` | - | Batch update failures (re-queued) |

## Batch Updater (PUT /task)

| Metric | Labels | Description |
|--------|--------|-------------|
| `batch_update_events_total` | - | Counter-update events accepted by the PUT handler |
| `batch_channel_send_wait_seconds` | - | Time awaiting the channel `send()` (backpressure signal) |
| `batch_channel_capacity_available` | - | Available channel permits, sampled at send time |
| `batch_updater_flush_rows` | - | Task rows persisted per flush |
| `batch_updater_flush_duration_seconds` | - | Duration of a DB flush |
| `batch_updater_pending_tasks` | - | Distinct tasks with un-persisted counters (crash-loss window) |

## Business / Batch & Claim

| Metric | Labels | Description |
|--------|--------|-------------|
| `tasks_deduped_total` | - | Tasks skipped by a `dedupe_strategy` match on insert |
| `batch_insert_tasks` | - | Tasks per `POST /task` batch |
| `batches_completed_total` | - | `batch_complete` signals enqueued (last task terminal) |
| `claim_pages_scanned` | - | Keyset pages scanned per start-loop claim iteration |

## Circuit Breaker

| Metric | Labels | Description |
|--------|--------|-------------|
| `circuit_breaker_state_transitions_total` | `to_state` | State transitions |
| `circuit_breaker_rejections_total` | - | Requests rejected |
