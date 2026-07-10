# API Reference

## Health

### Health Check
```http
GET /health
```

Returns database connectivity status and connection pool stats.

Response `200 OK` (healthy) or `503 Service Unavailable` (degraded):
```json
{
  "status": "ok",
  "database": "healthy",
  "pool_size": 10,
  "pool_idle": 5
}
```

### Readiness Check
```http
GET /ready
```

Stricter check - verifies pool is not exhausted and a connection can be acquired.

Response `200 OK` or `503 Service Unavailable`:
```json
{"status": "ready"}
```

## Tasks

### Create Tasks
```http
POST /task
Content-Type: application/json

[
  {
    "id": "local-id-1",
    "name": "My Task",
    "kind": "data-processing",
    "timeout": 60,
    "metadata": {"key": "value"},
    "on_start": {
      "kind": "Webhook",
      "params": {
        "url": "https://example.com/webhook",
        "verb": "Post",
        "body": {"optional": "payload"},
        "headers": {"X-Custom": "header"}
      }
    },
    "dependencies": [
      {"id": "local-id-0", "requires_success": true}
    ],
    "on_success": [
      {"kind": "Webhook", "params": {"url": "https://example.com/done", "verb": "Post"}}
    ],
    "on_failure": [
      {"kind": "Webhook", "params": {"url": "https://example.com/failed", "verb": "Post"}}
    ],
    "priority": 500,
    "rules": [
      {
        "type": "Concurency",
        "matcher": {"kind": "data-processing", "status": "Running", "fields": []},
        "max_concurency": 5
      }
    ],
    "dedupe_strategy": [
      {"kind": "data-processing", "status": "Pending", "fields": ["key"]}
    ]
  }
]
```

Fields:
- `timeout` (seconds): Maximum inactivity time. The task is marked as `Failure` if `last_updated` exceeds this duration. Batch counter updates (`PUT /task/{id}`) refresh `last_updated`, resetting the timeout clock.
- `priority` (integer, optional): Scheduling priority, range -1000 to 1000, default 0. Higher values are processed first when the worker picks up pending tasks.

Response: `201 Created` with array of created tasks, includes `X-Batch-ID` header.
If all tasks were deduplicated: `204 No Content` with `X-Batch-ID` header.

On validation failure: `400 Bad Request`:
```json
{
  "error": "Validation failed",
  "batch_id": "uuid",
  "details": ["error message 1", "error message 2"]
}
```

### Get Task
```http
GET /task/{task_id}
```

Response: Full task details with all actions (fetched via single LEFT JOIN query).

If the task has been moved to the cold archive by retention (Audit 2, D6), this endpoint still serves it from `task_archive` with the **same response shape** — every field is present, except `actions` is an empty array (an archived task's actions are dropped on archive). Only `GET /task/{id}` reads the archive; listings, the DAG view, and writes (`PATCH`/`PUT`/`DELETE`) do not — a write to an archived task is a `404`.

```json
{
  "id": "uuid",
  "name": "My Task",
  "kind": "data-processing",
  "status": "Running",
  "timeout": 60,
  "priority": 0,
  "rules": [],
  "metadata": {"key": "value"},
  "actions": [
    {"kind": "Webhook", "trigger": "Start", "params": {"url": "...", "verb": "Post"}}
  ],
  "created_at": "2024-01-01T00:00:00Z",
  "started_at": "2024-01-01T00:00:01Z",
  "ended_at": null,
  "last_updated": "2024-01-01T00:00:01Z",
  "success": 0,
  "failures": 0,
  "failure_reason": null,
  "batch_id": "uuid"
}
```

### List Tasks
```http
GET /task?page=0&page_size=50&status=Running&kind=data-processing&batch_id=uuid&name=my&metadata={"key":"value"}
```

Query parameters:
- `page`: Page number (default: 0)
- `page_size`: Items per page (default: 50, max: 100)
- `status`: Filter by status (optional)
- `kind`: Filter by kind (substring match, optional)
- `name`: Filter by name (substring match, optional)
- `batch_id`: Filter by batch ID (optional)
- `metadata`: Filter by metadata JSON containment (optional)

Response: `200 OK` with array of `BasicTaskDto`.

### Update Task
```http
PATCH /task/{task_id}
Content-Type: application/json

{
  "status": "Success",
  "new_success": 10,
  "new_failures": 2,
  "metadata": {"updated": true},
  "priority": 100,
  "failure_reason": "Error message (required if status=Failure)"
}
```

Only `Success` or `Failure` status transitions are allowed. Setting status triggers end actions and dependency propagation. Failed tasks cannot be updated further. The `priority` field can be updated independently of status (range -1000 to 1000).

### Cancel Task
```http
DELETE /task/{task_id}
```

Cancels a pending or running task. For running tasks, executes cancel actions.
**Cancellation propagates** to dependent children that require success.

### Pause Task
```http
PATCH /task/pause/{task_id}
```

Pauses a task (sets status to `Paused`).

### Batch Update (High-throughput)
```http
PUT /task/{task_id}
Content-Type: application/json

{
  "new_success": 5,
  "new_failures": 2
}
```

This endpoint efficiently batches counter updates using a lock-free `DashMap` architecture for high concurrency. At least one of `new_success` or `new_failures` must be non-zero. Returns `202 Accepted` when the update is queued.

Each batch update refreshes the task's `last_updated` timestamp, which resets the timeout clock. This prevents active tasks (still receiving updates) from being incorrectly timed out.

## Batches

### Get Batch Stats
```http
GET /batch/{batch_id}
```

Returns aggregated counters for a batch: total success, total failures, total expected count, and per-status task counts. Use this to track overall progress of a batch.

Response `200 OK` or `404 Not Found`.

### Stop Batch
```http
DELETE /batch/{batch_id}
```

Cancel all non-terminal tasks in a batch. Waiting, Pending, Paused, and Running tasks are set to `Canceled` with failure_reason `"Batch stopped"`. Running tasks with registered cancel webhooks will have those webhooks fired.

Tasks already in a terminal state (Success, Failure, Canceled) are not modified.

Response `200 OK` with counts per status category, or `404 Not Found`.

### Update Batch Rules
```http
PATCH /batch/{batch_id}/rules
Content-Type: application/json

{
  "kind": "data-processing",
  "rules": [
    {
      "type": "Concurency",
      "matcher": {"kind": "data-processing", "status": "Running", "fields": []},
      "max_concurency": 10
    }
  ]
}
```

Update concurrency/capacity rules for all non-terminal tasks of a given kind in a batch. Pass an empty `rules` array to remove all rules.

Response `200 OK` with the count of affected tasks.

### List Batches
```http
GET /batches?page=0&page_size=50
```

Returns a paginated list of batches with aggregated task statistics. Supports filtering by task name, kind, status, and creation time range.

Response `200 OK` with array of batch summaries.

## DAG Visualization

### Get DAG Data
```http
GET /dag/{batch_id}
```

Returns tasks and links for a batch in JSON format:
```json
{
  "tasks": [
    {"id": "uuid", "name": "...", "kind": "...", "status": "Running", "priority": 0, ...}
  ],
  "links": [
    {"parent_id": "uuid", "child_id": "uuid", "requires_success": true}
  ]
}
```

### View DAG UI
```http
GET /view?batch={batch_id}
```

Opens the built-in DAG visualization UI with:
- Cytoscape.js with Dagre auto-layout
- Color-coded nodes by status
- Click on nodes for task details
- Auto-refresh option (5s interval)
- Pan, zoom, and fit controls

## Metrics
```http
GET /metrics
```

Prometheus-format metrics (see [Metrics catalog](metrics.md)).

## Webhook Execution

When a task starts, the `on_start` webhook is called with a `?handle=<host_url>/task/<task_id>` query parameter. This allows the webhook target to update the task status directly.

Webhook params:
```json
{
  "url": "https://example.com/webhook",
  "verb": "Post",
  "body": {"optional": "json payload"},
  "headers": {"X-Custom": "header value"}
}
```

Supported HTTP verbs: `Get`, `Post`, `Put`, `Patch`, `Delete`.

The runner sends these headers on webhook requests:
- `Idempotency-Key`: `"<task_id>:start"`, `"<task_id>:end:success"`, `"<task_id>:end:failure"`, `"<task_id>:cancel"`
- `X-Task-Id`: task UUID
- `X-Task-Trigger`: `start`, `end`, or `cancel`

The `on_start` webhook response body can optionally contain a `NewActionDto` JSON to register a cancel action for the task.
