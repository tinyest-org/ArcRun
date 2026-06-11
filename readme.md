<p align="center">
  <img src="static/icon.png" alt="ArcRun" width="128" />
</p>

<h1 align="center">ArcRun</h1>

<p align="center">A Rust service for orchestrating task execution with DAG (Directed Acyclic Graph) dependencies, concurrency control, and webhook-based actions.</p>

## Features

- **DAG Dependencies**: Tasks can depend on other tasks, with support for `requires_success` flag
- **Cascading Propagation**: Failures and cancellations automatically propagate through the dependency chain
- **Concurrency Control**: Limit concurrent task execution per task kind using rules
- **Webhook Actions**: Execute webhooks on task start, success, failure, or cancellation
- **At-least-once Webhook Delivery**: End/cancel webhooks use a transactional outbox — committed in the status-change transaction (so the API response reflects durable state), delivered asynchronously with retries and exponential backoff, surviving crashes. `on_start` stays synchronous (control-flow). Inspect deliveries via `GET /webhook-deliveries?status=exhausted`
- **Task States**: Waiting, Pending, Claimed, Running, Success, Failure, Canceled, Paused
- **DAG Visualization**: Built-in web UI for visualizing task DAGs with auto-layout
- **Batch Operations**: Batch stats, stop, and live rule updates
- **Batch Counter Updates**: Efficient high-throughput endpoint for success/failure counters
- **Capacity Rules**: Limit total remaining work across matching running tasks
- **Idempotent Webhooks**: Automatic deduplication of webhook executions
- **Data Retention**: Automatic cleanup of old terminal tasks
- **Prometheus Metrics**: Built-in observability with custom metrics
- **Deduplication**: Skip duplicate tasks based on metadata fields
- **Input Validation**: Comprehensive validation with detailed error messages
- **Circuit Breaker**: Connection pool resilience with automatic recovery
- **Distributed Tracing**: OpenTelemetry support with OTLP export
- **SSRF Protection**: Webhook URL validation to prevent server-side request forgery
- **Health Checks**: Liveness and readiness probes for Kubernetes deployments
- **Task Priority Scheduling**: Integer priority (-1000 to 1000) controls processing order within pending tasks

## Quick Start

### With Docker

```bash
docker pull plawn/arcrun:latest

docker run -e DATABASE_URL=postgres://user:pass@host/db \
           -e HOST_URL=http://localhost:8085 \
           -p 8085:8085 \
           plawn/arcrun:latest
```

### From Source

```bash
# Run migrations
diesel migration run

# Start server
cargo run --bin server
```

## Task Lifecycle

```
         +-----------------------------------------------------+
         |                                                     |
         v                                                     |
     +-------+   +---------+   +---------+   +---------+      |
---->|Pending|-->| Claimed |-->| Running |-->| Success |      |
     +-------+   +---------+   +---------+   +---------+      |
         |             |             |                         |
         |             |             |         +---------+     |
         |             |             +-------->| Failure |     |
         |             |                       +---------+     |
         |             |                                     |
         | (has deps)  | (start timeout -> requeue)          |
         v             |                                     |
     +-------+         |                                     |
     |Waiting|---------+-------------------------------------+
     +-------+  (all dependencies complete)

     +----------+     +----------+
     | Canceled |     |  Paused  |
     +----------+     +----------+
```

## Documentation

- [API Reference](docs/api.md) - All endpoints, request/response formats, webhook execution
- [Configuration](docs/configuration.md) - Environment variables for all subsystems
- [Concepts](docs/concepts.md) - Dependencies, propagation, concurrency rules, capacity rules, deduplication
- [Metrics](docs/metrics.md) - Full Prometheus metrics catalog

## Architecture

- **Actix-web**: HTTP server with async handlers
- **Diesel + diesel-async**: Async PostgreSQL ORM with bb8 connection pooling
- **Start Loop**: Background loop that:
  - Checks pending tasks against concurrency rules
  - Claims and starts eligible tasks (executes on_start webhooks)
  - Propagates completions to dependent children
- **Timeout Loop**: Background loop that:
  - Finds running tasks where `last_updated` exceeds the timeout duration
  - Marks them as failed, propagates to children, enqueues on_failure outbox rows
- **Delivery Loop**: Background loop that drains the webhook outbox:
  - Selects mature `pending` end/cancel rows (`FOR UPDATE SKIP LOCKED`), gated so `start` is delivered before `end` per task
  - Delivers via the shared HTTP client; marks `success`, or retries with exponential backoff, or `exhausted` after the max attempts
- **Batch Updater**: High-throughput counter updates using:
  - `DashMap` for lock-free concurrent access (per-shard locking)
  - Atomic counters (`AtomicI32`) for success/failure counts
  - Automatic retry on DB failure (re-queues counts)
  - Periodic cleanup of zero-count entries
- **Retention Loop**: Automatic cleanup of old terminal tasks based on configurable retention period
- **Circuit Breaker**: Connection pool resilience with states:
  - Closed (normal) -> Open (rejecting) -> HalfOpen (testing recovery)
- **OpenTelemetry**: Distributed tracing with OTLP export
- **Prometheus**: Metrics exposition with custom registry
- **Cytoscape.js + Dagre**: DAG visualization with auto-layout

## Project Structure

```
src/
+-- main.rs              # HTTP server, startup, worker spawning
+-- test_server.rs       # Test server binary
+-- cache_helper.rs      # Cache utility binary
+-- lib.rs               # Module declarations, DB pool initialization
+-- models.rs            # Database models (Task, Action, Link, enums)
+-- schema.rs            # Diesel schema (auto-generated)
+-- action.rs            # Webhook action execution
+-- rule.rs              # Concurrency rules and matchers
+-- config.rs            # Configuration loading from env vars
+-- metrics.rs           # Prometheus metrics
+-- error.rs             # Typed error definitions
+-- circuit_breaker.rs   # Circuit breaker for DB pool resilience
+-- tracing.rs           # OpenTelemetry distributed tracing
+-- handlers/            # HTTP handlers and route configuration
|   +-- mod.rs           # Route registration (configure_routes)
|   +-- task.rs          # Task CRUD endpoints
|   +-- batch.rs         # Batch stats, stop, rules endpoints
|   +-- dag.rs           # DAG visualization endpoints
|   +-- health.rs        # Health and readiness probes
|   +-- response.rs      # Response helpers
+-- dtos/                # API DTOs and query parameters
|   +-- task.rs          # Task input/output DTOs
|   +-- batch.rs         # Batch DTOs (stats, stop, rules)
|   +-- dag.rs           # DAG DTOs
|   +-- query.rs         # Pagination and filter DTOs
+-- db/                  # Database operations
|   +-- task_crud.rs     # Task insert, update, delete
|   +-- task_lifecycle.rs # Status transitions, propagation triggers
|   +-- task_query.rs    # Task queries (get, list, filter)
|   +-- batch_listing.rs # Batch stats and listing
|   +-- cleanup.rs       # Batch stop, retention cleanup
|   +-- webhook_execution.rs # Webhook execution tracking
+-- workers/             # Background worker loops
|   +-- start_loop.rs    # Pending -> Running task processing
|   +-- timeout_loop.rs  # Timeout detection
|   +-- propagation.rs   # Dependency propagation
|   +-- batch_updater.rs # High-throughput counter updates
|   +-- webhooks.rs      # Webhook firing logic
|   +-- retention.rs     # Automatic cleanup of old tasks
+-- validation/          # Input validation
|   +-- task.rs          # Task batch validation
|   +-- action.rs        # Action/webhook validation
|   +-- ssrf.rs          # SSRF protection
|   +-- constants.rs     # Validation constants
sdk/                     # Rust client SDK (workspace member)
static/
+-- dag.html             # DAG visualization UI
test/
+-- test.ts              # Manual testing script (Bun)
migrations/              # Diesel migrations
tests/
+-- integration/         # Integration tests with testcontainers
    +-- common/          # Shared test helpers and builders
    +-- test_*.rs        # Test modules (CRUD, DAG, propagation, etc.)
```

## Development

```bash
# Run migrations
diesel migration run

# Start server
cargo run --bin server

# Run tests (requires Docker for testcontainers)
cargo test

# Run integration tests only
cargo test --test integration

# Manual testing with bun
cd test && bun test.ts dag
```

### Test Commands

```bash
# Create a CI/CD pipeline DAG
bun test.ts dag

# Create a single task
bun test.ts single

# List all tasks
bun test.ts list

# Update a task status
bun test.ts update <task_id> Success
bun test.ts update <task_id> Failure

# View DAG data as JSON
bun test.ts view <batch_id>
```

## Releasing

Create a new release by pushing a Git tag:

```bash
# Create and push a version tag
git tag v1.0.0
git push origin v1.0.0
```

This triggers the CI pipeline which builds multi-arch Docker images (amd64 + arm64) and pushes them to DockerHub with the following tags:

| Tag | Example | Description |
|-----|---------|-------------|
| `{version}` | `1.0.0` | Full semantic version |
| `{major}.{minor}` | `1.0` | Major.minor version |
| `{major}` | `1` | Major version (not created for v0.x) |
| `sha-{commit}` | `sha-abc1234` | Git commit SHA |
| `latest` | `latest` | Updated on main/master branch pushes |

Pull the image:
```bash
docker pull plawn/arcrun:1.0.0
# or
docker pull plawn/arcrun:latest
```

## TODO

- [x] DAG visualization UI
- [x] Cascading failure propagation
- [x] Cancel propagation to children
- [x] Circuit breaker for connection pool
- [x] Distributed tracing
- [x] SSRF protection
- [x] Health and readiness endpoints
- [ ] Automatic rule reuse
- [ ] Automatic action reuse
- [ ] Failure count on actions for retries
- [ ] Task retry with backoff
