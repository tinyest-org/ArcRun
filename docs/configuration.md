# Configuration

All configuration is via environment variables.

## Required

| Variable | Description |
|----------|-------------|
| `DATABASE_URL` | PostgreSQL connection string |
| `HOST_URL` | Public URL for webhook callbacks (must start with `http://` or `https://`) |

## Optional

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `8085` | Server port |
| `RUST_LOG` | `info` | Log level |

## Connection Pool

| Variable | Default | Description |
|----------|---------|-------------|
| `POOL_MAX_SIZE` | `10` | Maximum connections |
| `POOL_MIN_IDLE` | `5` | Minimum idle connections |
| `POOL_ACQUIRE_RETRIES` | `3` | Connection acquire retries |
| `POOL_TIMEOUT_SECS` | `30` | Connection timeout in seconds |

## Pagination

| Variable | Default | Description |
|----------|---------|-------------|
| `PAGINATION_DEFAULT` | `50` | Default items per page |
| `PAGINATION_MAX` | `100` | Maximum items per page |

## Worker

| Variable | Default | Description |
|----------|---------|-------------|
| `WORKER_LOOP_INTERVAL_MS` | `1000` | Worker loop interval in ms |
| `WORKER_CLAIM_TIMEOUT_SECS` | `30` | Max time a task can stay Claimed before requeue |
| `BATCH_CHANNEL_CAPACITY` | `100` | Batch update channel size |

## Circuit Breaker

| Variable | Default | Description |
|----------|---------|-------------|
| `CIRCUIT_BREAKER_ENABLED` | `1` | Enable circuit breaker (0 to disable) |
| `CIRCUIT_BREAKER_FAILURE_THRESHOLD` | `5` | Failures before circuit opens |
| `CIRCUIT_BREAKER_FAILURE_WINDOW_SECS` | `10` | Time window for counting failures |
| `CIRCUIT_BREAKER_RECOVERY_TIMEOUT_SECS` | `30` | Time before trying half-open |
| `CIRCUIT_BREAKER_SUCCESS_THRESHOLD` | `2` | Successes in half-open to close |

## Observability

| Variable | Default | Description |
|----------|---------|-------------|
| `SLOW_QUERY_THRESHOLD_MS` | `100` | Slow query warning threshold in ms |
| `METRICS_SAMPLER_INTERVAL_SECS` | `15` | Interval for the metrics sampler (tasks_by_status, running_tasks_by_kind, db_pool_connections gauges) |
| `TRACING_ENABLED` | `0` | Enable OpenTelemetry distributed tracing |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | - | OTLP endpoint URL (e.g., `http://localhost:4317`) |
| `OTEL_SERVICE_NAME` | `arcrun` | Service name for traces |
| `OTEL_SAMPLING_RATIO` | `1.0` | Sampling ratio (0.0 to 1.0) |

## Retention

| Variable | Default | Description |
|----------|---------|-------------|
| `RETENTION_ENABLED` | `0` | Enable automatic cleanup of old terminal tasks |
| `RETENTION_DAYS` | `30` | Number of days to retain terminal tasks before cleanup |
| `RETENTION_CLEANUP_INTERVAL_SECS` | `3600` | Interval between cleanup runs in seconds |
| `RETENTION_BATCH_SIZE` | `1000` | Number of tasks to delete per cleanup run |

## Security

| Variable | Default | Description |
|----------|---------|-------------|
| `SKIP_SSRF_VALIDATION` | `1` (debug) / `0` (release) | Skip SSRF validation on webhook URLs |
| `BLOCKED_HOSTNAMES` | `localhost,127.0.0.1,::1,0.0.0.0,local,internal` | Comma-separated blocked hostnames |
| `BLOCKED_HOSTNAME_SUFFIXES` | `.local,.internal,.localdomain,.localhost` | Comma-separated blocked hostname suffixes |
