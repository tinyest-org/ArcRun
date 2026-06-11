//! Prometheus metrics for ArcRun observability.
//!
//! Provides custom metrics for tracking task execution, dependencies, and system health.

use prometheus::{
    Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGaugeVec, Opts, Registry,
};
use std::sync::LazyLock;

/// Global metrics registry
pub static REGISTRY: LazyLock<Registry> = LazyLock::new(Registry::new);

// ============================================================================
// Metric Registration Macros
// ============================================================================

macro_rules! register_int_counter {
    ($name:ident, $metric_name:expr, $help:expr) => {
        pub static $name: LazyLock<IntCounter> = LazyLock::new(|| {
            let m = IntCounter::new($metric_name, $help).expect("metric can be created");
            REGISTRY.register(Box::new(m.clone())).unwrap();
            m
        });
    };
}

macro_rules! register_int_counter_vec {
    ($name:ident, $metric_name:expr, $help:expr, $labels:expr) => {
        pub static $name: LazyLock<IntCounterVec> = LazyLock::new(|| {
            let m = IntCounterVec::new(Opts::new($metric_name, $help), $labels)
                .expect("metric can be created");
            REGISTRY.register(Box::new(m.clone())).unwrap();
            m
        });
    };
}

macro_rules! register_int_gauge_vec {
    ($name:ident, $metric_name:expr, $help:expr, $labels:expr) => {
        pub static $name: LazyLock<IntGaugeVec> = LazyLock::new(|| {
            let m = IntGaugeVec::new(Opts::new($metric_name, $help), $labels)
                .expect("metric can be created");
            REGISTRY.register(Box::new(m.clone())).unwrap();
            m
        });
    };
}

macro_rules! register_histogram {
    ($name:ident, $metric_name:expr, $help:expr, $buckets:expr) => {
        pub static $name: LazyLock<Histogram> = LazyLock::new(|| {
            let m = Histogram::with_opts(HistogramOpts::new($metric_name, $help).buckets($buckets))
                .expect("metric can be created");
            REGISTRY.register(Box::new(m.clone())).unwrap();
            m
        });
    };
}

macro_rules! register_histogram_vec {
    ($name:ident, $metric_name:expr, $help:expr, $labels:expr, $buckets:expr) => {
        pub static $name: LazyLock<HistogramVec> = LazyLock::new(|| {
            let m = HistogramVec::new(
                HistogramOpts::new($metric_name, $help).buckets($buckets),
                $labels,
            )
            .expect("metric can be created");
            REGISTRY.register(Box::new(m.clone())).unwrap();
            m
        });
    };
}

// ============================================================================
// Task Counters
// ============================================================================

register_int_counter!(
    TASKS_CREATED_TOTAL,
    "tasks_created_total",
    "Total number of tasks created"
);
register_int_counter_vec!(
    TASK_STATUS_TRANSITIONS,
    "task_status_transitions_total",
    "Number of task status transitions",
    &["from_status", "to_status"]
);
register_int_counter_vec!(
    TASKS_COMPLETED_TOTAL,
    "tasks_completed_total",
    "Total number of tasks completed",
    &["outcome", "kind"]
);
register_int_counter!(
    TASKS_CANCELLED_TOTAL,
    "tasks_cancelled_total",
    "Total number of tasks cancelled"
);
register_int_counter!(
    TASKS_TIMED_OUT_TOTAL,
    "tasks_timed_out_total",
    "Total number of tasks that timed out"
);

// ============================================================================
// Task Gauges (current state)
// ============================================================================

register_int_gauge_vec!(
    TASKS_BY_STATUS,
    "tasks_by_status",
    "Current number of tasks by status",
    &["status"]
);
register_int_gauge_vec!(
    RUNNING_TASKS_BY_KIND,
    "running_tasks_by_kind",
    "Current number of running tasks by kind",
    &["kind"]
);

// ============================================================================
// Dependency Metrics
// ============================================================================

register_int_counter!(
    TASKS_WITH_DEPENDENCIES,
    "tasks_with_dependencies_total",
    "Total number of tasks created with dependencies"
);
register_int_counter_vec!(
    DEPENDENCY_PROPAGATIONS,
    "dependency_propagations_total",
    "Number of dependency propagations when parent tasks complete",
    &["parent_outcome"]
);
register_int_counter!(
    TASKS_UNBLOCKED,
    "tasks_unblocked_total",
    "Total number of tasks unblocked after dependencies completed"
);
register_int_counter!(
    TASKS_FAILED_BY_DEPENDENCY,
    "tasks_failed_by_dependency_total",
    "Total number of tasks failed due to required dependency failure"
);
register_int_counter!(
    TASKS_CANCELED_DEAD_END_TOTAL,
    "tasks_canceled_dead_end_total",
    "Total number of ancestor tasks canceled by dead-end detection"
);
register_int_counter!(
    TASKS_DB_SAVE_FAILURES,
    "tasks_db_save_failures_total",
    "Total number of tasks where database save failed after max retries"
);
register_int_counter!(
    BATCH_UPDATE_FAILURES,
    "batch_update_failures_total",
    "Total number of batch update failures (counts re-queued for retry)"
);

// ============================================================================
// Action Metrics
// ============================================================================

register_int_counter_vec!(
    WEBHOOK_EXECUTIONS,
    "webhook_executions_total",
    "Number of webhook executions",
    &["trigger", "outcome"]
);
register_int_counter_vec!(
    WEBHOOK_ATTEMPTS_TOTAL,
    "webhook_attempts_total",
    "Number of webhook attempts (including failures)",
    &["trigger", "outcome"]
);
register_histogram_vec!(
    WEBHOOK_DURATION_SECONDS,
    "webhook_duration_seconds",
    "Webhook execution duration in seconds",
    &["trigger"],
    vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
);

// ============================================================================
// Webhook Idempotency Metrics
// ============================================================================

register_int_counter_vec!(
    WEBHOOK_IDEMPOTENT_SKIPS,
    "webhook_idempotent_skips_total",
    "Number of webhook executions skipped due to idempotency (already succeeded)",
    &["trigger"]
);
register_int_counter!(
    WEBHOOK_IDEMPOTENT_CONFLICTS,
    "webhook_idempotent_conflicts_total",
    "Number of idempotency conflicts when claiming webhook executions"
);

// ============================================================================
// Webhook Outbox / Delivery Metrics (Lot 2)
// ============================================================================

register_int_counter_vec!(
    WEBHOOK_DELIVERY_RETRIES,
    "webhook_delivery_retries_total",
    "Number of outbox webhook delivery attempts that failed and were rescheduled",
    &["trigger"]
);
register_int_counter_vec!(
    WEBHOOK_DELIVERY_EXHAUSTED,
    "webhook_delivery_exhausted_total",
    "Number of outbox webhook deliveries that exhausted all retry attempts",
    &["trigger"]
);
register_histogram!(
    WEBHOOK_DELIVERY_LAG_SECONDS,
    "webhook_delivery_lag_seconds",
    "Lag between outbox row creation and successful delivery, in seconds",
    vec![0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0]
);

// ============================================================================
// Concurrency Metrics
// ============================================================================

register_int_counter!(
    TASKS_BLOCKED_BY_CONCURRENCY,
    "tasks_blocked_by_concurrency_total",
    "Total number of tasks blocked due to concurrency rules"
);

// ============================================================================
// Duration Metrics
// ============================================================================

register_histogram_vec!(
    TASK_DURATION_SECONDS,
    "task_duration_seconds",
    "Task execution duration in seconds from Running to completion",
    &["kind", "outcome"],
    vec![1.0, 5.0, 10.0, 30.0, 60.0, 300.0, 600.0, 1800.0, 3600.0]
);
register_histogram_vec!(
    TASK_WAIT_SECONDS,
    "task_wait_seconds",
    "Task wait time in seconds from Pending to Running",
    &["kind"],
    vec![0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 300.0]
);

// ============================================================================
// Worker Loop Metrics
// ============================================================================

register_int_counter!(
    WORKER_LOOP_ITERATIONS,
    "worker_loop_iterations_total",
    "Total number of worker loop iterations"
);
register_histogram!(
    WORKER_LOOP_DURATION_SECONDS,
    "worker_loop_duration_seconds",
    "Duration of worker loop iterations in seconds",
    vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0]
);
register_histogram!(
    TASKS_PROCESSED_PER_LOOP,
    "tasks_processed_per_loop",
    "Number of tasks processed per worker loop iteration",
    vec![0.0, 1.0, 5.0, 10.0, 25.0, 50.0, 100.0]
);

// ============================================================================
// Database Metrics
// ============================================================================

register_histogram_vec!(
    DB_QUERY_DURATION_SECONDS,
    "db_query_duration_seconds",
    "Database query duration in seconds",
    &["query"],
    vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]
);
register_int_counter_vec!(
    SLOW_QUERIES_TOTAL,
    "slow_queries_total",
    "Total number of queries exceeding slow query threshold",
    &["query"]
);

// ============================================================================
// Circuit Breaker Metrics
// ============================================================================

register_int_counter_vec!(
    CIRCUIT_BREAKER_STATE_TRANSITIONS,
    "circuit_breaker_state_transitions_total",
    "Number of circuit breaker state transitions",
    &["to_state"]
);
register_int_counter!(
    CIRCUIT_BREAKER_REJECTIONS,
    "circuit_breaker_rejections_total",
    "Total number of requests rejected by circuit breaker"
);

// ============================================================================
// Retention Cleanup Metrics
// ============================================================================

register_int_counter!(
    RETENTION_TASKS_CLEANED,
    "retention_tasks_cleaned_total",
    "Total number of tasks deleted by retention cleanup"
);
register_int_counter_vec!(
    RETENTION_CLEANUP_RUNS,
    "retention_cleanup_runs_total",
    "Number of retention cleanup runs by outcome",
    &["outcome"]
);
register_histogram!(
    RETENTION_CLEANUP_DURATION,
    "retention_cleanup_duration_seconds",
    "Duration of retention cleanup cycles in seconds",
    vec![0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0]
);

// ============================================================================
// Webhook In-Flight Gauge
// ============================================================================

register_int_gauge_vec!(
    WEBHOOKS_IN_FLIGHT,
    "webhooks_in_flight",
    "Current number of webhook executions in progress",
    &["phase"]
);

// ============================================================================
// Helper Functions
// ============================================================================

pub fn record_task_created() {
    TASKS_CREATED_TOTAL.inc();
}

pub fn record_task_with_dependencies() {
    TASKS_WITH_DEPENDENCIES.inc();
}

pub fn record_status_transition(from: &str, to: &str) {
    TASK_STATUS_TRANSITIONS.with_label_values(&[from, to]).inc();
}

pub fn record_task_completed(outcome: &str, kind: &str) {
    TASKS_COMPLETED_TOTAL
        .with_label_values(&[outcome, kind])
        .inc();
}

pub fn record_task_cancelled() {
    TASKS_CANCELLED_TOTAL.inc();
}

pub fn record_task_timeout() {
    TASKS_TIMED_OUT_TOTAL.inc();
}

pub fn record_dependency_propagation(parent_outcome: &str) {
    DEPENDENCY_PROPAGATIONS
        .with_label_values(&[parent_outcome])
        .inc();
}

pub fn record_task_unblocked() {
    TASKS_UNBLOCKED.inc();
}

pub fn record_task_failed_by_dependency() {
    TASKS_FAILED_BY_DEPENDENCY.inc();
}

pub fn record_task_canceled_dead_end() {
    TASKS_CANCELED_DEAD_END_TOTAL.inc();
}

pub fn record_task_db_save_failure() {
    TASKS_DB_SAVE_FAILURES.inc();
}

pub fn record_batch_update_failure() {
    BATCH_UPDATE_FAILURES.inc();
}

pub fn record_webhook_idempotent_skip(trigger: &str) {
    WEBHOOK_IDEMPOTENT_SKIPS.with_label_values(&[trigger]).inc();
}

pub fn record_webhook_idempotent_conflict() {
    WEBHOOK_IDEMPOTENT_CONFLICTS.inc();
}

pub fn record_webhook_delivery_retry(trigger: &str) {
    WEBHOOK_DELIVERY_RETRIES.with_label_values(&[trigger]).inc();
}

pub fn record_webhook_delivery_exhausted(trigger: &str) {
    WEBHOOK_DELIVERY_EXHAUSTED
        .with_label_values(&[trigger])
        .inc();
}

pub fn record_webhook_delivery_lag(lag_secs: f64) {
    WEBHOOK_DELIVERY_LAG_SECONDS.observe(lag_secs);
}

pub fn record_webhook_execution(trigger: &str, outcome: &str, duration_secs: f64) {
    WEBHOOK_EXECUTIONS
        .with_label_values(&[trigger, outcome])
        .inc();
    WEBHOOK_ATTEMPTS_TOTAL
        .with_label_values(&[trigger, outcome])
        .inc();
    WEBHOOK_DURATION_SECONDS
        .with_label_values(&[trigger])
        .observe(duration_secs);
}

pub fn record_task_duration(kind: &str, outcome: &str, duration_secs: f64) {
    TASK_DURATION_SECONDS
        .with_label_values(&[kind, outcome])
        .observe(duration_secs);
}

pub fn record_task_blocked_by_concurrency() {
    TASKS_BLOCKED_BY_CONCURRENCY.inc();
}

pub fn set_tasks_by_status(status: &str, count: i64) {
    TASKS_BY_STATUS.with_label_values(&[status]).set(count);
}

pub fn set_running_tasks_by_kind(kind: &str, count: i64) {
    RUNNING_TASKS_BY_KIND.with_label_values(&[kind]).set(count);
}

pub fn record_worker_loop_iteration(duration_secs: f64, tasks_processed: usize) {
    WORKER_LOOP_ITERATIONS.inc();
    WORKER_LOOP_DURATION_SECONDS.observe(duration_secs);
    TASKS_PROCESSED_PER_LOOP.observe(tasks_processed as f64);
}

pub fn record_db_query(query_name: &str, duration_secs: f64) {
    DB_QUERY_DURATION_SECONDS
        .with_label_values(&[query_name])
        .observe(duration_secs);
}

pub fn record_db_query_with_slow_warning(query_name: &str, duration_secs: f64, threshold_ms: u64) {
    DB_QUERY_DURATION_SECONDS
        .with_label_values(&[query_name])
        .observe(duration_secs);

    let duration_ms = (duration_secs * 1000.0) as u64;
    if duration_ms > threshold_ms {
        log::warn!(
            "Slow query detected: {} took {}ms (threshold: {}ms)",
            query_name,
            duration_ms,
            threshold_ms
        );
        SLOW_QUERIES_TOTAL.with_label_values(&[query_name]).inc();
    }
}

pub fn record_circuit_breaker_state(to_state: &str) {
    CIRCUIT_BREAKER_STATE_TRANSITIONS
        .with_label_values(&[to_state])
        .inc();
}

pub fn record_circuit_breaker_rejection() {
    CIRCUIT_BREAKER_REJECTIONS.inc();
}

pub fn inc_webhooks_in_flight(phase: &str) {
    WEBHOOKS_IN_FLIGHT.with_label_values(&[phase]).inc();
}

pub fn dec_webhooks_in_flight(phase: &str) {
    WEBHOOKS_IN_FLIGHT.with_label_values(&[phase]).dec();
}

/// RAII guard that increments the in-flight gauge on creation and decrements
/// on drop — even if the owning future panics during unwind.
pub struct WebhooksInFlightGuard {
    phase: &'static str,
}

impl WebhooksInFlightGuard {
    pub fn new(phase: &'static str) -> Self {
        inc_webhooks_in_flight(phase);
        Self { phase }
    }
}

impl Drop for WebhooksInFlightGuard {
    fn drop(&mut self) {
        dec_webhooks_in_flight(self.phase);
    }
}

pub fn record_retention_cleanup(outcome: &str, tasks_deleted: usize, duration_secs: f64) {
    RETENTION_CLEANUP_RUNS.with_label_values(&[outcome]).inc();
    RETENTION_CLEANUP_DURATION.observe(duration_secs);
    if tasks_deleted > 0 {
        RETENTION_TASKS_CLEANED.inc_by(tasks_deleted as u64);
    }
}

/// Initialize all metrics (call at startup to register them)
pub fn init_metrics() {
    // Force lazy initialization of all metrics
    let _ = &*TASKS_CREATED_TOTAL;
    let _ = &*TASK_STATUS_TRANSITIONS;
    let _ = &*TASKS_COMPLETED_TOTAL;
    let _ = &*TASKS_CANCELLED_TOTAL;
    let _ = &*TASKS_TIMED_OUT_TOTAL;
    let _ = &*TASKS_BY_STATUS;
    let _ = &*RUNNING_TASKS_BY_KIND;
    let _ = &*TASKS_WITH_DEPENDENCIES;
    let _ = &*DEPENDENCY_PROPAGATIONS;
    let _ = &*TASKS_UNBLOCKED;
    let _ = &*TASKS_FAILED_BY_DEPENDENCY;
    let _ = &*TASKS_CANCELED_DEAD_END_TOTAL;
    let _ = &*TASKS_DB_SAVE_FAILURES;
    let _ = &*BATCH_UPDATE_FAILURES;
    let _ = &*WEBHOOK_EXECUTIONS;
    let _ = &*WEBHOOK_DURATION_SECONDS;
    let _ = &*WEBHOOK_IDEMPOTENT_SKIPS;
    let _ = &*WEBHOOK_DELIVERY_RETRIES;
    let _ = &*WEBHOOK_DELIVERY_EXHAUSTED;
    let _ = &*WEBHOOK_DELIVERY_LAG_SECONDS;
    let _ = &*TASKS_BLOCKED_BY_CONCURRENCY;
    let _ = &*TASK_DURATION_SECONDS;
    let _ = &*TASK_WAIT_SECONDS;
    let _ = &*WORKER_LOOP_ITERATIONS;
    let _ = &*WORKER_LOOP_DURATION_SECONDS;
    let _ = &*TASKS_PROCESSED_PER_LOOP;
    let _ = &*DB_QUERY_DURATION_SECONDS;
    let _ = &*SLOW_QUERIES_TOTAL;
    let _ = &*CIRCUIT_BREAKER_STATE_TRANSITIONS;
    let _ = &*CIRCUIT_BREAKER_REJECTIONS;
    let _ = &*RETENTION_TASKS_CLEANED;
    let _ = &*RETENTION_CLEANUP_RUNS;
    let _ = &*RETENTION_CLEANUP_DURATION;
    let _ = &*WEBHOOKS_IN_FLIGHT;
}
