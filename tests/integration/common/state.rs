use arcrun::DbPool;
use arcrun::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};
use arcrun::config::{Config, SecurityConfig};
use arcrun::handlers::AppState;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Create test configuration
pub fn test_config() -> Arc<Config> {
    Arc::new(Config {
        port: 8080,
        host_url: "http://localhost:8080".to_string(),
        database_url: "".to_string(),
        pool: arcrun::config::PoolConfig {
            max_size: 5,
            min_idle: 1,
            max_lifetime: std::time::Duration::from_secs(3600),
            idle_timeout: std::time::Duration::from_secs(120),
            connection_timeout: std::time::Duration::from_secs(30),
            acquire_retries: 3,
            retry_delay: std::time::Duration::from_millis(100),
        },
        pagination: arcrun::config::PaginationConfig {
            default_per_page: 50,
            max_per_page: 100,
        },
        worker: arcrun::config::WorkerConfig {
            loop_interval: std::time::Duration::from_secs(1),
            timeout_check_interval: std::time::Duration::from_secs(1),
            claim_timeout: std::time::Duration::from_secs(30),
            batch_flush_interval: std::time::Duration::from_millis(100),
            batch_channel_capacity: 100,
            dead_end_cancel_enabled: true,
            start_batch_size: 50,
            webhook_concurrency: 10,
            webhook_delivery_interval: std::time::Duration::from_millis(1000),
            webhook_delivery_batch_size: 50,
            webhook_max_attempts: 10,
            webhook_retry_backoff_base_secs: 2,
            webhook_retry_backoff_cap_secs: 300,
            webhook_delivery_lease_secs: 120,
            webhook_delivery_concurrency: 10,
            metrics_sampler_interval: std::time::Duration::from_secs(15),
        },
        circuit_breaker: arcrun::config::CircuitBreakerConfig {
            enabled: true,
            failure_threshold: 5,
            failure_window_secs: 10,
            recovery_timeout_secs: 30,
            success_threshold: 2,
        },
        observability: arcrun::config::ObservabilityConfig {
            slow_query_threshold_ms: 100,
            tracing_enabled: false,
            otlp_endpoint: None,
            service_name: "arcrun-test".to_string(),
            sampling_ratio: 1.0,
        },
        security: SecurityConfig::default(),
        retention: arcrun::config::RetentionConfig::default(),
    })
}

/// Create a test circuit breaker (with high thresholds so it doesn't trip during tests)
pub fn test_circuit_breaker() -> Arc<CircuitBreaker> {
    Arc::new(CircuitBreaker::new(CircuitBreakerConfig {
        failure_threshold: 100, // High threshold for tests
        failure_window: std::time::Duration::from_secs(60),
        recovery_timeout: std::time::Duration::from_secs(1),
        success_threshold: 1,
    }))
}

/// Create app state for tests (without batch updater)
pub fn create_test_state(pool: DbPool) -> AppState {
    let (sender, _receiver) = mpsc::channel(100);
    let config = test_config();
    AppState {
        pool,
        sender,
        action_executor: arcrun::action::ActionExecutor::new(arcrun::action::ActionContext {
            host_address: "http://localhost:8080".to_string(),
            webhook_idempotency_timeout: config.worker.claim_timeout,
        }),
        config,
        circuit_breaker: test_circuit_breaker(),
    }
}

/// Wrapper to keep the shutdown sender alive for the duration of the test.
pub struct TestStateWithBatchUpdater {
    pub state: AppState,
    pub _shutdown_tx: tokio::sync::watch::Sender<bool>,
}

/// Create app state with batch updater running (for batch update tests)
pub fn create_test_state_with_batch_updater(pool: DbPool) -> TestStateWithBatchUpdater {
    let (sender, receiver) = mpsc::channel(100);
    let config = test_config();

    // Spawn the batch updater background task with a shutdown channel
    let pool_clone = pool.clone();
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let flush_interval = std::time::Duration::from_millis(100);
    tokio::spawn(async move {
        arcrun::workers::batch_updater(pool_clone, receiver, flush_interval, shutdown_rx).await;
    });

    TestStateWithBatchUpdater {
        state: AppState {
            pool,
            sender,
            action_executor: arcrun::action::ActionExecutor::new(arcrun::action::ActionContext {
                host_address: "http://localhost:8080".to_string(),
                webhook_idempotency_timeout: config.worker.claim_timeout,
            }),
            config,
            circuit_breaker: test_circuit_breaker(),
        },
        _shutdown_tx: shutdown_tx,
    }
}
