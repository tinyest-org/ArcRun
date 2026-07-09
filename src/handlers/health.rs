use std::time::Duration;

use actix_web::http::header::{CacheControl, CacheDirective, ContentType};
use actix_web::{HttpResponse, web};

use super::{AppState, HealthResponse};

/// Short, probe-specific bound on connection acquisition (Audit 2, B7).
///
/// The pool's own `connection_timeout` is up to 30 s. Under pool exhaustion a
/// probe that waited that long would hang the kubelet, triggering a restart
/// storm. Probes instead use this tight bound (no retries) so they always
/// answer fast, and interpret a timeout as "pool saturated" — a transient,
/// non-fatal condition (see the per-endpoint semantics below).
const PROBE_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(2);

/// Serve the application icon
pub async fn favicon() -> HttpResponse {
    HttpResponse::Ok()
        .content_type(ContentType::png())
        .insert_header(CacheControl(vec![
            CacheDirective::Public,
            CacheDirective::MaxAge(86400),
        ]))
        .body(&include_bytes!("../../static/icon.png")[..])
}

#[utoipa::path(
    get,
    path = "/health",
    summary = "Health check",
    description = "Liveness probe. Verifies database connectivity (under a short 2s bound) and returns pool statistics. Always returns 200: a healthy body when the DB is reachable, a `degraded` body when the pool is saturated/unreachable (a restart cannot fix that — use /ready to shed traffic).",
    responses(
        (status = 200, description = "Service is alive (body reports healthy or degraded)", body = HealthResponse),
    ),
    tag = "health"
)]
/// Health check endpoint (liveness) - verifies database connectivity.
///
/// Semantics (Audit 2, B7): this is a **liveness** probe, so it must NOT ask the
/// kubelet to restart the pod for a condition a restart cannot fix — a saturated
/// or briefly-unreachable connection pool. It therefore acquires a connection
/// under a tight [`PROBE_ACQUIRE_TIMEOUT`] (no 30 s hang) and, on failure,
/// returns **200 with a `degraded` body** rather than 503: the process is alive
/// and will recover once the pool frees up. Traffic-shedding on pool exhaustion
/// is the readiness probe's job (`/ready` returns 503, removing the pod from the
/// load balancer without killing it).
pub async fn health_check(state: web::Data<AppState>) -> HttpResponse {
    let pool_state = state.pool.state();

    // Try to get a connection under a tight bound to verify DB is accessible.
    let db_status = match tokio::time::timeout(PROBE_ACQUIRE_TIMEOUT, state.pool.get()).await {
        Ok(Ok(_conn)) => "healthy".to_string(),
        Ok(Err(e)) => {
            log::warn!("Health check: database connection failed: {}", e);
            "unhealthy".to_string()
        }
        Err(_) => {
            log::warn!(
                "Health check: connection acquire exceeded {:?} (pool likely saturated)",
                PROBE_ACQUIRE_TIMEOUT
            );
            "unhealthy".to_string()
        }
    };

    let is_healthy = db_status == "healthy";

    let response = HealthResponse {
        status: if is_healthy {
            "ok".to_string()
        } else {
            "degraded".to_string()
        },
        database: db_status,
        pool_size: pool_state.connections,
        pool_idle: pool_state.idle_connections,
    };

    // Always 200: liveness must not restart the pod for a transient pool issue.
    HttpResponse::Ok().json(response)
}

#[utoipa::path(
    get,
    path = "/ready",
    summary = "Readiness check",
    description = "Stricter than /health — also verifies the connection pool is not exhausted. Returns 503 if all pool connections are in use. Use this for readiness probes.",
    responses(
        (status = 200, description = "Service is ready to accept traffic"),
        (status = 503, description = "Service is not ready — pool exhausted or DB unreachable"),
    ),
    tag = "health"
)]
/// Readiness check - more strict than health check
pub async fn readiness_check(state: web::Data<AppState>) -> HttpResponse {
    // Check if we have at least one idle connection
    let pool_state = state.pool.state();

    if pool_state.idle_connections == 0 && pool_state.connections >= state.config.pool.max_size {
        return HttpResponse::ServiceUnavailable().json(serde_json::json!({
            "status": "not_ready",
            "reason": "connection pool exhausted"
        }));
    }

    // Verify we can actually get a connection, under a tight bound (Audit 2, B7):
    // a readiness probe that blocked on the pool's 30s connection_timeout would
    // hang the kubelet. On timeout/error return 503 fast — readiness 503 removes
    // the pod from the load balancer (the correct signal for a saturated pool)
    // without killing it.
    match tokio::time::timeout(PROBE_ACQUIRE_TIMEOUT, state.pool.get()).await {
        Ok(Ok(_)) => HttpResponse::Ok().json(serde_json::json!({"status": "ready"})),
        Ok(Err(_)) => HttpResponse::ServiceUnavailable().json(serde_json::json!({
            "status": "not_ready",
            "reason": "cannot acquire database connection"
        })),
        Err(_) => HttpResponse::ServiceUnavailable().json(serde_json::json!({
            "status": "not_ready",
            "reason": "connection acquire timed out (pool saturated)"
        })),
    }
}
