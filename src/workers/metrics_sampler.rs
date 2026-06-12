//! Periodic metrics sampler (Lot M3).
//!
//! Some metrics are *state of the world* gauges that no single event can keep
//! current: the count of tasks per status, running tasks per kind, and the DB
//! pool occupancy. This loop samples them on a fixed cadence (default 15s) with
//! a couple of cheap indexed aggregations, well away from any HTTP path.
//!
//! It is deliberately a separate, lightweight worker (not grafted onto the
//! timeout loop) so its cadence is independent and a slow sample can never delay
//! task timeouts.

use crate::{DbPool, db_operation, metrics};
use actix_web::rt;
use tokio::sync::watch;

pub async fn metrics_sampler_loop(
    pool: DbPool,
    interval: std::time::Duration,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        let loop_start = std::time::Instant::now();

        // DB pool occupancy is read from bb8 state — no query, always available.
        let state = pool.state();
        let idle = state.idle_connections as i64;
        let in_use = (state.connections as i64 - idle).max(0);
        metrics::set_db_pool_connections(in_use, idle);

        match pool.get().await {
            Ok(mut conn) => {
                match db_operation::count_tasks_by_status(&mut conn).await {
                    Ok(counts) => metrics::set_tasks_by_status_snapshot(&counts),
                    Err(e) => log::warn!("Metrics sampler: tasks_by_status failed: {:?}", e),
                }
                match db_operation::count_running_tasks_by_kind(&mut conn).await {
                    Ok(counts) => metrics::set_running_tasks_by_kind_snapshot(&counts),
                    Err(e) => {
                        log::warn!("Metrics sampler: running_tasks_by_kind failed: {:?}", e)
                    }
                }
            }
            Err(e) => log::warn!("Metrics sampler: could not acquire DB connection: {:?}", e),
        }

        metrics::record_worker_loop_iteration(
            "metrics_sampler",
            loop_start.elapsed().as_secs_f64(),
        );

        tokio::select! {
            _ = shutdown.changed() => {
                log::info!("Metrics sampler: shutdown signal received, exiting");
                return;
            }
            _ = rt::time::sleep(interval) => {}
        }
    }
}
