use crate::{DbPool, config::RetentionConfig, db_operation, metrics};
use actix_web::rt;
use tokio::sync::watch;

/// Background loop that periodically deletes old terminal tasks based on retention config.
/// Returns immediately if retention is not enabled.
pub async fn retention_cleanup_loop(
    pool: DbPool,
    retention_config: RetentionConfig,
    mut shutdown: watch::Receiver<bool>,
) {
    if !retention_config.enabled {
        log::info!("Retention cleanup: disabled, exiting");
        return;
    }

    log::info!(
        "Retention cleanup: enabled, retention_days={}, interval={}s, batch_size={}",
        retention_config.retention_days,
        retention_config.cleanup_interval_secs,
        retention_config.batch_size
    );

    let interval = std::time::Duration::from_secs(retention_config.cleanup_interval_secs);

    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                log::info!("Retention cleanup: shutdown signal received, exiting");
                return;
            }
            _ = rt::time::sleep(interval) => {}
        }

        let start = std::time::Instant::now();
        match pool.get().await {
            Ok(mut conn) => {
                match db_operation::cleanup_old_terminal_tasks(
                    &mut conn,
                    retention_config.retention_days,
                    retention_config.batch_size,
                )
                .await
                {
                    Ok(count) => {
                        let duration = start.elapsed().as_secs_f64();
                        if count > 0 {
                            log::info!(
                                "Retention cleanup: deleted {} tasks in {:.2}s",
                                count,
                                duration
                            );
                        } else {
                            log::debug!("Retention cleanup: no tasks to clean up");
                        }
                        metrics::record_retention_cleanup("success", count, duration);
                    }
                    Err(e) => {
                        let duration = start.elapsed().as_secs_f64();
                        log::error!("Retention cleanup: error: {:?}", e);
                        metrics::record_retention_cleanup("error", 0, duration);
                    }
                }
            }
            Err(e) => {
                let duration = start.elapsed().as_secs_f64();
                log::error!(
                    "Retention cleanup: could not acquire DB connection: {:?}",
                    e
                );
                metrics::record_retention_cleanup("error", 0, duration);
            }
        }
        metrics::record_worker_loop_iteration("retention", start.elapsed().as_secs_f64());
    }
}
