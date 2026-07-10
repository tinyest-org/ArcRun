use crate::{DbPool, config::RetentionConfig, db_operation, metrics};
use actix_web::rt;
use tokio::sync::watch;

/// Background loop that periodically deletes old terminal tasks based on retention config.
///
/// The loop itself ALWAYS runs: the `rule_slot` GC (Audit 2, D1) must happen even when
/// task retention is disabled (the default) — slot keys are metadata-derived and
/// unbounded, so skipping GC would leak `rule_slot` rows forever. Only the terminal-task
/// cleanup is gated by `retention_config.enabled`.
pub async fn retention_cleanup_loop(
    pool: DbPool,
    retention_config: RetentionConfig,
    mut shutdown: watch::Receiver<bool>,
) {
    if retention_config.enabled {
        log::info!(
            "Retention cleanup: enabled, retention_days={}, interval={}s, batch_size={}",
            retention_config.retention_days,
            retention_config.cleanup_interval_secs,
            retention_config.batch_size
        );
    } else {
        log::info!(
            "Retention cleanup: task retention disabled — loop still runs (interval={}s) \
             for the rule_slot GC only",
            retention_config.cleanup_interval_secs
        );
    }

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
                if retention_config.enabled {
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

                    // Archive purge (Audit 2, D6): only when RETENTION_ARCHIVE_DAYS > 0.
                    // `0` means keep the archive forever, so we never purge. Gated by
                    // `enabled` like the task move above. Best-effort: a failure is
                    // logged, not fatal.
                    if retention_config.archive_retention_days > 0 {
                        match db_operation::purge_old_archived_tasks(
                            &mut conn,
                            retention_config.archive_retention_days,
                            retention_config.batch_size,
                        )
                        .await
                        {
                            Ok(n) if n > 0 => log::info!(
                                "Retention cleanup: purged {} archived tasks older than {} days",
                                n,
                                retention_config.archive_retention_days
                            ),
                            Ok(_) => {}
                            Err(e) => {
                                log::error!("Retention cleanup: archive purge error: {:?}", e)
                            }
                        }
                    }
                }

                // GC empty concurrency slot rows (Audit 2, D1). Metadata-derived slot
                // keys are unbounded, so without this the `rule_slot` table grows
                // forever. Best-effort: a failure is logged, not fatal.
                match db_operation::gc_empty_rule_slots(&mut conn).await {
                    Ok(n) if n > 0 => log::debug!("Retention cleanup: GC'd {} empty rule slots", n),
                    Ok(_) => {}
                    Err(e) => log::error!("Retention cleanup: rule_slot GC error: {:?}", e),
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
