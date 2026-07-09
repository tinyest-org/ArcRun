use crate::{DbPool, db_operation, metrics, workers::WorkerNudges};
use actix_web::rt;
use tokio::sync::watch;

/// Background loop that detects timed-out Running tasks and failed stale claims.
///
/// For each timed-out task, the mark-failed + child propagation + outbox enqueue
/// runs inside a single transaction so a crash between them cannot leave children
/// stuck in Waiting or lose the on_failure notification. Webhooks themselves are
/// delivered async by the delivery loop (Lot 2).
pub async fn timeout_loop(
    pool: DbPool,
    interval: std::time::Duration,
    claim_timeout: std::time::Duration,
    dead_end_enabled: bool,
    mut shutdown: watch::Receiver<bool>,
    nudges: WorkerNudges,
) {
    loop {
        let loop_start = std::time::Instant::now();
        // note that obtaining a connection from the pool is also potentially blocking
        let conn = pool.get();

        if let Ok(mut conn) = conn.await {
            // --- Requeue stale Claimed tasks ---
            let requeued =
                db_operation::requeue_stale_claimed_tasks(&mut conn, claim_timeout).await;
            match requeued {
                Ok(tasks) => {
                    if !tasks.is_empty() {
                        for _ in &tasks {
                            metrics::record_status_transition("Claimed", "Pending");
                        }
                        log::warn!(
                            "Timeout worker: requeued {} stale claimed tasks, {:?}",
                            tasks.len(),
                            tasks.iter().map(|t| t.id).collect::<Vec<_>>()
                        );
                    } else {
                        log::debug!("Timeout worker: no stale claimed tasks");
                    }
                }
                Err(e) => {
                    log::error!("Timeout worker: error requeuing claimed tasks: {:?}", e);
                }
            }

            // --- Timeout Running tasks ---
            // Step 1: find timed-out task IDs (read-only, no lock)
            let timed_out_ids = match db_operation::find_timed_out_tasks(&mut conn).await {
                Ok(ids) => ids,
                Err(e) => {
                    log::error!("Timeout worker: error finding timed-out tasks: {:?}", e);
                    vec![]
                }
            };

            if !timed_out_ids.is_empty() {
                log::warn!(
                    "Timeout worker: {} tasks timed out, {:?}",
                    timed_out_ids.len(),
                    &timed_out_ids
                );
            } else {
                log::debug!("Timeout worker: no tasks timed out");
            }

            // Step 2: for each, atomically mark failed + propagate (in tx), then fire webhooks
            let mut enqueued_outbox = false;
            for task_id in timed_out_ids {
                let result =
                    db_operation::timeout_task_and_propagate(&mut conn, task_id, dead_end_enabled)
                        .await;

                match result {
                    Ok(Some((_failed_task, _cascade_failed, _canceled_ancestors))) => {
                        metrics::record_task_timeout();
                        metrics::record_status_transition("Running", "Failure");
                        // on_failure notifications (task + cascade + dead-end
                        // ancestors) were enqueued into the outbox inside the
                        // transaction; the delivery loop sends them async.
                        enqueued_outbox = true;
                    }
                    Ok(None) => {
                        // Task already transitioned concurrently, nothing to do
                        log::debug!(
                            "Timeout worker: task {} already transitioned, skipping",
                            task_id
                        );
                    }
                    Err(e) => {
                        log::error!(
                            "Timeout worker: failed to timeout task {}: {:?}",
                            task_id,
                            e
                        );
                    }
                }
            }

            // B4: wake the delivery loop for the on_failure outbox rows just enqueued,
            // rather than letting them wait a full delivery tick. Best-effort — the
            // delivery poll is the fallback.
            if enqueued_outbox {
                nudges.nudge_delivery();
            }
        }
        metrics::record_worker_loop_iteration("timeout", loop_start.elapsed().as_secs_f64());
        tokio::select! {
            _ = shutdown.changed() => {
                log::info!("Timeout worker: shutdown signal received, exiting");
                return;
            }
            _ = rt::time::sleep(interval) => {}
        }
    }
}
