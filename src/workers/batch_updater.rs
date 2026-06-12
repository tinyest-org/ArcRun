use crate::{Conn, DbPool, db_operation::DbError, metrics};
use actix_web::rt;
use dashmap::DashMap;
use diesel_async::RunQueryDsl;
use std::sync::{Arc, atomic::AtomicI32, atomic::Ordering};
use tokio::sync::{mpsc, watch};

#[derive(Debug)]
pub struct UpdateEvent {
    pub success: i32,
    pub failures: i32,
    pub task_id: uuid::Uuid,
}

#[derive(Debug, Default)]
struct Entry {
    success: AtomicI32,
    failures: AtomicI32,
}

/// Receives success/failure update events and batches them to the database.
/// Uses DashMap for lock-free concurrent access between receiver and updater.
pub async fn batch_updater(
    pool: DbPool,
    receiver: mpsc::Receiver<UpdateEvent>,
    flush_interval: std::time::Duration,
    mut shutdown: watch::Receiver<bool>,
) {
    let data: Arc<DashMap<uuid::Uuid, Entry>> = Arc::new(DashMap::new());
    let mut receiver = receiver;

    // Receiver task - continuously drains channel without blocking updater
    tokio::spawn({
        let data = Arc::clone(&data);
        async move {
            while let Some(evt) = receiver.recv().await {
                // Single entry() call holds the shard lock for both updates
                let entry = data.entry(evt.task_id).or_default();
                entry.success.fetch_add(evt.success, Ordering::Relaxed);
                entry.failures.fetch_add(evt.failures, Ordering::Relaxed);
            }
        }
    });

    // Updater loop - persists batched counts to database
    loop {
        let loop_start = std::time::Instant::now();
        if let Ok(mut conn) = pool.get().await {
            // Collect updates - DashMap iter() doesn't block other operations on different shards
            let updates: Vec<(uuid::Uuid, i32, i32)> = data
                .iter()
                .map(|entry| {
                    let task_id = *entry.key();
                    let s = entry.value().success.swap(0, Ordering::Relaxed);
                    let f = entry.value().failures.swap(0, Ordering::Relaxed);
                    (task_id, s, f)
                })
                .filter(|(_, s, f)| *s != 0 || *f != 0)
                .collect();

            // Process updates in a single batched SQL query
            if !updates.is_empty() {
                let row_count = updates.len();
                log::debug!("Batch update: {} tasks to persist", row_count);

                let flush_start = std::time::Instant::now();
                if let Err(e) = handle_batch_with_counts(&updates, &mut conn).await {
                    log::error!(
                        "Failed to apply batched update for {} tasks: {:?}, re-queuing all counts",
                        row_count,
                        e
                    );
                    // Re-add all counts back for retry
                    for (task_id, success_count, failure_count) in updates {
                        let entry = data.entry(task_id).or_default();
                        entry.success.fetch_add(success_count, Ordering::Relaxed);
                        entry.failures.fetch_add(failure_count, Ordering::Relaxed);
                    }
                    metrics::record_batch_update_failure();
                }
                metrics::record_batch_updater_flush(row_count, flush_start.elapsed().as_secs_f64());
            }

            // Cleanup zero entries periodically
            data.retain(|_, entry| {
                AtomicI32::load(&entry.success, Ordering::Relaxed) != 0
                    || AtomicI32::load(&entry.failures, Ordering::Relaxed) != 0
            });
            metrics::set_batch_updater_pending_tasks(data.len());
        }
        metrics::record_worker_loop_iteration("batch_updater", loop_start.elapsed().as_secs_f64());
        tokio::select! {
            _ = shutdown.changed() => {
                log::info!("Batch updater: shutdown signal received, flushing remaining data");
                final_flush_batch_data(&data, &pool).await;
                log::info!("Batch updater: final flush complete, exiting");
                return;
            }
            _ = rt::time::sleep(flush_interval) => {}
        }
    }
}

/// Flush all remaining batch data to the database before shutdown.
async fn final_flush_batch_data(data: &DashMap<uuid::Uuid, Entry>, pool: &DbPool) {
    let updates: Vec<(uuid::Uuid, i32, i32)> = data
        .iter()
        .map(|e| {
            (
                *e.key(),
                e.value().success.swap(0, Ordering::Relaxed),
                e.value().failures.swap(0, Ordering::Relaxed),
            )
        })
        .filter(|(_, s, f)| *s != 0 || *f != 0)
        .collect();

    if updates.is_empty() {
        return;
    }

    log::info!(
        "Batch updater final flush: {} entries to persist",
        updates.len()
    );

    if let Ok(mut conn) = pool.get().await {
        for (task_id, s, f) in updates {
            if let Err(e) = handle_one_with_counts(task_id, &mut conn, s, f).await {
                log::error!("Final flush FAILED for task {}: {:?}", task_id, e);
            }
        }
    } else {
        log::error!("Final flush: could not acquire DB connection, data lost");
    }
}

/// Apply counter updates for multiple tasks in a single SQL statement using UNNEST.
/// This reduces N round-trips to 1 for the common case.
async fn handle_batch_with_counts<'a>(
    updates: &[(uuid::Uuid, i32, i32)],
    conn: &mut Conn<'a>,
) -> Result<(), DbError> {
    let ids: Vec<uuid::Uuid> = updates.iter().map(|(id, _, _)| *id).collect();
    let successes: Vec<i32> = updates.iter().map(|(_, s, _)| *s).collect();
    let fail_counts: Vec<i32> = updates.iter().map(|(_, _, f)| *f).collect();

    diesel::sql_query(
        "UPDATE task SET \
            success = task.success + batch.s, \
            failures = task.failures + batch.f, \
            last_updated = NOW() \
        FROM UNNEST($1::uuid[], $2::int[], $3::int[]) AS batch(id, s, f) \
        WHERE task.id = batch.id",
    )
    .bind::<diesel::sql_types::Array<diesel::sql_types::Uuid>, _>(&ids)
    .bind::<diesel::sql_types::Array<diesel::sql_types::Integer>, _>(&successes)
    .bind::<diesel::sql_types::Array<diesel::sql_types::Integer>, _>(&fail_counts)
    .execute(conn)
    .await?;

    Ok(())
}

/// Apply counter update for a single task. Used during final shutdown flush
/// where individual error handling per task is needed.
async fn handle_one_with_counts<'a>(
    task_id: uuid::Uuid,
    conn: &mut Conn<'a>,
    success_count: i32,
    failure_count: i32,
) -> Result<(), DbError> {
    handle_batch_with_counts(&[(task_id, success_count, failure_count)], conn).await
}
