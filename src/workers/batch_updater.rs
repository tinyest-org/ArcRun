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

type CountMap = DashMap<uuid::Uuid, Entry>;

/// A single pending counter delta for one task.
type Update = (uuid::Uuid, i32, i32);

/// Accumulate one incoming event into the shared map. A single `entry()` call
/// holds the shard lock for both counter updates.
fn apply_event(data: &CountMap, evt: &UpdateEvent) {
    let entry = data.entry(evt.task_id).or_default();
    entry.success.fetch_add(evt.success, Ordering::Relaxed);
    entry.failures.fetch_add(evt.failures, Ordering::Relaxed);
}

/// Re-add a set of counts to the map for a later retry (transient-failure path).
fn requeue(data: &CountMap, updates: impl IntoIterator<Item = Update>) {
    for (task_id, success_count, failure_count) in updates {
        let entry = data.entry(task_id).or_default();
        entry.success.fetch_add(success_count, Ordering::Relaxed);
        entry.failures.fetch_add(failure_count, Ordering::Relaxed);
    }
}

/// Atomically swap out the accumulated counters, returning the non-zero deltas.
fn drain_updates(data: &CountMap) -> Vec<Update> {
    data.iter()
        .map(|entry| {
            let task_id = *entry.key();
            let s = entry.value().success.swap(0, Ordering::Relaxed);
            let f = entry.value().failures.swap(0, Ordering::Relaxed);
            (task_id, s, f)
        })
        .filter(|(_, s, f)| *s != 0 || *f != 0)
        .collect()
}

/// Receiver-side drain loop. Continuously moves channel events into the shared
/// map so the updater loop never has to touch the channel. On shutdown it drains
/// every already-buffered event (`try_recv`) before exiting, so no in-flight
/// event is lost between the channel and the final flush snapshot.
async fn receiver_drain_loop(
    data: Arc<CountMap>,
    mut receiver: mpsc::Receiver<UpdateEvent>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            maybe = receiver.recv() => match maybe {
                Some(evt) => apply_event(&data, &evt),
                // All senders dropped: nothing more will ever arrive.
                None => break,
            },
            _ = shutdown.changed() => {
                // Shutdown requested (or the watch sender was dropped). Drain
                // everything currently buffered in the channel, then exit so the
                // updater's final flush sees a complete snapshot.
                while let Ok(evt) = receiver.try_recv() {
                    apply_event(&data, &evt);
                }
                break;
            }
        }
    }
}

/// Receives success/failure update events and batches them to the database.
/// Uses DashMap for lock-free concurrent access between receiver and updater.
pub async fn batch_updater(
    pool: DbPool,
    receiver: mpsc::Receiver<UpdateEvent>,
    flush_interval: std::time::Duration,
    mut shutdown: watch::Receiver<bool>,
) {
    let data: Arc<CountMap> = Arc::new(DashMap::new());

    // Receiver task - continuously drains channel without blocking updater.
    // It shares the shutdown watch so it can flush the channel before exit.
    let recv_handle = tokio::spawn(receiver_drain_loop(
        Arc::clone(&data),
        receiver,
        shutdown.clone(),
    ));

    // Updater loop - persists batched counts to database
    loop {
        let loop_start = std::time::Instant::now();
        if let Ok(mut conn) = pool.get().await {
            // Collect updates - DashMap iter() doesn't block other operations on different shards
            let updates = drain_updates(&data);

            // Process updates in a single batched SQL query
            if !updates.is_empty() {
                let row_count = updates.len();
                log::debug!("Batch update: {} tasks to persist", row_count);

                let flush_start = std::time::Instant::now();
                flush_updates(updates, &mut conn, &data).await;
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
                log::info!("Batch updater: shutdown signal received, draining channel then flushing");
                // Let the receiver drain any still-buffered events into the map and
                // exit BEFORE we snapshot for the final flush. This closes the race
                // where in-flight events were lost at shutdown (audit A7, C6).
                let _ = recv_handle.await;
                final_flush_batch_data(&data, &pool).await;
                log::info!("Batch updater: final flush complete, exiting");
                return;
            }
            _ = rt::time::sleep(flush_interval) => {}
        }
    }
}

/// Persist one drained batch, with anti-poison recovery.
///
/// The happy path is a single UNNEST statement. Rows whose task is terminal
/// (or gone) simply don't match and their counts are **dropped on the floor** —
/// a terminal task's counters are frozen and were
/// already delivered with the end notification, so re-queuing them would diverge
/// forever (audit A7). A drop is NOT an error: on `Ok` we never re-queue.
///
/// On a DB error we fall back to per-row so a single deterministically-faulty
/// ("poison") row cannot wedge the whole pipeline.
async fn flush_updates(updates: Vec<Update>, conn: &mut Conn<'_>, data: &CountMap) {
    if let Err(e) = handle_batch_with_counts(&updates, conn).await {
        log::warn!(
            "Batched update failed for {} tasks ({:?}), recovering per-row",
            updates.len(),
            e
        );
        recover_per_row(updates, conn, data).await;
    }
}

/// Per-row recovery after a failed batch flush (anti-poison).
///
/// Each row is applied on its own. Two outcomes:
/// * **Every** row fails ⇒ the connection / DB is presumed down (a transient
///   fault): re-queue all counts for the next iteration — no data loss.
/// * **Some** rows succeed but others fail ⇒ the connection is demonstrably
///   alive, so the failing rows are *deterministically* faulty (poison). They
///   are dropped and logged (with a failure metric) so the pipeline keeps
///   flowing; the succeeding rows are already persisted.
async fn recover_per_row(updates: Vec<Update>, conn: &mut Conn<'_>, data: &CountMap) {
    let mut succeeded = 0usize;
    let mut failed: Vec<(Update, DbError)> = Vec::new();

    for update in updates {
        match handle_one_with_counts(update.0, conn, update.1, update.2).await {
            Ok(()) => succeeded += 1,
            Err(e) => failed.push((update, e)),
        }
    }

    if failed.is_empty() {
        return;
    }

    if succeeded == 0 {
        // Whole connection appears down -> transient, keep the data for retry.
        log::error!(
            "Per-row recovery: all {} rows failed (DB appears unavailable), re-queuing for retry",
            failed.len()
        );
        requeue(data, failed.into_iter().map(|(u, _)| u));
        metrics::record_batch_update_failure();
    } else {
        // Connection is alive but these specific rows keep failing -> poison.
        for ((task_id, s, f), e) in failed {
            log::error!(
                "Dropping poison batch-update row task={} (+{} success, +{} failures): {:?}",
                task_id,
                s,
                f,
                e
            );
            metrics::record_batch_update_failure();
        }
    }
}

/// Flush all remaining batch data to the database before shutdown.
async fn final_flush_batch_data(data: &CountMap, pool: &DbPool) {
    let updates = drain_updates(data);

    if updates.is_empty() {
        return;
    }

    log::info!(
        "Batch updater final flush: {} entries to persist",
        updates.len()
    );

    if let Ok(mut conn) = pool.get().await {
        // Same anti-poison / terminal-drop semantics as the steady-state flush,
        // but any transient re-queue here is best-effort (we are exiting).
        flush_updates(updates, &mut conn, data).await;
    } else {
        log::error!("Final flush: could not acquire DB connection, data lost");
    }
}

/// Apply counter updates for multiple tasks in a single SQL statement using UNNEST.
/// This reduces N round-trips to 1 for the common case.
///
/// Two audit-A7 guards live in the SQL:
/// * `AND task.status NOT IN ('success','failure','canceled')` — never mutate a
///   **terminal** task (its counters are frozen and were already delivered with
///   its end notification). Terminal rows silently don't match; their counts are
///   dropped by the caller (not re-queued). The audit phrased this as
///   `IN ('running','claimed')`; we gate on the terminal set instead so a genuine
///   progress update on a still-active but non-`running` task (e.g. `paused` after
///   a worker's last flush, or a `pending`/`waiting` task in the test harness) is
///   not silently discarded — same "terminal immutability" intent, no false drop.
/// * `LEAST(task.<c>::bigint + batch.<d>, 2147483647)` — the sum is computed in
///   `bigint` so `int + int` can never overflow, then clamped back to `int4`'s
///   max. Without the cast a large accumulated delta would raise `integer out of
///   range` and poison the flush forever.
async fn handle_batch_with_counts(updates: &[Update], conn: &mut Conn<'_>) -> Result<(), DbError> {
    let ids: Vec<uuid::Uuid> = updates.iter().map(|(id, _, _)| *id).collect();
    let successes: Vec<i32> = updates.iter().map(|(_, s, _)| *s).collect();
    let fail_counts: Vec<i32> = updates.iter().map(|(_, _, f)| *f).collect();

    diesel::sql_query(
        "UPDATE task SET \
            success = LEAST(task.success::bigint + batch.s, 2147483647), \
            failures = LEAST(task.failures::bigint + batch.f, 2147483647), \
            last_updated = NOW() \
        FROM UNNEST($1::uuid[], $2::int[], $3::int[]) AS batch(id, s, f) \
        WHERE task.id = batch.id \
          AND task.status NOT IN ('success', 'failure', 'canceled')",
    )
    .bind::<diesel::sql_types::Array<diesel::sql_types::Uuid>, _>(&ids)
    .bind::<diesel::sql_types::Array<diesel::sql_types::Integer>, _>(&successes)
    .bind::<diesel::sql_types::Array<diesel::sql_types::Integer>, _>(&fail_counts)
    .execute(conn)
    .await?;

    Ok(())
}

/// Apply counter update for a single task. Used by the per-row anti-poison
/// recovery and the final shutdown flush.
async fn handle_one_with_counts(
    task_id: uuid::Uuid,
    conn: &mut Conn<'_>,
    success_count: i32,
    failure_count: i32,
) -> Result<(), DbError> {
    handle_batch_with_counts(&[(task_id, success_count, failure_count)], conn).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_event_accumulates_into_single_entry() {
        let data = DashMap::new();
        let id = uuid::Uuid::new_v4();
        apply_event(
            &data,
            &UpdateEvent {
                success: 3,
                failures: 1,
                task_id: id,
            },
        );
        apply_event(
            &data,
            &UpdateEvent {
                success: 2,
                failures: 4,
                task_id: id,
            },
        );
        let entry = data.get(&id).unwrap();
        assert_eq!(AtomicI32::load(&entry.success, Ordering::Relaxed), 5);
        assert_eq!(AtomicI32::load(&entry.failures, Ordering::Relaxed), 5);
    }

    #[test]
    fn drain_updates_swaps_and_filters_zero() {
        let data = DashMap::new();
        let a = uuid::Uuid::new_v4();
        let b = uuid::Uuid::new_v4();
        apply_event(
            &data,
            &UpdateEvent {
                success: 7,
                failures: 0,
                task_id: a,
            },
        );
        // b accumulates a net-zero (should be filtered out of the drain)
        apply_event(
            &data,
            &UpdateEvent {
                success: 0,
                failures: 0,
                task_id: b,
            },
        );

        let mut updates = drain_updates(&data);
        updates.sort();
        assert_eq!(updates, vec![(a, 7, 0)]);

        // After draining, the counters were swapped to zero.
        assert_eq!(
            AtomicI32::load(&data.get(&a).unwrap().success, Ordering::Relaxed),
            0
        );
    }

    #[test]
    fn requeue_readds_counts_only_for_given_rows() {
        let data = DashMap::new();
        let a = uuid::Uuid::new_v4();
        let b = uuid::Uuid::new_v4();
        // a already has some residual counts; requeue must ADD, not replace.
        apply_event(
            &data,
            &UpdateEvent {
                success: 1,
                failures: 1,
                task_id: a,
            },
        );

        requeue(&data, vec![(a, 4, 0), (b, 0, 9)]);

        assert_eq!(
            AtomicI32::load(&data.get(&a).unwrap().success, Ordering::Relaxed),
            5
        );
        assert_eq!(
            AtomicI32::load(&data.get(&a).unwrap().failures, Ordering::Relaxed),
            1
        );
        assert_eq!(
            AtomicI32::load(&data.get(&b).unwrap().failures, Ordering::Relaxed),
            9
        );
    }

    /// Drain-before-flush at shutdown: events still buffered in the channel when
    /// the shutdown signal fires must land in the map before the receiver exits,
    /// so the final flush snapshot is complete (audit A7, C6). Without the
    /// `try_recv` drain the receiver would race the shutdown and drop them.
    #[tokio::test]
    async fn receiver_drains_buffered_events_before_exit() {
        let data = Arc::new(DashMap::new());
        let (tx, rx) = mpsc::channel(1024);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        // Enqueue a burst of events WITHOUT giving the receiver a chance to run,
        // so they sit buffered in the channel.
        let ids: Vec<uuid::Uuid> = (0..50).map(|_| uuid::Uuid::new_v4()).collect();
        for id in &ids {
            tx.send(UpdateEvent {
                success: 1,
                failures: 0,
                task_id: *id,
            })
            .await
            .unwrap();
        }

        let handle = tokio::spawn(receiver_drain_loop(Arc::clone(&data), rx, shutdown_rx));

        // Trigger shutdown; the receiver must drain the buffered burst before exit.
        shutdown_tx.send(true).unwrap();
        handle.await.unwrap();

        assert_eq!(
            data.len(),
            ids.len(),
            "every buffered event must be drained into the map before shutdown"
        );
        for id in &ids {
            assert_eq!(
                AtomicI32::load(&data.get(id).unwrap().success, Ordering::Relaxed),
                1
            );
        }
    }
}
