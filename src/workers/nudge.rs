//! In-process worker nudges (Audit 2, B4 — polling floor latency).
//!
//! The `start` and `delivery` worker loops poll the database on a fixed interval
//! (`WORKER_LOOP_INTERVAL_MS` / `WEBHOOK_DELIVERY_INTERVAL_MS`). At depth-N DAGs of
//! instantaneous tasks, every edge pays up to one full tick of scheduling latency,
//! so a 20-deep pipeline can spend tens of seconds in pure polling.
//!
//! `WorkerNudges` closes that floor: after a mutating handler (or a worker) commits
//! a transaction that could create work for a loop, it "nudges" that loop, which
//! then runs one extra iteration immediately instead of waiting for the next tick.
//!
//! ## Semantics — `notify_one`, NOT `notify_waiters`
//!
//! Each nudge calls [`tokio::sync::Notify::notify_one`], which stores a **permit** if
//! no consumer is currently parked in `notified()`. So a nudge fired while the loop
//! is mid-iteration is never lost: the permit survives, and the loop's *next*
//! `notified()` returns immediately, triggering exactly one more iteration. Coalescing
//! is intentional — many nudges during one iteration collapse to a single extra pass,
//! which is all that is needed (the pass drains the whole backlog). `notify_waiters`
//! would drop a signal sent while the loop is busy, reintroducing the latency floor.
//!
//! ## Contract — best-effort; the poll is the correctness
//!
//! Nudges are a pure latency optimization layered on top of the unchanged polling
//! loop. A nudge too many costs one harmless empty iteration; a nudge that is missed
//! (or a producer that forgets to nudge) is always caught by the next poll tick. No
//! correctness property depends on a nudge ever being delivered — which is why this
//! is in-process only. (A multi-replica deployment would need LISTEN/NOTIFY, and even
//! then only as the same kind of optimization, never as correctness.)

use std::sync::Arc;
use tokio::sync::Notify;

/// Clonable bundle of the in-process wakeup signals for the poll-driven worker loops.
/// Cloning shares the same underlying `Notify`s (they are `Arc`-wrapped), so a clone
/// handed to a handler nudges the very loop spawned at startup.
#[derive(Clone)]
pub struct WorkerNudges {
    /// Wakes the `start` loop (new Pending work: task creation, unblocked children,
    /// resume).
    pub start: Arc<Notify>,
    /// Wakes the `delivery` loop (new outbox rows: end/cancel/failure/batch_complete).
    pub delivery: Arc<Notify>,
}

impl WorkerNudges {
    /// Fresh, independent signals. Used at startup and by tests that don't drive the
    /// loops through the nudge path.
    pub fn new() -> Self {
        Self {
            start: Arc::new(Notify::new()),
            delivery: Arc::new(Notify::new()),
        }
    }

    /// Nudge the start loop to run one extra iteration ASAP (best-effort — see module
    /// docs; the poll is the correctness).
    pub fn nudge_start(&self) {
        self.start.notify_one();
    }

    /// Nudge the delivery loop to run one extra iteration ASAP (best-effort).
    pub fn nudge_delivery(&self) {
        self.delivery.notify_one();
    }
}

impl Default for WorkerNudges {
    fn default() -> Self {
        Self::new()
    }
}
