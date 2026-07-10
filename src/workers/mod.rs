mod batch_updater;
pub(crate) mod delivery_loop;
mod metrics_sampler;
mod nudge;
pub(crate) mod propagation;
mod retention;
mod start_loop;
mod timeout_loop;
pub(crate) mod webhooks;

pub use batch_updater::{UpdateEvent, batch_updater, run_counter_flush_once};
pub use delivery_loop::{DeliveryConfig, delivery_loop};
pub use nudge::WorkerNudges;
// Exposed for integration tests to drive outbox delivery deterministically (Lot 2).
pub use delivery_loop::run_delivery_once;
pub use metrics_sampler::metrics_sampler_loop;
pub use propagation::cancel_task;
pub(crate) use propagation::{cancel_dead_end_ancestors, propagate_to_children};
pub use retention::retention_cleanup_loop;
pub use start_loop::{start_loop, start_loop_leased};
// Exposed for integration tests of the paginated claim loop (Lot 1).
pub use start_loop::run_claim_loop;
pub use timeout_loop::timeout_loop;
pub(crate) use webhooks::{
    enqueue_cancel_outbox, enqueue_end_outbox_with_cascade, enqueue_outbox_for_canceled_ancestors,
};
