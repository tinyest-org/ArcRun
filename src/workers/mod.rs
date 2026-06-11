mod batch_updater;
pub(crate) mod propagation;
mod retention;
mod start_loop;
mod timeout_loop;
pub(crate) mod webhooks;

pub use batch_updater::{UpdateEvent, batch_updater};
pub use propagation::cancel_task;
pub(crate) use propagation::{cancel_dead_end_ancestors, propagate_to_children};
pub use retention::retention_cleanup_loop;
pub use start_loop::start_loop;
// Exposed for integration tests of the paginated claim loop (Lot 1).
pub use start_loop::run_claim_loop;
pub use timeout_loop::timeout_loop;
pub(crate) use webhooks::{
    fire_cancel_webhooks, fire_end_webhooks_with_cascade, fire_webhooks_for_canceled_ancestors,
};
