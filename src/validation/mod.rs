//! Input validation for task creation and updates.
//!
//! This module provides validation functions for DTOs before they are processed.

mod action;
pub mod constants;
mod ssrf;
mod task;

use crate::config::SecurityConfig;
use std::sync::OnceLock;

pub use action::validate_action_params;
pub use ssrf::{validate_webhook_url, validate_webhook_url_with_config};

// Internal SSRF helper shared with the delivery-time DNS resolver (Audit 2, A5).
pub(crate) use ssrf::check_resolved_ips;
pub use task::{
    validate_batch_meta, validate_new_task, validate_rules, validate_task_batch,
    validate_update_task, validate_update_task_counters,
};

/// Global security configuration for validation.
/// Must be initialized before validation is used.
static SECURITY_CONFIG: OnceLock<SecurityConfig> = OnceLock::new();

/// Initialize the security configuration for validation.
/// Should be called once at application startup.
pub fn init_security_config(config: SecurityConfig) {
    SECURITY_CONFIG
        .set(config)
        .expect("Security config already initialized");
}

/// Get the current security configuration.
/// Returns the default config if not initialized (useful for tests).
pub(crate) fn get_security_config() -> SecurityConfig {
    SECURITY_CONFIG
        .get()
        .cloned()
        .unwrap_or_else(SecurityConfig::default)
}

/// Validation error with details about what failed.
#[derive(Debug, Clone)]
pub struct ValidationError {
    pub field: String,
    pub message: String,
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.field, self.message)
    }
}

/// Result of validation - either Ok or a list of errors.
pub type ValidationResult = Result<(), Vec<ValidationError>>;
