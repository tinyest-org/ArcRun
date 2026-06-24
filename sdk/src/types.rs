//! Types mirroring the ArcRun API DTOs.
//!
//! These are standalone (no diesel/actix dependencies) and can be used
//! directly with the SDK client.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

// =============================================================================
// Enums
// =============================================================================

/// Task lifecycle status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum StatusKind {
    Waiting,
    Pending,
    Claimed,
    Running,
    Failure,
    Success,
    Paused,
    Canceled,
}

/// Action type. Currently only Webhook is supported.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ActionKindEnum {
    Webhook,
}

/// When an action is triggered in the task lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TriggerKind {
    Start,
    End,
    Cancel,
}

/// Condition that determines which End-trigger action fires.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TriggerCondition {
    Success,
    Failure,
}

/// HTTP method for webhook calls.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HttpVerb {
    Get,
    Post,
    Delete,
    Put,
    Patch,
}

// =============================================================================
// Webhook params
// =============================================================================

/// Parameters for a Webhook action.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookParams {
    pub url: String,
    pub verb: HttpVerb,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub headers: Option<HashMap<String, String>>,
}

// =============================================================================
// Concurrency rules
// =============================================================================

/// Criteria for matching tasks in concurrency rules and deduplication.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Matcher {
    pub status: StatusKind,
    pub kind: String,
    pub fields: Vec<String>,
}

/// Defines a concurrency limit.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ConcurencyRule {
    pub max_concurency: i32,
    pub matcher: Matcher,
}

/// A concurrency control strategy.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(tag = "type")]
pub enum Strategy {
    Concurency(ConcurencyRule),
}

/// Array of concurrency strategies attached to a task.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct Rules(pub Vec<Strategy>);

// =============================================================================
// Action DTOs
// =============================================================================

/// Input DTO for creating actions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewActionDto {
    pub kind: ActionKindEnum,
    pub params: serde_json::Value,
}

/// Output DTO for a registered action on a task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionDto {
    pub kind: ActionKindEnum,
    pub trigger: TriggerKind,
    pub params: serde_json::Value,
}

// =============================================================================
// Task DTOs
// =============================================================================

/// A dependency on another task within the same batch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dependency {
    pub id: String,
    pub requires_success: bool,
}

/// Input DTO for creating a new task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewTaskDto {
    pub id: String,
    pub name: String,
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dedupe_strategy: Option<Vec<Matcher>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rules: Option<Rules>,
    pub on_start: NewActionDto,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dependencies: Option<Vec<Dependency>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_count: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on_failure: Option<Vec<NewActionDto>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on_success: Option<Vec<NewActionDto>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dead_end_barrier: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority: Option<i32>,
}

/// Lightweight task representation returned by list endpoints.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BasicTaskDto {
    pub id: uuid::Uuid,
    pub name: String,
    pub kind: String,
    pub status: StatusKind,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub started_at: Option<chrono::DateTime<chrono::Utc>>,
    pub success: i32,
    pub failures: i32,
    pub expected_count: Option<i32>,
    pub ended_at: Option<chrono::DateTime<chrono::Utc>>,
    pub batch_id: Option<uuid::Uuid>,
    #[serde(default)]
    pub dead_end_barrier: bool,
    #[serde(default)]
    pub priority: i32,
}

/// Full task representation returned by GET /task/{id}.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskDto {
    pub id: uuid::Uuid,
    pub name: String,
    pub kind: String,
    pub status: StatusKind,
    pub timeout: i32,
    pub rules: Rules,
    pub metadata: serde_json::Value,
    pub actions: Vec<ActionDto>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub started_at: Option<chrono::DateTime<chrono::Utc>>,
    pub ended_at: Option<chrono::DateTime<chrono::Utc>>,
    pub last_updated: chrono::DateTime<chrono::Utc>,
    pub success: i32,
    pub failures: i32,
    pub expected_count: Option<i32>,
    pub failure_reason: Option<String>,
    pub batch_id: Option<uuid::Uuid>,
    #[serde(default)]
    pub dead_end_barrier: bool,
    #[serde(default)]
    pub priority: i32,
}

/// Payload for updating a task.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UpdateTaskDto {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<StatusKind>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_success: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_failures: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_count: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority: Option<i32>,
}

// =============================================================================
// DAG DTOs
// =============================================================================

/// A directed dependency link between two tasks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinkDto {
    pub parent_id: uuid::Uuid,
    pub child_id: uuid::Uuid,
    pub requires_success: bool,
}

/// Complete DAG structure for a batch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DagDto {
    pub tasks: Vec<BasicTaskDto>,
    pub links: Vec<LinkDto>,
}

// =============================================================================
// Pagination & Filter DTOs
// =============================================================================

/// Pagination parameters for list endpoints.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PaginationParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_size: Option<i64>,
}

/// Filter parameters for task listing.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TaskFilter {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<StatusKind>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_id: Option<uuid::Uuid>,
}

// =============================================================================
// Batch DTOs
// =============================================================================

/// Per-status task counts within a batch.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BatchStatusCounts {
    pub waiting: i64,
    pub pending: i64,
    pub claimed: i64,
    pub running: i64,
    pub success: i64,
    pub failure: i64,
    pub paused: i64,
    pub canceled: i64,
}

/// Summary of a batch with aggregated task statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchSummaryDto {
    pub batch_id: uuid::Uuid,
    pub total_tasks: i64,
    pub first_created_at: chrono::DateTime<chrono::Utc>,
    pub latest_updated_at: chrono::DateTime<chrono::Utc>,
    pub status_counts: BatchStatusCounts,
    pub kinds: Vec<String>,
    /// Optional business-level label for the batch (`None` when none was set).
    #[serde(default)]
    pub scope: Option<String>,
    /// Structured metadata attached to the batch (`{}` when none was set).
    #[serde(default)]
    pub metadata: serde_json::Value,
}

/// Filter parameters for batch listing.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BatchFilter {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<StatusKind>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_after: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_before: Option<chrono::DateTime<chrono::Utc>>,
    /// Filter by batch scope (exact match).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    /// Filter by batch metadata using JSONB containment (`@>`). A JSON object
    /// serialized to a string, e.g. `{"env":"prod"}`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<String>,
    /// Free-text substring search across the batch scope and its metadata (as text).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub search: Option<String>,
}

// =============================================================================
// Health DTOs
// =============================================================================

/// Health check response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
    pub database: String,
    pub pool_size: u32,
    pub pool_idle: u32,
}

/// Readiness check response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadyResponse {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

// =============================================================================
// Batch creation request (object form)
// =============================================================================

/// Object-form `POST /task` body: a task list plus optional batch-level fields
/// (`on_batch_complete` webhook, `scope`, `metadata`). Use [`Client::create_batch`]
/// to send it; for the legacy bare-array form use [`Client::create_tasks`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTaskBatchDto {
    pub tasks: Vec<NewTaskDto>,
    /// Webhook actions fired exactly once when the last task of the batch becomes terminal.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on_batch_complete: Option<Vec<NewActionDto>>,
    /// Optional business-level label attached to the batch (filterable/searchable).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    /// Optional structured metadata attached to the batch (filterable/searchable).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

impl CreateTaskBatchDto {
    /// Start an object-form batch request from a task list.
    pub fn new(tasks: Vec<NewTaskDto>) -> Self {
        Self {
            tasks,
            on_batch_complete: None,
            scope: None,
            metadata: None,
        }
    }

    /// Attach a business-level scope label to the batch.
    pub fn scope(mut self, scope: impl Into<String>) -> Self {
        self.scope = Some(scope.into());
        self
    }

    /// Attach structured metadata to the batch.
    pub fn metadata(mut self, metadata: serde_json::Value) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Register batch-complete webhook actions.
    pub fn on_batch_complete(mut self, actions: Vec<NewActionDto>) -> Self {
        self.on_batch_complete = Some(actions);
        self
    }
}

// =============================================================================
// Batch creation result
// =============================================================================

/// Result of creating a task batch.
#[derive(Debug, Clone)]
pub struct CreateBatchResult {
    /// The batch UUID (from the X-Batch-ID header).
    pub batch_id: uuid::Uuid,
    /// Created tasks. Empty if all tasks were deduplicated (HTTP 204).
    pub tasks: Vec<BasicTaskDto>,
}

// =============================================================================
// Builders
// =============================================================================

/// Builder for constructing `NewTaskDto` ergonomically.
pub struct TaskBuilder {
    id: String,
    name: String,
    kind: String,
    on_start: NewActionDto,
    timeout: Option<i32>,
    metadata: Option<serde_json::Value>,
    dedupe_strategy: Option<Vec<Matcher>>,
    rules: Option<Rules>,
    dependencies: Option<Vec<Dependency>>,
    expected_count: Option<i32>,
    on_failure: Option<Vec<NewActionDto>>,
    on_success: Option<Vec<NewActionDto>>,
    dead_end_barrier: Option<bool>,
    priority: Option<i32>,
}

impl TaskBuilder {
    /// Create a new task builder with required fields.
    pub fn new(
        id: impl Into<String>,
        name: impl Into<String>,
        kind: impl Into<String>,
        on_start: NewActionDto,
    ) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            kind: kind.into(),
            on_start,
            timeout: None,
            metadata: None,
            dedupe_strategy: None,
            rules: None,
            dependencies: None,
            expected_count: None,
            on_failure: None,
            on_success: None,
            dead_end_barrier: None,
            priority: None,
        }
    }

    pub fn timeout(mut self, timeout: i32) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn metadata(mut self, metadata: serde_json::Value) -> Self {
        self.metadata = Some(metadata);
        self
    }

    pub fn dedupe_strategy(mut self, strategy: Vec<Matcher>) -> Self {
        self.dedupe_strategy = Some(strategy);
        self
    }

    pub fn rules(mut self, rules: Rules) -> Self {
        self.rules = Some(rules);
        self
    }

    pub fn dependency(mut self, id: impl Into<String>, requires_success: bool) -> Self {
        self.dependencies
            .get_or_insert_with(Vec::new)
            .push(Dependency {
                id: id.into(),
                requires_success,
            });
        self
    }

    pub fn expected_count(mut self, count: i32) -> Self {
        self.expected_count = Some(count);
        self
    }

    pub fn on_failure(mut self, action: NewActionDto) -> Self {
        self.on_failure.get_or_insert_with(Vec::new).push(action);
        self
    }

    pub fn on_success(mut self, action: NewActionDto) -> Self {
        self.on_success.get_or_insert_with(Vec::new).push(action);
        self
    }

    pub fn dead_end_barrier(mut self, val: bool) -> Self {
        self.dead_end_barrier = Some(val);
        self
    }

    pub fn priority(mut self, priority: i32) -> Self {
        self.priority = Some(priority);
        self
    }

    /// Build the `NewTaskDto`.
    pub fn build(self) -> NewTaskDto {
        NewTaskDto {
            id: self.id,
            name: self.name,
            kind: self.kind,
            timeout: self.timeout,
            metadata: self.metadata,
            dedupe_strategy: self.dedupe_strategy,
            rules: self.rules,
            on_start: self.on_start,
            dependencies: self.dependencies,
            expected_count: self.expected_count,
            on_failure: self.on_failure,
            on_success: self.on_success,
            dead_end_barrier: self.dead_end_barrier,
            priority: self.priority,
        }
    }
}

/// Helper to create a webhook action quickly.
pub fn webhook(url: impl Into<String>, verb: HttpVerb) -> NewActionDto {
    NewActionDto {
        kind: ActionKindEnum::Webhook,
        params: serde_json::to_value(WebhookParams {
            url: url.into(),
            verb,
            body: None,
            headers: None,
        })
        .expect("WebhookParams serialization cannot fail"),
    }
}

/// Helper to create a webhook action with a JSON body.
pub fn webhook_with_body(
    url: impl Into<String>,
    verb: HttpVerb,
    body: serde_json::Value,
) -> NewActionDto {
    NewActionDto {
        kind: ActionKindEnum::Webhook,
        params: serde_json::to_value(WebhookParams {
            url: url.into(),
            verb,
            body: Some(body),
            headers: None,
        })
        .expect("WebhookParams serialization cannot fail"),
    }
}
