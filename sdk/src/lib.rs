//! # ArcRun SDK
//!
//! Rust client library for the ArcRun API.
//!
//! ## Quick start
//!
//! ```rust,no_run
//! use arcrun_sdk::{Client, TaskBuilder, webhook, HttpVerb};
//!
//! #[tokio::main]
//! async fn main() -> arcrun_sdk::Result<()> {
//!     let client = Client::new("http://localhost:8085");
//!
//!     // Check health
//!     let health = client.health().await?;
//!     println!("Status: {}", health.status);
//!
//!     // Create a task batch (DAG)
//!     let tasks = vec![
//!         TaskBuilder::new("build", "Build App", "ci", webhook("https://ci.example.com/build", HttpVerb::Post))
//!             .timeout(300)
//!             .build(),
//!         TaskBuilder::new("deploy", "Deploy App", "ci", webhook("https://ci.example.com/deploy", HttpVerb::Post))
//!             .dependency("build", true)
//!             .build(),
//!     ];
//!
//!     let result = client.create_tasks(tasks).await?;
//!     println!("Batch: {}, tasks created: {}", result.batch_id, result.tasks.len());
//!
//!     // Update a task status
//!     let task_id = result.tasks[0].id;
//!     client.succeed_task(task_id).await?;
//!
//!     Ok(())
//! }
//! ```

mod error;
mod types;

pub use error::{Error, Result};
pub use types::*;

use uuid::Uuid;

/// Client for the ArcRun API.
#[derive(Debug, Clone)]
pub struct Client {
    base_url: String,
    http: reqwest::Client,
}

impl Client {
    /// Create a new client pointing to the given ArcRun base URL.
    ///
    /// ```rust
    /// let client = arcrun_sdk::Client::new("http://localhost:8085");
    /// ```
    pub fn new(base_url: impl Into<String>) -> Self {
        let mut base = base_url.into();
        // Strip trailing slash for consistent URL building
        while base.ends_with('/') {
            base.pop();
        }
        Self {
            base_url: base,
            http: reqwest::Client::new(),
        }
    }

    /// Create a client with a custom `reqwest::Client` (e.g. for custom TLS, timeouts).
    pub fn with_http_client(base_url: impl Into<String>, http: reqwest::Client) -> Self {
        let mut base = base_url.into();
        while base.ends_with('/') {
            base.pop();
        }
        Self {
            base_url: base,
            http,
        }
    }

    // =========================================================================
    // Health
    // =========================================================================

    /// Check service health (GET /health).
    pub async fn health(&self) -> Result<HealthResponse> {
        let resp = self
            .http
            .get(format!("{}/health", self.base_url))
            .send()
            .await?;
        Self::parse_json(resp).await
    }

    /// Check service readiness (GET /ready).
    pub async fn ready(&self) -> Result<ReadyResponse> {
        let resp = self
            .http
            .get(format!("{}/ready", self.base_url))
            .send()
            .await?;
        Self::parse_json(resp).await
    }

    // =========================================================================
    // Task CRUD
    // =========================================================================

    /// Create a batch of tasks, optionally forming a DAG (POST /task).
    ///
    /// Sends the legacy bare-array body. To attach batch-level `scope`/`metadata`
    /// or an `on_batch_complete` webhook, use [`Client::create_batch`].
    ///
    /// Returns the batch ID and created tasks. If all tasks were deduplicated,
    /// `tasks` will be empty.
    pub async fn create_tasks(&self, tasks: Vec<NewTaskDto>) -> Result<CreateBatchResult> {
        let resp = self
            .http
            .post(format!("{}/task", self.base_url))
            .json(&tasks)
            .send()
            .await?;
        Self::handle_create_response(resp).await
    }

    /// Create a batch using the object-form body (POST /task), carrying optional
    /// batch-level `scope`, `metadata`, and `on_batch_complete` webhook.
    ///
    /// ```no_run
    /// # use arcrun_sdk::{Client, NewTaskDto, CreateTaskBatchDto};
    /// # async fn f(client: Client, tasks: Vec<NewTaskDto>) -> arcrun_sdk::Result<()> {
    /// let req = CreateTaskBatchDto::new(tasks)
    ///     .scope("team-payments")
    ///     .metadata(serde_json::json!({ "env": "prod" }));
    /// let result = client.create_batch(req).await?;
    /// # Ok(()) }
    /// ```
    pub async fn create_batch(&self, body: CreateTaskBatchDto) -> Result<CreateBatchResult> {
        let resp = self
            .http
            .post(format!("{}/task", self.base_url))
            .json(&body)
            .send()
            .await?;
        Self::handle_create_response(resp).await
    }

    /// Parse a `POST /task` response into a [`CreateBatchResult`] (shared by
    /// `create_tasks` and `create_batch`).
    async fn handle_create_response(resp: reqwest::Response) -> Result<CreateBatchResult> {
        let status = resp.status();
        let batch_id = resp
            .headers()
            .get("X-Batch-ID")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| Uuid::parse_str(v).ok())
            .unwrap_or_else(Uuid::nil);

        if status == reqwest::StatusCode::NO_CONTENT {
            return Ok(CreateBatchResult {
                batch_id,
                tasks: vec![],
            });
        }

        if !status.is_success() {
            return Err(Self::make_api_error(resp).await);
        }

        let tasks: Vec<BasicTaskDto> = resp
            .json()
            .await
            .map_err(|e| Error::Deserialize(e.to_string()))?;

        Ok(CreateBatchResult { batch_id, tasks })
    }

    /// Get full task details (GET /task/{id}).
    ///
    /// Returns `None` if the task is not found.
    pub async fn get_task(&self, task_id: Uuid) -> Result<Option<TaskDto>> {
        let resp = self
            .http
            .get(format!("{}/task/{}", self.base_url, task_id))
            .send()
            .await?;

        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }

        if !resp.status().is_success() {
            return Err(Self::make_api_error(resp).await);
        }

        let task: TaskDto = resp
            .json()
            .await
            .map_err(|e| Error::Deserialize(e.to_string()))?;
        Ok(Some(task))
    }

    /// List tasks with optional filtering and pagination (GET /task).
    pub async fn list_tasks(
        &self,
        filter: &TaskFilter,
        pagination: &PaginationParams,
    ) -> Result<Vec<BasicTaskDto>> {
        let resp = self
            .http
            .get(format!("{}/task", self.base_url))
            .query(filter)
            .query(pagination)
            .send()
            .await?;
        Self::parse_json(resp).await
    }

    /// Update a running task's status synchronously (PATCH /task/{id}).
    ///
    /// Use this to report task completion (Success or Failure).
    pub async fn update_task(&self, task_id: Uuid, update: UpdateTaskDto) -> Result<()> {
        let resp = self
            .http
            .patch(format!("{}/task/{}", self.base_url, task_id))
            .json(&update)
            .send()
            .await?;

        if !resp.status().is_success() {
            return Err(Self::make_api_error(resp).await);
        }
        Ok(())
    }

    /// Mark a task as succeeded (PATCH /task/{id} with status=Success).
    pub async fn succeed_task(&self, task_id: Uuid) -> Result<()> {
        self.update_task(
            task_id,
            UpdateTaskDto {
                status: Some(StatusKind::Success),
                ..Default::default()
            },
        )
        .await
    }

    /// Mark a task as failed (PATCH /task/{id} with status=Failure).
    pub async fn fail_task(&self, task_id: Uuid, reason: impl Into<String>) -> Result<()> {
        self.update_task(
            task_id,
            UpdateTaskDto {
                status: Some(StatusKind::Failure),
                failure_reason: Some(reason.into()),
                ..Default::default()
            },
        )
        .await
    }

    /// Queue a batch counter update (PUT /task/{id}).
    ///
    /// This is asynchronous on the server side. Returns immediately (202 Accepted).
    pub async fn batch_update(
        &self,
        task_id: Uuid,
        new_success: i32,
        new_failures: i32,
    ) -> Result<()> {
        let update = UpdateTaskDto {
            new_success: Some(new_success),
            new_failures: Some(new_failures),
            ..Default::default()
        };

        let resp = self
            .http
            .put(format!("{}/task/{}", self.base_url, task_id))
            .json(&update)
            .send()
            .await?;

        let status = resp.status();
        if !status.is_success() && status != reqwest::StatusCode::ACCEPTED {
            return Err(Self::make_api_error(resp).await);
        }
        Ok(())
    }

    /// Cancel a task (DELETE /task/{id}).
    pub async fn cancel_task(&self, task_id: Uuid) -> Result<()> {
        let resp = self
            .http
            .delete(format!("{}/task/{}", self.base_url, task_id))
            .send()
            .await?;

        if !resp.status().is_success() {
            return Err(Self::make_api_error(resp).await);
        }
        Ok(())
    }

    /// Pause a task (PATCH /task/pause/{id}).
    pub async fn pause_task(&self, task_id: Uuid) -> Result<()> {
        let resp = self
            .http
            .patch(format!("{}/task/pause/{}", self.base_url, task_id))
            .send()
            .await?;

        if !resp.status().is_success() {
            return Err(Self::make_api_error(resp).await);
        }
        Ok(())
    }

    // =========================================================================
    // DAG
    // =========================================================================

    /// Get the DAG structure for a batch (GET /dag/{batch_id}).
    pub async fn get_dag(&self, batch_id: Uuid) -> Result<DagDto> {
        let resp = self
            .http
            .get(format!("{}/dag/{}", self.base_url, batch_id))
            .send()
            .await?;
        Self::parse_json(resp).await
    }

    // =========================================================================
    // Batches
    // =========================================================================

    /// List batches with aggregated statistics (GET /batches).
    pub async fn list_batches(
        &self,
        filter: &BatchFilter,
        pagination: &PaginationParams,
    ) -> Result<Vec<BatchSummaryDto>> {
        let resp = self
            .http
            .get(format!("{}/batches", self.base_url))
            .query(filter)
            .query(pagination)
            .send()
            .await?;
        Self::parse_json(resp).await
    }

    // =========================================================================
    // Internal
    // =========================================================================

    async fn parse_json<T: serde::de::DeserializeOwned>(resp: reqwest::Response) -> Result<T> {
        if !resp.status().is_success() {
            return Err(Self::make_api_error(resp).await);
        }
        resp.json()
            .await
            .map_err(|e| Error::Deserialize(e.to_string()))
    }

    async fn make_api_error(resp: reqwest::Response) -> Error {
        let status = resp.status().as_u16();
        match resp.json::<error::ApiErrorBody>().await {
            Ok(body) => Error::Api {
                status,
                message: body.error.unwrap_or_else(|| "Unknown error".into()),
                details: body.details,
            },
            Err(_) => Error::Api {
                status,
                message: format!("HTTP {}", status),
                details: None,
            },
        }
    }
}

// =============================================================================
// TaskHandle — opaque handle for external workers
// =============================================================================

/// A handle to a single task, constructed from the opaque `?handle=` URL
/// passed to your `on_start` webhook.
///
/// This is the primary interface for external workers. It only exposes
/// the operations a worker needs: reporting progress and completion.
///
/// ```rust,no_run
/// use arcrun_sdk::TaskHandle;
///
/// async fn my_webhook(handle_url: &str) -> arcrun_sdk::Result<()> {
///     let handle = TaskHandle::new(handle_url);
///
///     // Report progress incrementally
///     handle.report_progress(10, 2).await?;
///
///     // Done
///     handle.succeed().await?;
///     Ok(())
/// }
/// ```
#[derive(Debug, Clone)]
pub struct TaskHandle {
    /// The raw handle URL (e.g. `http://host:8085/task/<uuid>`).
    url: String,
    http: reqwest::Client,
}

impl TaskHandle {
    /// Create a handle from the opaque URL received in `?handle=`.
    pub fn new(handle_url: impl Into<String>) -> Self {
        Self {
            url: handle_url.into(),
            http: reqwest::Client::new(),
        }
    }

    /// Create a handle with a custom `reqwest::Client`.
    pub fn with_http_client(handle_url: impl Into<String>, http: reqwest::Client) -> Self {
        Self {
            url: handle_url.into(),
            http,
        }
    }

    /// The raw handle URL.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Mark the task as succeeded (PATCH with status=Success).
    pub async fn succeed(&self) -> Result<()> {
        self.update(UpdateTaskDto {
            status: Some(StatusKind::Success),
            ..Default::default()
        })
        .await
    }

    /// Mark the task as failed (PATCH with status=Failure).
    pub async fn fail(&self, reason: impl Into<String>) -> Result<()> {
        self.update(UpdateTaskDto {
            status: Some(StatusKind::Failure),
            failure_reason: Some(reason.into()),
            ..Default::default()
        })
        .await
    }

    /// Report incremental progress counters (PUT, batched on server side).
    pub async fn report_progress(&self, success: i32, failures: i32) -> Result<()> {
        let update = UpdateTaskDto {
            new_success: Some(success),
            new_failures: Some(failures),
            ..Default::default()
        };

        let resp = self.http.put(&self.url).json(&update).send().await?;

        let status = resp.status();
        if !status.is_success() && status != reqwest::StatusCode::ACCEPTED {
            return Err(Client::make_api_error(resp).await);
        }
        Ok(())
    }

    /// Update the task with an arbitrary payload (PATCH).
    pub async fn update(&self, dto: UpdateTaskDto) -> Result<()> {
        let resp = self.http.patch(&self.url).json(&dto).send().await?;

        if !resp.status().is_success() {
            return Err(Client::make_api_error(resp).await);
        }
        Ok(())
    }
}
