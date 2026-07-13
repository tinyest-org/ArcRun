use actix_web::{HttpResponse, web};
use uuid::Uuid;

use crate::{db_operation, dtos, error::ApiError, metrics, validation};

use super::AppState;
use super::response::validation_error_response;

#[utoipa::path(
    delete,
    path = "/batch/{batch_id}",
    summary = "Stop a batch",
    description = "Cancel all non-terminal tasks in a batch. Waiting, Pending, Paused, and Running tasks are set to `Canceled` with failure_reason `\"Batch stopped\"`. Running tasks with registered cancel webhooks will have those webhooks fired.

Tasks already in a terminal state (Success, Failure, Canceled) are not modified.

Returns a summary of how many tasks were affected per status category.",
    params(("batch_id" = Uuid, Path, description = "The batch UUID (from the X-Batch-ID response header of POST /task)")),
    responses(
        (status = 200, description = "Batch stopped. Returns counts per status category.", body = dtos::StopBatchResponseDto),
        (status = 404, description = "No tasks found for this batch_id"),
    ),
    tag = "batches"
)]
/// Stop all non-terminal tasks in a batch
pub async fn stop_batch(
    state: web::Data<AppState>,
    batch_id: web::Path<Uuid>,
) -> actix_web::Result<HttpResponse> {
    let mut conn = state.conn().await?;
    let batch_id = *batch_id;

    let result = db_operation::stop_batch(&mut conn, batch_id)
        .await
        .map_err(ApiError::from)?;

    // Cancel webhooks for formerly-Running tasks were enqueued into the outbox
    // inside the stop_batch transaction; the delivery loop delivers them async.
    let canceled_running = result.canceled_running_ids.len() as i64;

    // Record metrics for each canceled task
    let total_canceled = result.canceled_waiting
        + result.canceled_pending
        + result.canceled_claimed
        + canceled_running
        + result.canceled_paused;
    for _ in 0..total_canceled {
        metrics::record_task_cancelled();
    }

    log::info!(
        "Batch {} stopped: waiting={}, pending={}, claimed={}, running={}, paused={}, already_terminal={}",
        batch_id,
        result.canceled_waiting,
        result.canceled_pending,
        result.canceled_claimed,
        canceled_running,
        result.canceled_paused,
        result.already_terminal,
    );

    // B4: cancel outbox rows for formerly-Running/Claimed tasks (and any batch_complete
    // signal) were enqueued in the stop_batch tx — nudge the delivery loop. Also nudge
    // start (harmless): the batch's tasks are all terminal now, so nothing new is
    // schedulable, but the delivery wake is the point.
    state.nudges.nudge_delivery();

    Ok(HttpResponse::Ok().json(dtos::StopBatchResponseDto {
        batch_id,
        canceled_waiting: result.canceled_waiting,
        canceled_pending: result.canceled_pending,
        canceled_claimed: result.canceled_claimed,
        canceled_running,
        canceled_paused: result.canceled_paused,
        already_terminal: result.already_terminal,
    }))
}

#[utoipa::path(
    get,
    path = "/batch/{batch_id}",
    summary = "Get batch stats",
    description = "Returns aggregated counters for a batch: total success, total failures, total expected count, and per-status task counts. Use this to track overall progress of a batch.",
    params(("batch_id" = Uuid, Path, description = "The batch UUID (from the X-Batch-ID response header of POST /task)")),
    responses(
        (status = 200, description = "Batch stats with aggregated counters", body = dtos::BatchStatsDto),
        (status = 404, description = "No tasks found for this batch_id"),
    ),
    tag = "batches"
)]
/// Get aggregated stats for a batch
pub async fn get_batch_stats(
    state: web::Data<AppState>,
    batch_id: web::Path<Uuid>,
) -> actix_web::Result<HttpResponse> {
    let mut conn = state.conn().await?;

    let stats = db_operation::get_batch_stats(&mut conn, *batch_id)
        .await
        .map_err(ApiError::from)?;

    Ok(match stats {
        Some(s) => HttpResponse::Ok().json(s),
        None => HttpResponse::NotFound().json(serde_json::json!({
            "error": "No tasks found for this batch_id"
        })),
    })
}

#[utoipa::path(
    patch,
    path = "/batch/{batch_id}/rules",
    summary = "Update batch rules by kind",
    description = "Update the concurrency/capacity rules for tasks that have not started yet (Waiting, Pending, or Paused) of a given kind in a batch. Claimed/Running tasks keep the rules and slots acquired when they started. Terminal tasks are not modified. Pass an empty rules array to remove all rules.

This is useful when you need to adjust capacity limits or concurrency constraints after a batch has been created — for example, scaling up allowed concurrency mid-run. The `kind` field targets only tasks of that category within the batch.",
    params(("batch_id" = Uuid, Path, description = "The batch UUID (from the X-Batch-ID response header of POST /task)")),
    request_body(content = dtos::UpdateBatchRulesDto, description = "Kind to target and new rules to apply."),
    responses(
        (status = 200, description = "Rules updated. Returns the count of affected tasks.", body = dtos::UpdateBatchRulesResponseDto),
        (status = 400, description = "Validation failed (invalid rules or empty kind)."),
        (status = 404, description = "No tasks found for this batch_id"),
    ),
    tag = "batches"
)]
/// Update concurrency/capacity rules for not-yet-active tasks of a given kind in a batch
pub async fn update_batch_rules(
    state: web::Data<AppState>,
    batch_id: web::Path<Uuid>,
    form: web::Json<dtos::UpdateBatchRulesDto>,
) -> actix_web::Result<HttpResponse> {
    // Validate kind
    if form.kind.trim().is_empty() {
        return Ok(HttpResponse::BadRequest().json(serde_json::json!({
            "error": "Validation failed",
            "details": ["kind: Task kind cannot be empty"]
        })));
    }

    // Validate the rules
    if let Err(errors) = validation::validate_rules(&form.rules) {
        return Ok(validation_error_response(&errors));
    }

    let mut conn = state.conn().await?;
    let batch_id = *batch_id;
    let kind = form.0.kind;
    let rules = form.0.rules;

    let updated_count = db_operation::update_batch_rules(&mut conn, batch_id, &kind, rules)
        .await
        .map_err(ApiError::from)?;

    log::info!(
        "Batch {} rules updated for kind '{}': {} tasks affected",
        batch_id,
        kind,
        updated_count,
    );

    Ok(HttpResponse::Ok().json(dtos::UpdateBatchRulesResponseDto {
        batch_id,
        kind,
        updated_count,
    }))
}

#[utoipa::path(
    get,
    path = "/batches",
    summary = "List batches with statistics",
    description = "Returns a paginated list of batches with aggregated task statistics per batch. Supports filtering by task name, kind, status, and creation time range. Each batch includes status counts, distinct kinds, and timestamps.",
    params(dtos::PaginationDto, dtos::BatchFilterDto),
    responses(
        (status = 200, description = "Paginated array of batch summaries", body = Vec<dtos::BatchSummaryDto>),
        (status = 400, description = "A filter value is invalid (for example malformed metadata JSON)"),
    ),
    tag = "batches"
)]
/// List batches with aggregated statistics
pub async fn list_batches(
    state: web::Data<AppState>,
    pagination: web::Query<dtos::PaginationDto>,
    filter: web::Query<dtos::BatchFilterDto>,
) -> actix_web::Result<HttpResponse> {
    let mut conn = state.conn().await?;
    let pagination = pagination.0.resolve(&state.config);

    let batches = db_operation::list_batches(&mut conn, pagination, filter.0)
        .await
        .map_err(ApiError::from)?;
    Ok(HttpResponse::Ok().json(batches))
}
