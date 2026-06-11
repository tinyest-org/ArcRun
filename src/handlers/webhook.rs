use actix_web::{HttpResponse, web};

use crate::{db_operation, dtos, error::ApiError};

use super::AppState;

#[utoipa::path(
    get,
    path = "/webhook-deliveries",
    summary = "List webhook deliveries (outbox)",
    description = "Returns a paginated list of webhook delivery records from the transactional outbox. \
End and cancel webhooks are delivered at-least-once by the delivery loop; this endpoint exposes their \
state for observability — most usefully `?status=exhausted` to find deliveries that permanently failed \
after exhausting all retry attempts.

Filter by `status` (case-insensitive): `pending`, `success`, `failure`, `exhausted`. \
Supports standard `page`/`page_size` pagination. Ordered by most recently updated.",
    params(dtos::PaginationDto, dtos::WebhookDeliveryFilterDto),
    responses(
        (status = 200, description = "Paginated array of webhook delivery records", body = Vec<dtos::WebhookDeliveryDto>),
        (status = 400, description = "Invalid status filter value"),
    ),
    tag = "webhooks"
)]
/// List webhook delivery (outbox) records, optionally filtered by status.
pub async fn list_webhook_deliveries(
    state: web::Data<AppState>,
    pagination: web::Query<dtos::PaginationDto>,
    filter: web::Query<dtos::WebhookDeliveryFilterDto>,
) -> actix_web::Result<HttpResponse> {
    let status = filter.resolve_status().map_err(ApiError::BadRequest)?;

    let pagination = pagination.0.resolve(&state.config);

    let mut conn = state.conn().await?;
    let rows = db_operation::list_webhook_deliveries(
        &mut conn,
        status,
        pagination.limit,
        pagination.offset,
    )
    .await
    .map_err(ApiError::from)?;

    let body: Vec<dtos::WebhookDeliveryDto> = rows
        .into_iter()
        .map(dtos::WebhookDeliveryDto::from)
        .collect();
    Ok(HttpResponse::Ok().json(body))
}
