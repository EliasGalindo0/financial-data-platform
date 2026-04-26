use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::Deserialize;
use uuid::Uuid;

use shared::domain::Currency;

use crate::{handlers::transactions::ApiError, routes::AppState};

#[derive(Debug, Deserialize)]
pub struct CreateAccountRequest {
    pub external_id: String,
    pub owner_id: String,
    pub currency: Currency,
}

pub async fn create_account(
    State(state): State<AppState>,
    Json(body): Json<CreateAccountRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let id = Uuid::now_v7();
    sqlx::query!(
        r#"
        INSERT INTO accounts (id, external_id, owner_id, currency)
        VALUES ($1, $2, $3, $4::currency_code)
        "#,
        id,
        body.external_id,
        body.owner_id,
        body.currency as Currency,
    )
    .execute(&state.pool)
    .await?;

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({ "id": id, "external_id": body.external_id })),
    ))
}

pub async fn get_account(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    let row = sqlx::query!(
        r#"
        SELECT id, external_id, owner_id, currency::text AS currency,
               balance, is_active, created_at
        FROM accounts WHERE id = $1
        "#,
        id
    )
    .fetch_optional(&state.pool)
    .await?
    .ok_or(ApiError::NotFound(id))?;

    Ok(Json(serde_json::json!({
        "id": row.id,
        "external_id": row.external_id,
        "owner_id": row.owner_id,
        "currency": row.currency,
        "balance": row.balance,
        "is_active": row.is_active,
        "created_at": row.created_at,
    })))
}
