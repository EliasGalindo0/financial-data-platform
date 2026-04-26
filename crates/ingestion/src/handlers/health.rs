use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};

use crate::routes::AppState;

pub async fn health_check() -> impl IntoResponse {
    StatusCode::OK
}

pub async fn readiness_check(State(state): State<AppState>) -> impl IntoResponse {
    // Check DB connectivity
    match sqlx::query("SELECT 1").fetch_one(&state.pool).await {
        Ok(_) => (StatusCode::OK, Json(serde_json::json!({"status": "ready"}))).into_response(),
        Err(e) => {
            tracing::error!(error = %e, "readiness check failed: database unreachable");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"status": "not_ready", "reason": "database"})),
            )
                .into_response()
        }
    }
}
