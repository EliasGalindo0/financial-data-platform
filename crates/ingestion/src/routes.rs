use axum::{
    routing::{get, post},
    Router,
};
use sqlx::PgPool;

use crate::config::AppConfig;
use crate::handlers;

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
    pub redis: redis::aio::ConnectionManager,
    pub config: AppConfig,
}

pub fn build_router(state: AppState) -> Router {
    Router::new()
        // Health / readiness
        .route("/health", get(handlers::health::health_check))
        .route("/ready", get(handlers::health::readiness_check))
        // Transaction ingestion
        .route("/v1/transactions", post(handlers::transactions::submit_transaction))
        .route("/v1/transactions/:id", get(handlers::transactions::get_transaction))
        // Account management
        .route("/v1/accounts", post(handlers::accounts::create_account))
        .route("/v1/accounts/:id", get(handlers::accounts::get_account))
        // Metrics (Prometheus scrape endpoint)
        .route("/metrics", get(handlers::metrics::metrics_handler))
        .with_state(state)
}
