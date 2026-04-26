/// Reporting Service — CQRS read side.
///
/// This service reads only from PostgreSQL (read replicas in production).
/// It never writes to the main transaction tables — it's purely a projection
/// over the ledger and audit_log.
///
/// Reports generated:
///   - Daily transaction summary
///   - High-value transaction report (> configurable threshold)
///   - Suspicious Activity Report (SAR) scaffolding
///   - Currency Transaction Report (CTR) for > $10,000 cash equivalents
use axum::{routing::get, Router};
use tracing::info;

mod config;
mod reports;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = config::ReportingConfig::load()?;
    tracing_subscriber::fmt().json().init();

    let pool = shared::db::create_pool(&cfg.database_url, cfg.db_max_connections).await?;

    let state = reports::ReportState { pool };

    let app = Router::new()
        .route("/v1/reports/daily", get(reports::daily_summary))
        .route("/v1/reports/high-value", get(reports::high_value_transactions))
        .route("/v1/reports/ctr", get(reports::currency_transaction_report))
        .route("/health", get(|| async { "ok" }))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&cfg.listen_addr).await?;
    info!("reporting service listening on {}", cfg.listen_addr);
    axum::serve(listener, app).await?;

    Ok(())
}
