use std::net::SocketAddr;

use tower_http::{
    request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer},
    timeout::TimeoutLayer,
    trace::TraceLayer,
};
use tracing::info;

mod config;
mod handlers;
mod middleware;
mod repository;
mod routes;

pub use config::AppConfig;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load config from environment / config files
    let cfg = AppConfig::load()?;

    // Initialize tracing (structured JSON logs + OTLP export)
    init_tracing(&cfg)?;

    // DB connection pool
    let pool = shared::db::create_pool(&cfg.database_url, cfg.db_max_connections).await?;

    // Run migrations on startup — safe in prod (sqlx checks applied migrations)
    sqlx::migrate!("../../migrations").run(&pool).await?;

    // Redis for idempotency cache + rate limiting
    let redis = redis::Client::open(cfg.redis_url.as_str())?;
    let redis_conn = redis::aio::ConnectionManager::new(redis).await?;

    // Build application state
    let state = routes::AppState {
        pool,
        redis: redis_conn,
        config: cfg.clone(),
    };

    // Build router
    let app = routes::build_router(state)
        .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid))
        .layer(PropagateRequestIdLayer::x_request_id())
        .layer(TraceLayer::new_for_http())
        .layer(TimeoutLayer::new(std::time::Duration::from_secs(30)));

    let addr: SocketAddr = cfg.listen_addr.parse()?;
    info!("ingestion service listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

fn init_tracing(cfg: &AppConfig) -> anyhow::Result<()> {
    use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(&cfg.log_level));

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer().json())
        .init();

    Ok(())
}
