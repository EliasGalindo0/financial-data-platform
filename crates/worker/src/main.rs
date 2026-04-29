use std::sync::Arc;
use tokio::signal;
use tracing::info;

mod config;
mod consumer;
mod fraud;
mod ledger;
mod processor;

use config::WorkerConfig;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = WorkerConfig::load()?;

    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&cfg.log_level)),
        )
        .init();

    info!("worker starting up");

    let pool = shared::db::create_pool(&cfg.database_url, cfg.db_max_connections).await?;
    sqlx::migrate!("../../migrations").run(&pool).await?;

    let pool = Arc::new(pool);
    let cfg = Arc::new(cfg);

    // Spawn N parallel consumer tasks
    let mut handles = Vec::new();
    for worker_id in 0..cfg.worker_concurrency {
        let worker_id = format!("{}-{worker_id}", cfg.worker_id_prefix);
        let pool = Arc::clone(&pool);
        let cfg = Arc::clone(&cfg);

        let handle = tokio::spawn(async move {
            let consumer = consumer::TransactionConsumer::new(worker_id, pool, cfg);
            consumer.run().await
        });

        handles.push(handle);
    }

    // Graceful shutdown on SIGTERM / Ctrl+C
    signal::ctrl_c().await?;
    info!("shutdown signal received, draining...");

    // Workers will finish their current message and stop
    for handle in handles {
        let _ = handle.await;
    }

    info!("worker stopped cleanly");
    Ok(())
}
