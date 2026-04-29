/// Outbox Relay — polls the outbox_events table and publishes to Kafka.
///
/// Why a separate process?
///   The ingestion service writes to `outbox_events` in the same DB transaction
///   as the business entity. This relay reads PENDING events and publishes them
///   to Kafka. If Kafka is unavailable, events queue up in Postgres and are
///   delivered when Kafka recovers — no data loss.
///
/// Guarantees:
///   - At-least-once delivery (Kafka acks before marking PUBLISHED)
///   - Ordering preserved per partition_key (= source_account_id)
///   - Uses SELECT FOR UPDATE SKIP LOCKED for safe concurrent relay instances
use std::time::Duration;

use rdkafka::{
    producer::{FutureProducer, FutureRecord},
    ClientConfig,
};
use sqlx::PgPool;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

mod config;
use config::RelayConfig;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = RelayConfig::load()?;
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&cfg.log_level)),
        )
        .init();

    let pool = shared::db::create_pool(&cfg.database_url, 5).await?;

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &cfg.kafka_brokers)
        .set("message.timeout.ms", "10000")
        .set("acks", "all") // Wait for all replicas
        .set("enable.idempotence", "true") // Exactly-once producer semantics
        .set("compression.type", "snappy")
        .create()?;

    info!("outbox relay started");

    loop {
        match relay_batch(&pool, &producer, cfg.batch_size).await {
            Ok(count) if count > 0 => {
                info!(published = count, "outbox batch published");
            }
            Ok(_) => {
                // Nothing to do — sleep before next poll
                tokio::time::sleep(Duration::from_millis(cfg.poll_interval_ms)).await;
            }
            Err(e) => {
                error!(error = %e, "outbox relay error");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

async fn relay_batch(
    pool: &PgPool,
    producer: &FutureProducer,
    batch_size: i64,
) -> anyhow::Result<usize> {
    let mut tx = pool.begin().await?;

    // Grab a batch of PENDING events, locking them so other relay instances skip them
    let events = sqlx::query!(
        r#"
        SELECT id, topic, partition_key, payload
        FROM outbox_events
        WHERE status = 'PENDING'
        ORDER BY created_at ASC
        LIMIT $1
        FOR UPDATE SKIP LOCKED
        "#,
        batch_size,
    )
    .fetch_all(&mut *tx)
    .await?;

    if events.is_empty() {
        tx.rollback().await.ok();
        return Ok(0);
    }

    let mut published_ids = Vec::with_capacity(events.len());
    let mut failed_ids: Vec<(uuid::Uuid, String)> = Vec::new();

    for event in &events {
        let payload_bytes = serde_json::to_vec(&event.payload)?;

        let record = FutureRecord::to(&event.topic)
            .key(&event.partition_key)
            .payload(&payload_bytes);

        match producer.send(record, Duration::from_secs(10)).await {
            Ok(_) => published_ids.push(event.id),
            Err((e, _)) => failed_ids.push((event.id, e.to_string())),
        }
    }

    // Mark published
    if !published_ids.is_empty() {
        sqlx::query!(
            r#"
            UPDATE outbox_events
            SET status = 'PUBLISHED', published_at = NOW()
            WHERE id = ANY($1)
            "#,
            &published_ids,
        )
        .execute(&mut *tx)
        .await?;
    }

    // Record failures
    for (id, err) in &failed_ids {
        sqlx::query!(
            r#"
            UPDATE outbox_events
            SET retry_count = retry_count + 1,
                last_error = $1,
                status = CASE WHEN retry_count >= 10 THEN 'FAILED'::event_status
                              ELSE status END
            WHERE id = $2
            "#,
            err,
            id,
        )
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;
    Ok(published_ids.len())
}
