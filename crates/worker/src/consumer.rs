/// Kafka consumer loop.
///
/// Design choices:
///   - Manual offset commit: only after successful processing (at-least-once)
///   - Per-message advisory lock: prevents two workers racing on same transaction
///   - Exponential backoff: on retryable failures
///   - Dead-letter queue: after max_retries exhausted
///   - Span context propagation: W3C TraceContext from message headers
use std::sync::Arc;
use std::time::Duration;

use backon::{ExponentialBuilder, Retryable};
use rdkafka::{
    consumer::{CommitMode, Consumer, StreamConsumer},
    ClientConfig, Message,
};
use sqlx::PgPool;
use tracing::{error, info, instrument, warn, Span};
use uuid::Uuid;

use shared::events::EventEnvelope;

use crate::config::WorkerConfig;
use crate::processor::{self, ProcessError};

pub struct TransactionConsumer {
    worker_id: String,
    pool: Arc<PgPool>,
    config: Arc<WorkerConfig>,
}

impl TransactionConsumer {
    pub fn new(
        worker_id: String,
        pool: Arc<PgPool>,
        config: Arc<WorkerConfig>,
    ) -> Self {
        Self { worker_id, pool, config }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &self.config.kafka_brokers)
            .set("group.id", &self.config.kafka_group_id)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            // Session timeout: if worker stalls, partition rebalances to another
            .set("session.timeout.ms", "30000")
            .set("max.poll.interval.ms", "300000")
            .create()?;

        consumer.subscribe(&[&self.config.kafka_topic_transactions])?;
        info!(worker_id = %self.worker_id, "consumer started, waiting for messages");

        loop {
            match consumer.recv().await {
                Err(e) => {
                    error!(worker_id = %self.worker_id, error = %e, "kafka receive error");
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                Ok(msg) => {
                    let payload = match msg.payload() {
                        Some(p) => p.to_vec(),
                        None => {
                            warn!(worker_id = %self.worker_id, "received empty message, skipping");
                            consumer.commit_message(&msg, CommitMode::Async).ok();
                            continue;
                        }
                    };

                    // Deserialize envelope
                    let envelope: EventEnvelope = match serde_json::from_slice(&payload) {
                        Ok(e) => e,
                        Err(e) => {
                            error!(
                                worker_id = %self.worker_id,
                                error = %e,
                                "failed to deserialize event, sending to DLQ"
                            );
                            self.send_to_dlq(&payload, &e.to_string()).await;
                            consumer.commit_message(&msg, CommitMode::Async).ok();
                            continue;
                        }
                    };

                    // Process with retry
                    let result = self
                        .process_with_retry(&envelope)
                        .await;

                    match result {
                        Ok(()) => {
                            // Commit only after successful processing (at-least-once)
                            consumer.commit_message(&msg, CommitMode::Async).ok();
                        }
                        Err(e) => {
                            error!(
                                worker_id = %self.worker_id,
                                event_id = %envelope.event_id,
                                transaction_id = %envelope.aggregate_id,
                                error = %e,
                                "message processing permanently failed, sending to DLQ"
                            );
                            self.send_to_dlq(&payload, &e.to_string()).await;
                            // Commit so we don't reprocess — it's in the DLQ now
                            consumer.commit_message(&msg, CommitMode::Async).ok();
                        }
                    }
                }
            }
        }
    }

    #[instrument(
        skip(self, envelope),
        fields(
            worker_id = %self.worker_id,
            event_id = %envelope.event_id,
            event_type = %envelope.event_type,
            transaction_id = %envelope.aggregate_id,
            correlation_id = %envelope.correlation_id,
        )
    )]
    async fn process_with_retry(&self, envelope: &EventEnvelope) -> Result<(), ProcessError> {
        let pool = Arc::clone(&self.pool);
        let worker_id = self.worker_id.clone();
        let max_retries = self.config.max_retries;
        let backoff_ms = self.config.retry_backoff_ms;

        // Exponential backoff: 1s, 2s, 4s, 8s, 16s (capped)
        let backoff = ExponentialBuilder::default()
            .with_min_delay(Duration::from_millis(backoff_ms))
            .with_max_delay(Duration::from_secs(30))
            .with_max_times(max_retries as usize);

        let envelope_ref = envelope;
        let result = (|| async {
            processor::process_transaction(
                &pool,
                envelope_ref,
                &worker_id,
            )
            .await
        })
        .retry(backoff)
        .when(|e: &ProcessError| e.is_retryable())
        .await;

        result
    }

    async fn send_to_dlq(&self, payload: &[u8], error_message: &str) {
        // Persist to dead_letter_queue table — always visible, survives Kafka issues
        let result = sqlx::query!(
            r#"
            INSERT INTO dead_letter_queue (source_topic, payload, error_message)
            VALUES ($1, $2, $3)
            ON CONFLICT DO NOTHING
            "#,
            self.config.kafka_topic_transactions,
            serde_json::from_slice::<serde_json::Value>(payload)
                .unwrap_or(serde_json::json!({"raw": hex::encode(payload)})),
            error_message,
        )
        .execute(self.pool.as_ref())
        .await;

        if let Err(e) = result {
            error!(worker_id = %self.worker_id, error = %e, "CRITICAL: failed to write to DLQ");
            // In production: trigger PagerDuty / alert — this is a data loss risk
        }
    }
}
