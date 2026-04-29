/// Kafka consumer loop.
///
/// Design choices:
///   - Manual offset commit: only after successful processing (at-least-once delivery).
///   - Per-message advisory lock: prevents two workers racing on the same transaction.
///   - Exponential backoff: on retryable failures before sending to DLQ.
///   - Dead-letter queue: after max_retries exhausted.
///
/// DLQ offset-commit policy:
///   - Processing errors (valid messages that failed): commit ONLY after a successful
///     DLQ write. If the DLQ write fails, do NOT commit the offset so the message is
///     redelivered on restart and gets another chance to be written.
///   - Deserialization errors (malformed bytes that can never be processed): log the
///     full payload and commit regardless. The message is unrecoverable; blocking the
///     consumer indefinitely would be worse than the data loss.
use std::sync::Arc;
use std::time::Duration;

use backon::{ExponentialBuilder, Retryable};
use rdkafka::{
    consumer::{CommitMode, Consumer, StreamConsumer},
    ClientConfig, Message,
};
use sqlx::PgPool;
use tracing::{error, info, instrument, warn};

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
            // Session timeout: if this worker stalls, the partition rebalances.
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

                    // ── Deserialize ───────────────────────────────────────────
                    let envelope: EventEnvelope = match serde_json::from_slice(&payload) {
                        Ok(e) => e,
                        Err(e) => {
                            error!(
                                worker_id = %self.worker_id,
                                error = %e,
                                "failed to deserialize event envelope, sending to DLQ"
                            );
                            // Malformed bytes — unrecoverable regardless of retries.
                            // Attempt DLQ write; if it fails, log full payload and
                            // commit anyway to unblock the consumer.
                            if let Err(dlq_err) =
                                self.send_to_dlq(&payload, &e.to_string()).await
                            {
                                error!(
                                    worker_id = %self.worker_id,
                                    error = %dlq_err,
                                    payload_hex = %hex::encode(&payload),
                                    "CRITICAL: DLQ write failed for malformed message \
                                     — committing offset to unblock consumer (message lost)"
                                );
                            }
                            consumer.commit_message(&msg, CommitMode::Async).ok();
                            continue;
                        }
                    };

                    // ── Process with retry ────────────────────────────────────
                    let result = self.process_with_retry(&envelope).await;

                    match result {
                        Ok(()) => {
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
                            match self.send_to_dlq(&payload, &e.to_string()).await {
                                Ok(()) => {
                                    // DLQ accepted the message — safe to advance the offset.
                                    consumer.commit_message(&msg, CommitMode::Async).ok();
                                }
                                Err(dlq_err) => {
                                    // DLQ write failed. Do NOT commit the offset.
                                    // The message will be redelivered on restart, giving
                                    // the DLQ another chance once the DB recovers.
                                    error!(
                                        worker_id = %self.worker_id,
                                        transaction_id = %envelope.aggregate_id,
                                        error = %dlq_err,
                                        "CRITICAL: DLQ write failed — NOT committing Kafka \
                                         offset to prevent message loss (will redeliver)"
                                    );
                                }
                            }
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

        // Exponential backoff: 1s → 2s → 4s … capped at 30s.
        let backoff = ExponentialBuilder::default()
            .with_min_delay(Duration::from_millis(backoff_ms))
            .with_max_delay(Duration::from_secs(30))
            .with_max_times(max_retries as usize);

        let envelope_ref = envelope;
        (|| async {
            processor::process_transaction(&pool, envelope_ref, &worker_id).await
        })
        .retry(backoff)
        .when(|e: &ProcessError| e.is_retryable())
        .await
    }

    /// Write a failed message to the dead-letter queue table.
    ///
    /// Returns `Ok(())` on success. Returns `Err` if the DB write fails — the
    /// caller decides whether to commit the Kafka offset based on this result.
    async fn send_to_dlq(&self, payload: &[u8], error_message: &str) -> Result<(), sqlx::Error> {
        let json_payload = serde_json::from_slice::<serde_json::Value>(payload)
            .unwrap_or_else(|_| serde_json::json!({"raw": hex::encode(payload)}));

        sqlx::query!(
            r#"
            INSERT INTO dead_letter_queue (source_topic, payload, error_message)
            VALUES ($1, $2, $3)
            ON CONFLICT DO NOTHING
            "#,
            self.config.kafka_topic_transactions,
            json_payload,
            error_message,
        )
        .execute(self.pool.as_ref())
        .await?;

        Ok(())
    }
}
