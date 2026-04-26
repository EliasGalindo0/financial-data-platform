/// Domain events — published via the outbox to Kafka.
/// These are the "facts" that have happened; workers react to them.
///
/// Naming convention: <Aggregate><PastTense>
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::{Currency, TransactionType};

pub const TOPIC_TRANSACTIONS: &str = "financial.transactions.v1";
pub const TOPIC_ACCOUNTS: &str = "financial.accounts.v1";
pub const TOPIC_AUDIT: &str = "financial.audit.v1";

// ============================================================================
// ENVELOPE — all events are wrapped in this for versioning + tracing
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEnvelope {
    /// UUID v7 — time-sortable, good for Kafka key dedup
    pub event_id: Uuid,
    pub event_type: String,
    pub aggregate_type: String,
    pub aggregate_id: Uuid,
    pub occurred_at: DateTime<Utc>,
    /// Incremental version of the aggregate at time of event
    pub aggregate_version: i64,
    /// Distributed trace ID — propagate through all services
    pub correlation_id: Uuid,
    pub payload: serde_json::Value,
}

impl EventEnvelope {
    pub fn new(
        aggregate_type: impl Into<String>,
        aggregate_id: Uuid,
        aggregate_version: i64,
        event_type: impl Into<String>,
        correlation_id: Uuid,
        payload: impl Serialize,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            event_id: Uuid::now_v7(),
            event_type: event_type.into(),
            aggregate_type: aggregate_type.into(),
            aggregate_id,
            occurred_at: Utc::now(),
            aggregate_version,
            correlation_id,
            payload: serde_json::to_value(payload)?,
        })
    }
}

// ============================================================================
// TRANSACTION EVENTS
// ============================================================================

#[derive(Debug, Serialize, Deserialize)]
pub struct TransactionSubmitted {
    pub transaction_id: Uuid,
    pub idempotency_key: String,
    pub transaction_type: TransactionType,
    pub amount: Decimal,
    pub currency: Currency,
    pub source_account_id: Option<Uuid>,
    pub dest_account_id: Option<Uuid>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TransactionProcessing {
    pub transaction_id: Uuid,
    pub worker_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TransactionSettled {
    pub transaction_id: Uuid,
    pub source_account_id: Option<Uuid>,
    pub dest_account_id: Option<Uuid>,
    pub amount: Decimal,
    pub currency: Currency,
    pub ledger_entry_ids: Vec<Uuid>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TransactionFailed {
    pub transaction_id: Uuid,
    pub reason: String,
    pub retry_count: i32,
    pub is_final: bool, // false = will retry, true = sent to DLQ
}

// ============================================================================
// ACCOUNT EVENTS
// ============================================================================

#[derive(Debug, Serialize, Deserialize)]
pub struct AccountDebited {
    pub account_id: Uuid,
    pub transaction_id: Uuid,
    pub amount: Decimal,
    pub new_balance: Decimal,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AccountCredited {
    pub account_id: Uuid,
    pub transaction_id: Uuid,
    pub amount: Decimal,
    pub new_balance: Decimal,
}
