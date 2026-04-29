use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ============================================================================
// VALUE OBJECTS
// ============================================================================

/// ISO 4217 currency code. We use a typed wrapper to prevent mixing currencies.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "currency_code", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Currency {
    Usd,
    Eur,
    Gbp,
    Jpy,
    Chf,
    Cad,
    Aud,
}

impl std::fmt::Display for Currency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Currency::Usd => "USD",
            Currency::Eur => "EUR",
            Currency::Gbp => "GBP",
            Currency::Jpy => "JPY",
            Currency::Chf => "CHF",
            Currency::Cad => "CAD",
            Currency::Aud => "AUD",
        };
        write!(f, "{s}")
    }
}

/// Money: amount + currency. We NEVER use f64. Decimal is exact.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Money {
    pub amount: Decimal,
    pub currency: Currency,
}

impl Money {
    pub fn new(amount: Decimal, currency: Currency) -> Result<Self, crate::error::DomainError> {
        if amount <= Decimal::ZERO {
            return Err(crate::error::DomainError::InvalidAmount(amount.to_string()));
        }
        Ok(Self { amount, currency })
    }
}

// ============================================================================
// TRANSACTION STATUS STATE MACHINE
//
//  PENDING ──► PROCESSING ──► SETTLED
//      │                          │
//      └──► FAILED                └──► REVERSED
//
// Only the worker may transition PENDING→PROCESSING (via advisory lock).
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "transaction_status", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TransactionStatus {
    Pending,
    Processing,
    Settled,
    Failed,
    Reversed,
}

impl TransactionStatus {
    pub fn can_transition_to(&self, next: &TransactionStatus) -> bool {
        use TransactionStatus::*;
        matches!(
            (self, next),
            (Pending, Processing)
                | (Processing, Settled)
                | (Processing, Failed)
                | (Processing, Pending) // retry reset
                | (Settled, Reversed)
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "transaction_type", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TransactionType {
    Credit,
    Debit,
    Transfer,
    Fee,
    Reversal,
    Adjustment,
}

impl std::fmt::Display for TransactionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            TransactionType::Credit => "CREDIT",
            TransactionType::Debit => "DEBIT",
            TransactionType::Transfer => "TRANSFER",
            TransactionType::Fee => "FEE",
            TransactionType::Reversal => "REVERSAL",
            TransactionType::Adjustment => "ADJUSTMENT",
        };
        write!(f, "{s}")
    }
}

// ============================================================================
// AGGREGATES
// ============================================================================

/// Transaction — the central aggregate.
/// We keep this intentionally flat for DB mapping; no nested structs.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct Transaction {
    pub id: Uuid,
    pub idempotency_key: String,
    pub transaction_type: TransactionType,
    pub status: TransactionStatus,
    pub amount: Decimal,
    pub currency: Currency,
    pub source_account_id: Option<Uuid>,
    pub dest_account_id: Option<Uuid>,
    pub description: Option<String>,
    pub metadata: serde_json::Value,
    pub request_hash: String,
    pub submitted_at: DateTime<Utc>,
    pub processed_at: Option<DateTime<Utc>>,
    pub settled_at: Option<DateTime<Utc>>,
    pub version: i64,
    pub worker_id: Option<String>,
    pub retry_count: i32,
    pub last_error: Option<String>,
    pub regulatory_flags: serde_json::Value,
    pub risk_score: Option<Decimal>,
    pub compliance_checked: bool,
}

impl Transaction {
    pub fn transition(&mut self, next: TransactionStatus) -> Result<(), crate::error::DomainError> {
        if !self.status.can_transition_to(&next) {
            return Err(crate::error::DomainError::InvalidStateTransition {
                id: self.id,
                from: format!("{:?}", self.status),
                to: format!("{next:?}"),
            });
        }
        self.status = next;
        self.version += 1;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct Account {
    pub id: Uuid,
    pub external_id: String,
    pub owner_id: String,
    pub currency: Currency,
    pub balance: Decimal,
    pub version: i64,
    pub is_active: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Account {
    pub fn debit(&mut self, amount: Decimal) -> Result<(), crate::error::DomainError> {
        if !self.is_active {
            return Err(crate::error::DomainError::AccountInactive(self.id));
        }
        if self.balance < amount {
            return Err(crate::error::DomainError::InsufficientFunds {
                account_id: self.id,
                required: amount.to_string(),
                available: self.balance.to_string(),
            });
        }
        self.balance -= amount;
        self.version += 1;
        Ok(())
    }

    pub fn credit(&mut self, amount: Decimal) -> Result<(), crate::error::DomainError> {
        if !self.is_active {
            return Err(crate::error::DomainError::AccountInactive(self.id));
        }
        self.balance += amount;
        self.version += 1;
        Ok(())
    }
}

/// Ledger entry — immutable. One transaction creates two (debit + credit).
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct LedgerEntry {
    pub id: Uuid,
    pub transaction_id: Uuid,
    pub account_id: Uuid,
    pub entry_type: String, // "DEBIT" | "CREDIT"
    pub amount: Decimal,
    pub currency: Currency,
    pub running_balance: Decimal,
    pub posted_at: DateTime<Utc>,
    pub sequence_num: i64,
}
