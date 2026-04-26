use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum DomainError {
    // Idempotency
    #[error("duplicate request: idempotency_key={0} already processed")]
    DuplicateRequest(String),

    // Validation
    #[error("validation failed: {0}")]
    ValidationError(String),

    #[error("amount must be positive, got {0}")]
    InvalidAmount(String),

    #[error("currency mismatch: source={source}, dest={dest}")]
    CurrencyMismatch { source: String, dest: String },

    // Business logic
    #[error("insufficient funds: account={account_id}, required={required}, available={available}")]
    InsufficientFunds {
        account_id: Uuid,
        required: String,
        available: String,
    },

    #[error("account not found: {0}")]
    AccountNotFound(Uuid),

    #[error("account is inactive: {0}")]
    AccountInactive(Uuid),

    #[error("transaction not found: {0}")]
    TransactionNotFound(Uuid),

    #[error("invalid state transition: {from} → {to} for transaction {id}")]
    InvalidStateTransition {
        id: Uuid,
        from: String,
        to: String,
    },

    // Optimistic locking
    #[error("concurrent modification detected for {entity} {id} (expected version {expected}, got {actual})")]
    OptimisticLockConflict {
        entity: String,
        id: Uuid,
        expected: i64,
        actual: i64,
    },

    // Infrastructure
    #[error("database error: {0}")]
    DatabaseError(#[from] sqlx::Error),

    #[error("serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("internal error: {0}")]
    Internal(String),
}

impl DomainError {
    /// Whether this error is safe to retry
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            DomainError::OptimisticLockConflict { .. }
                | DomainError::DatabaseError(sqlx::Error::PoolTimedOut)
        )
    }

    /// HTTP status code for API responses
    pub fn http_status(&self) -> u16 {
        match self {
            DomainError::DuplicateRequest(_) => 200, // Idempotent success — return original response
            DomainError::ValidationError(_)
            | DomainError::InvalidAmount(_)
            | DomainError::CurrencyMismatch { .. } => 422,
            DomainError::AccountNotFound(_) | DomainError::TransactionNotFound(_) => 404,
            DomainError::AccountInactive(_)
            | DomainError::InsufficientFunds { .. }
            | DomainError::InvalidStateTransition { .. } => 409,
            DomainError::OptimisticLockConflict { .. } => 409,
            _ => 500,
        }
    }
}
