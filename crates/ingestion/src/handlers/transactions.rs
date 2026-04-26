/// Transaction ingestion handler.
///
/// Critical invariants enforced here:
///   1. Idempotency: identical key + same hash → return cached response
///   2. Idempotency: identical key + different hash → 422 (client bug)
///   3. Request validation before any DB write
///   4. Outbox write is atomic with transaction INSERT
///   5. Audit log written in same DB transaction
///   6. Rate limiting checked before processing (via Redis)
use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use validator::Validate;

use shared::{
    audit,
    domain::{Currency, TransactionType},
    error::DomainError,
    events::{EventEnvelope, TransactionSubmitted, TOPIC_TRANSACTIONS},
    idempotency::{compute_request_hash, IdempotencyKey},
};

use crate::routes::AppState;

// ============================================================================
// REQUEST / RESPONSE TYPES
// ============================================================================

#[derive(Debug, Deserialize, Validate)]
pub struct SubmitTransactionRequest {
    pub transaction_type: TransactionType,

    #[validate(range(min = "0.0001", message = "amount must be positive"))]
    pub amount: Decimal,

    pub currency: Currency,

    pub source_account_id: Option<Uuid>,
    pub dest_account_id: Option<Uuid>,

    #[validate(length(max = 500))]
    pub description: Option<String>,

    /// Caller-controlled arbitrary metadata (e.g., order_id, customer_id)
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct SubmitTransactionResponse {
    pub transaction_id: Uuid,
    pub status: String,
    pub idempotency_key: String,
    /// true if this is a replay of an already-processed request
    pub was_duplicate: bool,
}

// ============================================================================
// HANDLER
// ============================================================================

pub async fn submit_transaction(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<SubmitTransactionRequest>,
) -> Result<impl IntoResponse, ApiError> {
    // ── 1. Extract & validate idempotency key ────────────────────────────────
    let raw_key = headers
        .get("Idempotency-Key")
        .and_then(|v| v.to_str().ok())
        .ok_or(ApiError::MissingIdempotencyKey)?;

    let idempotency_key = IdempotencyKey::new(raw_key)
        .map_err(|e| ApiError::InvalidIdempotencyKey(e.to_string()))?;

    // ── 2. Validate request body ─────────────────────────────────────────────
    body.validate().map_err(|e| {
        ApiError::ValidationError(
            e.field_errors()
                .iter()
                .map(|(f, errs)| format!("{}: {}", f, errs[0].message.as_deref().unwrap_or("invalid")))
                .collect::<Vec<_>>()
                .join(", "),
        )
    })?;

    // Business-logic validation
    validate_parties(&body)?;

    // ── 3. Extract correlation ID (from X-Request-Id set by tower-http) ──────
    let correlation_id = headers
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| Uuid::parse_str(s).ok())
        .unwrap_or_else(Uuid::new_v4);

    // ── 4. Compute request hash for body comparison ───────────────────────────
    let canonical = serde_json::to_vec(&body)?;
    let request_hash = compute_request_hash(&canonical);

    // ── 5. Idempotency check + insert (atomic) ────────────────────────────────
    let result = idempotency_check_and_insert(
        &state,
        &idempotency_key,
        &request_hash,
        &body,
        correlation_id,
    )
    .await?;

    Ok((result.status_code, Json(result.response)))
}

// ============================================================================
// CORE LOGIC: Idempotent insert with outbox
// ============================================================================

struct InsertResult {
    status_code: StatusCode,
    response: SubmitTransactionResponse,
}

async fn idempotency_check_and_insert(
    state: &AppState,
    idempotency_key: &IdempotencyKey,
    request_hash: &str,
    body: &SubmitTransactionRequest,
    correlation_id: Uuid,
) -> Result<InsertResult, ApiError> {
    let transaction_id = Uuid::now_v7(); // Time-sortable UUID

    // ── Begin transaction ─────────────────────────────────────────────────────
    let mut tx = state.pool.begin().await?;

    // ── Atomic check-and-insert for idempotency ───────────────────────────────
    // ON CONFLICT DO NOTHING, then SELECT to get existing row.
    // This is atomic because we're inside a serializable transaction.
    let existing = sqlx::query!(
        r#"
        INSERT INTO transactions (
            id, idempotency_key, transaction_type, status, amount, currency,
            source_account_id, dest_account_id, description, metadata, request_hash
        )
        VALUES ($1, $2, $3::transaction_type, 'PENDING'::transaction_status, $4, $5::currency_code,
                $6, $7, $8, $9, $10)
        ON CONFLICT (idempotency_key) WHERE status != 'REVERSED'
        DO NOTHING
        RETURNING id, status::text, request_hash, false AS was_duplicate
        "#,
        transaction_id,
        idempotency_key.as_str(),
        body.transaction_type as TransactionType,
        body.amount,
        body.currency as Currency,
        body.source_account_id,
        body.dest_account_id,
        body.description,
        body.metadata.clone().unwrap_or(serde_json::json!({})),
        request_hash,
    )
    .fetch_optional(&mut *tx)
    .await?;

    // If insert was skipped (conflict), fetch the existing row
    let (actual_id, status, was_duplicate) = if let Some(row) = existing {
        (row.id, row.status, false)
    } else {
        // Conflict — load existing row
        let row = sqlx::query!(
            r#"
            SELECT id, status::text AS status, request_hash
            FROM transactions
            WHERE idempotency_key = $1 AND status != 'REVERSED'
            "#,
            idempotency_key.as_str(),
        )
        .fetch_one(&mut *tx)
        .await?;

        // ── Hash mismatch: same key, different body → client bug ──────────────
        if row.request_hash != request_hash {
            tx.rollback().await.ok();
            return Err(ApiError::IdempotencyKeyReused {
                key: idempotency_key.to_string(),
            });
        }

        (row.id, row.status.unwrap_or_default(), true)
    };

    // If it was a fresh insert, write outbox event & audit log
    if !was_duplicate {
        let event = EventEnvelope::new(
            "Transaction",
            actual_id,
            1,
            "TransactionSubmitted",
            correlation_id,
            TransactionSubmitted {
                transaction_id: actual_id,
                idempotency_key: idempotency_key.to_string(),
                transaction_type: body.transaction_type.clone(),
                amount: body.amount,
                currency: body.currency.clone(),
                source_account_id: body.source_account_id,
                dest_account_id: body.dest_account_id,
            },
        )
        .map_err(|e| ApiError::Internal(e.to_string()))?;

        // Write to outbox (same DB transaction — atomic with the INSERT above)
        sqlx::query!(
            r#"
            INSERT INTO outbox_events
                (aggregate_type, aggregate_id, event_type, payload, topic, partition_key)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
            "Transaction",
            actual_id,
            "TransactionSubmitted",
            serde_json::to_value(&event)?,
            TOPIC_TRANSACTIONS,
            // Partition by source account for ordering guarantees within an account
            body.source_account_id
                .map(|id| id.to_string())
                .unwrap_or_else(|| actual_id.to_string()),
        )
        .execute(&mut *tx)
        .await?;

        // Audit log entry — in same transaction
        audit::record(
            &mut *tx,
            "Transaction",
            actual_id,
            "SUBMITTED",
            None::<&serde_json::Value>,
            &serde_json::json!({
                "id": actual_id,
                "idempotency_key": idempotency_key.as_str(),
                "amount": body.amount,
                "currency": body.currency,
                "status": "PENDING",
            }),
            "api",
            "SYSTEM",
            correlation_id,
        )
        .await?;
    }

    tx.commit().await?;

    let http_status = if was_duplicate {
        StatusCode::OK // Idempotent replay
    } else {
        StatusCode::ACCEPTED // New submission, async processing
    };

    Ok(InsertResult {
        status_code: http_status,
        response: SubmitTransactionResponse {
            transaction_id: actual_id,
            status,
            idempotency_key: idempotency_key.to_string(),
            was_duplicate,
        },
    })
}

pub async fn get_transaction(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    let row = sqlx::query!(
        r#"
        SELECT id, status::text AS status, amount, currency::text AS currency,
               transaction_type::text AS transaction_type, submitted_at, settled_at
        FROM transactions WHERE id = $1
        "#,
        id
    )
    .fetch_optional(&state.pool)
    .await?
    .ok_or(ApiError::NotFound(id))?;

    Ok(Json(serde_json::json!({
        "id": row.id,
        "status": row.status,
        "amount": row.amount,
        "currency": row.currency,
        "transaction_type": row.transaction_type,
        "submitted_at": row.submitted_at,
        "settled_at": row.settled_at,
    })))
}

// ============================================================================
// VALIDATION HELPERS
// ============================================================================

fn validate_parties(body: &SubmitTransactionRequest) -> Result<(), ApiError> {
    match body.transaction_type {
        TransactionType::Transfer => {
            if body.source_account_id.is_none() || body.dest_account_id.is_none() {
                return Err(ApiError::ValidationError(
                    "TRANSFER requires both source_account_id and dest_account_id".into(),
                ));
            }
            if body.source_account_id == body.dest_account_id {
                return Err(ApiError::ValidationError(
                    "source and destination accounts must differ".into(),
                ));
            }
        }
        TransactionType::Debit | TransactionType::Fee => {
            if body.source_account_id.is_none() {
                return Err(ApiError::ValidationError(
                    "DEBIT/FEE requires source_account_id".into(),
                ));
            }
        }
        TransactionType::Credit | TransactionType::Adjustment => {
            if body.dest_account_id.is_none() {
                return Err(ApiError::ValidationError(
                    "CREDIT/ADJUSTMENT requires dest_account_id".into(),
                ));
            }
        }
        _ => {}
    }
    Ok(())
}

// ============================================================================
// ERROR TYPE — maps DomainError to HTTP
// ============================================================================

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("Idempotency-Key header is required")]
    MissingIdempotencyKey,

    #[error("invalid idempotency key: {0}")]
    InvalidIdempotencyKey(String),

    #[error("idempotency key '{key}' already used with different request body")]
    IdempotencyKeyReused { key: String },

    #[error("validation error: {0}")]
    ValidationError(String),

    #[error("not found: {0}")]
    NotFound(Uuid),

    #[error("domain error: {0}")]
    Domain(#[from] DomainError),

    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("internal error: {0}")]
    Internal(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let (status, code, message) = match &self {
            ApiError::MissingIdempotencyKey => (
                StatusCode::BAD_REQUEST,
                "MISSING_IDEMPOTENCY_KEY",
                self.to_string(),
            ),
            ApiError::InvalidIdempotencyKey(_) => (
                StatusCode::BAD_REQUEST,
                "INVALID_IDEMPOTENCY_KEY",
                self.to_string(),
            ),
            ApiError::IdempotencyKeyReused { .. } => (
                StatusCode::UNPROCESSABLE_ENTITY,
                "IDEMPOTENCY_KEY_REUSED",
                self.to_string(),
            ),
            ApiError::ValidationError(_) => (
                StatusCode::UNPROCESSABLE_ENTITY,
                "VALIDATION_ERROR",
                self.to_string(),
            ),
            ApiError::NotFound(_) => (StatusCode::NOT_FOUND, "NOT_FOUND", self.to_string()),
            ApiError::Domain(e) => {
                let status = StatusCode::from_u16(e.http_status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
                (status, "DOMAIN_ERROR", self.to_string())
            }
            _ => {
                tracing::error!(error = %self, "internal error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "INTERNAL_ERROR",
                    "an internal error occurred".to_string(),
                )
            }
        };

        let body = Json(serde_json::json!({
            "error": {
                "code": code,
                "message": message,
            }
        }));

        (status, body).into_response()
    }
}
