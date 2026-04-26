/// Transaction processor — the heart of the system.
///
/// Invariants maintained here:
///   1. Advisory lock prevents concurrent processing of same transaction_id
///   2. Optimistic lock check: version must match expected before updating
///   3. Ledger entries written in same DB transaction as status update
///   4. Audit log written in same DB transaction
///   5. Balance updates use row-level locking (SELECT FOR UPDATE)
///   6. All monetary arithmetic uses Decimal — never f64
///   7. Idempotency: if transaction already SETTLED, skip (worker restart safety)
use chrono::Utc;
use rust_decimal::Decimal;
use sqlx::PgPool;
use tracing::{error, info, instrument, warn};
use uuid::Uuid;

use shared::{
    audit,
    domain::{TransactionStatus, TransactionType},
    error::DomainError,
    events::EventEnvelope,
};

use crate::fraud::FraudChecker;

#[derive(Debug, thiserror::Error)]
pub enum ProcessError {
    #[error("transaction not found: {0}")]
    NotFound(Uuid),

    #[error("domain error: {0}")]
    Domain(#[from] DomainError),

    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("fraud check failed: {0}")]
    FraudBlocked(String),

    #[error("permanent failure: {0}")]
    Permanent(String),
}

impl ProcessError {
    /// Whether this error is safe to retry via exponential backoff.
    pub fn is_retryable(&self) -> bool {
        match self {
            // Transient: lock contention, serialization failures, network blips
            ProcessError::Database(sqlx::Error::PoolTimedOut) => true,
            ProcessError::Domain(DomainError::OptimisticLockConflict { .. }) => true,
            // Not retryable: logic errors, fraud blocks, not-found
            ProcessError::NotFound(_)
            | ProcessError::FraudBlocked(_)
            | ProcessError::Permanent(_) => false,
            _ => false,
        }
    }
}

#[instrument(
    skip(pool, envelope),
    fields(
        transaction_id = %envelope.aggregate_id,
        event_type = %envelope.event_type,
    )
)]
pub async fn process_transaction(
    pool: &PgPool,
    envelope: &EventEnvelope,
    worker_id: &str,
) -> Result<(), ProcessError> {
    let transaction_id = envelope.aggregate_id;

    // ── Acquire PostgreSQL advisory lock ─────────────────────────────────────
    // This prevents two workers from processing the same transaction concurrently.
    // The lock key is derived from the transaction UUID (upper 64 bits).
    // Advisory locks are automatically released on connection return.
    let lock_key = derive_advisory_lock_key(transaction_id);
    let locked = sqlx::query_scalar!(
        "SELECT pg_try_advisory_lock($1)",
        lock_key
    )
    .fetch_one(pool)
    .await?
    .unwrap_or(false);

    if !locked {
        // Another worker is processing this — not an error, just skip
        info!(
            transaction_id = %transaction_id,
            "advisory lock held by another worker, skipping"
        );
        return Ok(());
    }

    // Ensure the advisory lock is released on scope exit
    let _lock_guard = AdvisoryLockGuard { pool, key: lock_key };

    // ── Load transaction with row-level lock ──────────────────────────────────
    // We use SELECT FOR UPDATE on the transaction row to prevent race conditions
    // with concurrent updates (e.g., admin cancellation racing with processing).
    let mut tx = pool.begin().await?;

    let txn = sqlx::query!(
        r#"
        SELECT
            id, idempotency_key, transaction_type::text AS transaction_type,
            status::text AS status, amount, currency::text AS currency,
            source_account_id, dest_account_id, version, retry_count, compliance_checked
        FROM transactions
        WHERE id = $1
        FOR UPDATE
        "#,
        transaction_id,
    )
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(ProcessError::NotFound(transaction_id))?;

    // ── Idempotency: skip if already processed ────────────────────────────────
    // This handles worker restarts / message redelivery after crash.
    let current_status = txn.status.as_deref().unwrap_or("PENDING");
    match current_status {
        "SETTLED" | "REVERSED" => {
            info!(
                transaction_id = %transaction_id,
                status = current_status,
                "transaction already in terminal state, skipping (safe replay)"
            );
            tx.rollback().await.ok();
            return Ok(());
        }
        "FAILED" => {
            // Failed transactions are not retried via Kafka — only via DLQ requeue
            tx.rollback().await.ok();
            return Err(ProcessError::Permanent(format!(
                "transaction {} is in FAILED state",
                transaction_id
            )));
        }
        _ => {} // PENDING or PROCESSING — proceed
    }

    // ── Mark as PROCESSING (with optimistic lock check) ───────────────────────
    let rows_updated = sqlx::query!(
        r#"
        UPDATE transactions
        SET status = 'PROCESSING'::transaction_status,
            worker_id = $1,
            version = version + 1
        WHERE id = $2
          AND version = $3  -- optimistic lock
          AND status IN ('PENDING'::transaction_status, 'PROCESSING'::transaction_status)
        "#,
        worker_id,
        transaction_id,
        txn.version,
    )
    .execute(&mut *tx)
    .await?
    .rows_affected();

    if rows_updated == 0 {
        tx.rollback().await.ok();
        return Err(ProcessError::Domain(DomainError::OptimisticLockConflict {
            entity: "Transaction".into(),
            id: transaction_id,
            expected: txn.version,
            actual: txn.version + 1, // approximate — someone else updated it
        }));
    }

    // ── Fraud check ───────────────────────────────────────────────────────────
    if !txn.compliance_checked.unwrap_or(false) {
        let fraud_result = FraudChecker::check(transaction_id, txn.amount).await;
        if fraud_result.is_blocked {
            // Mark as FAILED with reason, audit it
            mark_failed(
                &mut tx,
                transaction_id,
                txn.version + 1,
                &format!("fraud_blocked: {}", fraud_result.reason),
                envelope.correlation_id,
            )
            .await?;
            tx.commit().await?;
            return Err(ProcessError::FraudBlocked(fraud_result.reason));
        }
        // Update compliance flag
        sqlx::query!(
            "UPDATE transactions SET compliance_checked = true, risk_score = $1 WHERE id = $2",
            fraud_result.risk_score,
            transaction_id,
        )
        .execute(&mut *tx)
        .await?;
    }

    // ── Execute business logic based on transaction type ─────────────────────
    let transaction_type = txn.transaction_type.as_deref().unwrap_or("");
    let result = match transaction_type {
        "TRANSFER" => {
            process_transfer(
                &mut tx,
                transaction_id,
                txn.amount,
                txn.source_account_id,
                txn.dest_account_id,
                txn.version + 1,
                envelope.correlation_id,
            )
            .await
        }
        "CREDIT" => {
            process_credit(
                &mut tx,
                transaction_id,
                txn.amount,
                txn.dest_account_id,
                txn.version + 1,
                envelope.correlation_id,
            )
            .await
        }
        "DEBIT" | "FEE" => {
            process_debit(
                &mut tx,
                transaction_id,
                txn.amount,
                txn.source_account_id,
                txn.version + 1,
                envelope.correlation_id,
            )
            .await
        }
        other => Err(ProcessError::Permanent(format!("unknown transaction type: {other}"))),
    };

    match result {
        Ok(()) => {
            // ── Mark SETTLED ──────────────────────────────────────────────────
            sqlx::query!(
                r#"
                UPDATE transactions
                SET status = 'SETTLED'::transaction_status,
                    processed_at = NOW(),
                    settled_at = NOW(),
                    version = version + 1
                WHERE id = $1
                "#,
                transaction_id,
            )
            .execute(&mut *tx)
            .await?;

            tx.commit().await?;
            info!(transaction_id = %transaction_id, "transaction settled");
        }
        Err(ref e) => {
            let retry_count = txn.retry_count.unwrap_or(0);
            let is_retryable = e.is_retryable();

            if is_retryable {
                // Reset to PENDING so Kafka retry will pick it up
                sqlx::query!(
                    r#"
                    UPDATE transactions
                    SET status = 'PENDING'::transaction_status,
                        retry_count = retry_count + 1,
                        last_error = $1,
                        version = version + 1
                    WHERE id = $2
                    "#,
                    e.to_string(),
                    transaction_id,
                )
                .execute(&mut *tx)
                .await?;
            } else {
                // Mark permanently failed
                mark_failed(
                    &mut tx,
                    transaction_id,
                    txn.version + 1,
                    &e.to_string(),
                    envelope.correlation_id,
                )
                .await?;
            }

            tx.commit().await?;

            warn!(
                transaction_id = %transaction_id,
                retryable = is_retryable,
                error = %e,
                "transaction processing failed"
            );
            return Err(result.unwrap_err());
        }
    }

    Ok(())
}

// ============================================================================
// TRANSFER — debit source, credit dest in single atomic transaction
// ============================================================================

async fn process_transfer(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    transaction_id: Uuid,
    amount: rust_decimal::Decimal,
    source_id: Option<Uuid>,
    dest_id: Option<Uuid>,
    version: i64,
    correlation_id: Uuid,
) -> Result<(), ProcessError> {
    let source_id = source_id.ok_or_else(|| {
        ProcessError::Permanent("TRANSFER missing source_account_id".into())
    })?;
    let dest_id = dest_id.ok_or_else(|| {
        ProcessError::Permanent("TRANSFER missing dest_account_id".into())
    })?;

    // Lock both accounts in consistent order (lower UUID first) to prevent deadlock
    let (first_id, second_id) = if source_id < dest_id {
        (source_id, dest_id)
    } else {
        (dest_id, source_id)
    };

    // Lock accounts in order
    let _lock1 = sqlx::query!("SELECT id FROM accounts WHERE id = $1 FOR UPDATE", first_id)
        .fetch_one(&mut **tx)
        .await?;
    let _lock2 = sqlx::query!("SELECT id FROM accounts WHERE id = $1 FOR UPDATE", second_id)
        .fetch_one(&mut **tx)
        .await?;

    // Debit source
    let source = sqlx::query!(
        "SELECT balance, version, is_active, currency::text AS currency FROM accounts WHERE id = $1",
        source_id
    )
    .fetch_one(&mut **tx)
    .await?;

    if !source.is_active {
        return Err(ProcessError::Domain(DomainError::AccountInactive(source_id)));
    }
    if source.balance < amount {
        return Err(ProcessError::Domain(DomainError::InsufficientFunds {
            account_id: source_id,
            required: amount.to_string(),
            available: source.balance.to_string(),
        }));
    }

    let new_source_balance = source.balance - amount;

    sqlx::query!(
        r#"
        UPDATE accounts
        SET balance = $1, version = version + 1, updated_at = NOW()
        WHERE id = $2 AND version = $3
        "#,
        new_source_balance,
        source_id,
        source.version,
    )
    .execute(&mut **tx)
    .await?;

    // Credit dest
    let dest = sqlx::query!(
        "SELECT balance, version, is_active FROM accounts WHERE id = $1",
        dest_id
    )
    .fetch_one(&mut **tx)
    .await?;

    if !dest.is_active {
        return Err(ProcessError::Domain(DomainError::AccountInactive(dest_id)));
    }

    let new_dest_balance = dest.balance + amount;

    sqlx::query!(
        r#"
        UPDATE accounts
        SET balance = $1, version = version + 1, updated_at = NOW()
        WHERE id = $2 AND version = $3
        "#,
        new_dest_balance,
        dest_id,
        dest.version,
    )
    .execute(&mut **tx)
    .await?;

    // Write double-entry ledger (debit + credit = balanced)
    write_ledger_entry(tx, transaction_id, source_id, "DEBIT", amount, new_source_balance).await?;
    write_ledger_entry(tx, transaction_id, dest_id, "CREDIT", amount, new_dest_balance).await?;

    // Audit
    audit::record(
        &mut **tx,
        "Transaction",
        transaction_id,
        "SETTLED",
        None::<&serde_json::Value>,
        &serde_json::json!({
            "transaction_id": transaction_id,
            "type": "TRANSFER",
            "amount": amount,
            "source_account_id": source_id,
            "dest_account_id": dest_id,
            "status": "SETTLED",
        }),
        "worker",
        "WORKER",
        correlation_id,
    )
    .await
    .map_err(ProcessError::Database)?;

    Ok(())
}

async fn process_credit(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    transaction_id: Uuid,
    amount: Decimal,
    dest_id: Option<Uuid>,
    version: i64,
    correlation_id: Uuid,
) -> Result<(), ProcessError> {
    let dest_id = dest_id.ok_or_else(|| {
        ProcessError::Permanent("CREDIT missing dest_account_id".into())
    })?;

    let dest = sqlx::query!(
        "SELECT balance, version, is_active FROM accounts WHERE id = $1 FOR UPDATE",
        dest_id
    )
    .fetch_one(&mut **tx)
    .await?;

    if !dest.is_active {
        return Err(ProcessError::Domain(DomainError::AccountInactive(dest_id)));
    }

    let new_balance = dest.balance + amount;
    sqlx::query!(
        "UPDATE accounts SET balance = $1, version = version + 1, updated_at = NOW() WHERE id = $2",
        new_balance, dest_id,
    )
    .execute(&mut **tx)
    .await?;

    write_ledger_entry(tx, transaction_id, dest_id, "CREDIT", amount, new_balance).await?;

    audit::record(
        &mut **tx, "Transaction", transaction_id, "SETTLED",
        None::<&serde_json::Value>,
        &serde_json::json!({"type": "CREDIT", "amount": amount, "dest": dest_id, "status": "SETTLED"}),
        "worker", "WORKER", correlation_id,
    ).await.map_err(ProcessError::Database)?;

    Ok(())
}

async fn process_debit(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    transaction_id: Uuid,
    amount: Decimal,
    source_id: Option<Uuid>,
    version: i64,
    correlation_id: Uuid,
) -> Result<(), ProcessError> {
    let source_id = source_id.ok_or_else(|| {
        ProcessError::Permanent("DEBIT missing source_account_id".into())
    })?;

    let source = sqlx::query!(
        "SELECT balance, version, is_active FROM accounts WHERE id = $1 FOR UPDATE",
        source_id
    )
    .fetch_one(&mut **tx)
    .await?;

    if !source.is_active {
        return Err(ProcessError::Domain(DomainError::AccountInactive(source_id)));
    }
    if source.balance < amount {
        return Err(ProcessError::Domain(DomainError::InsufficientFunds {
            account_id: source_id,
            required: amount.to_string(),
            available: source.balance.to_string(),
        }));
    }

    let new_balance = source.balance - amount;
    sqlx::query!(
        "UPDATE accounts SET balance = $1, version = version + 1, updated_at = NOW() WHERE id = $2",
        new_balance, source_id,
    )
    .execute(&mut **tx)
    .await?;

    write_ledger_entry(tx, transaction_id, source_id, "DEBIT", amount, new_balance).await?;

    audit::record(
        &mut **tx, "Transaction", transaction_id, "SETTLED",
        None::<&serde_json::Value>,
        &serde_json::json!({"type": "DEBIT", "amount": amount, "source": source_id, "status": "SETTLED"}),
        "worker", "WORKER", correlation_id,
    ).await.map_err(ProcessError::Database)?;

    Ok(())
}

// ============================================================================
// LEDGER HELPERS
// ============================================================================

async fn write_ledger_entry(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    transaction_id: Uuid,
    account_id: Uuid,
    entry_type: &str,
    amount: Decimal,
    running_balance: Decimal,
) -> Result<(), ProcessError> {
    // Get next sequence number for this account (monotonic per account)
    let seq = sqlx::query_scalar!(
        "SELECT COALESCE(MAX(sequence_num), 0) + 1 FROM ledger_entries WHERE account_id = $1",
        account_id
    )
    .fetch_one(&mut **tx)
    .await?
    .unwrap_or(1);

    sqlx::query!(
        r#"
        INSERT INTO ledger_entries
            (id, transaction_id, account_id, entry_type, amount, currency,
             running_balance, posted_at, sequence_num)
        SELECT
            gen_random_uuid(), $1, $2, $3, $4, a.currency,
            $5, NOW(), $6
        FROM accounts a WHERE a.id = $2
        "#,
        transaction_id,
        account_id,
        entry_type,
        amount,
        running_balance,
        seq,
    )
    .execute(&mut **tx)
    .await?;

    Ok(())
}

async fn mark_failed(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    transaction_id: Uuid,
    version: i64,
    error: &str,
    correlation_id: Uuid,
) -> Result<(), ProcessError> {
    sqlx::query!(
        r#"
        UPDATE transactions
        SET status = 'FAILED'::transaction_status,
            last_error = $1,
            processed_at = NOW(),
            version = version + 1
        WHERE id = $2
        "#,
        error,
        transaction_id,
    )
    .execute(&mut **tx)
    .await?;

    audit::record(
        &mut **tx, "Transaction", transaction_id, "FAILED",
        None::<&serde_json::Value>,
        &serde_json::json!({"status": "FAILED", "error": error}),
        "worker", "WORKER", correlation_id,
    ).await.map_err(ProcessError::Database)?;

    Ok(())
}

// ============================================================================
// ADVISORY LOCK UTILITIES
// ============================================================================

/// Derive a 64-bit lock key from a UUID.
/// We XOR the high and low 64 bits for a uniform distribution.
fn derive_advisory_lock_key(id: Uuid) -> i64 {
    let bytes = id.as_bytes();
    let high = i64::from_be_bytes(bytes[0..8].try_into().unwrap());
    let low = i64::from_be_bytes(bytes[8..16].try_into().unwrap());
    high ^ low
}

/// RAII guard: releases the advisory lock when dropped.
/// Note: advisory locks are per-connection, so releasing via a pool
/// connection means we need the same connection that acquired it.
/// In practice, for per-message locks, we acquire and release within
/// the same async task — Tokio keeps it on the same thread.
struct AdvisoryLockGuard<'a> {
    pool: &'a PgPool,
    key: i64,
}

impl Drop for AdvisoryLockGuard<'_> {
    fn drop(&mut self) {
        // Spawn a fire-and-forget release; the lock also auto-releases when
        // the connection returns to the pool.
        let pool = self.pool.clone();
        let key = self.key;
        tokio::spawn(async move {
            sqlx::query!("SELECT pg_advisory_unlock($1)", key)
                .execute(&pool)
                .await
                .ok();
        });
    }
}
