/// Transaction processor — the heart of the system.
///
/// Invariants maintained here:
///   1. Transaction-level advisory lock (pg_try_advisory_xact_lock) prevents concurrent
///      processing of the same transaction_id. The lock is automatically released when
///      the enclosing DB transaction commits or rolls back — no manual guard needed.
///   2. Optimistic lock check: version must match expected before updating.
///   3. Ledger entries written in same DB transaction as status update.
///   4. Audit log written in same DB transaction.
///   5. Balance updates use row-level locking (SELECT FOR UPDATE).
///   6. All monetary arithmetic uses Decimal — never f64.
///   7. Idempotency: if transaction already SETTLED/REVERSED, skip (worker restart safety).
use std::time::Duration;

use rust_decimal::Decimal;
use sqlx::PgPool;
use tracing::{error, info, instrument, warn};
use uuid::Uuid;

use shared::{
    audit,
    error::DomainError,
    events::EventEnvelope,
};

use crate::fraud::FraudChecker;

/// Timeout for the fraud/compliance check. If the fraud service does not respond
/// within this window, the transaction is permanently failed and routed to the DLQ
/// for manual review rather than blocking the worker indefinitely.
const FRAUD_CHECK_TIMEOUT_SECS: u64 = 5;

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

    #[error("fraud check blocked: {0}")]
    FraudBlocked(String),

    /// Fraud service did not respond within FRAUD_CHECK_TIMEOUT_SECS.
    /// Treated as a permanent failure so the transaction is routed to the DLQ
    /// for operator review rather than retried (which could loop indefinitely).
    #[error("fraud check timed out after {0}s")]
    FraudCheckTimeout(u64),

    #[error("permanent failure: {0}")]
    Permanent(String),
}

impl ProcessError {
    /// Whether this error is safe to retry via exponential backoff.
    pub fn is_retryable(&self) -> bool {
        match self {
            // Transient: pool exhaustion, optimistic lock contention
            ProcessError::Database(sqlx::Error::PoolTimedOut) => true,
            ProcessError::Domain(DomainError::OptimisticLockConflict { .. }) => true,
            // Permanent: business logic errors, compliance blocks, timeouts, missing records
            ProcessError::NotFound(_)
            | ProcessError::FraudBlocked(_)
            | ProcessError::FraudCheckTimeout(_)
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

    // ── Begin DB transaction ───────────────────────────────────────────────────
    // The advisory lock is acquired *inside* this transaction via
    // pg_try_advisory_xact_lock, so it is automatically released on commit
    // or rollback. This is correct and avoids the session-level lock pitfalls
    // (session locks survive connection return to the pool).
    let mut tx = pool.begin().await?;

    // ── Acquire transaction-level advisory lock ───────────────────────────────
    let lock_key = derive_advisory_lock_key(transaction_id);
    let locked = sqlx::query_scalar!(
        "SELECT pg_try_advisory_xact_lock($1)",
        lock_key
    )
    .fetch_one(&mut *tx)
    .await?
    .unwrap_or(false);

    if !locked {
        // Another worker is processing this transaction — skip gracefully.
        info!(
            transaction_id = %transaction_id,
            "advisory lock held by another worker, skipping"
        );
        tx.rollback().await.ok();
        return Ok(());
    }

    // ── Load transaction with row-level lock ──────────────────────────────────
    // SELECT FOR UPDATE prevents concurrent admin cancellations or other writes
    // from racing with our processing within the same DB transaction.
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

    // ── Idempotency: skip if already in a terminal state ──────────────────────
    // Handles worker restarts and Kafka message redelivery after a crash.
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
            // FAILED transactions are not retried via Kafka; they require explicit
            // requeue through the dead-letter queue workflow.
            tx.rollback().await.ok();
            return Err(ProcessError::Permanent(format!(
                "transaction {} is in FAILED state",
                transaction_id
            )));
        }
        _ => {} // PENDING or PROCESSING — proceed
    }

    // ── Mark as PROCESSING (with optimistic lock check) ───────────────────────
    // The version check ensures we don't overwrite a concurrent update that
    // slipped past the advisory lock (belt-and-suspenders).
    let rows_updated = sqlx::query!(
        r#"
        UPDATE transactions
        SET status = 'PROCESSING'::transaction_status,
            worker_id = $1,
            version   = version + 1
        WHERE id      = $2
          AND version = $3
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
            actual: txn.version + 1, // approximate — another writer updated first
        }));
    }

    // ── Fraud / compliance check ──────────────────────────────────────────────
    // Wrapped in a hard timeout: if the fraud service is unavailable or slow,
    // we fail the transaction permanently rather than blocking the worker.
    if !txn.compliance_checked.unwrap_or(false) {
        let fraud_result = match tokio::time::timeout(
            Duration::from_secs(FRAUD_CHECK_TIMEOUT_SECS),
            FraudChecker::check(transaction_id, txn.amount),
        )
        .await
        {
            Ok(result) => result,
            Err(_elapsed) => {
                error!(
                    transaction_id = %transaction_id,
                    timeout_secs = FRAUD_CHECK_TIMEOUT_SECS,
                    "fraud check timed out — marking transaction as permanently failed"
                );
                mark_failed(
                    &mut tx,
                    transaction_id,
                    &format!(
                        "fraud_check_timeout: no response within {}s",
                        FRAUD_CHECK_TIMEOUT_SECS
                    ),
                    envelope.correlation_id,
                )
                .await?;
                tx.commit().await?;
                return Err(ProcessError::FraudCheckTimeout(FRAUD_CHECK_TIMEOUT_SECS));
            }
        };

        if fraud_result.is_blocked {
            mark_failed(
                &mut tx,
                transaction_id,
                &format!("fraud_blocked: {}", fraud_result.reason),
                envelope.correlation_id,
            )
            .await?;
            tx.commit().await?;
            return Err(ProcessError::FraudBlocked(fraud_result.reason));
        }

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
                envelope.correlation_id,
            )
            .await
        }
        other => Err(ProcessError::Permanent(format!(
            "unknown transaction type: {other}"
        ))),
    };

    match result {
        Ok(()) => {
            // ── Mark SETTLED ──────────────────────────────────────────────────
            sqlx::query!(
                r#"
                UPDATE transactions
                SET status       = 'SETTLED'::transaction_status,
                    processed_at = NOW(),
                    settled_at   = NOW(),
                    version      = version + 1
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
            let is_retryable = e.is_retryable();

            if is_retryable {
                // Reset to PENDING so the Kafka retry will pick it up with backoff.
                sqlx::query!(
                    r#"
                    UPDATE transactions
                    SET status      = 'PENDING'::transaction_status,
                        retry_count = retry_count + 1,
                        last_error  = $1,
                        version     = version + 1
                    WHERE id = $2
                    "#,
                    e.to_string(),
                    transaction_id,
                )
                .execute(&mut *tx)
                .await?;
            } else {
                mark_failed(
                    &mut tx,
                    transaction_id,
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
// TRANSFER — debit source, credit dest in a single atomic transaction
// ============================================================================

async fn process_transfer(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    transaction_id: Uuid,
    amount: Decimal,
    source_id: Option<Uuid>,
    dest_id: Option<Uuid>,
    correlation_id: Uuid,
) -> Result<(), ProcessError> {
    let source_id = source_id.ok_or_else(|| {
        ProcessError::Permanent("TRANSFER missing source_account_id".into())
    })?;
    let dest_id = dest_id.ok_or_else(|| {
        ProcessError::Permanent("TRANSFER missing dest_account_id".into())
    })?;

    // Lock accounts in consistent UUID order to prevent deadlock on concurrent
    // transfers between the same pair of accounts in opposite directions.
    // Combining the FOR UPDATE lock with the data read eliminates two round-trips.
    let (first_id, second_id) = if source_id <= dest_id {
        (source_id, dest_id)
    } else {
        (dest_id, source_id)
    };

    let first = sqlx::query!(
        "SELECT id, balance, version, is_active FROM accounts WHERE id = $1 FOR UPDATE",
        first_id
    )
    .fetch_one(&mut **tx)
    .await?;

    let second = sqlx::query!(
        "SELECT id, balance, version, is_active FROM accounts WHERE id = $1 FOR UPDATE",
        second_id
    )
    .fetch_one(&mut **tx)
    .await?;

    // Map the ordered rows back to source/dest roles.
    let (source_balance, source_version, source_is_active) = if first_id == source_id {
        (first.balance, first.version, first.is_active)
    } else {
        (second.balance, second.version, second.is_active)
    };
    let (dest_balance, dest_version, dest_is_active) = if first_id == dest_id {
        (first.balance, first.version, first.is_active)
    } else {
        (second.balance, second.version, second.is_active)
    };

    if !source_is_active {
        return Err(ProcessError::Domain(DomainError::AccountInactive(source_id)));
    }
    if source_balance < amount {
        return Err(ProcessError::Domain(DomainError::InsufficientFunds {
            account_id: source_id,
            required: amount.to_string(),
            available: source_balance.to_string(),
        }));
    }
    if !dest_is_active {
        return Err(ProcessError::Domain(DomainError::AccountInactive(dest_id)));
    }

    let new_source_balance = source_balance - amount;
    let new_dest_balance = dest_balance + amount;

    sqlx::query!(
        r#"
        UPDATE accounts
        SET balance = $1, version = version + 1, updated_at = NOW()
        WHERE id = $2 AND version = $3
        "#,
        new_source_balance,
        source_id,
        source_version,
    )
    .execute(&mut **tx)
    .await?;

    sqlx::query!(
        r#"
        UPDATE accounts
        SET balance = $1, version = version + 1, updated_at = NOW()
        WHERE id = $2 AND version = $3
        "#,
        new_dest_balance,
        dest_id,
        dest_version,
    )
    .execute(&mut **tx)
    .await?;

    // Write double-entry ledger (debit + credit must balance).
    write_ledger_entry(tx, transaction_id, source_id, "DEBIT", amount, new_source_balance).await?;
    write_ledger_entry(tx, transaction_id, dest_id, "CREDIT", amount, new_dest_balance).await?;

    audit::record(
        &mut **tx,
        "Transaction",
        transaction_id,
        "SETTLED",
        None::<&serde_json::Value>,
        &serde_json::json!({
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

// ============================================================================
// CREDIT — add funds to destination account
// ============================================================================

async fn process_credit(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    transaction_id: Uuid,
    amount: Decimal,
    dest_id: Option<Uuid>,
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
        new_balance,
        dest_id,
    )
    .execute(&mut **tx)
    .await?;

    write_ledger_entry(tx, transaction_id, dest_id, "CREDIT", amount, new_balance).await?;

    audit::record(
        &mut **tx,
        "Transaction",
        transaction_id,
        "SETTLED",
        None::<&serde_json::Value>,
        &serde_json::json!({
            "type": "CREDIT",
            "amount": amount,
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

// ============================================================================
// DEBIT / FEE — remove funds from source account
// ============================================================================

async fn process_debit(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    transaction_id: Uuid,
    amount: Decimal,
    source_id: Option<Uuid>,
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
        new_balance,
        source_id,
    )
    .execute(&mut **tx)
    .await?;

    write_ledger_entry(tx, transaction_id, source_id, "DEBIT", amount, new_balance).await?;

    audit::record(
        &mut **tx,
        "Transaction",
        transaction_id,
        "SETTLED",
        None::<&serde_json::Value>,
        &serde_json::json!({
            "type": "DEBIT",
            "amount": amount,
            "source_account_id": source_id,
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
    // Derive the next monotonic sequence number for this account's ledger.
    // We hold a FOR UPDATE lock on the account row, so no concurrent writer
    // can insert a ledger entry for the same account within this transaction.
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
    error: &str,
    correlation_id: Uuid,
) -> Result<(), ProcessError> {
    sqlx::query!(
        r#"
        UPDATE transactions
        SET status       = 'FAILED'::transaction_status,
            last_error   = $1,
            processed_at = NOW(),
            version      = version + 1
        WHERE id = $2
        "#,
        error,
        transaction_id,
    )
    .execute(&mut **tx)
    .await?;

    audit::record(
        &mut **tx,
        "Transaction",
        transaction_id,
        "FAILED",
        None::<&serde_json::Value>,
        &serde_json::json!({"status": "FAILED", "error": error}),
        "worker",
        "WORKER",
        correlation_id,
    )
    .await
    .map_err(ProcessError::Database)?;

    Ok(())
}

// ============================================================================
// ADVISORY LOCK UTILITIES
// ============================================================================

/// Derive a stable 64-bit lock key from a UUID by XOR-ing the two halves.
/// This gives a uniform distribution across the i64 key space.
fn derive_advisory_lock_key(id: Uuid) -> i64 {
    let bytes = id.as_bytes();
    let high = i64::from_be_bytes(bytes[0..8].try_into().unwrap());
    let low = i64::from_be_bytes(bytes[8..16].try_into().unwrap());
    high ^ low
}
