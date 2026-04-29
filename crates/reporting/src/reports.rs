use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use chrono::{Duration, Utc};
use rust_decimal::Decimal;
use serde::Serialize;
use sqlx::PgPool;
use uuid::Uuid;

// ============================================================================
// ERROR TYPE
// ============================================================================

/// Shared error type for all report handlers.
#[derive(Debug)]
pub enum AppError {
    Database(sqlx::Error),
}

impl From<sqlx::Error> for AppError {
    fn from(e: sqlx::Error) -> Self {
        AppError::Database(e)
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            AppError::Database(sqlx::Error::RowNotFound) => {
                (StatusCode::NOT_FOUND, "not found".to_string())
            }
            AppError::Database(e) => {
                tracing::error!(error = %e, "report database query failed");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal server error".to_string(),
                )
            }
        };
        (status, Json(serde_json::json!({"error": message}))).into_response()
    }
}

// ============================================================================
// SHARED STATE
// ============================================================================

#[derive(Clone)]
pub struct ReportState {
    pub pool: PgPool,
}

// ============================================================================
// DAILY SUMMARY
// ============================================================================

#[derive(Serialize)]
pub struct DailySummary {
    pub date: String,
    pub total_transactions: i64,
    pub total_volume: Decimal,
    pub settled_count: i64,
    pub failed_count: i64,
    pub by_currency: serde_json::Value,
    pub by_type: serde_json::Value,
}

pub async fn daily_summary(
    State(state): State<ReportState>,
) -> Result<Json<DailySummary>, AppError> {
    let today = Utc::now().date_naive();

    // Use a CTE to avoid scanning the table multiple times and to keep the
    // parameter binding simple. The by_currency and by_type subqueries use
    // proper GROUP BY — the original flat jsonb_object_agg(key, SUM(...)) was
    // an illegal nested aggregate that PostgreSQL would reject at runtime.
    let row = sqlx::query!(
        r#"
        WITH daily AS (
            SELECT amount, status::text AS status, currency::text AS currency,
                   transaction_type::text AS transaction_type
            FROM transactions
            WHERE submitted_at::date = $1
        )
        SELECT
            COUNT(*)                                    AS "total!: i64",
            COALESCE(SUM(amount), 0)                    AS "volume!: Decimal",
            COUNT(*) FILTER (WHERE status = 'SETTLED')  AS "settled!: i64",
            COUNT(*) FILTER (WHERE status = 'FAILED')   AS "failed!: i64",
            (
                SELECT COALESCE(jsonb_object_agg(currency, total_amount), '{}'::jsonb)
                FROM (
                    SELECT currency, SUM(amount) AS total_amount
                    FROM daily
                    GROUP BY currency
                ) c
            )                                           AS "by_currency!: serde_json::Value",
            (
                SELECT COALESCE(jsonb_object_agg(transaction_type, cnt), '{}'::jsonb)
                FROM (
                    SELECT transaction_type, COUNT(*) AS cnt
                    FROM daily
                    GROUP BY transaction_type
                ) t
            )                                           AS "by_type!: serde_json::Value"
        FROM daily
        "#,
        today,
    )
    .fetch_one(&state.pool)
    .await?;

    Ok(Json(DailySummary {
        date: today.to_string(),
        total_transactions: row.total,
        total_volume: row.volume,
        settled_count: row.settled,
        failed_count: row.failed,
        by_currency: row.by_currency,
        by_type: row.by_type,
    }))
}

// ============================================================================
// HIGH-VALUE TRANSACTION REPORT
// Uses ledger_entries as the source of truth (immutable, append-only).
// ============================================================================

#[derive(Serialize)]
pub struct HighValueEntry {
    pub transaction_id: Uuid,
    pub account_id: Uuid,
    pub amount: Decimal,
    pub currency: String,
    pub posted_at: chrono::DateTime<Utc>,
}

pub async fn high_value_transactions(
    State(state): State<ReportState>,
) -> Result<Json<Vec<HighValueEntry>>, AppError> {
    let threshold = Decimal::from(10_000u32);
    let since = Utc::now() - Duration::days(1);

    let rows = sqlx::query!(
        r#"
        SELECT
            le.transaction_id,
            le.account_id,
            le.amount,
            le.currency::text AS "currency!: String",
            le.posted_at
        FROM ledger_entries le
        WHERE le.amount >= $1
          AND le.posted_at >= $2
        ORDER BY le.amount DESC
        LIMIT 1000
        "#,
        threshold,
        since,
    )
    .fetch_all(&state.pool)
    .await?;

    Ok(Json(
        rows.into_iter()
            .map(|r| HighValueEntry {
                transaction_id: r.transaction_id,
                account_id: r.account_id,
                amount: r.amount,
                currency: r.currency,
                posted_at: r.posted_at,
            })
            .collect(),
    ))
}

// ============================================================================
// CURRENCY TRANSACTION REPORT (CTR) — US BSA / FinCEN requirement
//
// Report settled USD transactions >= $10,000 within the last 24 hours.
// Only customer-facing transaction types (TRANSFER, CREDIT, DEBIT) are
// included. FEE, ADJUSTMENT, and REVERSAL are internal movements and are
// explicitly excluded per FinCEN CTR guidance.
// ============================================================================

#[derive(Serialize)]
pub struct CtrEntry {
    pub transaction_id: Uuid,
    pub account_id: Uuid,
    pub owner_id: String,
    pub amount: Decimal,
    pub submitted_at: chrono::DateTime<Utc>,
}

pub async fn currency_transaction_report(
    State(state): State<ReportState>,
) -> Result<Json<Vec<CtrEntry>>, AppError> {
    let threshold = Decimal::from(10_000u32);
    let window_start = Utc::now() - Duration::days(1);

    let rows = sqlx::query!(
        r#"
        SELECT
            t.id        AS transaction_id,
            a.id        AS account_id,
            a.owner_id,
            t.amount,
            t.submitted_at
        FROM transactions t
        JOIN accounts a ON a.id = COALESCE(t.source_account_id, t.dest_account_id)
        WHERE t.amount >= $1
          AND t.submitted_at >= $2
          AND t.status = 'SETTLED'
          AND t.currency = 'USD'
          AND t.transaction_type IN (
                'TRANSFER'::transaction_type,
                'CREDIT'::transaction_type,
                'DEBIT'::transaction_type
              )
        ORDER BY t.amount DESC
        "#,
        threshold,
        window_start,
    )
    .fetch_all(&state.pool)
    .await?;

    Ok(Json(
        rows.into_iter()
            .map(|r| CtrEntry {
                transaction_id: r.transaction_id,
                account_id: r.account_id,
                owner_id: r.owner_id,
                amount: r.amount,
                submitted_at: r.submitted_at,
            })
            .collect(),
    ))
}
