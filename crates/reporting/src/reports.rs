use axum::{extract::State, Json};
use chrono::{Duration, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

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
) -> Json<DailySummary> {
    let today = Utc::now().date_naive();

    let row = sqlx::query!(
        r#"
        SELECT
            COUNT(*)                                          AS total,
            COALESCE(SUM(amount), 0)                         AS volume,
            COUNT(*) FILTER (WHERE status = 'SETTLED')       AS settled,
            COUNT(*) FILTER (WHERE status = 'FAILED')        AS failed,
            jsonb_object_agg(
                currency::text,
                COALESCE(SUM(amount) FILTER (WHERE currency = currency), 0)
            ) AS by_currency,
            jsonb_object_agg(
                transaction_type::text,
                COUNT(*)
            ) AS by_type
        FROM transactions
        WHERE submitted_at::date = $1
        "#,
        today,
    )
    .fetch_one(&state.pool)
    .await
    .unwrap();

    Json(DailySummary {
        date: today.to_string(),
        total_transactions: row.total.unwrap_or(0),
        total_volume: row.volume.unwrap_or(Decimal::ZERO),
        settled_count: row.settled.unwrap_or(0),
        failed_count: row.failed.unwrap_or(0),
        by_currency: row.by_currency.unwrap_or_default(),
        by_type: row.by_type.unwrap_or_default(),
    })
}

// ============================================================================
// HIGH-VALUE TRANSACTION REPORT
// Uses ledger_entries as source of truth (not transactions table).
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
) -> Json<Vec<HighValueEntry>> {
    let threshold = Decimal::from(10_000u32);
    let since = Utc::now() - Duration::days(1);

    let rows = sqlx::query!(
        r#"
        SELECT
            le.transaction_id,
            le.account_id,
            le.amount,
            le.currency::text AS currency,
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
    .await
    .unwrap_or_default();

    Json(
        rows.into_iter()
            .map(|r| HighValueEntry {
                transaction_id: r.transaction_id,
                account_id: r.account_id,
                amount: r.amount,
                currency: r.currency.unwrap_or_default(),
                posted_at: r.posted_at,
            })
            .collect(),
    )
}

// ============================================================================
// CURRENCY TRANSACTION REPORT (CTR) — US BSA / FinCEN requirement
// Report any single cash transaction >= $10,000 within 15 days
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
) -> Json<Vec<CtrEntry>> {
    let threshold = Decimal::from(10_000u32);
    let window_start = Utc::now() - Duration::days(1);

    let rows = sqlx::query!(
        r#"
        SELECT
            t.id AS transaction_id,
            a.id AS account_id,
            a.owner_id,
            t.amount,
            t.submitted_at
        FROM transactions t
        JOIN accounts a ON a.id = COALESCE(t.source_account_id, t.dest_account_id)
        WHERE t.amount >= $1
          AND t.submitted_at >= $2
          AND t.status = 'SETTLED'
          AND t.currency = 'USD'
        ORDER BY t.amount DESC
        "#,
        threshold,
        window_start,
    )
    .fetch_all(&state.pool)
    .await
    .unwrap_or_default();

    Json(
        rows.into_iter()
            .map(|r| CtrEntry {
                transaction_id: r.transaction_id,
                account_id: r.account_id,
                owner_id: r.owner_id,
                amount: r.amount,
                submitted_at: r.submitted_at,
            })
            .collect(),
    )
}
