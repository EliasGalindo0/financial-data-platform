// Ledger module — helpers for balance reconciliation queries.
// The actual entry writing is in processor.rs (co-located with the transaction).

use rust_decimal::Decimal;
use sqlx::PgPool;
use sqlx::Row;
use uuid::Uuid;

/// Compute the expected balance from ledger entries and compare to accounts.balance.
/// Returns None if they match, or Some((ledger_balance, account_balance)) if they diverge.
/// Run this in a reconciliation job, not in the hot path.
#[allow(dead_code)]
pub async fn verify_account_balance(
    pool: &PgPool,
    account_id: Uuid,
) -> anyhow::Result<Option<(Decimal, Decimal)>> {
    let row = sqlx::query(
        r#"
        WITH ledger_balance AS (
            SELECT
                SUM(CASE WHEN entry_type = 'CREDIT' THEN amount ELSE -amount END) AS balance
            FROM ledger_entries
            WHERE account_id = $1
        )
        SELECT
            lb.balance AS ledger_balance,
            a.balance AS account_balance
        FROM ledger_balance lb
        CROSS JOIN accounts a
        WHERE a.id = $1
        "#,
    )
    .bind(account_id)
    .fetch_one(pool)
    .await?;

    let ledger: Option<Decimal> = row.try_get("ledger_balance")?;
    let ledger = ledger.unwrap_or(Decimal::ZERO);
    let account: Decimal = row.try_get("account_balance")?;

    if ledger != account {
        Ok(Some((ledger, account)))
    } else {
        Ok(None)
    }
}
