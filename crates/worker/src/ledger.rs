// Ledger module — helpers for balance reconciliation queries.
// The actual entry writing is in processor.rs (co-located with the transaction).

use rust_decimal::Decimal;
use sqlx::PgPool;
use uuid::Uuid;

/// Compute the expected balance from ledger entries and compare to accounts.balance.
/// Returns None if they match, or Some((ledger_balance, account_balance)) if they diverge.
/// Run this in a reconciliation job, not in the hot path.
pub async fn verify_account_balance(
    pool: &PgPool,
    account_id: Uuid,
) -> anyhow::Result<Option<(Decimal, Decimal)>> {
    let result = sqlx::query!(
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
        account_id,
    )
    .fetch_one(pool)
    .await?;

    let ledger = result.ledger_balance.unwrap_or(Decimal::ZERO);
    let account = result.account_balance;

    if ledger != account {
        Ok(Some((ledger, account)))
    } else {
        Ok(None)
    }
}
