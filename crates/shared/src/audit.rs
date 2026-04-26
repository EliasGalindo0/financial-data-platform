use chrono::Utc;
use serde::Serialize;
use sqlx::PgExecutor;
use uuid::Uuid;

/// Write a row to audit_log within the current transaction/connection.
/// Call this inside the same DB transaction as the business operation
/// so the audit entry is atomic with the change it records.
pub async fn record<'e, E, T>(
    executor: E,
    entity_type: &str,
    entity_id: Uuid,
    action: &str,
    old_state: Option<&T>,
    new_state: &T,
    actor_id: &str,
    actor_type: &str,     // "USER" | "SYSTEM" | "WORKER"
    correlation_id: Uuid,
) -> Result<(), sqlx::Error>
where
    E: PgExecutor<'e>,
    T: Serialize,
{
    let old_json = old_state
        .map(|s| serde_json::to_value(s).unwrap_or(serde_json::Value::Null));
    let new_json = serde_json::to_value(new_state).unwrap_or(serde_json::Value::Null);

    sqlx::query!(
        r#"
        INSERT INTO audit_log
            (entity_type, entity_id, action, old_state, new_state,
             actor_id, actor_type, correlation_id, occurred_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        "#,
        entity_type,
        entity_id,
        action,
        old_json,
        new_json,
        actor_id,
        actor_type,
        correlation_id,
        Utc::now(),
    )
    .execute(executor)
    .await?;

    Ok(())
}
