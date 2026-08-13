use sqlx::PgPool;

use crate::state::AppState;

const DELETE_BATCH_SIZE: i64 = 5_000;

pub async fn run(state: AppState) {
    let mut interval = tokio::time::interval(state.config.retention_sweep);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        if let Err(error) = sweep(&state).await {
            tracing::error!(error = ?error, "retention sweep failed");
        }
    }
}

async fn sweep(state: &AppState) -> crate::error::Result<()> {
    let recovered = sqlx::query(
        r#"UPDATE webhook_deliveries
              SET state = 'failed', error_summary = 'delivery lease expired',
                  next_attempt_at = now(), locked_at = NULL, updated_at = now()
            WHERE state = 'delivering' AND locked_at < now() - interval '5 minutes'"#,
    )
    .execute(&state.db)
    .await?
    .rows_affected();

    let updates = drain_expired(
        &state.db,
        r#"DELETE FROM updates WHERE id IN (
               SELECT id FROM updates
                WHERE expires_at <= now()
                ORDER BY expires_at, id
                LIMIT $1
           )"#,
    )
    .await?;
    let sessions = drain_expired(
        &state.db,
        r#"DELETE FROM sessions WHERE id IN (
               SELECT id FROM sessions
                WHERE expires_at <= now()
                ORDER BY expires_at, id
                LIMIT $1
           )"#,
    )
    .await?;
    let outbound_messages = drain_expired(
        &state.db,
        r#"DELETE FROM outbound_messages WHERE id IN (
               SELECT id FROM outbound_messages
                WHERE expires_at <= now()
                ORDER BY expires_at, id
                LIMIT $1
           )"#,
    )
    .await?;
    let api_calls = drain_expired(
        &state.db,
        r#"DELETE FROM api_calls WHERE id IN (
               SELECT id FROM api_calls
                WHERE expires_at <= now()
                ORDER BY expires_at, id
                LIMIT $1
           )"#,
    )
    .await?;
    let conversations = drain_expired(
        &state.db,
        r#"DELETE FROM conversations WHERE (bot_id, chat_id) IN (
               SELECT bot_id, chat_id FROM conversations
                WHERE expires_at <= now()
                ORDER BY expires_at, bot_id, chat_id
                LIMIT $1
           )"#,
    )
    .await?;
    let audit_logs = drain_expired(
        &state.db,
        r#"DELETE FROM audit_log WHERE id IN (
               SELECT id FROM audit_log
                WHERE expires_at <= now()
                ORDER BY expires_at, id
                LIMIT $1
           )"#,
    )
    .await?;

    if updates + sessions + outbound_messages + api_calls + conversations + audit_logs + recovered
        > 0
    {
        tracing::info!(
            updates,
            sessions,
            outbound_messages,
            api_calls,
            conversations,
            audit_logs,
            recovered_deliveries = recovered,
            "retention sweep completed"
        );
    }
    Ok(())
}

async fn drain_expired(pool: &PgPool, statement: &'static str) -> crate::error::Result<u64> {
    let mut total = 0_u64;
    loop {
        let deleted = sqlx::query(statement)
            .bind(DELETE_BATCH_SIZE)
            .execute(pool)
            .await?
            .rows_affected();
        total = total.saturating_add(deleted);
        if deleted < DELETE_BATCH_SIZE as u64 {
            return Ok(total);
        }
        tokio::task::yield_now().await;
    }
}
