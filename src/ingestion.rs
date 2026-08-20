use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::Value;
use sqlx::{PgPool, Postgres, Transaction, postgres::PgListener};
use uuid::Uuid;

use crate::{
    error::{AppError, Result},
    state::{AppState, StoredUpdate},
    telegram::ALL_UPDATE_TYPES,
};

pub const UPDATE_NOTIFY_CHANNEL: &str = "phenogram_updates";

#[derive(Clone, Copy, Debug)]
pub struct IngestionBot {
    pub id: Uuid,
    pub telegram_bot_id: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IngestionSource {
    ManagedWebhook,
    OfficialTap,
}

impl IngestionSource {
    fn as_str(self) -> &'static str {
        match self {
            Self::ManagedWebhook => "managed_webhook",
            Self::OfficialTap => "official_tap",
        }
    }
}

#[derive(Debug)]
pub enum IngestionOutcome {
    Inserted(StoredUpdate),
    Duplicate,
}

#[derive(Debug, Eq, PartialEq)]
pub enum ManagedLifecycleOutcome {
    Queued,
    Duplicate,
}

#[derive(Clone, Copy, Debug)]
pub struct ManagedLifecycleDelivery {
    pub data_plane_pool: &'static str,
    pub telegram_test_dc: bool,
    pub observer_event_id: i64,
    pub delivery_nonce: i64,
    pub expires_at: u32,
}

#[derive(Debug, Clone)]
struct ManagedBotIdentity {
    telegram_bot_id: i64,
    owner_telegram_user_id: i64,
    username: String,
    display_name: String,
}

#[derive(Debug)]
struct ConversationProjection {
    chat_id: i64,
    telegram_message_id: Option<i64>,
    business_connection_id: Option<String>,
    guest_query_id: Option<String>,
    message_thread_id: Option<i64>,
    direct_messages_topic_id: Option<i64>,
    receiver_user_id: Option<i64>,
    ephemeral_message_id: Option<i64>,
    edit_date: Option<DateTime<Utc>>,
    user_id: Option<i64>,
    chat_type: Option<String>,
    title: Option<String>,
    username: Option<String>,
    display_name: Option<String>,
    preview: Option<String>,
}

/// Persist one canonical Telegram Update and all existing Phenogram
/// projections in a single transaction. PostgreSQL emits a post-commit NOTIFY
/// hint via the migration trigger; consumers always reload the row from the
/// database, which remains the replay source of truth.
pub async fn ingest_update(
    db: &PgPool,
    bot: IngestionBot,
    payload: Value,
    source: IngestionSource,
    expected_update_id: Option<i64>,
) -> Result<IngestionOutcome> {
    let update_id = payload
        .get("update_id")
        .and_then(Value::as_i64)
        .ok_or_else(|| AppError::Validation("missing update_id".into()))?;
    if expected_update_id.is_some_and(|expected| expected != update_id) {
        return Err(AppError::Validation(
            "tap event id does not match the update payload".into(),
        ));
    }
    let event_type = event_type(&payload).to_owned();
    let managed_identity =
        managed_bot_identity(&payload).map_err(|message| AppError::Validation(message.into()))?;
    let projection = conversation_projection(&payload);
    let projected_chat_id = projection.as_ref().map(|value| value.chat_id);
    let projected_telegram_user_id = projection.as_ref().and_then(|value| value.user_id);
    let projected_message_id = projection
        .as_ref()
        .and_then(|value| value.telegram_message_id);
    let projected_ephemeral_message_id = projection
        .as_ref()
        .and_then(|value| value.ephemeral_message_id);
    let projected_edit_date = projection.as_ref().and_then(|value| value.edit_date);

    let mut tx = db.begin().await?;
    // This lock spans every ingress process. Besides making shadow webhook/tap
    // races idempotent, it ensures BIGSERIAL row cursors commit in bot-local
    // order so an SSE resume cursor cannot skip a later commit with a lower id.
    let lock_key = i64::from_be_bytes(bot.id.as_bytes()[..8].try_into().expect("UUID prefix"));
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(lock_key)
        .execute(&mut *tx)
        .await?;
    if source == IngestionSource::ManagedWebhook {
        let detached = sqlx::query_scalar::<_, bool>(
            r#"SELECT EXISTS (
                   SELECT 1
                     FROM bot_data_plane_operations
                    WHERE bot_id = $1
                      AND phase IN (
                          'webhook_deleted',
                          'logout_started',
                          'close_started',
                          'source_logged_out',
                          'source_closed',
                          'target_initialized',
                          'webhook_restored',
                          'route_published',
                          'rollback_published',
                          'manual_recovery'
                      )
               )"#,
        )
        .bind(bot.id)
        .fetch_one(&mut *tx)
        .await?;
        if detached {
            return Err(AppError::Conflict(
                "Bot API migration is draining the previous webhook".into(),
            ));
        }
    }
    let inserted =
        sqlx::query_as::<_, (i64, Option<i64>, Option<i64>, DateTime<Utc>, DateTime<Utc>)>(
            r#"INSERT INTO updates
                  (bot_id, update_id, event_type, chat_id, telegram_user_id,
                   telegram_message_id, ephemeral_message_id, edit_date, payload,
                   expires_at, ingestion_source)
           SELECT bots.id, $2, $3, $4, $5, $6, $7, $8, $9,
                  now() + make_interval(days => bot_effective_retention_days(bots.id)), $10
             FROM bots
            WHERE bots.id = $1
           ON CONFLICT (bot_id, update_id) DO NOTHING
           RETURNING id, chat_id, telegram_user_id, received_at, expires_at"#,
        )
        .bind(bot.id)
        .bind(update_id)
        .bind(&event_type)
        .bind(projected_chat_id)
        .bind(projected_telegram_user_id)
        .bind(projected_message_id)
        .bind(projected_ephemeral_message_id)
        .bind(projected_edit_date)
        .bind(&payload)
        .bind(source.as_str())
        .fetch_optional(&mut *tx)
        .await?;

    let Some((row_id, chat_id, telegram_user_id, received_at, expires_at)) = inserted else {
        let existing_row_id = sqlx::query_scalar::<_, i64>(
            r#"UPDATE updates
                  SET ingestion_source = CASE
                          WHEN ingestion_source = $3 THEN ingestion_source
                          ELSE 'both'
                      END
                WHERE bot_id = $1 AND update_id = $2
            RETURNING id"#,
        )
        .bind(bot.id)
        .bind(update_id)
        .bind(source.as_str())
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(AppError::Internal)?;
        if let Some(identity) = &managed_identity {
            queue_managed_bot_sync(&mut tx, bot, update_id, existing_row_id, identity).await?;
        }
        tx.commit().await?;
        return Ok(IngestionOutcome::Duplicate);
    };

    if let Some(identity) = &managed_identity {
        queue_managed_bot_sync(&mut tx, bot, update_id, row_id, identity).await?;
    }
    if let Some(projection) = projection {
        let event_preview = projection.preview.clone();
        let linked_conversation_id = if matches!(
            event_type.as_str(),
            "message_reaction" | "message_reaction_count"
        ) {
            if let Some(message_id) = projection.telegram_message_id.filter(|id| *id != 0) {
                sqlx::query_scalar::<_, Uuid>(
                    r#"SELECT events.conversation_id
                         FROM conversation_events AS events
                         JOIN conversations ON conversations.id = events.conversation_id
                        WHERE events.bot_id = $1
                          AND conversations.chat_id = $2
                          AND events.telegram_message_id = $3
                          AND events.direction <> 'action'
                          AND (
                              events.direction = 'outgoing'
                              OR events.event_type IN (
                                  'message', 'edited_message', 'channel_post',
                                  'edited_channel_post', 'business_message',
                                  'edited_business_message', 'guest_message'
                              )
                          )
                        ORDER BY events.id DESC
                        LIMIT 1"#,
                )
                .bind(bot.id)
                .bind(projection.chat_id)
                .bind(message_id)
                .fetch_optional(&mut *tx)
                .await?
            } else {
                None
            }
        } else {
            None
        };
        let conversation_id = if let Some(conversation_id) = linked_conversation_id {
            sqlx::query(
                r#"UPDATE conversations
                      SET last_update_at = GREATEST(last_update_at, $2),
                          expires_at = GREATEST(expires_at, $3)
                    WHERE id = $1"#,
            )
            .bind(conversation_id)
            .bind(received_at)
            .bind(expires_at)
            .execute(&mut *tx)
            .await?;
            conversation_id
        } else {
            sqlx::query_scalar::<_, Uuid>(
            r#"INSERT INTO conversations
                   (bot_id, chat_id, business_connection_id, guest_query_id,
                    message_thread_id, direct_messages_topic_id, receiver_user_id,
                    chat_type, title, username, display_name,
                    last_message_preview, last_update_at, expires_at)
               SELECT bots.id, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, now(),
                      now() + make_interval(days => bot_effective_retention_days(bots.id))
                 FROM bots
                WHERE bots.id = $1
               ON CONFLICT ON CONSTRAINT conversations_identity_unique DO UPDATE SET
                   chat_type = COALESCE(EXCLUDED.chat_type, conversations.chat_type),
                   title = COALESCE(EXCLUDED.title, conversations.title),
                   username = COALESCE(EXCLUDED.username, conversations.username),
                   display_name = COALESCE(EXCLUDED.display_name, conversations.display_name),
                   last_message_preview = COALESCE(EXCLUDED.last_message_preview, conversations.last_message_preview),
                   last_update_at = EXCLUDED.last_update_at,
                   expires_at = EXCLUDED.expires_at
               RETURNING id"#,
            )
            .bind(bot.id)
            .bind(projection.chat_id)
            .bind(projection.business_connection_id.as_deref())
            .bind(projection.guest_query_id.as_deref())
            .bind(projection.message_thread_id)
            .bind(projection.direct_messages_topic_id)
            .bind(projection.receiver_user_id)
            .bind(projection.chat_type.as_deref())
            .bind(projection.title.as_deref())
            .bind(projection.username.as_deref())
            .bind(projection.display_name.as_deref())
            .bind(projection.preview.as_deref())
            .fetch_one(&mut *tx)
            .await?
        };
        sqlx::query("UPDATE updates SET conversation_id = $2 WHERE id = $1")
            .bind(row_id)
            .bind(conversation_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            r#"INSERT INTO conversation_events
                   (bot_id, conversation_id, direction, event_type, source_table,
                    source_id, telegram_message_id, receiver_user_id,
                    ephemeral_message_id, edit_date, text, status, payload,
                    created_at, expires_at)
               VALUES ($1, $2, 'incoming', $3, 'updates', $4, $5, $6, $7, $8,
                       $9, 'received', $10, $11, $12)"#,
        )
        .bind(bot.id)
        .bind(conversation_id)
        .bind(&event_type)
        .bind(row_id)
        .bind(projected_message_id)
        .bind(projection.receiver_user_id)
        .bind(projected_ephemeral_message_id)
        .bind(projected_edit_date)
        .bind(event_preview.as_deref())
        .bind(&payload)
        .bind(received_at)
        .bind(expires_at)
        .execute(&mut *tx)
        .await?;
        if event_type == "deleted_business_messages"
            && let Some(message_ids) = payload
                .pointer("/deleted_business_messages/message_ids")
                .and_then(Value::as_array)
        {
            for message_id in message_ids.iter().filter_map(Value::as_i64) {
                sqlx::query(
                    r#"INSERT INTO conversation_events
                           (bot_id, conversation_id, direction, event_type, source_table,
                            telegram_message_id, status, payload, created_at, expires_at)
                       VALUES ($1, $2, 'action', 'deleted_business_message', 'updates',
                               $3, 'deleted', $4, $5, $6)"#,
                )
                .bind(bot.id)
                .bind(conversation_id)
                .bind(message_id)
                .bind(&payload)
                .bind(received_at)
                .bind(expires_at)
                .execute(&mut *tx)
                .await?;
            }
        }
    } else if let Some(poll_id) = payload
        .pointer("/poll/id")
        .or_else(|| payload.pointer("/poll_answer/poll_id"))
        .and_then(Value::as_str)
    {
        let linked_conversation_id = sqlx::query_scalar::<_, Uuid>(
            r#"SELECT events.conversation_id
                 FROM conversation_events AS events
                WHERE events.bot_id = $1
                  AND (
                      events.direction = 'outgoing'
                      OR events.event_type IN (
                          'message', 'edited_message', 'channel_post',
                          'edited_channel_post', 'business_message',
                          'edited_business_message', 'guest_message'
                      )
                  )
                  AND jsonb_path_exists(
                      events.payload,
                      '$.**.poll.id ? (@ == $poll_id)',
                      jsonb_build_object('poll_id', to_jsonb($2::text))
                  )
                ORDER BY events.id DESC
                LIMIT 1"#,
        )
        .bind(bot.id)
        .bind(poll_id)
        .fetch_optional(&mut *tx)
        .await?;
        if let Some(conversation_id) = linked_conversation_id {
            sqlx::query("UPDATE updates SET conversation_id = $2 WHERE id = $1")
                .bind(row_id)
                .bind(conversation_id)
                .execute(&mut *tx)
                .await?;
            sqlx::query(
                r#"INSERT INTO conversation_events
                       (bot_id, conversation_id, direction, event_type, source_table,
                        source_id, text, status, payload, created_at, expires_at)
                   VALUES ($1, $2, 'incoming', $3, 'updates', $4, NULL, 'received',
                           $5, $6, $7)"#,
            )
            .bind(bot.id)
            .bind(conversation_id)
            .bind(&event_type)
            .bind(row_id)
            .bind(&payload)
            .bind(received_at)
            .bind(expires_at)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                r#"UPDATE conversations
                      SET last_update_at = GREATEST(last_update_at, $2),
                          expires_at = GREATEST(expires_at, $3)
                    WHERE id = $1"#,
            )
            .bind(conversation_id)
            .bind(received_at)
            .bind(expires_at)
            .execute(&mut *tx)
            .await?;
        }
    }

    // The official Bot API server is the canonical webhook/polling owner for
    // tapped updates. Phenogram only journals and projects that observer copy;
    // consulting legacy bot_update_state here could duplicate a webhook the
    // official server has already delivered. Keep the old queue exclusively
    // on the managed-webhook ingress path while it still exists.
    if source == IngestionSource::ManagedWebhook {
        let (has_webhook, allowed_updates) = sqlx::query_as::<_, (bool, Option<Value>)>(
            "SELECT downstream_webhook_url IS NOT NULL, allowed_updates FROM bot_update_state WHERE bot_id = $1",
        )
        .bind(bot.id)
        .fetch_one(&mut *tx)
        .await?;
        let allowed = allowed_updates.as_ref().and_then(parse_string_array);
        let deliver_event = allowed.as_ref().is_none_or(|types| {
            if types.is_empty() {
                !matches!(
                    event_type.as_str(),
                    "chat_member" | "message_reaction" | "message_reaction_count"
                )
            } else {
                types.iter().any(|kind| kind == &event_type)
            }
        });
        if has_webhook && deliver_event {
            sqlx::query(
                "INSERT INTO webhook_deliveries (bot_id, update_row_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            )
            .bind(bot.id)
            .bind(row_id)
            .execute(&mut *tx)
            .await?;
        } else if !deliver_event {
            // Telegram applies allowed_updates when the update is created.
            // Keep the observer copy while excluding the legacy ingress event
            // from legacy polling/delivery.
            sqlx::query("UPDATE updates SET consumed_at = now() WHERE id = $1")
                .bind(row_id)
                .execute(&mut *tx)
                .await?;
        }
    }
    // A tap proves update flow, not lifecycle safety. It must not erase a
    // provisioning/degraded/manual-recovery state owned by the control plane.
    sqlx::query(
        r#"UPDATE bots
              SET last_update_at = now(),
                  status = CASE WHEN $2 = 'managed_webhook' THEN 'healthy' ELSE status END,
                  updated_at = now()
            WHERE id = $1"#,
    )
    .bind(bot.id)
    .bind(source.as_str())
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;

    Ok(IngestionOutcome::Inserted(StoredUpdate {
        row_id,
        update_id,
        event_type,
        chat_id,
        telegram_user_id,
        payload,
        received_at,
        expires_at,
    }))
}

async fn queue_managed_bot_sync(
    tx: &mut Transaction<'_, Postgres>,
    manager: IngestionBot,
    source_update_id: i64,
    source_update_row_id: i64,
    identity: &ManagedBotIdentity,
) -> Result<()> {
    if identity.telegram_bot_id == manager.telegram_bot_id {
        return Err(AppError::Validation("invalid managed bot identity".into()));
    }
    sqlx::query(
        r#"INSERT INTO managed_bot_sync_jobs
               (manager_bot_id, managed_telegram_bot_id, managed_owner_telegram_user_id,
                username, display_name, source_update_id, source_update_row_id)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (manager_bot_id, managed_telegram_bot_id) DO UPDATE SET
               managed_owner_telegram_user_id = EXCLUDED.managed_owner_telegram_user_id,
               username = EXCLUDED.username,
               display_name = EXCLUDED.display_name,
               source_update_id = EXCLUDED.source_update_id,
               source_update_row_id = EXCLUDED.source_update_row_id,
               source_generation = nextval('managed_bot_sync_source_generation_seq'),
               state = 'pending', attempt = 0, next_attempt_at = now(),
               locked_at = NULL, error_summary = NULL, completed_at = NULL,
               updated_at = now()
           WHERE EXCLUDED.source_update_row_id > managed_bot_sync_jobs.source_update_row_id"#,
    )
    .bind(manager.id)
    .bind(identity.telegram_bot_id)
    .bind(identity.owner_telegram_user_id)
    .bind(&identity.username)
    .bind(&identity.display_name)
    .bind(source_update_id)
    .bind(source_update_row_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Schedule managed-bot discovery from the durable lifecycle observer. The
/// delivery receipt and job mutation commit atomically; an exact replay returns
/// `Duplicate` without changing the job generation or state. A zero
/// `source_update_row_id` is reserved for lifecycle-only signals, while a later
/// canonical Update has a positive row id and supersedes the placeholder.
pub async fn ingest_managed_bot_lifecycle(
    db: &PgPool,
    manager: IngestionBot,
    delivery: ManagedLifecycleDelivery,
    managed_owner_telegram_user_id: i64,
    managed_telegram_bot_id: i64,
) -> Result<ManagedLifecycleOutcome> {
    if !matches!(delivery.data_plane_pool, "standard" | "local")
        || delivery.observer_event_id <= 0
        || delivery.delivery_nonce <= 0
        || delivery.expires_at == 0
        || managed_owner_telegram_user_id <= 0
        || managed_telegram_bot_id <= 0
        || managed_telegram_bot_id == manager.telegram_bot_id
    {
        return Err(AppError::Validation(
            "invalid managed bot lifecycle identity".into(),
        ));
    }
    let mut tx = db.begin().await?;
    let lock_key = i64::from_be_bytes(manager.id.as_bytes()[..8].try_into().expect("UUID prefix"));
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(lock_key)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM managed_bot_lifecycle_receipts WHERE expires_at <= now()")
        .execute(&mut *tx)
        .await?;
    let receipt_inserted = sqlx::query_scalar::<_, i64>(
        r#"INSERT INTO managed_bot_lifecycle_receipts
                  (data_plane_pool, telegram_test_dc, manager_bot_id,
                   parent_telegram_bot_id, delivery_nonce, observer_event_id,
                   managed_owner_telegram_user_id, managed_telegram_bot_id, expires_at)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, to_timestamp($9))
           ON CONFLICT DO NOTHING
        RETURNING delivery_nonce"#,
    )
    .bind(delivery.data_plane_pool)
    .bind(delivery.telegram_test_dc)
    .bind(manager.id)
    .bind(manager.telegram_bot_id)
    .bind(delivery.delivery_nonce)
    .bind(delivery.observer_event_id)
    .bind(managed_owner_telegram_user_id)
    .bind(managed_telegram_bot_id)
    .bind(i64::from(delivery.expires_at))
    .fetch_optional(&mut *tx)
    .await?;
    if receipt_inserted.is_none() {
        let exact_duplicate = sqlx::query_scalar::<_, bool>(
            r#"SELECT EXISTS (
                   SELECT 1
                     FROM managed_bot_lifecycle_receipts
                    WHERE data_plane_pool = $1
                      AND telegram_test_dc = $2
                      AND manager_bot_id = $3
                      AND parent_telegram_bot_id = $4
                      AND delivery_nonce = $5
                      AND observer_event_id = $6
                      AND managed_owner_telegram_user_id = $7
                      AND managed_telegram_bot_id = $8
                      AND expires_at = to_timestamp($9)
               )"#,
        )
        .bind(delivery.data_plane_pool)
        .bind(delivery.telegram_test_dc)
        .bind(manager.id)
        .bind(manager.telegram_bot_id)
        .bind(delivery.delivery_nonce)
        .bind(delivery.observer_event_id)
        .bind(managed_owner_telegram_user_id)
        .bind(managed_telegram_bot_id)
        .bind(i64::from(delivery.expires_at))
        .fetch_one(&mut *tx)
        .await?;
        if !exact_duplicate {
            return Err(AppError::Validation(
                "managed lifecycle delivery nonce collision".into(),
            ));
        }
        tx.commit().await?;
        return Ok(ManagedLifecycleOutcome::Duplicate);
    }
    let placeholder = format!("managed_{managed_telegram_bot_id}_bot");
    let queued = sqlx::query_scalar::<_, Uuid>(
        r#"INSERT INTO managed_bot_sync_jobs
               (manager_bot_id, managed_telegram_bot_id, managed_owner_telegram_user_id,
                username, display_name, source_update_id, source_update_row_id)
           VALUES ($1, $2, $3, $4, $4, 0, 0)
           ON CONFLICT (manager_bot_id, managed_telegram_bot_id) DO UPDATE SET
               managed_owner_telegram_user_id = EXCLUDED.managed_owner_telegram_user_id,
               source_generation = nextval('managed_bot_sync_source_generation_seq'),
               state = 'pending', attempt = 0, next_attempt_at = now(),
               locked_at = NULL, error_summary = NULL, completed_at = NULL,
               updated_at = now()
        RETURNING id"#,
    )
    .bind(manager.id)
    .bind(managed_telegram_bot_id)
    .bind(managed_owner_telegram_user_id)
    .bind(placeholder)
    .fetch_optional(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(if queued.is_some() {
        ManagedLifecycleOutcome::Queued
    } else {
        ManagedLifecycleOutcome::Duplicate
    })
}

pub async fn start_update_notification_listener(
    state: AppState,
) -> Result<tokio::task::JoinHandle<()>> {
    let listener = connect_update_notification_listener(&state).await?;
    Ok(tokio::spawn(run_update_notification_listener(
        state, listener,
    )))
}

async fn run_update_notification_listener(state: AppState, mut listener: PgListener) {
    loop {
        if let Err(error) = listen_for_update_notifications(&state, &mut listener).await {
            tracing::error!(error = ?error, "update notification listener disconnected");
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            loop {
                match connect_update_notification_listener(&state).await {
                    Ok(connected) => {
                        listener = connected;
                        break;
                    }
                    Err(error) => {
                        tracing::error!(error = ?error, "could not reconnect update notification listener");
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                }
            }
        }
    }
}

async fn connect_update_notification_listener(state: &AppState) -> Result<PgListener> {
    let mut listener = PgListener::connect_with(&state.db).await?;
    listener.listen(UPDATE_NOTIFY_CHANNEL).await?;
    tracing::info!(
        channel = UPDATE_NOTIFY_CHANNEL,
        "update notification listener ready"
    );
    Ok(listener)
}

async fn listen_for_update_notifications(
    state: &AppState,
    listener: &mut PgListener,
) -> Result<()> {
    loop {
        let notification = listener.recv().await?;
        let hint: UpdateNotification = match serde_json::from_str(notification.payload()) {
            Ok(hint) => hint,
            Err(error) => {
                tracing::warn!(error = ?error, "ignored malformed update notification");
                continue;
            }
        };
        let update = sqlx::query_as::<_, StoredUpdate>(
            r#"SELECT id AS row_id, update_id, event_type, chat_id, telegram_user_id,
                      payload, received_at, expires_at
                 FROM updates
                WHERE id = $1 AND bot_id = $2 AND expires_at > now()"#,
        )
        .bind(hint.row_id)
        .bind(hint.bot_id)
        .fetch_optional(&state.db)
        .await?;
        if let Some(update) = update {
            state.events.publish(hint.bot_id, update).await;
        }
    }
}

#[derive(Debug, Deserialize)]
struct UpdateNotification {
    bot_id: Uuid,
    row_id: i64,
}

pub(crate) fn event_type(payload: &Value) -> &str {
    ALL_UPDATE_TYPES
        .iter()
        .find(|kind| payload.get(**kind).is_some())
        .copied()
        .unwrap_or("unknown")
}

fn managed_bot_identity(
    payload: &Value,
) -> std::result::Result<Option<ManagedBotIdentity>, &'static str> {
    let Some(managed) = payload.get("managed_bot") else {
        return Ok(None);
    };
    let bot = managed.get("bot").ok_or("missing managed bot identity")?;
    if bot.get("is_bot").and_then(Value::as_bool) != Some(true) {
        return Err("invalid managed bot identity");
    }
    let telegram_bot_id = bot
        .get("id")
        .and_then(Value::as_i64)
        .filter(|id| *id > 0)
        .ok_or("invalid managed bot identifier")?;
    let owner_telegram_user_id = managed
        .pointer("/user/id")
        .and_then(Value::as_i64)
        .filter(|id| *id > 0)
        .ok_or("invalid managed bot owner")?;
    let username = bot
        .get("username")
        .and_then(Value::as_str)
        .filter(|username| !username.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| format!("managed_{telegram_bot_id}_bot"));
    let display_name = bot
        .get("first_name")
        .and_then(Value::as_str)
        .filter(|name| !name.is_empty())
        .unwrap_or(&username)
        .to_owned();
    Ok(Some(ManagedBotIdentity {
        telegram_bot_id,
        owner_telegram_user_id,
        username,
        display_name,
    }))
}

fn conversation_projection(payload: &Value) -> Option<ConversationProjection> {
    let event = ALL_UPDATE_TYPES.iter().find_map(|kind| payload.get(*kind));
    let message = event.and_then(|value| value.get("message").or(Some(value)))?;
    let chat = message.get("chat")?;
    let chat_id = chat.get("id")?.as_i64()?;
    // Wrapper updates such as callback_query have their own actor. The nested
    // Message.from is the author of the referenced message (often the bot), not
    // the human who triggered this update.
    let from = event
        .and_then(|value| value.get("from"))
        .or_else(|| message.get("from"));
    let first_name = chat.get("first_name").and_then(Value::as_str);
    let last_name = chat.get("last_name").and_then(Value::as_str);
    let display_name = match (first_name, last_name) {
        (Some(first), Some(last)) => Some(format!("{first} {last}")),
        (Some(first), None) => Some(first.to_owned()),
        _ => None,
    };
    let preview = message
        .get("text")
        .or_else(|| message.get("caption"))
        .and_then(Value::as_str)
        .map(|value| value.chars().take(180).collect());
    Some(ConversationProjection {
        chat_id,
        telegram_message_id: message.get("message_id").and_then(Value::as_i64),
        business_connection_id: message
            .get("business_connection_id")
            .or_else(|| event.and_then(|value| value.get("business_connection_id")))
            .and_then(Value::as_str)
            .map(str::to_owned),
        guest_query_id: message
            .get("guest_query_id")
            .or_else(|| event.and_then(|value| value.get("guest_query_id")))
            .and_then(Value::as_str)
            .map(str::to_owned),
        message_thread_id: message.get("message_thread_id").and_then(Value::as_i64),
        direct_messages_topic_id: message
            .pointer("/direct_messages_topic/topic_id")
            .or_else(|| message.pointer("/direct_messages_topic/id"))
            .and_then(Value::as_i64),
        receiver_user_id: message
            .get("ephemeral_message_id")
            .and_then(Value::as_i64)
            .and_then(|_| {
                from.and_then(|value| value.get("id"))
                    .and_then(Value::as_i64)
            }),
        ephemeral_message_id: message.get("ephemeral_message_id").and_then(Value::as_i64),
        edit_date: message
            .get("edit_date")
            .and_then(Value::as_i64)
            .and_then(|seconds| DateTime::from_timestamp(seconds, 0)),
        user_id: from
            .and_then(|value| value.get("id"))
            .and_then(Value::as_i64),
        chat_type: chat.get("type").and_then(Value::as_str).map(str::to_owned),
        title: chat.get("title").and_then(Value::as_str).map(str::to_owned),
        username: chat
            .get("username")
            .and_then(Value::as_str)
            .map(str::to_owned),
        display_name,
        preview,
    })
}

fn parse_string_array(value: &Value) -> Option<Vec<String>> {
    value.as_array().map(|values| {
        values
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect()
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{event_type, managed_bot_identity};

    #[test]
    fn identifies_managed_bot_update_type() {
        assert_eq!(
            event_type(&json!({"update_id": 1, "managed_bot": {}})),
            "managed_bot"
        );
    }

    #[test]
    fn parses_managed_bot_identity_without_a_token() {
        let identity = managed_bot_identity(&json!({
            "update_id": 1,
            "managed_bot": {
                "user": {"id": 42},
                "bot": {"id": 99, "is_bot": true, "username": "child_bot", "first_name": "Child"}
            }
        }))
        .expect("valid managed update")
        .expect("managed identity");
        assert_eq!(identity.telegram_bot_id, 99);
        assert_eq!(identity.owner_telegram_user_id, 42);
        assert_eq!(identity.username, "child_bot");
        assert_eq!(identity.display_name, "Child");
    }

    #[test]
    fn rejects_malformed_managed_bot_identity() {
        assert!(
            managed_bot_identity(&json!({
                "update_id": 1,
                "managed_bot": {"bot": {"id": 99, "is_bot": false}}
            }))
            .is_err()
        );
    }
}
