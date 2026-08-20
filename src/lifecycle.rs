use std::{net::Ipv4Addr, time::Duration};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sqlx::{Acquire, FromRow, Postgres, Transaction};
use uuid::Uuid;
use zeroize::{Zeroize, ZeroizeOnDrop};

use crate::{
    crypto::Ciphertext,
    error::{AppError, Result},
    models::BotRecord,
    state::AppState,
    telegram::{
        ExistingWebhook, ExistingWebhookPolicy, decrypt_token, existing_webhook,
        raw_telegram_json_for_dc,
    },
};

const WORKER_IDLE: Duration = Duration::from_millis(500);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DataPlanePool {
    Standard,
    Local,
}

impl DataPlanePool {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Standard => "standard",
            Self::Local => "local",
        }
    }

    pub fn routing_mode(self) -> &'static str {
        match self {
            Self::Standard => "cloud",
            Self::Local => "local",
        }
    }

    pub fn from_routing_mode(value: &str) -> Result<Self> {
        match value {
            "cloud" => Ok(Self::Standard),
            "local" => Ok(Self::Local),
            _ => Err(AppError::Internal),
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "standard" => Ok(Self::Standard),
            "local" => Ok(Self::Local),
            _ => Err(AppError::Internal),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SourcePool {
    Cloud,
    Standard,
    Local,
}

impl SourcePool {
    fn as_str(self) -> &'static str {
        match self {
            Self::Cloud => "cloud",
            Self::Standard => "standard",
            Self::Local => "local",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "cloud" => Ok(Self::Cloud),
            "standard" => Ok(Self::Standard),
            "local" => Ok(Self::Local),
            _ => Err(AppError::Internal),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LifecycleOperation {
    Connect,
    ManagedSync,
    ManagedRotate,
}

impl LifecycleOperation {
    fn as_str(self) -> &'static str {
        match self {
            Self::Connect => "connect",
            Self::ManagedSync => "managed_sync",
            Self::ManagedRotate => "managed_rotate",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "connect" => Ok(Self::Connect),
            "managed_sync" => Ok(Self::ManagedSync),
            "managed_rotate" => Ok(Self::ManagedRotate),
            _ => Err(AppError::Internal),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LifecycleOutcome {
    Active {
        webhook_transferred: bool,
        secret_reentry_required: bool,
    },
    RolledBack,
    Busy,
}

#[derive(Debug, FromRow)]
struct OperationRow {
    bot_id: Uuid,
    operation: String,
    source_pool: String,
    target_pool: Option<String>,
    phase: String,
    withdraw_generation: i64,
    publication_generation: i64,
    previous_webhook_ciphertext: Option<Vec<u8>>,
    previous_webhook_nonce: Option<Vec<u8>>,
    webhook_resolution_ciphertext: Option<Vec<u8>>,
    webhook_resolution_nonce: Option<Vec<u8>>,
    source_token_ciphertext: Option<Vec<u8>>,
    source_token_nonce: Option<Vec<u8>>,
    target_token_ciphertext: Option<Vec<u8>>,
    target_token_nonce: Option<Vec<u8>>,
    attempt: i32,
}

pub struct PreparedWebhook {
    encrypted: Option<Ciphertext>,
    reported_ip_address_preserved: bool,
}

impl PreparedWebhook {
    pub fn reported_ip_address_preserved(&self) -> bool {
        self.reported_ip_address_preserved
    }
}

pub struct PreparedRotation {
    source_token: Ciphertext,
    target_token: Ciphertext,
}

#[derive(Clone, Copy, Default)]
pub struct ExistingWebhookResolution<'a> {
    pub secret: Option<&'a str>,
    pub confirmed_no_secret: bool,
    pub ip_address: Option<&'a str>,
    pub confirmed_no_ip_address: bool,
}

#[derive(Debug, Deserialize, Serialize)]
struct WebhookSnapshot {
    url: String,
    allowed_updates: Option<Value>,
    max_connections: i32,
    ip_address: Option<String>,
    secret_token: Option<String>,
    #[serde(default)]
    secret_confirmed_absent: bool,
}

#[derive(Default, Deserialize, Serialize, Zeroize, ZeroizeOnDrop)]
struct WebhookResolutionSnapshot {
    secret: Option<String>,
    confirmed_no_secret: bool,
    ip_address: Option<String>,
    confirmed_no_ip_address: bool,
}

pub fn source_for_bot(bot: &BotRecord) -> Result<SourcePool> {
    match bot.data_plane_pool.as_deref() {
        Some("standard") => Ok(SourcePool::Standard),
        Some("local") => Ok(SourcePool::Local),
        Some(_) => Err(AppError::Internal),
        None => Ok(SourcePool::Cloud),
    }
}

pub fn gateway_base(state: &AppState) -> Result<&str> {
    state
        .config
        .data_plane_gateway_url
        .as_deref()
        .ok_or_else(|| AppError::Config("data-plane gateway is not configured".into()))
}

pub fn direct_pool_base(state: &AppState, pool: DataPlanePool) -> Result<&str> {
    match pool {
        DataPlanePool::Standard => state.config.data_plane_standard_api_url.as_deref(),
        DataPlanePool::Local => state.config.data_plane_local_api_url.as_deref(),
    }
    .ok_or_else(|| AppError::Config("data-plane Bot API pool is not configured".into()))
}

pub(crate) fn source_base(state: &AppState, pool: SourcePool) -> Result<&str> {
    match pool {
        SourcePool::Cloud => Ok(&state.config.telegram_cloud_api_url),
        SourcePool::Standard => direct_pool_base(state, DataPlanePool::Standard),
        SourcePool::Local => direct_pool_base(state, DataPlanePool::Local),
    }
}

pub async fn create_operation(
    tx: &mut Transaction<'_, Postgres>,
    bot_id: Uuid,
    operation: LifecycleOperation,
    source: SourcePool,
    target: DataPlanePool,
    prepared_webhook: &PreparedWebhook,
) -> Result<()> {
    sqlx::query(
        r#"INSERT INTO bot_data_plane_operations
                  (bot_id, operation, source_pool, target_pool, phase,
                   previous_webhook_ciphertext, previous_webhook_nonce)
           VALUES ($1, $2, $3, $4, 'route_withdrawn', $5, $6)"#,
    )
    .bind(bot_id)
    .bind(operation.as_str())
    .bind(source.as_str())
    .bind(target.as_str())
    .bind(
        prepared_webhook
            .encrypted
            .as_ref()
            .map(|value| value.data.as_slice()),
    )
    .bind(
        prepared_webhook
            .encrypted
            .as_ref()
            .map(|value| value.nonce.as_slice()),
    )
    .execute(&mut **tx)
    .await?;
    Ok(())
}

pub async fn create_rotation_operation(
    tx: &mut Transaction<'_, Postgres>,
    bot_id: Uuid,
    source: SourcePool,
    target: DataPlanePool,
    prepared_rotation: &PreparedRotation,
) -> Result<()> {
    if !matches!(
        (source, target),
        (SourcePool::Standard, DataPlanePool::Standard) | (SourcePool::Local, DataPlanePool::Local)
    ) {
        return Err(AppError::Conflict(
            "Managed bot token rotation must stay in its current Bot API pool".into(),
        ));
    }
    sqlx::query(
        r#"INSERT INTO bot_data_plane_operations
                  (bot_id, operation, source_pool, target_pool, phase,
                   previous_webhook_ciphertext, previous_webhook_nonce,
                   source_token_ciphertext, source_token_nonce,
                   target_token_ciphertext, target_token_nonce)
           VALUES ($1, 'managed_rotate', $2, $3, 'route_withdrawn',
                   NULL, NULL, $4, $5, $6, $7)"#,
    )
    .bind(bot_id)
    .bind(source.as_str())
    .bind(target.as_str())
    .bind(&prepared_rotation.source_token.data)
    .bind(&prepared_rotation.source_token.nonce)
    .bind(&prepared_rotation.target_token.data)
    .bind(&prepared_rotation.target_token.nonce)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

pub(crate) fn validate_migration_path(
    source: SourcePool,
    target: DataPlanePool,
    _operation: LifecycleOperation,
) -> Result<()> {
    match (source, target) {
        (SourcePool::Cloud, _) => Ok(()),
        (SourcePool::Standard, DataPlanePool::Standard)
        | (SourcePool::Local, DataPlanePool::Local) => Ok(()),
        (SourcePool::Standard | SourcePool::Local, _) => Err(AppError::Conflict(
            "Moving a bot between official Bot API pools requires a coordinated data-directory transfer and is not available yet"
                .into(),
        )),
    }
}

pub async fn has_operation(state: &AppState, bot_id: Uuid) -> Result<bool> {
    sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM bot_data_plane_operations WHERE bot_id = $1)",
    )
    .bind(bot_id)
    .fetch_one(&state.db)
    .await
    .map_err(Into::into)
}

pub async fn run_bot_operation(state: &AppState, bot_id: Uuid) -> Result<LifecycleOutcome> {
    run_bot_operation_with_webhook_resolution(state, bot_id, ExistingWebhookResolution::default())
        .await
}

pub async fn run_bot_operation_with_webhook_resolution(
    state: &AppState,
    bot_id: Uuid,
    webhook_resolution: ExistingWebhookResolution<'_>,
) -> Result<LifecycleOutcome> {
    if !state.config.data_plane_enabled {
        return Err(AppError::Conflict(
            "The official Phenogram data plane is not enabled".into(),
        ));
    }
    let mut connection = state.db.acquire().await?;
    let locked =
        sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_lock(hashtextextended($1::text, 0))")
            .bind(bot_id)
            .fetch_one(&mut *connection)
            .await?;
    if !locked {
        return Ok(LifecycleOutcome::Busy);
    }
    let result = run_locked(state, bot_id, &mut connection, webhook_resolution).await;
    let unlock =
        sqlx::query_scalar::<_, bool>("SELECT pg_advisory_unlock(hashtextextended($1::text, 0))")
            .bind(bot_id)
            .fetch_one(&mut *connection)
            .await;
    if let Err(error) = unlock {
        tracing::error!(%bot_id, error = ?error, "could not release bot lifecycle lock");
    }
    result
}

async fn run_locked(
    state: &AppState,
    bot_id: Uuid,
    connection: &mut sqlx::pool::PoolConnection<Postgres>,
    webhook_resolution: ExistingWebhookResolution<'_>,
) -> Result<LifecycleOutcome> {
    sqlx::query(
        "UPDATE bot_data_plane_operations SET attempt = attempt + 1, updated_at = now() WHERE bot_id = $1",
    )
    .bind(bot_id)
    .execute(&mut **connection)
    .await?;

    loop {
        let Some(operation) = load_operation(connection, bot_id).await? else {
            return Err(AppError::Conflict(
                "No pending Bot API lifecycle operation was found".into(),
            ));
        };
        let kind = LifecycleOperation::parse(&operation.operation)?;
        let result = run_phase(state, connection, &operation, webhook_resolution).await;
        match result {
            Ok(Some(outcome)) => return Ok(outcome),
            Ok(None) => continue,
            Err(error) => {
                if matches!(error, AppError::GatewayDrainPending) {
                    // Route snapshots can still be propagating, while long
                    // polling and uploads can remain admitted for much longer
                    // than one control-plane request. Keep the operation
                    // fenced, mutate no Telegram state, and retry with the
                    // normal durable worker backoff.
                    record_failure(connection, &operation, &error).await;
                    return Err(error);
                }
                if kind == LifecycleOperation::ManagedRotate
                    && managed_webhook_resolution_error(&error)
                    && matches!(
                        operation.phase.as_str(),
                        "route_withdrawn" | "webhook_resolution_required"
                    )
                {
                    // The route is deliberately held withdrawn while the
                    // native webhook remains active. Operator recovery will
                    // refetch it under the same gateway admission fence.
                    return Err(error);
                }
                if kind == LifecycleOperation::ManagedRotate
                    && operation.phase == "webhook_resolution_required"
                {
                    // This phase is intentionally manual. A transient error
                    // must not republish the route or arm a blind retry that
                    // has no secret/IP intent.
                    return Err(error);
                }
                let rolled_back = if matches!(
                    operation.phase.as_str(),
                    "route_withdrawn" | "webhook_captured"
                ) {
                    if kind == LifecycleOperation::ManagedRotate {
                        rollback_rotation_before_close(state, connection, &operation)
                            .await
                            .is_ok()
                    } else {
                        rollback_before_logout(state, connection, &operation)
                            .await
                            .is_ok()
                    }
                } else {
                    false
                };
                if rolled_back {
                    return Err(error);
                }
                record_failure(connection, &operation, &error).await;
                return Err(error);
            }
        }
    }
}

async fn load_operation(
    connection: &mut sqlx::pool::PoolConnection<Postgres>,
    bot_id: Uuid,
) -> Result<Option<OperationRow>> {
    sqlx::query_as::<_, OperationRow>(
        r#"SELECT bot_id, operation, source_pool, target_pool, phase,
                  withdraw_generation, publication_generation,
                  previous_webhook_ciphertext,
                  previous_webhook_nonce,
                  webhook_resolution_ciphertext,
                  webhook_resolution_nonce,
                  source_token_ciphertext, source_token_nonce,
                  target_token_ciphertext, target_token_nonce,
                  attempt
             FROM bot_data_plane_operations
            WHERE bot_id = $1"#,
    )
    .bind(bot_id)
    .fetch_optional(&mut **connection)
    .await
    .map_err(Into::into)
}

async fn run_phase(
    state: &AppState,
    connection: &mut sqlx::pool::PoolConnection<Postgres>,
    operation: &OperationRow,
    webhook_resolution: ExistingWebhookResolution<'_>,
) -> Result<Option<LifecycleOutcome>> {
    let kind = LifecycleOperation::parse(&operation.operation)?;
    let source = SourcePool::parse(&operation.source_pool)?;
    let target = DataPlanePool::parse(operation.target_pool.as_deref().ok_or(AppError::Internal)?)?;
    if kind == LifecycleOperation::ManagedRotate {
        return run_rotation_phase(
            state,
            connection,
            operation,
            source,
            target,
            webhook_resolution,
        )
        .await;
    }
    let bot = load_bot(state, connection, operation.bot_id).await?;
    let token = decrypt_token(state, &bot)?;
    let token = std::str::from_utf8(&token).map_err(|_| AppError::Internal)?;
    let telegram_bot_id = bot.telegram_bot_id;
    let telegram_test_dc = bot.telegram_test_dc;

    match operation.phase.as_str() {
        "route_withdrawn" => {
            if operation.withdraw_generation > 0 {
                gateway_acknowledged(state, operation.withdraw_generation).await?;
            }
            // The webhook restore plan was captured before route withdrawal,
            // so no secret-bearing metadata needs to be fetched or persisted
            // in plaintext while the public route is fenced.
            advance_phase(connection, operation.bot_id, "webhook_captured").await?;
            Ok(None)
        }
        "webhook_captured" => {
            telegram_ok(
                raw_telegram_json_for_dc(
                    &state.telegram,
                    source_base(state, source)?,
                    token,
                    telegram_test_dc,
                    "deleteWebhook",
                    &json!({"drop_pending_updates": false}),
                )
                .await?,
                "Telegram did not confirm webhook detachment",
            )?;
            advance_phase(connection, operation.bot_id, "webhook_deleted").await?;
            Ok(None)
        }
        "webhook_deleted" => {
            // Persist intent before the non-transactional external call. If the
            // process dies after this checkpoint, the next worker must not call
            // logOut again: it cannot know whether Telegram completed the first
            // request before the response/checkpoint was lost.
            advance_phase(connection, operation.bot_id, "logout_started").await?;
            let logout = match raw_telegram_json_for_dc(
                &state.telegram,
                source_base(state, source)?,
                token,
                telegram_test_dc,
                "logOut",
                &json!({}),
            )
            .await
            {
                Ok(response) => response,
                Err(_) => {
                    mark_manual_recovery(connection, operation, "logout_outcome_unknown").await?;
                    return Err(manual_recovery_error());
                }
            };
            if logout.0.is_server_error() {
                mark_manual_recovery(connection, operation, "logout_outcome_unknown").await?;
                return Err(manual_recovery_error());
            }
            if let Err(error) = telegram_ok(logout, "Telegram did not confirm Bot API logout") {
                // A structured non-success response is a confirmed rejection:
                // source ownership did not move, so restoring the exact source
                // webhook and route is safe. Transport/5xx ambiguity above is
                // terminal and is never retried or inferred from target getMe.
                rollback_before_logout(state, connection, operation).await?;
                return Err(error);
            }
            advance_phase(connection, operation.bot_id, "source_logged_out").await?;
            Ok(None)
        }
        "logout_started" => {
            mark_manual_recovery(connection, operation, "logout_outcome_unknown").await?;
            Err(manual_recovery_error())
        }
        "source_logged_out" => {
            verify_identity(
                state,
                direct_pool_base(state, target)?,
                token,
                telegram_bot_id,
                telegram_test_dc,
            )
            .await?;
            advance_phase(connection, operation.bot_id, "target_initialized").await?;
            Ok(None)
        }
        "target_initialized" => {
            if operation.previous_webhook_ciphertext.is_some() {
                let payload = restore_webhook_payload(state, operation)?;
                telegram_ok(
                    raw_telegram_json_for_dc(
                        &state.telegram,
                        direct_pool_base(state, target)?,
                        token,
                        telegram_test_dc,
                        "setWebhook",
                        &Value::Object(payload),
                    )
                    .await?,
                    "Telegram did not restore the existing webhook",
                )?;
            }
            advance_phase(connection, operation.bot_id, "webhook_restored").await?;
            Ok(None)
        }
        "webhook_restored" => {
            let webhook = decrypt_webhook_snapshot(state, operation)?;
            let webhook_transferred = webhook.is_some();
            let mut tx = connection.begin().await?;
            sqlx::query(
                r#"UPDATE bots
                      SET data_plane_pool = $2, data_plane_target_pool = NULL,
                          routing_mode = $3,
                          update_mode = CASE WHEN $4 THEN 'webhook' ELSE 'polling' END,
                          status = 'provisioning', updated_at = now()
                    WHERE id = $1"#,
            )
            .bind(operation.bot_id)
            .bind(target.as_str())
            .bind(target.routing_mode())
            .bind(webhook_transferred)
            .execute(&mut *tx)
            .await?;
            let publication_generation = sqlx::query_scalar::<_, i64>(
                "SELECT generation FROM data_plane_route_state WHERE singleton = TRUE",
            )
            .fetch_one(&mut *tx)
            .await?;
            sqlx::query(
                r#"UPDATE bot_data_plane_operations
                      SET phase = 'route_published', publication_generation = $2,
                          last_error_code = NULL, updated_at = now()
                    WHERE bot_id = $1 AND phase = 'webhook_restored'"#,
            )
            .bind(operation.bot_id)
            .bind(publication_generation)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            Ok(None)
        }
        "route_published" => {
            gateway_acknowledged(state, operation.publication_generation).await?;
            let webhook = decrypt_webhook_snapshot(state, operation)?;
            let webhook_transferred = webhook.is_some();
            let secret_reentry_required = webhook.as_ref().is_some_and(|webhook| {
                webhook.secret_token.is_none() && !webhook.secret_confirmed_absent
            });
            let mut tx = connection.begin().await?;
            sqlx::query("UPDATE bots SET status = 'healthy', updated_at = now() WHERE id = $1")
                .bind(operation.bot_id)
                .execute(&mut *tx)
                .await?;
            sqlx::query(
                r#"INSERT INTO audit_log (user_id, bot_id, action, metadata, expires_at)
                   SELECT bots.user_id, bots.id, 'bot.data_plane_activated', $2,
                          now() + make_interval(days => bot_effective_retention_days(bots.id))
                     FROM bots WHERE bots.id = $1"#,
            )
            .bind(operation.bot_id)
            .bind(json!({
                "operation": kind.as_str(),
                "source_pool": source.as_str(),
                "target_pool": target.as_str(),
                "webhook_transferred": webhook_transferred,
                "webhook_secret_reentry_required": secret_reentry_required,
                "route_generation": operation.publication_generation,
            }))
            .execute(&mut *tx)
            .await?;
            sqlx::query("DELETE FROM bot_data_plane_operations WHERE bot_id = $1")
                .bind(operation.bot_id)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            Ok(Some(LifecycleOutcome::Active {
                webhook_transferred,
                secret_reentry_required,
            }))
        }
        "manual_recovery" => Err(manual_recovery_error()),
        _ => Err(AppError::Internal),
    }
}

async fn run_rotation_phase(
    state: &AppState,
    connection: &mut sqlx::pool::PoolConnection<Postgres>,
    operation: &OperationRow,
    source: SourcePool,
    target: DataPlanePool,
    webhook_resolution: ExistingWebhookResolution<'_>,
) -> Result<Option<LifecycleOutcome>> {
    let source_pool = source_data_plane_pool(source)?;
    if source_pool != target {
        return Err(AppError::Conflict(
            "Managed bot token rotation cannot move between Bot API pools".into(),
        ));
    }
    let source_token = decrypt_rotation_token(state, operation, RotationToken::Source)?;
    let target_token = decrypt_rotation_token(state, operation, RotationToken::Target)?;
    let source_token = std::str::from_utf8(&source_token).map_err(|_| AppError::Internal)?;
    let target_token = std::str::from_utf8(&target_token).map_err(|_| AppError::Internal)?;
    let bot = load_bot(state, connection, operation.bot_id).await?;
    let telegram_bot_id = bot.telegram_bot_id;
    let telegram_test_dc = bot.telegram_test_dc;

    match operation.phase.as_str() {
        "route_withdrawn" | "webhook_resolution_required" => {
            capture_rotation_webhook_after_withdrawal(
                state,
                connection,
                operation,
                source,
                source_token,
                telegram_test_dc,
                webhook_resolution,
            )
            .await?;
            Ok(None)
        }
        "webhook_captured" => {
            telegram_ok(
                raw_telegram_json_for_dc(
                    &state.telegram,
                    direct_pool_base(state, source_pool)?,
                    source_token,
                    telegram_test_dc,
                    "deleteWebhook",
                    &json!({"drop_pending_updates": false}),
                )
                .await?,
                "Telegram did not confirm old managed-bot webhook detachment",
            )?;
            advance_phase(connection, operation.bot_id, "webhook_deleted").await?;
            Ok(None)
        }
        "webhook_deleted" => {
            // As with logOut, persist intent before the non-transactional
            // request. A process restart can never safely infer that close did
            // or did not complete from a later getMe call.
            advance_phase(connection, operation.bot_id, "close_started").await?;
            let close = match raw_telegram_json_for_dc(
                &state.telegram,
                direct_pool_base(state, source_pool)?,
                source_token,
                telegram_test_dc,
                "close",
                &json!({}),
            )
            .await
            {
                Ok(response) => response,
                Err(_) => {
                    mark_manual_recovery(connection, operation, "close_outcome_unknown").await?;
                    return Err(manual_recovery_error());
                }
            };
            match classify_close_response(&close) {
                CloseDisposition::Confirmed => {
                    advance_phase(connection, operation.bot_id, "source_closed").await?;
                    Ok(None)
                }
                CloseDisposition::RetryableRejected => {
                    if start_rotation_rollback(
                        state,
                        connection,
                        operation,
                        source_pool,
                        source_token,
                    )
                    .await
                    .is_err()
                    {
                        mark_manual_recovery(connection, operation, "close_rollback_failed")
                            .await?;
                        return Err(manual_recovery_error());
                    }
                    Ok(None)
                }
                CloseDisposition::Ambiguous => {
                    mark_manual_recovery(connection, operation, "close_outcome_unknown").await?;
                    Err(manual_recovery_error())
                }
            }
        }
        "close_started" => {
            mark_manual_recovery(connection, operation, "close_outcome_unknown").await?;
            Err(manual_recovery_error())
        }
        "source_closed" => {
            verify_identity(
                state,
                direct_pool_base(state, target)?,
                target_token,
                telegram_bot_id,
                telegram_test_dc,
            )
            .await?;
            advance_phase(connection, operation.bot_id, "target_initialized").await?;
            Ok(None)
        }
        "target_initialized" => {
            if operation.previous_webhook_ciphertext.is_some() {
                let payload = restore_webhook_payload(state, operation)?;
                telegram_ok(
                    raw_telegram_json_for_dc(
                        &state.telegram,
                        direct_pool_base(state, target)?,
                        target_token,
                        telegram_test_dc,
                        "setWebhook",
                        &Value::Object(payload),
                    )
                    .await?,
                    "Telegram did not restore the managed bot webhook after token rotation",
                )?;
            }
            advance_phase(connection, operation.bot_id, "webhook_restored").await?;
            Ok(None)
        }
        "webhook_restored" => {
            let encrypted = state.crypto.encrypt(
                target_token.as_bytes(),
                format!("bot:{}:token", operation.bot_id).as_bytes(),
            )?;
            let token_fingerprint =
                crate::crypto::Crypto::token_fingerprint(target_token, telegram_test_dc);
            let token_lookup_hash = state.crypto.bot_public_id(target_token, telegram_test_dc);
            let webhook_transferred = operation.previous_webhook_ciphertext.is_some();
            let mut tx = connection.begin().await?;
            let updated = sqlx::query(
                r#"UPDATE bots
                      SET token_ciphertext = $2, token_nonce = $3,
                          token_fingerprint = $4, token_lookup_hash = $5,
                          data_plane_pool = $6, data_plane_target_pool = NULL,
                          routing_mode = $7,
                          update_mode = CASE WHEN $8 THEN 'webhook' ELSE 'polling' END,
                          status = 'provisioning', updated_at = now()
                    WHERE id = $1 AND data_plane_pool IS NULL
                      AND data_plane_target_pool = $6"#,
            )
            .bind(operation.bot_id)
            .bind(&encrypted.data)
            .bind(&encrypted.nonce)
            .bind(token_fingerprint)
            .bind(token_lookup_hash)
            .bind(target.as_str())
            .bind(target.routing_mode())
            .bind(webhook_transferred)
            .execute(&mut *tx)
            .await?;
            if updated.rows_affected() != 1 {
                return Err(AppError::Conflict(
                    "Managed bot route changed during token rotation".into(),
                ));
            }
            let publication_generation = sqlx::query_scalar::<_, i64>(
                "SELECT generation FROM data_plane_route_state WHERE singleton = TRUE",
            )
            .fetch_one(&mut *tx)
            .await?;
            sqlx::query(
                r#"UPDATE bot_data_plane_operations
                      SET phase = 'route_published', publication_generation = $2,
                          last_error_code = NULL, updated_at = now()
                    WHERE bot_id = $1 AND phase = 'webhook_restored'"#,
            )
            .bind(operation.bot_id)
            .bind(publication_generation)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            Ok(None)
        }
        "route_published" => {
            gateway_acknowledged(state, operation.publication_generation).await?;
            let webhook = decrypt_webhook_snapshot(state, operation)?;
            let webhook_transferred = webhook.is_some();
            let secret_reentry_required = webhook.as_ref().is_some_and(|webhook| {
                webhook.secret_token.is_none() && !webhook.secret_confirmed_absent
            });
            let mut tx = connection.begin().await?;
            sqlx::query("UPDATE bots SET status = 'healthy', updated_at = now() WHERE id = $1")
                .bind(operation.bot_id)
                .execute(&mut *tx)
                .await?;
            sqlx::query(
                r#"INSERT INTO audit_log (user_id, bot_id, action, metadata, expires_at)
                   SELECT bots.user_id, bots.id, 'bot.managed_token_rotated', $2,
                          now() + make_interval(days => bot_effective_retention_days(bots.id))
                     FROM bots WHERE bots.id = $1"#,
            )
            .bind(operation.bot_id)
            .bind(json!({
                "pool": target.as_str(),
                "webhook_transferred": webhook_transferred,
                "webhook_secret_reentry_required": secret_reentry_required,
                "route_generation": operation.publication_generation,
            }))
            .execute(&mut *tx)
            .await?;
            sqlx::query("DELETE FROM bot_data_plane_operations WHERE bot_id = $1")
                .bind(operation.bot_id)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            Ok(Some(LifecycleOutcome::Active {
                webhook_transferred,
                secret_reentry_required,
            }))
        }
        "rollback_published" => {
            finish_rotation_rollback(state, connection, operation).await?;
            Ok(Some(LifecycleOutcome::RolledBack))
        }
        "manual_recovery" => Err(manual_recovery_error()),
        _ => Err(AppError::Internal),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CloseDisposition {
    Confirmed,
    RetryableRejected,
    Ambiguous,
}

fn classify_close_response(response: &(reqwest::StatusCode, Value)) -> CloseDisposition {
    if response.0.is_success() && response.1.get("ok").and_then(Value::as_bool) == Some(true) {
        CloseDisposition::Confirmed
    } else if response.0 == reqwest::StatusCode::TOO_MANY_REQUESTS
        || response.1.get("error_code").and_then(Value::as_i64) == Some(429)
    {
        // The official server's 429 means the Client is still open (close is
        // unavailable during its initial lifetime), so restoring the old route
        // is safe. Every other non-success is ownership-ambiguous.
        CloseDisposition::RetryableRejected
    } else {
        CloseDisposition::Ambiguous
    }
}

pub async fn prepare_webhook_transfer(
    state: &AppState,
    bot_id: Uuid,
    source: SourcePool,
    token: &str,
    telegram_test_dc: bool,
) -> Result<PreparedWebhook> {
    prepare_webhook_transfer_inner(
        state,
        bot_id,
        source,
        token,
        telegram_test_dc,
        ExistingWebhookResolution::default(),
        false,
    )
    .await
}

/// A managed token rotation uses the same fail-closed webhook transfer as an
/// initial Connect. The ordinary worker has no secret input and therefore
/// stops before route withdrawal; the authenticated recovery flow supplies the
/// current secret (or explicitly confirms there is none) in memory.
pub async fn prepare_managed_rotation_webhook_transfer(
    state: &AppState,
    bot_id: Uuid,
    source: SourcePool,
    token: &str,
    telegram_test_dc: bool,
    resolution: ExistingWebhookResolution<'_>,
) -> Result<PreparedWebhook> {
    prepare_webhook_transfer_inner(
        state,
        bot_id,
        source,
        token,
        telegram_test_dc,
        resolution,
        true,
    )
    .await
}

pub async fn prepare_connect_webhook_transfer(
    state: &AppState,
    bot_id: Uuid,
    source: SourcePool,
    token: &str,
    telegram_test_dc: bool,
    resolution: ExistingWebhookResolution<'_>,
) -> Result<PreparedWebhook> {
    prepare_webhook_transfer_inner(
        state,
        bot_id,
        source,
        token,
        telegram_test_dc,
        resolution,
        true,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn prepare_webhook_transfer_inner(
    state: &AppState,
    bot_id: Uuid,
    source: SourcePool,
    token: &str,
    telegram_test_dc: bool,
    resolution: ExistingWebhookResolution<'_>,
    require_secret_resolution: bool,
) -> Result<PreparedWebhook> {
    let webhook = load_existing_webhook(state, source, token, telegram_test_dc).await?;
    let resolved_secret = if require_secret_resolution {
        resolve_connect_webhook_secret(
            webhook.as_ref(),
            resolution.secret,
            resolution.confirmed_no_secret,
        )?
    } else {
        None
    };
    let resolved_ip_address = if require_secret_resolution {
        resolve_connect_webhook_ip_address(
            webhook.as_ref(),
            resolution.ip_address,
            resolution.confirmed_no_ip_address,
        )?
    } else {
        None
    };
    let snapshot = webhook.map(|webhook| WebhookSnapshot {
        url: webhook.url,
        allowed_updates: webhook.allowed_updates,
        max_connections: webhook.max_connections,
        // getWebhookInfo reports the currently resolved address but does not
        // reveal whether setWebhook explicitly pinned it. Replay it only after
        // the operator explicitly chooses fixed-IP continuity.
        ip_address: resolved_ip_address,
        // Telegram deliberately never returns an existing secret_token. A
        // user-connected bot is fenced before mutation until the developer
        // supplies it or explicitly declares that the webhook has no secret.
        // New managed-bot discovery may retain Unknown here; rotations use a
        // stricter polling-only preflight and never reach this path.
        secret_token: resolved_secret,
        secret_confirmed_absent: require_secret_resolution && resolution.confirmed_no_secret,
    });

    let encrypted = snapshot
        .as_ref()
        .map(|snapshot| -> Result<_> {
            let serialized = zeroize::Zeroizing::new(
                serde_json::to_vec(snapshot).map_err(|_| AppError::Internal)?,
            );
            state.crypto.encrypt(
                &serialized,
                format!("bot:{bot_id}:data-plane-webhook").as_bytes(),
            )
        })
        .transpose()?;
    let reported_ip_address_preserved = resolution.ip_address.is_some()
        && snapshot
            .as_ref()
            .is_some_and(|snapshot| snapshot.ip_address.is_some());
    Ok(PreparedWebhook {
        encrypted,
        reported_ip_address_preserved,
    })
}

async fn load_existing_webhook(
    state: &AppState,
    source: SourcePool,
    token: &str,
    telegram_test_dc: bool,
) -> Result<Option<ExistingWebhook>> {
    let (_, response) = raw_telegram_json_for_dc(
        &state.telegram,
        source_base(state, source)?,
        token,
        telegram_test_dc,
        "getWebhookInfo",
        &json!({}),
    )
    .await?;
    existing_webhook(
        &response,
        &state.config.api_base_url,
        match source {
            SourcePool::Local => ExistingWebhookPolicy::Local,
            SourcePool::Cloud | SourcePool::Standard => ExistingWebhookPolicy::Cloud {
                allow_insecure_development: state.config.app_env != "production",
            },
        },
    )
}

pub(crate) fn resolve_connect_webhook_secret(
    webhook: Option<&ExistingWebhook>,
    existing_webhook_secret: Option<&str>,
    existing_webhook_has_no_secret: bool,
) -> Result<Option<String>> {
    if existing_webhook_secret.is_some() && existing_webhook_has_no_secret {
        return Err(AppError::Validation(
            "Provide the existing webhook secret or declare that there is no secret, not both"
                .into(),
        ));
    }
    let secret = existing_webhook_secret
        .map(str::trim)
        .filter(|secret| !secret.is_empty());
    if existing_webhook_secret.is_some() && secret.is_none() {
        return Err(AppError::Validation(
            "The existing webhook secret cannot be empty".into(),
        ));
    }
    if let Some(secret) = secret
        && (secret.len() > 256
            || !secret
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-')))
    {
        return Err(AppError::Validation(
            "The existing webhook secret must be 1-256 letters, digits, underscores, or hyphens"
                .into(),
        ));
    }
    let Some(webhook) = webhook else {
        return Ok(None);
    };
    if secret.is_none() && !existing_webhook_has_no_secret {
        return Err(AppError::WebhookSecretRequired {
            destination_host: safe_webhook_destination(&webhook.url),
        });
    }
    Ok(secret.map(str::to_owned))
}

pub(crate) fn resolve_connect_webhook_ip_address(
    webhook: Option<&ExistingWebhook>,
    existing_webhook_ip_address: Option<&str>,
    existing_webhook_has_no_ip_address: bool,
) -> Result<Option<String>> {
    if existing_webhook_ip_address.is_some() && existing_webhook_has_no_ip_address {
        return Err(AppError::Validation(
            "Provide the existing webhook IPv4 address or choose DNS resolution, not both".into(),
        ));
    }
    let supplied = existing_webhook_ip_address
        .map(str::trim)
        .filter(|address| !address.is_empty());
    if existing_webhook_ip_address.is_some() && supplied.is_none() {
        return Err(AppError::Validation(
            "The existing webhook IPv4 address cannot be empty".into(),
        ));
    }
    let supplied = supplied
        .map(|address| {
            let parsed = address.parse::<Ipv4Addr>().map_err(|_| {
                AppError::Validation(
                    "The existing webhook IP address must be a canonical IPv4 address".into(),
                )
            })?;
            if parsed.to_string() != address {
                return Err(AppError::Validation(
                    "The existing webhook IP address must be a canonical IPv4 address".into(),
                ));
            }
            Ok(address.to_owned())
        })
        .transpose()?;
    let Some(webhook) = webhook else {
        // The webhook may have been removed after the operator saw the
        // recovery prompt. With no current webhook there is no IP intent to
        // replay, so stale fixed/DNS input is safely ignored.
        return Ok(None);
    };
    let Some(reported) = webhook.reported_ip_address.as_deref() else {
        if supplied.is_some() || existing_webhook_has_no_ip_address {
            return Err(AppError::Validation(
                "Telegram did not report an existing webhook IP address".into(),
            ));
        }
        return Ok(None);
    };
    if let Some(supplied) = supplied {
        if supplied != reported {
            return Err(AppError::Validation(
                "The existing webhook IPv4 address must exactly match Telegram's current reported address"
                    .into(),
            ));
        }
        return Ok(Some(supplied));
    }
    if !existing_webhook_has_no_ip_address {
        return Err(AppError::WebhookIpAddressResolutionRequired {
            destination_host: safe_webhook_destination(&webhook.url),
            reported_ip_address: reported.to_owned(),
        });
    }
    Ok(None)
}

fn safe_webhook_destination(value: &str) -> String {
    let Ok(url) = url::Url::parse(value) else {
        return "the existing destination".into();
    };
    let Some(host) = url.host_str() else {
        return "the existing destination".into();
    };
    match url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_owned(),
    }
}

pub fn prepare_token_rotation(
    state: &AppState,
    bot_id: Uuid,
    source_token: &str,
    target_token: &str,
) -> Result<PreparedRotation> {
    if source_token == target_token {
        return Err(AppError::Validation(
            "Managed bot token rotation requires a changed token".into(),
        ));
    }
    Ok(PreparedRotation {
        source_token: state.crypto.encrypt(
            source_token.as_bytes(),
            rotation_token_aad(bot_id, RotationToken::Source).as_bytes(),
        )?,
        target_token: state.crypto.encrypt(
            target_token.as_bytes(),
            rotation_token_aad(bot_id, RotationToken::Target).as_bytes(),
        )?,
    })
}

#[derive(Clone, Copy)]
enum RotationToken {
    Source,
    Target,
}

fn rotation_token_aad(bot_id: Uuid, token: RotationToken) -> String {
    let role = match token {
        RotationToken::Source => "source",
        RotationToken::Target => "target",
    };
    format!("bot:{bot_id}:data-plane-rotation-{role}")
}

fn decrypt_rotation_token(
    state: &AppState,
    operation: &OperationRow,
    token: RotationToken,
) -> Result<zeroize::Zeroizing<Vec<u8>>> {
    let (data, nonce) = match token {
        RotationToken::Source => (
            operation.source_token_ciphertext.as_ref(),
            operation.source_token_nonce.as_ref(),
        ),
        RotationToken::Target => (
            operation.target_token_ciphertext.as_ref(),
            operation.target_token_nonce.as_ref(),
        ),
    };
    let (Some(data), Some(nonce)) = (data, nonce) else {
        return Err(AppError::Internal);
    };
    state.crypto.decrypt(
        &Ciphertext {
            data: data.clone(),
            nonce: nonce.clone(),
        },
        rotation_token_aad(operation.bot_id, token).as_bytes(),
    )
}

fn source_data_plane_pool(source: SourcePool) -> Result<DataPlanePool> {
    match source {
        SourcePool::Standard => Ok(DataPlanePool::Standard),
        SourcePool::Local => Ok(DataPlanePool::Local),
        SourcePool::Cloud => Err(AppError::Conflict(
            "Managed bot token rotation requires an active official Bot API pool".into(),
        )),
    }
}

fn managed_webhook_resolution_error(error: &AppError) -> bool {
    matches!(
        error,
        AppError::WebhookSecretRequired { .. }
            | AppError::WebhookIpAddressResolutionRequired { .. }
            | AppError::Validation(_)
    )
}

fn webhook_resolution_aad(bot_id: Uuid) -> String {
    format!("bot:{bot_id}:data-plane-webhook-resolution")
}

fn decrypt_webhook_resolution(
    state: &AppState,
    operation: &OperationRow,
) -> Result<WebhookResolutionSnapshot> {
    let (Some(data), Some(nonce)) = (
        operation.webhook_resolution_ciphertext.as_ref(),
        operation.webhook_resolution_nonce.as_ref(),
    ) else {
        if operation.webhook_resolution_ciphertext.is_some()
            || operation.webhook_resolution_nonce.is_some()
        {
            return Err(AppError::Internal);
        }
        return Ok(WebhookResolutionSnapshot::default());
    };
    let plaintext = state.crypto.decrypt(
        &Ciphertext {
            data: data.clone(),
            nonce: nonce.clone(),
        },
        webhook_resolution_aad(operation.bot_id).as_bytes(),
    )?;
    serde_json::from_slice(&plaintext).map_err(|_| AppError::Internal)
}

fn overlay_webhook_resolution(
    snapshot: &mut WebhookResolutionSnapshot,
    resolution: ExistingWebhookResolution<'_>,
) -> bool {
    let mut changed = false;
    if resolution.secret.is_some() || resolution.confirmed_no_secret {
        snapshot.secret = resolution.secret.map(str::to_owned);
        snapshot.confirmed_no_secret = resolution.confirmed_no_secret;
        changed = true;
    }
    if resolution.ip_address.is_some() || resolution.confirmed_no_ip_address {
        snapshot.ip_address = resolution.ip_address.map(str::to_owned);
        snapshot.confirmed_no_ip_address = resolution.confirmed_no_ip_address;
        changed = true;
    }
    changed
}

async fn persist_webhook_resolution(
    state: &AppState,
    connection: &mut sqlx::pool::PoolConnection<Postgres>,
    bot_id: Uuid,
    snapshot: &WebhookResolutionSnapshot,
) -> Result<()> {
    let serialized =
        zeroize::Zeroizing::new(serde_json::to_vec(snapshot).map_err(|_| AppError::Internal)?);
    let encrypted = state
        .crypto
        .encrypt(&serialized, webhook_resolution_aad(bot_id).as_bytes())?;
    let updated = sqlx::query(
        r#"UPDATE bot_data_plane_operations
              SET webhook_resolution_ciphertext = $2,
                  webhook_resolution_nonce = $3, updated_at = now()
            WHERE bot_id = $1
              AND phase IN ('route_withdrawn', 'webhook_resolution_required')"#,
    )
    .bind(bot_id)
    .bind(&encrypted.data)
    .bind(&encrypted.nonce)
    .execute(&mut **connection)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(AppError::Conflict(
            "Managed bot webhook recovery was superseded".into(),
        ));
    }
    Ok(())
}

async fn capture_rotation_webhook_after_withdrawal(
    state: &AppState,
    connection: &mut sqlx::pool::PoolConnection<Postgres>,
    operation: &OperationRow,
    source: SourcePool,
    source_token: &str,
    telegram_test_dc: bool,
    resolution: ExistingWebhookResolution<'_>,
) -> Result<()> {
    let mut stored_resolution = decrypt_webhook_resolution(state, operation)?;
    if overlay_webhook_resolution(&mut stored_resolution, resolution) {
        // Persist operator intent before waiting for long polling/uploads to
        // drain. A retry never asks the user to re-enter a secret merely
        // because admitted traffic took longer than this request.
        persist_webhook_resolution(state, connection, operation.bot_id, &stored_resolution).await?;
    }
    let effective_resolution = ExistingWebhookResolution {
        secret: stored_resolution.secret.as_deref(),
        confirmed_no_secret: stored_resolution.confirmed_no_secret,
        ip_address: stored_resolution.ip_address.as_deref(),
        confirmed_no_ip_address: stored_resolution.confirmed_no_ip_address,
    };
    let token_lookup_hash = state.crypto.bot_public_id(source_token, telegram_test_dc);
    gateway_route_drained(
        state,
        operation.withdraw_generation,
        &token_lookup_hash,
        source_token,
        telegram_test_dc,
    )
    .await?;

    let prepared = match prepare_managed_rotation_webhook_transfer(
        state,
        operation.bot_id,
        source,
        source_token,
        telegram_test_dc,
        effective_resolution,
    )
    .await
    {
        Ok(prepared) => prepared,
        Err(error) if managed_webhook_resolution_error(&error) => {
            let mut tx = connection.begin().await?;
            sqlx::query(
                r#"UPDATE bot_data_plane_operations
                      SET phase = 'webhook_resolution_required',
                          previous_webhook_ciphertext = NULL,
                          previous_webhook_nonce = NULL,
                          next_attempt_at = 'infinity'::timestamptz,
                          last_error_code = 'webhook_resolution_required',
                          updated_at = now()
                    WHERE bot_id = $1
                      AND phase IN ('route_withdrawn', 'webhook_resolution_required')"#,
            )
            .bind(operation.bot_id)
            .execute(&mut *tx)
            .await?;
            sqlx::query("UPDATE bots SET status = 'degraded', updated_at = now() WHERE id = $1")
                .bind(operation.bot_id)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            return Err(error);
        }
        Err(error) => return Err(error),
    };

    let mut tx = connection.begin().await?;
    let updated = sqlx::query(
        r#"UPDATE bot_data_plane_operations
              SET phase = 'webhook_captured',
                  previous_webhook_ciphertext = $2,
                  previous_webhook_nonce = $3,
                  webhook_resolution_ciphertext = NULL,
                  webhook_resolution_nonce = NULL,
                  next_attempt_at = now(), last_error_code = NULL,
                  updated_at = now()
            WHERE bot_id = $1
              AND phase IN ('route_withdrawn', 'webhook_resolution_required')"#,
    )
    .bind(operation.bot_id)
    .bind(
        prepared
            .encrypted
            .as_ref()
            .map(|value| value.data.as_slice()),
    )
    .bind(
        prepared
            .encrypted
            .as_ref()
            .map(|value| value.nonce.as_slice()),
    )
    .execute(&mut *tx)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(AppError::Conflict(
            "Managed bot webhook recovery was superseded".into(),
        ));
    }
    sqlx::query("UPDATE bots SET status = 'provisioning', updated_at = now() WHERE id = $1")
        .bind(operation.bot_id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

fn restore_webhook_payload(
    state: &AppState,
    operation: &OperationRow,
) -> Result<Map<String, Value>> {
    let snapshot = decrypt_webhook_snapshot(state, operation)?.ok_or(AppError::Internal)?;
    let mut payload = Map::new();
    payload.insert("url".into(), Value::String(snapshot.url));
    payload.insert("drop_pending_updates".into(), Value::Bool(false));
    payload.insert(
        "max_connections".into(),
        Value::from(snapshot.max_connections),
    );
    if let Some(allowed_updates) = snapshot.allowed_updates {
        payload.insert("allowed_updates".into(), allowed_updates);
    }
    if let Some(ip_address) = snapshot.ip_address {
        payload.insert("ip_address".into(), Value::String(ip_address));
    }
    if let Some(secret_token) = snapshot.secret_token {
        payload.insert("secret_token".into(), Value::String(secret_token));
    }
    Ok(payload)
}

fn decrypt_webhook_snapshot(
    state: &AppState,
    operation: &OperationRow,
) -> Result<Option<WebhookSnapshot>> {
    let (Some(data), Some(nonce)) = (
        &operation.previous_webhook_ciphertext,
        &operation.previous_webhook_nonce,
    ) else {
        if operation.previous_webhook_ciphertext.is_some()
            || operation.previous_webhook_nonce.is_some()
        {
            return Err(AppError::Internal);
        }
        return Ok(None);
    };
    let plaintext = state.crypto.decrypt(
        &Ciphertext {
            data: data.clone(),
            nonce: nonce.clone(),
        },
        format!("bot:{}:data-plane-webhook", operation.bot_id).as_bytes(),
    )?;
    serde_json::from_slice(&plaintext)
        .map(Some)
        .map_err(|_| AppError::Internal)
}

pub(crate) async fn gateway_acknowledged(state: &AppState, generation: i64) -> Result<()> {
    let base = state
        .config
        .data_plane_gateway_admin_url
        .as_deref()
        .ok_or_else(|| {
            AppError::Config("data-plane gateway admin origin is not configured".into())
        })?;
    let response = state
        .telegram
        .get(format!("{base}/health/ready"))
        .timeout(Duration::from_secs(3))
        .send()
        .await
        .map_err(|error| AppError::Upstream(error.without_url().to_string()))?;
    if response.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE {
        return Err(AppError::GatewayDrainPending);
    }
    if !response.status().is_success() {
        return Err(AppError::Upstream(
            "The data-plane gateway has not acknowledged route withdrawal".into(),
        ));
    }
    let body = response
        .json::<Value>()
        .await
        .map_err(|error| AppError::Upstream(error.without_url().to_string()))?;
    require_gateway_generation(&body, generation)
}

fn require_gateway_generation(body: &Value, generation: i64) -> Result<()> {
    let observed = body
        .get("snapshot_generation")
        .and_then(|value| {
            value
                .as_i64()
                .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
        })
        .ok_or_else(|| {
            AppError::Upstream(
                "The data-plane gateway returned an invalid route snapshot generation".into(),
            )
        })?;
    if observed < generation {
        return Err(AppError::GatewayDrainPending);
    }
    Ok(())
}

async fn gateway_route_drained(
    state: &AppState,
    minimum_generation: i64,
    token_lookup_hash: &str,
    bot_token: &str,
    telegram_test_dc: bool,
) -> Result<()> {
    if minimum_generation < 0 {
        return Err(AppError::Internal);
    }
    let base = state
        .config
        .data_plane_gateway_admin_url
        .as_deref()
        .ok_or_else(|| {
            AppError::Config("data-plane gateway admin origin is not configured".into())
        })?;
    let sync_token = state
        .config
        .data_plane_sync_token
        .as_deref()
        .ok_or_else(|| AppError::Config("data-plane sync token is not configured".into()))?;

    for attempt in 0..20 {
        let response = state
            .telegram
            .post(format!("{base}/internal/routes/drain"))
            .bearer_auth(sync_token)
            .json(&json!({
                "schema_version": 1,
                "token_lookup_hash": token_lookup_hash,
                "minimum_snapshot_generation": minimum_generation,
                "bot_token": bot_token,
                "telegram_test_dc": telegram_test_dc,
            }))
            .timeout(Duration::from_secs(3))
            .send()
            .await
            .map_err(|_| AppError::GatewayDrainPending)?;
        if !response.status().is_success() {
            if response.status().is_server_error()
                || response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS
            {
                return Err(AppError::GatewayDrainPending);
            }
            if matches!(
                response.status(),
                reqwest::StatusCode::BAD_REQUEST | reqwest::StatusCode::UNAUTHORIZED
            ) {
                return Err(AppError::Config(
                    "The data-plane gateway rejected the authenticated route-drain contract".into(),
                ));
            }
            return Err(AppError::Upstream(
                "The data-plane gateway did not accept the route-drain request".into(),
            ));
        }
        let body = response
            .json::<Value>()
            .await
            .map_err(|error| AppError::Upstream(error.without_url().to_string()))?;
        if parse_gateway_drain_proof(&body, minimum_generation as u64)? {
            return Ok(());
        }
        if attempt < 19 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    Err(AppError::GatewayDrainPending)
}

fn parse_gateway_drain_proof(body: &Value, minimum_generation: u64) -> Result<bool> {
    let invalid = || {
        AppError::Upstream("The data-plane gateway returned an invalid route-drain response".into())
    };
    let object = body.as_object().ok_or_else(invalid)?;
    if object.get("schema_version").and_then(Value::as_u64) != Some(1) {
        return Err(invalid());
    }
    let drained = object
        .get("drained")
        .and_then(Value::as_bool)
        .ok_or_else(invalid)?;
    let route_present = object
        .get("route_present")
        .and_then(Value::as_bool)
        .ok_or_else(invalid)?;
    let official_fenced = object
        .get("official_fenced")
        .and_then(Value::as_bool)
        .ok_or_else(invalid)?;
    let snapshot_generation = decimal_string_field(object, "snapshot_generation")?;
    let in_flight = decimal_string_field(object, "in_flight")?;
    let official = object
        .get("official_active_requests")
        .and_then(Value::as_object)
        .ok_or_else(invalid)?;
    let standard = optional_decimal_string_field(official, "standard")?;
    let local = optional_decimal_string_field(official, "local")?;
    let proof_complete = snapshot_generation >= minimum_generation
        && !route_present
        && in_flight == 0
        && official_fenced
        && standard == Some(0)
        && local == Some(0);
    if drained && !proof_complete {
        // A true result that contradicts any independently reported fence
        // condition is a broken private contract, not a reason to mutate
        // Telegram state.
        return Err(invalid());
    }
    Ok(drained && proof_complete)
}

fn decimal_string_field(object: &Map<String, Value>, field: &str) -> Result<u64> {
    object
        .get(field)
        .and_then(Value::as_str)
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| {
            AppError::Upstream(
                "The data-plane gateway returned an invalid route-drain response".into(),
            )
        })
}

fn optional_decimal_string_field(object: &Map<String, Value>, field: &str) -> Result<Option<u64>> {
    let value = object.get(field).ok_or_else(|| {
        AppError::Upstream("The data-plane gateway returned an invalid route-drain response".into())
    })?;
    if value.is_null() {
        return Ok(None);
    }
    value
        .as_str()
        .and_then(|value| value.parse::<u64>().ok())
        .map(Some)
        .ok_or_else(|| {
            AppError::Upstream(
                "The data-plane gateway returned an invalid route-drain response".into(),
            )
        })
}

async fn verify_identity(
    state: &AppState,
    base: &str,
    token: &str,
    expected_bot_id: i64,
    telegram_test_dc: bool,
) -> Result<()> {
    let response = raw_telegram_json_for_dc(
        &state.telegram,
        base,
        token,
        telegram_test_dc,
        "getMe",
        &json!({}),
    )
    .await?;
    telegram_ok(
        response.clone(),
        "Telegram did not initialize the target Bot API server",
    )?;
    if response.1.pointer("/result/id").and_then(Value::as_i64) != Some(expected_bot_id)
        || response
            .1
            .pointer("/result/is_bot")
            .and_then(Value::as_bool)
            != Some(true)
    {
        return Err(AppError::Conflict(
            "The Telegram bot identity changed during migration".into(),
        ));
    }
    Ok(())
}

fn telegram_ok(response: (reqwest::StatusCode, Value), fallback: &'static str) -> Result<Value> {
    if response.0.is_success() && response.1.get("ok").and_then(Value::as_bool) == Some(true) {
        return Ok(response.1);
    }
    Err(AppError::Upstream(
        response
            .1
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or(fallback)
            .to_owned(),
    ))
}

async fn advance_phase(
    connection: &mut sqlx::pool::PoolConnection<Postgres>,
    bot_id: Uuid,
    phase: &'static str,
) -> Result<()> {
    sqlx::query(
        "UPDATE bot_data_plane_operations SET phase = $2, last_error_code = NULL, updated_at = now() WHERE bot_id = $1",
    )
    .bind(bot_id)
    .bind(phase)
    .execute(&mut **connection)
    .await?;
    Ok(())
}

async fn load_bot(
    _state: &AppState,
    connection: &mut sqlx::pool::PoolConnection<Postgres>,
    bot_id: Uuid,
) -> Result<BotRecord> {
    sqlx::query_as::<_, BotRecord>(
        r#"SELECT id, user_id, telegram_bot_id, telegram_test_dc, username, display_name,
                  token_ciphertext, token_nonce, token_fingerprint, public_id,
                  ingress_secret_ciphertext, ingress_secret_nonce, status,
                  routing_mode, data_plane_pool, update_mode, last_update_at,
                  last_api_call_at, created_at
             FROM bots WHERE id = $1"#,
    )
    .bind(bot_id)
    .fetch_one(&mut **connection)
    .await
    .map_err(Into::into)
}

async fn rollback_before_logout(
    state: &AppState,
    connection: &mut sqlx::pool::PoolConnection<Postgres>,
    operation: &OperationRow,
) -> Result<()> {
    let source = SourcePool::parse(&operation.source_pool)?;
    let bot = load_bot(state, connection, operation.bot_id).await?;
    let token = decrypt_token(state, &bot)?;
    let token = std::str::from_utf8(&token).map_err(|_| AppError::Internal)?;
    let mut webhook_restored = true;
    let source_webhook_may_have_changed = operation.phase != "route_withdrawn";
    if source_webhook_may_have_changed && operation.previous_webhook_ciphertext.is_some() {
        let payload = restore_webhook_payload(state, operation)?;
        webhook_restored = raw_telegram_json_for_dc(
            &state.telegram,
            source_base(state, source)?,
            token,
            bot.telegram_test_dc,
            "setWebhook",
            &Value::Object(payload),
        )
        .await
        .ok()
        .is_some_and(|(status, body)| {
            status.is_success() && body.get("ok").and_then(Value::as_bool) == Some(true)
        });
    }
    if !webhook_restored {
        return Err(AppError::Upstream(
            "Telegram did not confirm restoration of the source webhook".into(),
        ));
    }
    // Clean-state operations only provision a brand-new connected/managed bot.
    // Once its pre-existing external webhook is restored, deleting the
    // provisional row returns the request to its exact starting state.
    sqlx::query("DELETE FROM bots WHERE id = $1")
        .bind(operation.bot_id)
        .execute(&mut **connection)
        .await?;
    Ok(())
}

async fn rollback_rotation_before_close(
    state: &AppState,
    connection: &mut sqlx::pool::PoolConnection<Postgres>,
    operation: &OperationRow,
) -> Result<()> {
    let source = source_data_plane_pool(SourcePool::parse(&operation.source_pool)?)?;
    let source_token = decrypt_rotation_token(state, operation, RotationToken::Source)?;
    let source_token = std::str::from_utf8(&source_token).map_err(|_| AppError::Internal)?;
    start_rotation_rollback(state, connection, operation, source, source_token).await?;
    let operation = load_operation(connection, operation.bot_id)
        .await?
        .ok_or(AppError::Internal)?;
    finish_rotation_rollback(state, connection, &operation).await
}

async fn start_rotation_rollback(
    state: &AppState,
    connection: &mut sqlx::pool::PoolConnection<Postgres>,
    operation: &OperationRow,
    source: DataPlanePool,
    source_token: &str,
) -> Result<()> {
    let telegram_test_dc = load_bot(state, connection, operation.bot_id)
        .await?
        .telegram_test_dc;
    if operation.phase != "route_withdrawn" && operation.previous_webhook_ciphertext.is_some() {
        let payload = restore_webhook_payload(state, operation)?;
        telegram_ok(
            raw_telegram_json_for_dc(
                &state.telegram,
                direct_pool_base(state, source)?,
                source_token,
                telegram_test_dc,
                "setWebhook",
                &Value::Object(payload),
            )
            .await?,
            "Telegram did not restore the old managed-bot webhook",
        )?;
    }
    let mut tx = connection.begin().await?;
    let updated = sqlx::query(
        r#"UPDATE bots
              SET data_plane_pool = $2, data_plane_target_pool = NULL,
                  routing_mode = $3, status = 'provisioning', updated_at = now()
            WHERE id = $1 AND data_plane_pool IS NULL"#,
    )
    .bind(operation.bot_id)
    .bind(source.as_str())
    .bind(source.routing_mode())
    .execute(&mut *tx)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(AppError::Conflict(
            "Managed bot route changed while token rotation rolled back".into(),
        ));
    }
    let publication_generation = sqlx::query_scalar::<_, i64>(
        "SELECT generation FROM data_plane_route_state WHERE singleton = TRUE",
    )
    .fetch_one(&mut *tx)
    .await?;
    sqlx::query(
        r#"UPDATE bot_data_plane_operations
              SET phase = 'rollback_published', publication_generation = $2,
                  last_error_code = 'rotation_rolled_back', updated_at = now()
            WHERE bot_id = $1"#,
    )
    .bind(operation.bot_id)
    .bind(publication_generation)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

async fn finish_rotation_rollback(
    state: &AppState,
    connection: &mut sqlx::pool::PoolConnection<Postgres>,
    operation: &OperationRow,
) -> Result<()> {
    gateway_acknowledged(state, operation.publication_generation).await?;
    let mut tx = connection.begin().await?;
    sqlx::query("UPDATE bots SET status = 'healthy', updated_at = now() WHERE id = $1")
        .bind(operation.bot_id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM bot_data_plane_operations WHERE bot_id = $1")
        .bind(operation.bot_id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

fn manual_recovery_error() -> AppError {
    AppError::Conflict(
        "Telegram did not confirm whether the Bot API session handoff completed. Phenogram stopped routing and disabled automatic retries to avoid running two clients for one bot. An operator must verify the source session before this operation can continue."
            .into(),
    )
}

async fn mark_manual_recovery(
    connection: &mut sqlx::pool::PoolConnection<Postgres>,
    operation: &OperationRow,
    error_code: &'static str,
) -> Result<()> {
    let source = SourcePool::parse(&operation.source_pool)?;
    let mut tx = connection.begin().await?;
    sqlx::query(
        r#"UPDATE bot_data_plane_operations
              SET phase = 'manual_recovery', last_error_code = $2,
                  next_attempt_at = 'infinity'::timestamptz, updated_at = now()
            WHERE bot_id = $1 AND phase IN ('webhook_deleted', 'logout_started', 'close_started')"#,
    )
    .bind(operation.bot_id)
    .bind(error_code)
    .execute(&mut *tx)
    .await?;
    sqlx::query("UPDATE bots SET status = 'degraded', updated_at = now() WHERE id = $1")
        .bind(operation.bot_id)
        .execute(&mut *tx)
        .await?;
    sqlx::query(
        r#"INSERT INTO audit_log (user_id, bot_id, action, metadata, expires_at)
           SELECT bots.user_id, bots.id, 'bot.data_plane_manual_recovery_required', $2,
                  now() + make_interval(days => bot_effective_retention_days(bots.id))
             FROM bots WHERE bots.id = $1"#,
    )
    .bind(operation.bot_id)
    .bind(json!({
        "operation": operation.operation,
        "source_pool": source.as_str(),
        "failed_phase": if error_code.starts_with("close_") { "close_started" } else { "logout_started" },
        "reason": error_code,
    }))
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    tracing::error!(
        bot_id = %operation.bot_id,
        operation = operation.operation,
        source_pool = source.as_str(),
        error_code,
        "bot lifecycle requires manual recovery"
    );
    Ok(())
}

async fn record_failure(
    connection: &mut sqlx::pool::PoolConnection<Postgres>,
    operation: &OperationRow,
    error: &AppError,
) {
    let error_code = lifecycle_error_code(error);
    let delay = 2_i64.pow(operation.attempt.clamp(1, 10) as u32).min(900);
    if let Err(database_error) = sqlx::query(
        r#"UPDATE bot_data_plane_operations
              SET last_error_code = $2,
                  next_attempt_at = now() + make_interval(secs => $3),
                  updated_at = now()
            WHERE bot_id = $1 AND phase <> 'manual_recovery'"#,
    )
    .bind(operation.bot_id)
    .bind(error_code)
    .bind(delay as f64)
    .execute(&mut **connection)
    .await
    {
        tracing::error!(bot_id = %operation.bot_id, error = ?database_error, "could not checkpoint lifecycle failure");
    }
    // A durable operation with a scheduled retry is still provisioning. Keep
    // `degraded` for phases that explicitly require human/terminal recovery;
    // in particular, do not overwrite a concurrently checkpointed manual
    // recovery phase with a transient status.
    let _ = sqlx::query(
        r#"UPDATE bots
              SET status = 'provisioning', updated_at = now()
            WHERE id = $1
              AND EXISTS (
                    SELECT 1
                      FROM bot_data_plane_operations
                     WHERE bot_id = $1
                       AND phase NOT IN ('manual_recovery', 'webhook_resolution_required')
              )"#,
    )
    .bind(operation.bot_id)
    .execute(&mut **connection)
    .await;
}

fn lifecycle_error_code(error: &AppError) -> &'static str {
    match error {
        AppError::Database(_) => "database_unavailable",
        AppError::GatewayDrainPending => "gateway_draining",
        AppError::Upstream(_) | AppError::TelegramRejected(_) => "telegram_or_gateway_unavailable",
        AppError::Validation(_) => "unsupported_webhook",
        AppError::Conflict(_) => "lifecycle_conflict",
        AppError::Crypto(_) => "credential_unavailable",
        AppError::Config(_) | AppError::Internal => "internal_error",
        AppError::Unauthorized | AppError::Forbidden | AppError::NotFound => "bot_unavailable",
        AppError::PlanLimit(_)
        | AppError::RateLimited
        | AppError::WebhookSecretRequired { .. }
        | AppError::WebhookIpAddressResolutionRequired { .. } => "operation_not_allowed",
    }
}

pub async fn run_worker(state: AppState) {
    if !state.config.data_plane_enabled {
        return;
    }
    loop {
        match sqlx::query_scalar::<_, Uuid>(
            r#"SELECT bot_id
                 FROM bot_data_plane_operations
                WHERE next_attempt_at <= now()
                  AND phase <> 'manual_recovery'
                ORDER BY next_attempt_at, updated_at, bot_id
                LIMIT 1"#,
        )
        .fetch_optional(&state.db)
        .await
        {
            Ok(Some(bot_id)) => match run_bot_operation(&state, bot_id).await {
                Ok(LifecycleOutcome::Busy) => tokio::time::sleep(WORKER_IDLE).await,
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(%bot_id, error_code = lifecycle_error_code(&error), "bot lifecycle did not complete");
                }
            },
            Ok(None) => tokio::time::sleep(WORKER_IDLE).await,
            Err(error) => {
                tracing::error!(error = ?error, "bot lifecycle worker could not load work");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::{
        CloseDisposition, DataPlanePool, LifecycleOperation, SourcePool, classify_close_response,
        parse_gateway_drain_proof, require_gateway_generation, resolve_connect_webhook_ip_address,
        resolve_connect_webhook_secret, validate_migration_path,
    };
    use crate::{error::AppError, telegram::ExistingWebhook};

    #[test]
    fn pool_mapping_is_explicit_and_never_membership_derived() {
        assert_eq!(
            DataPlanePool::from_routing_mode("cloud").unwrap(),
            DataPlanePool::Standard
        );
        assert_eq!(
            DataPlanePool::from_routing_mode("local").unwrap(),
            DataPlanePool::Local
        );
        assert!(DataPlanePool::from_routing_mode("premium").is_err());
    }

    #[test]
    fn mvp_allows_cloud_login_but_blocks_local_server_moves() {
        assert!(
            validate_migration_path(
                SourcePool::Cloud,
                DataPlanePool::Standard,
                LifecycleOperation::Connect,
            )
            .is_ok()
        );
        assert!(
            validate_migration_path(
                SourcePool::Cloud,
                DataPlanePool::Local,
                LifecycleOperation::ManagedSync,
            )
            .is_ok()
        );
        assert!(
            validate_migration_path(
                SourcePool::Standard,
                DataPlanePool::Local,
                LifecycleOperation::ManagedSync,
            )
            .is_err()
        );
        assert!(
            validate_migration_path(
                SourcePool::Standard,
                DataPlanePool::Standard,
                LifecycleOperation::ManagedRotate,
            )
            .is_ok()
        );
    }

    #[test]
    fn managed_rotation_retries_only_a_confirmed_still_open_close() {
        assert_eq!(
            classify_close_response(&(
                reqwest::StatusCode::OK,
                json!({"ok": true, "result": true}),
            )),
            CloseDisposition::Confirmed
        );
        assert_eq!(
            classify_close_response(&(
                reqwest::StatusCode::TOO_MANY_REQUESTS,
                json!({"ok": false, "error_code": 429}),
            )),
            CloseDisposition::RetryableRejected
        );
        assert_eq!(
            classify_close_response(&(
                reqwest::StatusCode::INTERNAL_SERVER_ERROR,
                json!({"ok": false, "error_code": 500}),
            )),
            CloseDisposition::Ambiguous
        );
        assert_eq!(
            classify_close_response(&(
                reqwest::StatusCode::BAD_REQUEST,
                json!({"ok": false, "error_code": 400, "description": "already closed"}),
            )),
            CloseDisposition::Ambiguous
        );
    }

    #[test]
    fn stale_gateway_generation_is_pending_not_degraded_failure() {
        let error = require_gateway_generation(&json!({"snapshot_generation": "41"}), 42)
            .expect_err("route publication must wait for the gateway snapshot");
        assert!(matches!(error, AppError::GatewayDrainPending));
        assert!(require_gateway_generation(&json!({"snapshot_generation": "42"}), 42).is_ok());
        assert!(require_gateway_generation(&json!({"snapshot_generation": 43}), 42).is_ok());
    }

    #[test]
    fn malformed_gateway_generation_is_an_upstream_contract_error() {
        let error = require_gateway_generation(&json!({"snapshot_generation": "not-a-number"}), 42)
            .expect_err("malformed gateway state must not be treated as propagation lag");
        assert!(matches!(error, AppError::Upstream(_)));
    }

    #[test]
    fn managed_rotation_requires_explicit_webhook_secret_resolution() {
        let webhook = ExistingWebhook {
            url: "https://receiver.example/secret-bearing-path".into(),
            allowed_updates: Some(json!(["message"])),
            max_connections: 40,
            reported_ip_address: Some("203.0.113.9".into()),
        };
        let error = resolve_connect_webhook_secret(Some(&webhook), None, false)
            .expect_err("an active native webhook must stop rotation before mutation");
        assert!(matches!(
            error,
            AppError::WebhookSecretRequired { destination_host }
                if destination_host == "receiver.example"
        ));
    }

    #[test]
    fn connect_requires_an_explicit_existing_webhook_secret_resolution() {
        let webhook = ExistingWebhook {
            url: "https://receiver.example/private/path?key=not-public".into(),
            allowed_updates: None,
            max_connections: 40,
            reported_ip_address: None,
        };
        let error = resolve_connect_webhook_secret(Some(&webhook), None, false)
            .expect_err("unknown webhook secret must stop before mutation");
        assert!(matches!(
            error,
            AppError::WebhookSecretRequired { destination_host }
                if destination_host == "receiver.example"
        ));
        assert_eq!(
            resolve_connect_webhook_secret(Some(&webhook), Some("Current_secret-1"), false)
                .unwrap(),
            Some("Current_secret-1".into())
        );
        assert_eq!(
            resolve_connect_webhook_secret(Some(&webhook), None, true).unwrap(),
            None
        );
        assert!(resolve_connect_webhook_secret(Some(&webhook), Some("bad secret"), false).is_err());
    }

    #[test]
    fn reported_webhook_ip_requires_explicit_pin_or_dns_intent() {
        let webhook = ExistingWebhook {
            url: "https://receiver.example/hook".into(),
            allowed_updates: None,
            max_connections: 40,
            reported_ip_address: Some("203.0.113.9".into()),
        };
        let error = resolve_connect_webhook_ip_address(Some(&webhook), None, false)
            .expect_err("a reported address is not proof of a fixed-IP pin");
        assert!(matches!(
            error,
            AppError::WebhookIpAddressResolutionRequired {
                destination_host,
                reported_ip_address,
            } if destination_host == "receiver.example" && reported_ip_address == "203.0.113.9"
        ));
        assert_eq!(
            resolve_connect_webhook_ip_address(Some(&webhook), Some("203.0.113.9"), false,)
                .unwrap(),
            Some("203.0.113.9".into())
        );
        assert_eq!(
            resolve_connect_webhook_ip_address(Some(&webhook), None, true).unwrap(),
            None
        );
        assert!(
            resolve_connect_webhook_ip_address(Some(&webhook), Some("203.0.113.10"), false,)
                .is_err()
        );
        assert!(
            resolve_connect_webhook_ip_address(Some(&webhook), Some("203.000.113.9"), false,)
                .is_err()
        );
    }

    #[test]
    fn drain_requires_fresh_zero_request_proof_from_both_official_pools() {
        let proof = json!({
            "schema_version": 1,
            "drained": true,
            "snapshot_generation": "42",
            "route_present": false,
            "in_flight": "0",
            "official_fenced": true,
            "official_active_requests": {
                "standard": "0",
                "local": "0"
            }
        });
        assert!(parse_gateway_drain_proof(&proof, 42).unwrap());

        for path in ["standard", "local"] {
            let mut unavailable = proof.clone();
            unavailable["drained"] = false.into();
            unavailable["official_fenced"] = false.into();
            unavailable["official_active_requests"][path] = Value::Null;
            assert!(!parse_gateway_drain_proof(&unavailable, 42).unwrap());

            let mut active = proof.clone();
            active["drained"] = false.into();
            active["official_active_requests"][path] = "1".into();
            assert!(!parse_gateway_drain_proof(&active, 42).unwrap());
        }
    }

    #[test]
    fn drain_never_trusts_gateway_local_state_without_official_fence_proof() {
        let gateway_only = json!({
            "schema_version": 1,
            "drained": false,
            "snapshot_generation": "42",
            "route_present": false,
            "in_flight": "0",
            "official_fenced": false,
            "official_active_requests": {
                "standard": null,
                "local": null
            }
        });
        assert!(!parse_gateway_drain_proof(&gateway_only, 42).unwrap());

        let missing_official_proof = json!({
            "schema_version": 1,
            "drained": false,
            "snapshot_generation": "42",
            "route_present": false,
            "in_flight": "0"
        });
        assert!(parse_gateway_drain_proof(&missing_official_proof, 42).is_err());
    }

    #[test]
    fn contradictory_or_malformed_drain_success_is_rejected() {
        let mut proof = json!({
            "schema_version": 1,
            "drained": true,
            "snapshot_generation": "42",
            "route_present": false,
            "in_flight": "0",
            "official_fenced": true,
            "official_active_requests": {
                "standard": "0",
                "local": "0"
            }
        });
        proof["official_active_requests"]["standard"] = "1".into();
        assert!(parse_gateway_drain_proof(&proof, 42).is_err());
        proof["official_active_requests"]["standard"] = "0".into();
        proof["snapshot_generation"] = 42.into();
        assert!(parse_gateway_drain_proof(&proof, 42).is_err());
    }
}
