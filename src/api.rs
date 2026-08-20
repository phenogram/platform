use axum::{
    Json,
    body::{Body, to_bytes},
    extract::{FromRequest, Multipart, Path, Query, Request, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode, header},
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
};
use chrono::{DateTime, Utc};
use futures_util::Stream;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sqlx::FromRow;
use std::{
    collections::{HashMap, HashSet},
    convert::Infallible,
    io,
    pin::Pin,
    time::Duration,
};
use uuid::Uuid;
use zeroize::Zeroizing;

use crate::{
    auth::{AuthUser, active_membership, membership},
    crypto::Crypto,
    error::{AppError, Result},
    lifecycle::{
        DataPlanePool, ExistingWebhookResolution, LifecycleOperation, LifecycleOutcome, SourcePool,
        create_operation, has_operation, prepare_connect_webhook_transfer,
        resolve_connect_webhook_secret, run_bot_operation,
    },
    models::{ActivitySummary, BotRecord, BotSummary, ConversationSummary, UpdateSummary},
    state::AppState,
    telegram::{
        ALL_UPDATE_TYPES, ExistingWebhookPolicy, OutboundMessageRecord, StreamQuery,
        console_event_stream, decrypt_token, existing_webhook, install_managed_webhook,
        prepare_file_link_path, raw_telegram_json_for_dc, record_outbound_message,
        recover_managed_bot_rotation, search_pattern, stream_authenticated_bot_file,
        telegram_json_envelope_for_bot, telegram_json_for_bot, telegram_raw_for_bot,
    },
};

pub async fn health(State(state): State<AppState>) -> Response {
    let database = sqlx::query_scalar::<_, i32>("SELECT 1")
        .fetch_one(&state.db)
        .await
        .is_ok();
    let status = if database {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        Json(health_payload(
            database,
            state.config.deployment_revision.as_str(),
        )),
    )
        .into_response()
}

fn health_payload(database: bool, deployment_revision: &str) -> Value {
    json!({
        "status": if database { "ok" } else { "degraded" },
        "database": database,
        "version": env!("CARGO_PKG_VERSION"),
        "deployment_revision": deployment_revision,
    })
}

pub async fn plans(State(state): State<AppState>) -> Result<Json<Value>> {
    let rows = sqlx::query_as::<_, PlanRow>(
        "SELECT id, name, bot_limit, retention_days, local_bot_api, monthly_price_cents FROM plan_definitions ORDER BY monthly_price_cents NULLS LAST",
    )
    .fetch_all(&state.db)
    .await?;
    Ok(Json(json!({"plans": rows})))
}

#[derive(Debug, Serialize, FromRow)]
struct PlanRow {
    id: String,
    name: String,
    bot_limit: i32,
    retention_days: i32,
    local_bot_api: bool,
    monthly_price_cents: Option<i32>,
}

#[derive(Deserialize)]
pub struct ConnectBotRequest {
    token: String,
    #[serde(default)]
    pool: Option<String>,
    #[serde(default)]
    test_dc: bool,
    #[serde(default)]
    existing_webhook_secret: Option<String>,
    #[serde(default)]
    existing_webhook_has_no_secret: bool,
    #[serde(default)]
    existing_webhook_ip_address: Option<String>,
    #[serde(default)]
    existing_webhook_has_no_ip_address: bool,
}

#[derive(Debug, Serialize)]
pub struct ConnectBotResponse {
    bot: BotSummary,
    warnings: Vec<String>,
    webhook_ip_address_preserved: bool,
}

pub async fn connect_bot(
    State(state): State<AppState>,
    user: AuthUser,
    Json(input): Json<ConnectBotRequest>,
) -> Result<(StatusCode, Json<ConnectBotResponse>)> {
    let token = input.token.trim().to_owned();
    let telegram_test_dc = input.test_dc;
    validate_bot_token(&token)?;
    let membership = active_membership(&state, user.id).await?;
    let target_pool = connect_target_pool(input.pool.as_deref())?;
    if target_pool == DataPlanePool::Local && !membership.local_bot_api {
        return Err(AppError::PlanLimit(
            "Phenogram Local requires a plan with Local Bot API access".into(),
        ));
    }
    if target_pool == DataPlanePool::Local && !state.config.data_plane_enabled {
        return Err(AppError::Conflict(
            "Phenogram Local is not enabled on this deployment".into(),
        ));
    }
    let bot_count = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM bots WHERE user_id = $1 AND bot_kind = 'connected'",
    )
    .bind(user.id)
    .fetch_one(&state.db)
    .await?;
    if bot_count >= membership.bot_limit as i64 {
        return Err(AppError::PlanLimit(format!(
            "The {} plan supports {} bot{}",
            membership.plan_name,
            membership.bot_limit,
            if membership.bot_limit == 1 { "" } else { "s" }
        )));
    }

    let (telegram_status, me) = raw_telegram_json_for_dc(
        &state.telegram,
        &state.config.telegram_cloud_api_url,
        &token,
        telegram_test_dc,
        "getMe",
        &json!({}),
    )
    .await?;
    validate_get_me_response(telegram_status, &me)?;
    let identity = me
        .get("result")
        .ok_or_else(|| AppError::Upstream("Telegram returned no bot identity".into()))?;
    let telegram_bot_id = identity
        .get("id")
        .and_then(Value::as_i64)
        .ok_or_else(|| AppError::Upstream("Telegram returned an invalid bot identity".into()))?;
    let username = identity
        .get("username")
        .and_then(Value::as_str)
        .unwrap_or("unnamed_bot")
        .to_owned();
    let display_name = identity
        .get("first_name")
        .and_then(Value::as_str)
        .unwrap_or(&username)
        .to_owned();

    if let Some(existing_owner) = sqlx::query_scalar::<_, Uuid>(
        "SELECT user_id FROM bots WHERE telegram_bot_id = $1 AND telegram_test_dc = $2",
    )
    .bind(telegram_bot_id)
    .bind(telegram_test_dc)
    .fetch_optional(&state.db)
    .await?
    {
        return Err(if existing_owner == user.id {
            AppError::Conflict("This bot is already connected to your account".into())
        } else {
            AppError::Conflict("This bot is already connected to another Phenogram account".into())
        });
    }

    let bot_id = Uuid::new_v4();
    let prepared_webhook = if state.config.data_plane_enabled {
        Some(
            prepare_connect_webhook_transfer(
                &state,
                bot_id,
                SourcePool::Cloud,
                &token,
                telegram_test_dc,
                ExistingWebhookResolution {
                    secret: input.existing_webhook_secret.as_deref(),
                    confirmed_no_secret: input.existing_webhook_has_no_secret,
                    ip_address: input.existing_webhook_ip_address.as_deref(),
                    confirmed_no_ip_address: input.existing_webhook_has_no_ip_address,
                },
            )
            .await?,
        )
    } else {
        None
    };
    let previous_webhook = if state.config.data_plane_enabled {
        None
    } else {
        let (_, webhook_info) = raw_telegram_json_for_dc(
            &state.telegram,
            &state.config.telegram_cloud_api_url,
            &token,
            telegram_test_dc,
            "getWebhookInfo",
            &json!({}),
        )
        .await?;
        existing_webhook(
            &webhook_info,
            &state.config.api_base_url,
            ExistingWebhookPolicy::Cloud {
                allow_insecure_development: state.config.app_env != "production",
            },
        )?
    };
    let previous_webhook_secret = if state.config.data_plane_enabled {
        None
    } else {
        resolve_connect_webhook_secret(
            previous_webhook.as_ref(),
            input.existing_webhook_secret.as_deref(),
            input.existing_webhook_has_no_secret,
        )?
    };
    let public_id = state.crypto.bot_public_id(&token, telegram_test_dc);
    let token_fingerprint = Crypto::token_fingerprint(&token, telegram_test_dc);
    let token_encrypted = state
        .crypto
        .encrypt(token.as_bytes(), format!("bot:{bot_id}:token").as_bytes())?;
    let ingress_secret = Crypto::random_token(32)?;
    let ingress_encrypted = state.crypto.encrypt(
        ingress_secret.as_bytes(),
        format!("bot:{bot_id}:ingress-secret").as_bytes(),
    )?;
    let downstream_secret_encrypted = previous_webhook_secret
        .as_ref()
        .map(|secret| {
            state.crypto.encrypt(
                secret.as_bytes(),
                format!("bot:{bot_id}:downstream-secret").as_bytes(),
            )
        })
        .transpose()?;

    if state.config.data_plane_enabled {
        let webhook_ip_address_preserved = prepared_webhook
            .as_ref()
            .is_some_and(|webhook| webhook.reported_ip_address_preserved());
        let mut tx = state.db.begin().await?;
        let insert = sqlx::query(
            r#"INSERT INTO bots
                   (id, user_id, telegram_bot_id, telegram_test_dc, username, display_name,
                    token_ciphertext, token_nonce, token_fingerprint, public_id,
                    token_lookup_hash, ingress_secret_ciphertext,
                    ingress_secret_nonce, data_plane_target_pool)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$10,$11,$12,$13)"#,
        )
        .bind(bot_id)
        .bind(user.id)
        .bind(telegram_bot_id)
        .bind(telegram_test_dc)
        .bind(&username)
        .bind(&display_name)
        .bind(&token_encrypted.data)
        .bind(&token_encrypted.nonce)
        .bind(&token_fingerprint)
        .bind(&public_id)
        .bind(&ingress_encrypted.data)
        .bind(&ingress_encrypted.nonce)
        .bind(target_pool.as_str())
        .execute(&mut *tx)
        .await;
        if let Err(error) = insert {
            if error.to_string().contains("bot plan limit reached") {
                return Err(AppError::PlanLimit("Your bot limit was reached".into()));
            }
            return Err(error.into());
        }
        sqlx::query("INSERT INTO bot_update_state (bot_id) VALUES ($1)")
            .bind(bot_id)
            .execute(&mut *tx)
            .await?;
        create_operation(
            &mut tx,
            bot_id,
            LifecycleOperation::Connect,
            SourcePool::Cloud,
            target_pool,
            prepared_webhook.as_ref().ok_or(AppError::Internal)?,
        )
        .await?;
        sqlx::query(
            r#"INSERT INTO audit_log (user_id, bot_id, action, metadata, expires_at)
               SELECT $1, bots.id, 'bot.connected', $3,
                      now() + make_interval(days => bot_effective_retention_days(bots.id))
                 FROM bots WHERE bots.id = $2"#,
        )
        .bind(user.id)
        .bind(bot_id)
        .bind(json!({
            "telegram_bot_id": telegram_bot_id,
            "telegram_test_dc": telegram_test_dc,
            "username": username,
            "data_plane_target": target_pool.as_str()
        }))
        .execute(&mut *tx)
        .await?;
        let reattached = sqlx::query(
            r#"UPDATE bots
                  SET manager_bot_id = $1, updated_at = now()
                WHERE user_id = $2
                  AND bot_kind = 'managed'
                  AND manager_bot_id IS NULL
                  AND manager_telegram_bot_id = $3
                  AND telegram_test_dc = $4"#,
        )
        .bind(bot_id)
        .bind(user.id)
        .bind(telegram_bot_id)
        .bind(telegram_test_dc)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        tx.commit().await?;
        if reattached == 0 {
            sqlx::query("SELECT refresh_user_bot_retention($1)")
                .bind(user.id)
                .execute(&state.db)
                .await?;
        }

        let mut warnings = Vec::new();
        if webhook_ip_address_preserved {
            warnings.push(
                "You chose fixed-IP continuity for the existing webhook. Phenogram preserved that exact IPv4 address on the official server."
                    .into(),
            );
        }
        match run_bot_operation(&state, bot_id).await {
            Ok(LifecycleOutcome::Active {
                webhook_transferred,
                secret_reentry_required,
            }) => {
                if webhook_transferred && secret_reentry_required {
                    warnings.push(
                        "Webhook transferred automatically. Telegram does not expose an existing secret_token, so configure that secret again with setWebhook through Phenogram if the receiver validates it."
                            .into(),
                    );
                }
            }
            Ok(LifecycleOutcome::Busy) => warnings.push(
                "The bot is saved and its Bot API migration is still running. Phenogram will finish it automatically."
                    .into(),
            ),
            Ok(LifecycleOutcome::RolledBack) => return Err(AppError::Internal),
            Err(error) => {
                let provisional_bot_still_exists = sqlx::query_scalar::<_, bool>(
                    "SELECT EXISTS(SELECT 1 FROM bots WHERE id = $1)",
                )
                .bind(bot_id)
                .fetch_one(&state.db)
                .await?;
                if !provisional_bot_still_exists {
                    return Err(error);
                }
                tracing::warn!(%bot_id, error = ?error, "initial data-plane provisioning will retry");
                let manual_recovery = sqlx::query_scalar::<_, bool>(
                    "SELECT EXISTS(SELECT 1 FROM bot_data_plane_operations WHERE bot_id = $1 AND phase = 'manual_recovery')",
                )
                .bind(bot_id)
                .fetch_one(&state.db)
                .await?;
                warnings.push(if manual_recovery {
                    "Telegram did not confirm whether logOut completed. Phenogram stopped routing and automatic retries; this bot requires operator recovery before it can be used."
                        .into()
                } else {
                    "The bot is saved, but its Bot API migration is not complete yet. Phenogram will retry automatically."
                        .into()
                });
            }
        }
        let bot = get_bot_summary(&state, user.id, bot_id).await?;
        return Ok((
            StatusCode::CREATED,
            Json(ConnectBotResponse {
                bot,
                warnings,
                webhook_ip_address_preserved,
            }),
        ));
    }

    let mut tx = state.db.begin().await?;
    let insert = sqlx::query(
        r#"INSERT INTO bots
               (id, user_id, telegram_bot_id, telegram_test_dc, username, display_name,
                token_ciphertext, token_nonce,
                token_fingerprint, public_id, token_lookup_hash,
                ingress_secret_ciphertext, ingress_secret_nonce)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$10,$11,$12)"#,
    )
    .bind(bot_id)
    .bind(user.id)
    .bind(telegram_bot_id)
    .bind(telegram_test_dc)
    .bind(&username)
    .bind(&display_name)
    .bind(&token_encrypted.data)
    .bind(&token_encrypted.nonce)
    .bind(&token_fingerprint)
    .bind(&public_id)
    .bind(&ingress_encrypted.data)
    .bind(&ingress_encrypted.nonce)
    .execute(&mut *tx)
    .await;
    if let Err(error) = insert {
        if error.to_string().contains("bot plan limit reached") {
            return Err(AppError::PlanLimit("Your bot limit was reached".into()));
        }
        return Err(error.into());
    }
    sqlx::query(
        r#"INSERT INTO bot_update_state
               (bot_id, allowed_updates, downstream_webhook_url,
                downstream_secret_ciphertext, downstream_secret_nonce,
                max_connections)
           VALUES ($1, $2, $3, $4, $5, $6)"#,
    )
    .bind(bot_id)
    .bind(
        previous_webhook
            .as_ref()
            .and_then(|webhook| webhook.allowed_updates.as_ref()),
    )
    .bind(
        previous_webhook
            .as_ref()
            .map(|webhook| webhook.url.as_str()),
    )
    .bind(
        downstream_secret_encrypted
            .as_ref()
            .map(|secret| secret.data.as_slice()),
    )
    .bind(
        downstream_secret_encrypted
            .as_ref()
            .map(|secret| secret.nonce.as_slice()),
    )
    .bind(
        previous_webhook
            .as_ref()
            .map_or(40, |webhook| webhook.max_connections),
    )
    .execute(&mut *tx)
    .await?;
    if previous_webhook.is_some() {
        sqlx::query("UPDATE bots SET update_mode = 'webhook' WHERE id = $1")
            .bind(bot_id)
            .execute(&mut *tx)
            .await?;
    }
    sqlx::query(
        r#"INSERT INTO audit_log (user_id, bot_id, action, metadata, expires_at)
           SELECT $1, bots.id, 'bot.connected', $3,
                  now() + make_interval(days => bot_effective_retention_days(bots.id))
             FROM bots
            WHERE bots.id = $2"#,
    )
    .bind(user.id)
    .bind(bot_id)
    .bind(json!({"telegram_bot_id": telegram_bot_id, "telegram_test_dc": telegram_test_dc, "username": username, "migrated_webhook": previous_webhook.is_some()}))
    .execute(&mut *tx)
    .await?;

    // A manager can be removed and later reconnected. Keep the stable Telegram
    // manager ID on children so the hierarchy heals without user action.
    let reattached = sqlx::query(
        r#"UPDATE bots
              SET manager_bot_id = $1, updated_at = now()
            WHERE user_id = $2
              AND bot_kind = 'managed'
              AND manager_bot_id IS NULL
              AND manager_telegram_bot_id = $3
              AND telegram_test_dc = $4"#,
    )
    .bind(bot_id)
    .bind(user.id)
    .bind(telegram_bot_id)
    .bind(telegram_test_dc)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    tx.commit().await?;

    // Reattachment itself invokes the hierarchy trigger. Without orphans, the
    // new direct bot can still consume a paid coverage slot from a managed bot.
    if reattached == 0 {
        sqlx::query("SELECT refresh_user_bot_retention($1)")
            .bind(user.id)
            .execute(&state.db)
            .await?;
    }

    let webhook_url = format!(
        "{}/telegram/webhook/{}",
        state.config.api_base_url, public_id
    );
    let webhook_result = raw_telegram_json_for_dc(
        &state.telegram,
        &state.config.telegram_cloud_api_url,
        &token,
        telegram_test_dc,
        "setWebhook",
        &json!({
            "url": webhook_url,
            "secret_token": ingress_secret,
            "allowed_updates": ALL_UPDATE_TYPES,
            "drop_pending_updates": false
        }),
    )
    .await;
    let provisioned = webhook_result
        .as_ref()
        .is_ok_and(|(_, body)| body.get("ok").and_then(Value::as_bool) == Some(true));
    let status = if provisioned { "healthy" } else { "degraded" };
    sqlx::query("UPDATE bots SET status = $2, updated_at = now() WHERE id = $1")
        .bind(bot_id)
        .bind(status)
        .execute(&state.db)
        .await?;
    let mut warnings: Vec<String> = Vec::new();
    if !provisioned {
        warnings.push("The bot was saved, but Telegram did not accept the Phenogram webhook. Retry setup from bot settings.".into());
    }
    let bot = get_bot_summary(&state, user.id, bot_id).await?;
    Ok((
        StatusCode::CREATED,
        Json(ConnectBotResponse {
            bot,
            warnings,
            webhook_ip_address_preserved: false,
        }),
    ))
}

pub async fn provision_bot(
    State(state): State<AppState>,
    user: AuthUser,
    Path(bot_id): Path<Uuid>,
) -> Result<Json<Value>> {
    active_membership(&state, user.id).await?;
    let bot = get_bot_record(&state, user.id, bot_id).await?;
    if state.config.data_plane_enabled {
        if !has_operation(&state, bot_id).await? {
            if managed_rotation_waits_for_webhook_secret(&state, bot_id).await? {
                return Err(AppError::Conflict(
                    "Managed bot setup is waiting for webhook recovery. The native webhook remains active and unchanged; use the dedicated recovery action before retrying setup."
                        .into(),
                ));
            }
            if bot.data_plane_pool.is_some() {
                telegram_json_for_bot(&state, &bot, "getMe", &json!({}), "system").await?;
                sqlx::query("UPDATE bots SET status = 'healthy', updated_at = now() WHERE id = $1")
                    .bind(bot_id)
                    .execute(&state.db)
                    .await?;
                return Ok(Json(json!({
                    "bot": get_bot_summary(&state, user.id, bot_id).await?,
                    "warnings": []
                })));
            }
            return Err(AppError::Conflict(
                "This clean-state release cannot migrate a legacy bot record. Reconnect the bot after the production reset."
                    .into(),
            ));
        }
        return match run_bot_operation(&state, bot_id).await? {
            LifecycleOutcome::Active { .. } => Ok(Json(json!({
                "bot": get_bot_summary(&state, user.id, bot_id).await?,
                "warnings": Vec::<&str>::new()
            }))),
            LifecycleOutcome::RolledBack => Err(AppError::Conflict(
                "The Bot API operation was safely rolled back".into(),
            )),
            LifecycleOutcome::Busy => Err(AppError::Conflict(
                "Bot API migration is already running".into(),
            )),
        };
    }
    let provisioned = install_managed_webhook(&state, &bot).await.unwrap_or(false);
    let status = if provisioned { "healthy" } else { "degraded" };
    sqlx::query("UPDATE bots SET status = $2, updated_at = now() WHERE id = $1")
        .bind(bot_id)
        .bind(status)
        .execute(&state.db)
        .await?;
    sqlx::query(
        r#"INSERT INTO audit_log (user_id, bot_id, action, metadata, expires_at)
           SELECT $1, bots.id, 'bot.provision_retried', $3,
                  now() + make_interval(days => bot_effective_retention_days(bots.id))
             FROM bots
            WHERE bots.id = $2"#,
    )
    .bind(user.id)
    .bind(bot_id)
    .bind(json!({"provisioned": provisioned, "routing_mode": bot.routing_mode}))
    .execute(&state.db)
    .await?;
    if !provisioned {
        return Err(AppError::Upstream(
            "Telegram did not accept the managed webhook".into(),
        ));
    }
    Ok(Json(json!({
        "bot": get_bot_summary(&state, user.id, bot_id).await?,
        "warnings": []
    })))
}

async fn managed_rotation_waits_for_webhook_secret(state: &AppState, bot_id: Uuid) -> Result<bool> {
    sqlx::query_scalar::<_, bool>(
        r#"SELECT EXISTS(
               SELECT 1
                 FROM bots child
                 JOIN managed_bot_sync_jobs jobs
                   ON jobs.manager_bot_id = child.manager_bot_id
                  AND jobs.managed_telegram_bot_id = child.telegram_bot_id
                WHERE child.id = $1
                  AND child.bot_kind = 'managed'
                  AND jobs.state = 'conflict'
                  AND jobs.error_summary = 'webhook_secret_required'
           )"#,
    )
    .bind(bot_id)
    .fetch_one(&state.db)
    .await
    .map_err(Into::into)
}

#[derive(Deserialize)]
pub struct ManagedWebhookRecoveryRequest {
    #[serde(default)]
    existing_webhook_secret: Option<String>,
    #[serde(default)]
    existing_webhook_has_no_secret: bool,
    #[serde(default)]
    existing_webhook_ip_address: Option<String>,
    #[serde(default)]
    existing_webhook_has_no_ip_address: bool,
}

pub async fn recover_managed_webhook(
    State(state): State<AppState>,
    user: AuthUser,
    Path(bot_id): Path<Uuid>,
    Json(input): Json<ManagedWebhookRecoveryRequest>,
) -> Result<Json<Value>> {
    active_membership(&state, user.id).await?;
    // Resolve ownership before consuming any operator-supplied secret. The
    // worker performs the stricter managed-job, route, and token checks.
    get_bot_record(&state, user.id, bot_id).await?;
    let existing_webhook_secret = input.existing_webhook_secret.map(Zeroizing::new);
    recover_managed_bot_rotation(
        &state,
        user.id,
        bot_id,
        existing_webhook_secret.as_deref().map(String::as_str),
        input.existing_webhook_has_no_secret,
        input.existing_webhook_ip_address.as_deref(),
        input.existing_webhook_has_no_ip_address,
    )
    .await?;
    Ok(Json(json!({
        "bot": get_bot_summary(&state, user.id, bot_id).await?,
        "warnings": Vec::<String>::new(),
    })))
}

pub async fn list_bots(State(state): State<AppState>, user: AuthUser) -> Result<Json<Value>> {
    let bots = sqlx::query_as::<_, BotSummary>(
        r#"SELECT bots.id, bots.telegram_bot_id, bots.telegram_test_dc,
                  bots.username, bots.display_name,
                  bots.public_id, bots.status, bots.routing_mode, bots.data_plane_pool,
                  bots.update_mode,
                  bots.last_update_at, bots.last_api_call_at, bots.created_at,
                  bots.bot_kind, bots.bot_kind = 'managed' AS is_managed,
                  bots.manager_bot_id, bots.manager_telegram_bot_id,
                  manager.username AS manager_username,
                  manager.display_name AS manager_display_name,
                  bots.managed_owner_telegram_user_id,
                  bot_plan_covered(bots.id) AS plan_covered,
                  bot_effective_retention_days(bots.id) AS effective_retention_days,
                  bot_retention_warning(bots.id) AS retention_warning,
                  EXISTS (
                      SELECT 1 FROM managed_bot_sync_jobs jobs
                       WHERE jobs.manager_bot_id = bots.manager_bot_id
                         AND jobs.managed_telegram_bot_id = bots.telegram_bot_id
                         AND jobs.state = 'conflict'
                         AND jobs.error_summary = 'webhook_secret_required'
                  ) AS webhook_secret_required
             FROM bots
             LEFT JOIN bots manager
               ON manager.id = bots.manager_bot_id AND manager.user_id = bots.user_id
            WHERE bots.user_id = $1
            ORDER BY bots.created_at DESC, bots.id DESC"#,
    )
    .bind(user.id)
    .fetch_all(&state.db)
    .await?;
    let coverage = sqlx::query_as::<_, BotCoverageSummary>(
        r#"SELECT memberships.plan_id, plans.bot_limit,
                  count(bots.id) FILTER (
                      WHERE bots.id IS NOT NULL AND bot_plan_covered(bots.id)
                  ) AS covered_bot_count,
                  count(bots.id) FILTER (
                      WHERE bots.bot_kind = 'managed'
                        AND NOT bot_plan_covered(bots.id)
                  ) AS uncovered_bot_count
             FROM memberships
             JOIN plan_definitions plans ON plans.id = memberships.plan_id
             LEFT JOIN bots ON bots.user_id = memberships.user_id
            WHERE memberships.user_id = $1
            GROUP BY memberships.plan_id, plans.bot_limit"#,
    )
    .bind(user.id)
    .fetch_one(&state.db)
    .await?;
    Ok(Json(json!({"bots": bots, "coverage": coverage})))
}

#[derive(Debug, Serialize, FromRow)]
struct BotCoverageSummary {
    plan_id: String,
    bot_limit: i32,
    covered_bot_count: i64,
    uncovered_bot_count: i64,
}

pub async fn get_bot(
    State(state): State<AppState>,
    user: AuthUser,
    Path(bot_id): Path<Uuid>,
) -> Result<Json<Value>> {
    let bot = get_bot_summary(&state, user.id, bot_id).await?;
    let membership = membership(&state, user.id).await?;
    let stats = sqlx::query_as::<_, (i64, i64, i64, Option<f64>)>(
        r#"SELECT
              (SELECT count(*) FROM updates WHERE bot_id = $1 AND received_at > now() - interval '24 hours'),
              (SELECT count(*) FROM api_calls WHERE bot_id = $1 AND created_at > now() - interval '24 hours'),
              (SELECT count(*) FROM webhook_deliveries WHERE bot_id = $1 AND state = 'failed'),
              (SELECT avg(latency_ms)::float8 FROM api_calls WHERE bot_id = $1 AND created_at > now() - interval '24 hours')"#,
    )
    .bind(bot_id)
    .fetch_one(&state.db)
    .await?;
    Ok(Json(json!({
        "bot": bot,
        "membership": membership,
        "stats": {"updates_24h": stats.0, "api_calls_24h": stats.1, "failed_deliveries": stats.2, "average_api_latency_ms": stats.3},
        "integration": {
            "api_base": format!(
                "{}/bot${{BOT_TOKEN}}{}",
                state.config.api_base_url,
                if bot.telegram_test_dc { "/test" } else { "" }
            ),
            "public_id": bot.public_id,
            "retention_days": bot.effective_retention_days
        }
    })))
}

#[derive(Debug, Deserialize)]
pub struct UpdatesQuery {
    limit: Option<i64>,
    #[serde(rename = "type")]
    event_type: Option<String>,
    query: Option<String>,
    chat_id: Option<i64>,
    before: Option<i64>,
}

pub async fn updates(
    State(state): State<AppState>,
    user: AuthUser,
    Path(bot_id): Path<Uuid>,
    Query(query): Query<UpdatesQuery>,
) -> Result<Json<Value>> {
    assert_bot_owner(&state, user.id, bot_id).await?;
    let limit = query.limit.unwrap_or(100).clamp(1, 200);
    let search = query.query.as_deref().map(search_pattern);
    let mut tx = state.db.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *tx)
        .await?;
    let stream_cursor = sqlx::query_scalar::<_, i64>(
        "SELECT COALESCE(max(id), 0) FROM updates WHERE bot_id = $1 AND expires_at > now()",
    )
    .bind(bot_id)
    .fetch_one(&mut *tx)
    .await?;
    let updates = sqlx::query_as::<_, UpdateSummary>(
        r#"SELECT id, update_id, event_type, chat_id, telegram_user_id, payload, received_at, expires_at
             FROM updates
            WHERE bot_id = $1
              AND expires_at > now()
              AND ($2::text IS NULL OR event_type = $2)
              AND ($3::text IS NULL OR payload::text ILIKE $3 ESCAPE E'\\')
              AND ($4::bigint IS NULL OR chat_id = $4)
              AND ($5::bigint IS NULL OR id < $5)
            ORDER BY id DESC LIMIT $6"#,
    )
    .bind(bot_id)
    .bind(query.event_type.as_deref())
    .bind(search.as_deref())
    .bind(query.chat_id)
    .bind(query.before)
    .bind(limit)
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(Json(json!({
        "updates": updates,
        "next_before": updates.last().map(|update| update.id),
        "stream_cursor": stream_cursor.to_string(),
    })))
}

pub async fn updates_stream(
    State(state): State<AppState>,
    user: AuthUser,
    Path(bot_id): Path<Uuid>,
    Query(query): Query<StreamQuery>,
    headers: HeaderMap,
) -> Result<Response> {
    assert_bot_owner(&state, user.id, bot_id).await?;
    console_event_stream(state, user.id, user.session_id, bot_id, query, headers).await
}

pub async fn activity(
    State(state): State<AppState>,
    user: AuthUser,
    Path(bot_id): Path<Uuid>,
) -> Result<Json<Value>> {
    assert_bot_owner(&state, user.id, bot_id).await?;
    let activity = sqlx::query_as::<_, ActivitySummary>(
        r#"SELECT id, method, source, http_status, telegram_ok, latency_ms, error_summary, trace_id, created_at
             FROM api_calls WHERE bot_id = $1 ORDER BY id DESC LIMIT 200"#,
    )
    .bind(bot_id)
    .fetch_all(&state.db)
    .await?;
    Ok(Json(json!({"activity": activity})))
}

pub async fn conversations(
    State(state): State<AppState>,
    user: AuthUser,
    Path(bot_id): Path<Uuid>,
) -> Result<Json<Value>> {
    assert_bot_owner(&state, user.id, bot_id).await?;
    let conversations = sqlx::query_as::<_, ConversationSummary>(
        r#"SELECT id, chat_id, business_connection_id, guest_query_id,
                  message_thread_id, direct_messages_topic_id, receiver_user_id,
                  chat_type, title, username, display_name, last_message_preview, last_update_at
             FROM conversations WHERE bot_id = $1 ORDER BY last_update_at DESC LIMIT 250"#,
    )
    .bind(bot_id)
    .fetch_all(&state.db)
    .await?;
    Ok(Json(json!({"conversations": conversations})))
}

#[derive(Debug, Clone, FromRow)]
struct TimelineRow {
    event_id: i64,
    cursor: i64,
    is_message_event: bool,
    is_message_revision: bool,
    actionable: bool,
    event_type: String,
    direction: String,
    telegram_message_id: Option<i64>,
    receiver_user_id: Option<i64>,
    ephemeral_message_id: Option<i64>,
    text: Option<String>,
    status: String,
    created_at: DateTime<Utc>,
    payload: Option<Value>,
    latest_poll: Option<Value>,
    latest_ephemeral_edit: Option<Value>,
}

#[derive(Debug, Serialize)]
struct TimelineMessage {
    id: String,
    cursor: String,
    event_type: String,
    direction: String,
    telegram_message_id: Option<i64>,
    receiver_user_id: Option<i64>,
    ephemeral_message_id: Option<i64>,
    text: Option<String>,
    status: String,
    actionable: bool,
    action_generation: Option<String>,
    created_at: DateTime<Utc>,
    payload: Option<Value>,
    content: Value,
}

#[derive(Debug, Deserialize)]
pub struct TimelineQuery {
    before: Option<i64>,
    after: Option<i64>,
    limit: Option<i64>,
}

pub async fn conversation_messages(
    State(state): State<AppState>,
    user: AuthUser,
    Path((bot_id, conversation_id)): Path<(Uuid, Uuid)>,
    Query(query): Query<TimelineQuery>,
) -> Result<Json<Value>> {
    assert_bot_owner(&state, user.id, bot_id).await?;
    if query.before.is_some() && query.after.is_some() {
        return Err(AppError::Validation(
            "Use either before or after, not both".into(),
        ));
    }
    let conversation = get_conversation(&state, bot_id, conversation_id).await?;
    let limit = query.limit.unwrap_or(100).clamp(1, 100);
    let rows =
        load_timeline_rows(&state, conversation_id, query.before, query.after, limit).await?;
    let messages = rows
        .iter()
        .cloned()
        .map(|row| timeline_message(bot_id, row))
        .collect::<Vec<_>>();
    let latest_cursor = sqlx::query_scalar::<_, Option<i64>>(
        "SELECT max(id) FROM conversation_events WHERE conversation_id = $1",
    )
    .bind(conversation_id)
    .fetch_one(&state.db)
    .await?
    .unwrap_or(0);
    let next_before = (rows.len() as i64 == limit)
        .then(|| rows.first().map(|row| row.cursor))
        .flatten();
    Ok(Json(json!({
        "conversation": conversation,
        "messages": messages,
        "next_before": next_before.map(|value| value.to_string()),
        "latest_cursor": latest_cursor.to_string(),
    })))
}

async fn load_timeline_rows(
    state: &AppState,
    conversation_id: Uuid,
    before: Option<i64>,
    after: Option<i64>,
    limit: i64,
) -> Result<Vec<TimelineRow>> {
    let mut rows = sqlx::query_as::<_, TimelineRow>(
        r#"WITH base AS (
               SELECT id AS event_id, event_type, direction, telegram_message_id,
                      receiver_user_id, ephemeral_message_id, text, status,
                      created_at, payload, edit_date,
                      (
                          direction = 'outgoing'
                          OR event_type IN (
                              'message', 'edited_message', 'channel_post',
                              'edited_channel_post', 'business_message',
                              'edited_business_message', 'guest_message'
                          )
                      ) AS is_message_event,
                      telegram_message_id IS NOT NULL
                          AND telegram_message_id <> 0
                          AND (
                              direction = 'outgoing'
                              OR event_type IN (
                                  'message', 'edited_message', 'channel_post',
                                  'edited_channel_post', 'business_message',
                                  'edited_business_message', 'guest_message'
                              )
                          ) AS is_message_revision
                 FROM conversation_events
                WHERE conversation_id = $1
           ), ranked AS (
               SELECT base.*,
                      CASE
                        WHEN is_message_revision
                        THEN row_number() OVER (
                            PARTITION BY
                                CASE WHEN is_message_revision THEN telegram_message_id END,
                                CASE WHEN is_message_revision THEN 0 ELSE event_id END
                            ORDER BY edit_date DESC NULLS LAST, event_id DESC
                        )
                        ELSE 1
                      END AS revision_rank
                 FROM base
           ), materialized AS (
               SELECT ranked.*,
                      CASE
                        WHEN direction <> 'action'
                         AND telegram_message_id IS NOT NULL
                         AND telegram_message_id <> 0
                        THEN (
                            SELECT max(tombstone.id)
                              FROM conversation_events AS tombstone
                             WHERE tombstone.conversation_id = $1
                               AND tombstone.direction = 'action'
                               AND tombstone.status = 'deleted'
                               AND tombstone.telegram_message_id = ranked.telegram_message_id
                               AND tombstone.id > ranked.event_id
                        )
                        WHEN direction <> 'action'
                         AND receiver_user_id IS NOT NULL
                         AND ephemeral_message_id IS NOT NULL
                        THEN (
                            SELECT min(tombstone.id)
                              FROM conversation_events AS tombstone
                             WHERE tombstone.conversation_id = $1
                               AND tombstone.direction = 'action'
                               AND tombstone.status = 'deleted'
                               AND tombstone.receiver_user_id = ranked.receiver_user_id
                               AND tombstone.ephemeral_message_id = ranked.ephemeral_message_id
                               AND tombstone.id > ranked.event_id
                               AND NOT EXISTS (
                                   SELECT 1
                                     FROM conversation_events AS next_generation
                                    WHERE next_generation.conversation_id = $1
                                      AND next_generation.direction <> 'action'
                                      AND next_generation.receiver_user_id = ranked.receiver_user_id
                                      AND next_generation.ephemeral_message_id = ranked.ephemeral_message_id
                                      AND next_generation.id > ranked.event_id
                                      AND next_generation.id < tombstone.id
                               )
                        )
                      END AS delete_cursor,
                      CASE
                        WHEN direction <> 'action'
                         AND receiver_user_id IS NOT NULL
                         AND ephemeral_message_id IS NOT NULL
                        THEN (
                            SELECT max(edit_event.id)
                              FROM conversation_events AS edit_event
                             WHERE edit_event.conversation_id = $1
                               AND edit_event.direction = 'action'
                               AND edit_event.event_type IN (
                                   'editEphemeralMessageText',
                                   'editEphemeralMessageCaption',
                                   'editEphemeralMessageMedia',
                                   'editEphemeralMessageReplyMarkup'
                               )
                               AND edit_event.receiver_user_id = ranked.receiver_user_id
                               AND edit_event.ephemeral_message_id = ranked.ephemeral_message_id
                               AND edit_event.id > ranked.event_id
                               AND NOT EXISTS (
                                   SELECT 1
                                     FROM conversation_events AS newer
                                    WHERE newer.conversation_id = $1
                                      AND newer.id > ranked.event_id
                                      AND newer.id < edit_event.id
                                      AND newer.receiver_user_id = ranked.receiver_user_id
                                      AND newer.ephemeral_message_id = ranked.ephemeral_message_id
                                      AND (
                                          newer.direction = 'outgoing'
                                          OR newer.event_type IN (
                                              'message', 'edited_message', 'channel_post',
                                              'edited_channel_post', 'business_message',
                                              'edited_business_message', 'guest_message'
                                          )
                                      )
                               )
                        )
                      END AS ephemeral_edit_cursor,
                      CASE WHEN is_message_event THEN (
                          SELECT max(poll_event.id)
                            FROM conversation_events AS poll_event
                           WHERE poll_event.conversation_id = $1
                             AND poll_event.event_type IN ('poll', 'stopPoll')
                             AND poll_event.id > ranked.event_id
                             AND jsonb_path_exists(
                                 ranked.payload,
                                 '$.**.poll.id ? (@ == $poll_id)',
                                 jsonb_build_object(
                                     'poll_id',
                                     COALESCE(
                                         poll_event.payload -> 'poll' -> 'id',
                                         poll_event.payload -> 'telegram_result' -> 'id'
                                     )
                                 )
                             )
                      ) END AS poll_cursor
                 FROM ranked
                WHERE revision_rank = 1
           ), projected AS (
               SELECT materialized.*,
                      GREATEST(
                          event_id,
                          COALESCE(delete_cursor, 0),
                          COALESCE(ephemeral_edit_cursor, 0),
                          COALESCE(poll_cursor, 0)
                      ) AS cursor
                 FROM materialized
           )
           SELECT event_id, cursor,
                  is_message_event, is_message_revision,
                  CASE
                    WHEN event_type = 'callback_query'
                     AND payload #>> '{callback_query,id}' IS NOT NULL
                    THEN NOT EXISTS (
                        SELECT 1
                          FROM conversation_events AS answer
                         WHERE answer.conversation_id = $1
                           AND answer.event_type = 'answerCallbackQuery'
                           AND answer.payload #>> '{request,callback_query_id}' =
                               projected.payload #>> '{callback_query,id}'
                    )
                    WHEN is_message_event AND delete_cursor IS NULL
                     AND receiver_user_id IS NOT NULL
                     AND ephemeral_message_id IS NOT NULL
                    THEN NOT EXISTS (
                        SELECT 1
                          FROM conversation_events AS newer
                         WHERE newer.conversation_id = $1
                           AND newer.id > projected.event_id
                           AND newer.receiver_user_id = projected.receiver_user_id
                           AND newer.ephemeral_message_id = projected.ephemeral_message_id
                           AND (
                               newer.direction = 'outgoing'
                               OR newer.event_type IN (
                                   'message', 'edited_message', 'channel_post',
                                   'edited_channel_post', 'business_message',
                                   'edited_business_message', 'guest_message'
                               )
                           )
                    )
                    WHEN is_message_event AND delete_cursor IS NULL THEN TRUE
                    ELSE FALSE
                  END AS actionable,
                  event_type,
                  CASE WHEN is_message_revision THEN COALESCE(
                      (
                          SELECT origin.direction
                            FROM conversation_events AS origin
                           WHERE origin.conversation_id = $1
                             AND origin.telegram_message_id = projected.telegram_message_id
                             AND origin.direction <> 'action'
                             AND (
                                 origin.direction = 'outgoing'
                                 OR origin.event_type IN (
                                     'message', 'edited_message', 'channel_post',
                                     'edited_channel_post', 'business_message',
                                     'edited_business_message', 'guest_message'
                                 )
                             )
                           ORDER BY origin.id ASC
                           LIMIT 1
                      ),
                      direction
                  ) ELSE direction END AS direction,
                  telegram_message_id,
                  receiver_user_id, ephemeral_message_id, text,
                  CASE WHEN delete_cursor IS NOT NULL THEN 'deleted' ELSE status END AS status,
                  created_at, payload,
                  CASE WHEN poll_cursor IS NOT NULL THEN (
                      SELECT COALESCE(
                                 poll_event.payload -> 'poll',
                                 poll_event.payload -> 'telegram_result'
                             )
                        FROM conversation_events AS poll_event
                       WHERE poll_event.id = projected.poll_cursor
                  ) END AS latest_poll,
                  CASE WHEN is_message_event
                         AND receiver_user_id IS NOT NULL
                         AND ephemeral_message_id IS NOT NULL
                  THEN (
                      SELECT edit_event.payload -> 'request'
                        FROM conversation_events AS edit_event
                       WHERE edit_event.id = projected.ephemeral_edit_cursor
                  ) END AS latest_ephemeral_edit
             FROM projected
            WHERE NOT (
                      (direction = 'action' AND status = 'deleted')
                      OR event_type = 'deleted_business_messages'
                      OR event_type IN (
                          'editEphemeralMessageText',
                          'editEphemeralMessageCaption',
                          'editEphemeralMessageMedia',
                          'editEphemeralMessageReplyMarkup',
                          'stopPoll', 'poll'
                      )
                  )
              AND ($2::bigint IS NULL OR cursor < $2)
              AND ($3::bigint IS NULL OR cursor > $3)
            ORDER BY
              CASE WHEN $3::bigint IS NOT NULL THEN cursor END ASC,
              CASE WHEN $3::bigint IS NULL THEN cursor END DESC
            LIMIT $4"#,
    )
    .bind(conversation_id)
    .bind(before)
    .bind(after)
    .bind(limit)
    .fetch_all(&state.db)
    .await?;
    if after.is_none() {
        rows.reverse();
    }
    Ok(rows)
}

async fn get_conversation(
    state: &AppState,
    bot_id: Uuid,
    conversation_id: Uuid,
) -> Result<ConversationSummary> {
    sqlx::query_as::<_, ConversationSummary>(
        r#"SELECT id, chat_id, business_connection_id, guest_query_id,
                  message_thread_id, direct_messages_topic_id, receiver_user_id,
                  chat_type, title, username, display_name, last_message_preview,
                  last_update_at
             FROM conversations
            WHERE id = $1 AND bot_id = $2"#,
    )
    .bind(conversation_id)
    .bind(bot_id)
    .fetch_optional(&state.db)
    .await?
    .ok_or(AppError::NotFound)
}

fn timeline_message(bot_id: Uuid, row: TimelineRow) -> TimelineMessage {
    let mut payload = row.payload.as_ref().map(sanitize_telegram_payload);
    if let (Some(payload), Some(poll)) = (&mut payload, row.latest_poll.as_ref()) {
        replace_message_field(payload, "poll", sanitize_telegram_payload(poll));
    }
    if let (Some(payload), Some(edit)) = (&mut payload, row.latest_ephemeral_edit.as_ref()) {
        apply_ephemeral_edit(payload, edit);
    }
    let callback_query = (row.event_type == "callback_query")
        .then(|| payload.as_ref()?.get("callback_query"))
        .flatten();
    let message = if callback_query.is_some() {
        None
    } else {
        payload.as_ref().and_then(telegram_message_value)
    };
    let text = message
        .and_then(|value| {
            value
                .get("text")
                .or_else(|| value.get("caption"))
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .or(row.text);
    let id = if row.is_message_revision
        && let Some(message_id) = row
            .telegram_message_id
            .filter(|message_id| *message_id != 0)
    {
        format!("{}-message-{message_id}", row.direction)
    } else {
        format!("event-{}", row.event_id)
    };
    let content = if let Some(callback) = callback_query {
        json!({
            "kind": "callback_query",
            "data": callback.get("data"),
            "game_short_name": callback.get("game_short_name"),
            "actor": callback.get("from").map(sanitize_telegram_payload),
            "target_message_id": callback
                .pointer("/message/message_id")
                .and_then(Value::as_i64),
        })
    } else {
        normalized_message_content(bot_id, message, text.as_deref())
    };
    let action_generation = (row.is_message_event
        && row.receiver_user_id.is_some()
        && row.ephemeral_message_id.is_some()
        || callback_query.is_some())
    .then(|| row.event_id.to_string());
    TimelineMessage {
        id,
        cursor: row.cursor.to_string(),
        event_type: row.event_type,
        direction: row.direction,
        telegram_message_id: row.telegram_message_id,
        receiver_user_id: row.receiver_user_id,
        ephemeral_message_id: row.ephemeral_message_id,
        text,
        status: row.status,
        actionable: row.actionable,
        action_generation,
        created_at: row.created_at,
        payload,
        content,
    }
}

fn replace_message_field(payload: &mut Value, field: &str, replacement: Value) {
    if payload.get("chat").is_some() || payload.get("message_id").is_some() {
        if let Some(message) = payload.as_object_mut() {
            message.insert(field.to_owned(), replacement);
        }
        return;
    }
    for key in [
        "message",
        "edited_message",
        "channel_post",
        "edited_channel_post",
        "business_message",
        "edited_business_message",
        "guest_message",
    ] {
        if let Some(message) = payload.get_mut(key).and_then(Value::as_object_mut) {
            message.insert(field.to_owned(), replacement);
            return;
        }
    }
}

fn apply_ephemeral_edit(payload: &mut Value, edit: &Value) {
    for field in ["text", "caption", "reply_markup"] {
        if let Some(value) = edit.get(field) {
            replace_message_field(payload, field, sanitize_telegram_payload(value));
        }
    }
    let Some(media) = edit.get("media").and_then(Value::as_object) else {
        return;
    };
    let Some(kind) = media.get("type").and_then(Value::as_str) else {
        return;
    };
    let Some(reference) = media.get("media").and_then(Value::as_str) else {
        return;
    };
    let replacement = if kind == "photo" {
        json!([{"file_id": reference}])
    } else {
        json!({"file_id": reference})
    };
    replace_message_field(payload, kind, replacement);
    if let Some(caption) = media.get("caption") {
        replace_message_field(payload, "caption", sanitize_telegram_payload(caption));
    }
}

fn telegram_message_value(payload: &Value) -> Option<&Value> {
    if payload.get("chat").is_some()
        || payload.get("text").is_some()
        || payload.get("photo").is_some()
    {
        return Some(payload);
    }
    for key in [
        "message",
        "edited_message",
        "channel_post",
        "edited_channel_post",
        "business_message",
        "edited_business_message",
        "guest_message",
    ] {
        if let Some(message) = payload.get(key) {
            return Some(message);
        }
    }
    payload
        .get("callback_query")
        .and_then(|callback| callback.get("message"))
}

fn sanitize_telegram_payload(value: &Value) -> Value {
    sanitize_telegram_payload_at_depth(value, 0)
}

fn sanitize_telegram_payload_at_depth(value: &Value, depth: usize) -> Value {
    if depth >= 64 {
        return Value::Null;
    }
    match value {
        Value::Object(values) => Value::Object(
            values
                .iter()
                .take(512)
                .filter(|(key, _)| {
                    let key = key.to_ascii_lowercase();
                    key != "file_path" && key != "authorization" && !key.contains("token")
                })
                .map(|(key, value)| {
                    (
                        key.clone(),
                        sanitize_telegram_payload_at_depth(value, depth + 1),
                    )
                })
                .collect(),
        ),
        Value::Array(values) => Value::Array(
            values
                .iter()
                .take(512)
                .map(|value| sanitize_telegram_payload_at_depth(value, depth + 1))
                .collect(),
        ),
        Value::String(value) if value.len() > 131_072 || value.chars().count() > 32_768 => {
            Value::String(value.chars().take(32_768).collect())
        }
        value => value.clone(),
    }
}

fn normalized_message_content(bot_id: Uuid, message: Option<&Value>, text: Option<&str>) -> Value {
    let Some(message) = message else {
        return json!({"kind": "action", "text": text});
    };
    let mut media = Vec::new();
    if let Some(photo) = message.get("photo").and_then(Value::as_array)
        && let Some(photo) = photo.last()
    {
        push_media(bot_id, &mut media, "photo", photo);
    }
    if let Some(live_photo) = message.get("live_photo") {
        push_media(bot_id, &mut media, "live_photo", live_photo);
    }
    for (kind, key) in [
        ("animation", "animation"),
        ("audio", "audio"),
        ("document", "document"),
        ("sticker", "sticker"),
        ("video", "video"),
        ("video_note", "video_note"),
        ("voice", "voice"),
    ] {
        if let Some(value) = message.get(key) {
            push_media(bot_id, &mut media, kind, value);
        }
    }
    let kind = media
        .first()
        .and_then(|value: &Value| value.get("type"))
        .and_then(Value::as_str)
        .unwrap_or_else(|| {
            for key in [
                "location",
                "venue",
                "contact",
                "poll",
                "dice",
                "rich_message",
                "checklist",
                "game",
                "invoice",
            ] {
                if message.get(key).is_some() {
                    return key;
                }
            }
            "text"
        });
    json!({
        "kind": kind,
        "text": message.get("text").and_then(Value::as_str).or(text),
        "caption": message.get("caption").and_then(Value::as_str),
        "media": media,
        "location": message.get("location"),
        "venue": message.get("venue"),
        "contact": message.get("contact"),
        "poll": message.get("poll"),
        "dice": message.get("dice"),
        "rich_message": message.get("rich_message"),
        "checklist": message.get("checklist"),
        "game": message.get("game"),
        "invoice": message.get("invoice"),
        "media_group_id": message.get("media_group_id"),
        "message_thread_id": message.get("message_thread_id"),
        "direct_messages_topic": message.get("direct_messages_topic"),
        "business_connection_id": message.get("business_connection_id"),
        "reply_to_message": message.get("reply_to_message").map(sanitize_telegram_payload),
        "reply_markup": message.get("reply_markup"),
        "entities": message.get("entities"),
        "caption_entities": message.get("caption_entities"),
    })
}

fn push_media(bot_id: Uuid, media: &mut Vec<Value>, kind: &str, value: &Value) {
    let Some(file_id) = value.get("file_id").and_then(Value::as_str) else {
        return;
    };
    if !valid_file_id(file_id)
        || media
            .iter()
            .any(|item| item.get("file_id").and_then(Value::as_str) == Some(file_id))
    {
        return;
    }
    media.push(json!({
        "type": kind,
        "file_id": file_id,
        "file_unique_id": value.get("file_unique_id"),
        "url": format!("/api/bots/{bot_id}/media/{file_id}"),
        "file_name": value.get("file_name"),
        "mime_type": value.get("mime_type"),
        "width": value.get("width"),
        "height": value.get("height"),
        "duration": value.get("duration"),
        "file_size": value.get("file_size"),
    }));
}

fn valid_file_id(value: &str) -> bool {
    (1..=512).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

fn collect_result_file_ids(value: &Value, depth: usize, file_ids: &mut Vec<String>) {
    if depth >= 64 || file_ids.len() >= 128 {
        return;
    }
    match value {
        Value::Object(values) => {
            if let Some(file_id) = values.get("file_id").and_then(Value::as_str)
                && valid_file_id(file_id)
                && !file_ids.iter().any(|value| value == file_id)
            {
                file_ids.push(file_id.to_owned());
            }
            for value in values.values().take(512) {
                collect_result_file_ids(value, depth + 1, file_ids);
                if file_ids.len() >= 128 {
                    break;
                }
            }
        }
        Value::Array(values) => {
            for value in values.iter().take(512) {
                collect_result_file_ids(value, depth + 1, file_ids);
                if file_ids.len() >= 128 {
                    break;
                }
            }
        }
        _ => {}
    }
}

#[derive(Debug, Deserialize)]
pub struct TimelineStreamQuery {
    after: Option<i64>,
}

pub async fn conversation_messages_stream(
    State(state): State<AppState>,
    user: AuthUser,
    Path((bot_id, conversation_id)): Path<(Uuid, Uuid)>,
    Query(query): Query<TimelineStreamQuery>,
    headers: HeaderMap,
) -> Result<Response> {
    assert_bot_owner(&state, user.id, bot_id).await?;
    get_conversation(&state, bot_id, conversation_id).await?;
    let _permit = state
        .console_stream_limiter
        .try_acquire(conversation_id.as_bytes())?;
    let mut cursor = headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<i64>().ok())
        .or(query.after)
        .unwrap_or(0)
        .max(0);
    let stream_state = state.clone();
    let stream = async_stream::stream! {
        let _permit = _permit;
        let minimum = sqlx::query_scalar::<_, Option<i64>>(
            "SELECT min(id) FROM conversation_events WHERE conversation_id = $1",
        )
        .bind(conversation_id)
        .fetch_one(&stream_state.db)
        .await
        .ok()
        .flatten();
        if cursor > 0 && minimum.is_some_and(|minimum| cursor < minimum.saturating_sub(1)) {
            let event = Event::default()
                .event("resync")
                .data(r#"{"reason":"cursor_expired"}"#);
            yield Ok::<Event, Infallible>(event);
            return;
        }
        loop {
            match load_timeline_rows(&stream_state, conversation_id, None, Some(cursor), 100).await {
                Ok(rows) => {
                    for row in rows {
                        cursor = cursor.max(row.cursor);
                        let message = timeline_message(bot_id, row);
                        if let Ok(data) = serde_json::to_string(&message) {
                            yield Ok(Event::default()
                                .event("message")
                                .id(cursor.to_string())
                                .data(data));
                        }
                    }
                }
                Err(error) => {
                    tracing::warn!(%conversation_id, error = ?error, "conversation stream query failed");
                    yield Ok(Event::default().event("resync").data(r#"{"reason":"unavailable"}"#));
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(750)).await;
        }
    };
    let mut response = Sse::new(stream)
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(15))
                .text("keep-alive"),
        )
        .into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-cache, no-store"),
    );
    response
        .headers_mut()
        .insert("x-accel-buffering", HeaderValue::from_static("no"));
    Ok(response)
}

pub async fn bot_media(
    State(state): State<AppState>,
    user: AuthUser,
    Path((bot_id, file_id)): Path<(Uuid, String)>,
    method: Method,
    headers: HeaderMap,
) -> Result<Response> {
    active_membership(&state, user.id).await?;
    if !matches!(method, Method::GET | Method::HEAD) || !valid_file_id(&file_id) {
        return Err(AppError::NotFound);
    }
    let bot = get_bot_record(&state, user.id, bot_id).await?;
    let belongs_to_bot = state.pending_media.contains(bot_id, &file_id)
        || sqlx::query_scalar::<_, bool>(
            r#"SELECT EXISTS (
                   SELECT 1 FROM conversation_events
                    WHERE bot_id = $1
                      AND jsonb_path_exists(
                          payload,
                          '$.**.file_id ? (@ == $target)',
                          jsonb_build_object('target', to_jsonb($2::text))
                      )
               )"#,
        )
        .bind(bot_id)
        .bind(&file_id)
        .fetch_one(&state.db)
        .await?;
    if !belongs_to_bot {
        return Err(AppError::NotFound);
    }
    let response = telegram_json_for_bot(
        &state,
        &bot,
        "getFile",
        &json!({"file_id": file_id}),
        "bot_view",
    )
    .await?;
    let file_path = response
        .pointer("/result/file_path")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            AppError::TelegramRejected("Telegram returned no downloadable file".into())
        })?;
    let inferred_content_type = mime_guess::from_path(file_path)
        .first_raw()
        .and_then(|value| HeaderValue::from_str(value).ok());
    let file_path = prepare_file_link_path(&state, &bot, file_path)?;
    let mut response = stream_authenticated_bot_file(
        &state,
        &bot,
        &file_path,
        headers.get(header::RANGE).cloned(),
    )
    .await?;
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("private, max-age=300"),
    );
    response.headers_mut().insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    response.headers_mut().insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static("sandbox; default-src 'none'"),
    );
    let missing_or_binary_content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_none_or(|value| value.eq_ignore_ascii_case("application/octet-stream"));
    if missing_or_binary_content_type && let Some(content_type) = inferred_content_type {
        response
            .headers_mut()
            .insert(header::CONTENT_TYPE, content_type);
    }
    let safe_inline = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value.starts_with("image/jpeg")
                || value.starts_with("image/png")
                || value.starts_with("image/gif")
                || value.starts_with("image/webp")
                || value.starts_with("audio/")
                || value.starts_with("video/")
        });
    if !safe_inline {
        response.headers_mut().insert(
            header::CONTENT_DISPOSITION,
            HeaderValue::from_static("attachment"),
        );
    }
    if method == Method::HEAD {
        *response.body_mut() = Body::empty();
    }
    Ok(response)
}

const BOT_VIEW_ACTION_METHODS: &[&str] = &[
    "sendMessage",
    "sendPhoto",
    "sendAudio",
    "sendDocument",
    "sendVideo",
    "sendAnimation",
    "sendVoice",
    "sendVideoNote",
    "sendLivePhoto",
    "sendSticker",
    "sendMediaGroup",
    "sendLocation",
    "sendVenue",
    "sendContact",
    "sendPoll",
    "sendDice",
    "sendChatAction",
    "sendRichMessage",
    "sendChecklist",
    "editMessageText",
    "editMessageCaption",
    "editMessageMedia",
    "editMessageReplyMarkup",
    "editMessageChecklist",
    "editMessageLiveLocation",
    "stopMessageLiveLocation",
    "editEphemeralMessageText",
    "editEphemeralMessageCaption",
    "editEphemeralMessageMedia",
    "editEphemeralMessageReplyMarkup",
    "deleteEphemeralMessage",
    "deleteMessage",
    "deleteMessages",
    "deleteBusinessMessages",
    "forwardMessage",
    "forwardMessages",
    "copyMessage",
    "copyMessages",
    "stopPoll",
    "setMessageReaction",
    "deleteMessageReaction",
    "deleteAllMessageReactions",
    "readBusinessMessage",
    "approveSuggestedPost",
    "declineSuggestedPost",
    "answerGuestQuery",
    "answerCallbackQuery",
];

const RESERVED_CONTEXT_FIELDS: &[&str] = &[
    "chat_id",
    "business_connection_id",
    "guest_query_id",
    "message_thread_id",
    "direct_messages_topic_id",
    "receiver_user_id",
    "callback_query_id",
    "inline_message_id",
    "_phenogram_action_generation",
];
const ACTION_GENERATION_HEADER: &str = "x-phenogram-action-generation";

pub async fn conversation_action(
    State(state): State<AppState>,
    user: AuthUser,
    Path((bot_id, conversation_id, method)): Path<(Uuid, Uuid, String)>,
    request: Request,
) -> Result<Response> {
    active_membership(&state, user.id).await?;
    if !BOT_VIEW_ACTION_METHODS.contains(&method.as_str()) {
        return Err(AppError::Validation(
            "This Bot API method is not available in Bot View".into(),
        ));
    }
    let bot = get_bot_record(&state, user.id, bot_id).await?;
    let conversation = get_conversation(&state, bot_id, conversation_id).await?;
    validate_action_for_context(&conversation, &method)?;
    let header_generation = request
        .headers()
        .get(ACTION_GENERATION_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value > 0);
    let content_type = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_owned();

    let (response, request_summary) = if content_type.starts_with("multipart/form-data") {
        if method == "answerCallbackQuery" {
            return Err(AppError::Validation(
                "answerCallbackQuery accepts a JSON body only".into(),
            ));
        }
        if method == "editEphemeralMessageMedia" {
            return Err(AppError::Validation(
                "Telegram does not allow a new upload when editing ephemeral media; use an existing file_id or HTTPS URL"
                    .into(),
            ));
        }
        let allowed_ephemeral_target = if let Some(generation) = header_generation {
            Some(validate_ephemeral_generation(&state, conversation_id, generation, None).await?)
        } else {
            None
        };
        if method.contains("Ephemeral") && allowed_ephemeral_target.is_none() {
            return Err(AppError::Validation(
                "This ephemeral message is no longer actionable; refresh the conversation".into(),
            ));
        }
        let multipart = Multipart::from_request(request, &state)
            .await
            .map_err(|_| AppError::Validation("Invalid multipart upload".into()))?;
        let (body, content_type) = trusted_multipart_body(
            multipart,
            &conversation,
            &method,
            bot.data_plane_pool.as_deref() == Some("local") || bot.routing_mode == "local",
            allowed_ephemeral_target,
        )?;
        (
            telegram_raw_for_bot(&state, &bot, &method, content_type, body, "bot_view").await,
            json!({"multipart": true}),
        )
    } else if content_type.starts_with("application/json") {
        let bytes = to_bytes(request.into_body(), 2 * 1024 * 1024)
            .await
            .map_err(|_| AppError::Validation("JSON action body is too large".into()))?;
        let mut params = serde_json::from_slice::<Value>(&bytes)
            .map_err(|_| AppError::Validation("Action body must be valid JSON".into()))?;
        let params = params
            .as_object_mut()
            .ok_or_else(|| AppError::Validation("Action body must be a JSON object".into()))?;
        let body_generation = params
            .remove("_phenogram_action_generation")
            .and_then(|value| match value {
                Value::Number(value) => value.as_i64(),
                Value::String(value) => value.parse::<i64>().ok(),
                _ => None,
            })
            .filter(|value| *value > 0);
        validate_json_media_sources(params)?;
        inject_trusted_context(params, &conversation, &method);
        let generation = body_generation.or(header_generation);
        if method == "answerCallbackQuery" {
            inject_trusted_callback_query_id(&state, conversation_id, params, generation).await?;
        } else {
            validate_ephemeral_target(&state, conversation_id, params, generation).await?;
        }
        let summary = sanitized_request_summary(params);
        (
            telegram_json_envelope_for_bot(
                &state,
                &bot,
                &method,
                &Value::Object(params.clone()),
                "bot_view",
            )
            .await,
            summary,
        )
    } else {
        return Err(AppError::Validation(
            "Bot View actions accept JSON or multipart/form-data".into(),
        ));
    };

    let accepted = response
        .as_ref()
        .ok()
        .and_then(|(_, body)| body.get("ok"))
        .and_then(Value::as_bool)
        .is_some_and(|ok| ok);
    let result = response
        .as_ref()
        .ok()
        .and_then(|(_, body)| body.get("result"))
        .cloned();
    let mut pending_file_ids = Vec::new();
    if accepted && let Some(result) = result.as_ref() {
        collect_result_file_ids(result, 0, &mut pending_file_ids);
        state
            .pending_media
            .authorize(bot.id, pending_file_ids.iter().map(String::as_str));
    }
    let timeline = if accepted {
        action_timeline_preview(
            bot.id,
            &conversation,
            &method,
            result.as_ref(),
            &request_summary,
        )
    } else {
        Vec::new()
    };
    if let Ok(permit) = state.observation_budget.clone().try_acquire_owned() {
        let persistence_state = state.clone();
        let persistence_bot = bot.clone();
        let persistence_conversation = conversation.clone();
        let persistence_method = method.clone();
        let persistence_summary = request_summary.clone();
        let persistence_result = result.clone();
        let audit_state = if accepted { "accepted" } else { "rejected" };
        tokio::spawn(async move {
            let _permit = permit;
            // Accepted message/action materialization has priority over its
            // lower-value audit metadata inside the same fail-open slot.
            if accepted {
                persist_action_result(
                    &persistence_state,
                    &persistence_bot,
                    &persistence_conversation,
                    user.id,
                    &persistence_method,
                    persistence_result.as_ref(),
                    &persistence_summary,
                )
                .await;
            }
            if let Err(error) = sqlx::query(
                r#"INSERT INTO audit_log (user_id, bot_id, action, metadata, expires_at)
                   SELECT $1, bots.id, 'bot_view.action', $3,
                          now() + make_interval(days => bot_effective_retention_days(bots.id))
                     FROM bots WHERE bots.id = $2"#,
            )
            .bind(user.id)
            .bind(bot_id)
            .bind(json!({
                "conversation_id": conversation_id,
                "chat_id": persistence_conversation.chat_id,
                "method": &persistence_method,
                "state": audit_state,
                "request": &persistence_summary,
            }))
            .execute(&persistence_state.db)
            .await
            {
                tracing::warn!(%bot_id, error = ?error, "could not store Bot View action audit");
            }
        });
    } else {
        tracing::warn!(%bot_id, %method, "dropping Bot View observation: budget is full");
    }
    let (status, mut response) = response?;
    if accepted && let Some(object) = response.as_object_mut() {
        object.insert("_phenogram".into(), json!({"timeline_messages": timeline}));
    }
    Ok((status, Json(response)).into_response())
}

fn validate_action_for_context(conversation: &ConversationSummary, method: &str) -> Result<()> {
    if conversation.guest_query_id.is_some() && method != "answerGuestQuery" {
        return Err(AppError::Validation(
            "Guest conversations can only be answered with answerGuestQuery".into(),
        ));
    }
    if conversation.guest_query_id.is_none() && method == "answerGuestQuery" {
        return Err(AppError::Validation(
            "answerGuestQuery requires a guest conversation".into(),
        ));
    }
    if method.contains("Ephemeral") && conversation.receiver_user_id.is_none() {
        return Err(AppError::Validation(
            "This conversation has no ephemeral-message recipient".into(),
        ));
    }
    if conversation.receiver_user_id.is_some()
        && !action_uses_receiver_user(method)
        && method != "answerCallbackQuery"
    {
        return Err(AppError::Validation(
            "This Bot API method cannot target an ephemeral conversation".into(),
        ));
    }
    if conversation.business_connection_id.is_some() && method == "deleteMessage" {
        return Err(AppError::Validation(
            "Business messages must be deleted with deleteBusinessMessages".into(),
        ));
    }
    if matches!(
        method,
        "deleteBusinessMessages" | "readBusinessMessage" | "sendChecklist" | "editMessageChecklist"
    ) && conversation.business_connection_id.is_none()
    {
        return Err(AppError::Validation(
            "This Bot API method requires a business conversation".into(),
        ));
    }
    if conversation.business_connection_id.is_some()
        && !action_accepts_business_connection(method)
        && !matches!(method, "deleteBusinessMessages" | "answerCallbackQuery")
    {
        return Err(AppError::Validation(
            "This Bot API method cannot safely target a business conversation".into(),
        ));
    }
    if conversation.direct_messages_topic_id.is_some()
        && method.starts_with("send")
        && !action_accepts_direct_messages_topic(method)
    {
        return Err(AppError::Validation(
            "This Bot API method cannot target a direct-messages topic".into(),
        ));
    }
    if matches!(method, "approveSuggestedPost" | "declineSuggestedPost")
        && conversation.direct_messages_topic_id.is_none()
    {
        return Err(AppError::Validation(
            "Suggested-post actions require a direct-messages conversation".into(),
        ));
    }
    Ok(())
}

fn inject_trusted_context(
    params: &mut Map<String, Value>,
    conversation: &ConversationSummary,
    method: &str,
) {
    for field in RESERVED_CONTEXT_FIELDS {
        params.remove(*field);
    }
    if method == "answerGuestQuery" {
        if let Some(value) = &conversation.guest_query_id {
            params.insert("guest_query_id".into(), Value::String(value.clone()));
        }
        return;
    }
    if action_accepts_chat_id(method) {
        params.insert("chat_id".into(), json!(conversation.chat_id));
    }
    if let Some(value) = &conversation.business_connection_id
        && action_accepts_business_connection(method)
    {
        params.insert(
            "business_connection_id".into(),
            Value::String(value.clone()),
        );
    }
    if action_accepts_message_thread(method)
        && let Some(value) = conversation.message_thread_id
    {
        params.insert("message_thread_id".into(), json!(value));
    }
    if action_accepts_direct_messages_topic(method)
        && let Some(value) = conversation.direct_messages_topic_id
    {
        params.insert("direct_messages_topic_id".into(), json!(value));
    }
    if action_uses_receiver_user(method)
        && let Some(value) = conversation.receiver_user_id
    {
        params.insert("receiver_user_id".into(), json!(value));
    }
}

async fn validate_ephemeral_target(
    state: &AppState,
    conversation_id: Uuid,
    params: &Map<String, Value>,
    generation: Option<i64>,
) -> Result<()> {
    let ephemeral_message_id = params
        .get("ephemeral_message_id")
        .and_then(Value::as_i64)
        .or_else(|| {
            params
                .get("reply_parameters")
                .and_then(|value| value.get("ephemeral_message_id"))
                .and_then(Value::as_i64)
        });
    let Some(ephemeral_message_id) = ephemeral_message_id else {
        return Ok(());
    };
    let Some(generation) = generation else {
        return Err(AppError::Validation(
            "This ephemeral message is no longer actionable; refresh the conversation".into(),
        ));
    };
    validate_ephemeral_generation(
        state,
        conversation_id,
        generation,
        Some(ephemeral_message_id),
    )
    .await?;
    Ok(())
}

async fn inject_trusted_callback_query_id(
    state: &AppState,
    conversation_id: Uuid,
    params: &mut Map<String, Value>,
    generation: Option<i64>,
) -> Result<()> {
    let Some(generation) = generation else {
        return Err(AppError::Validation(
            "This callback query is no longer actionable; refresh the conversation".into(),
        ));
    };
    let callback_query_id = sqlx::query_scalar::<_, String>(
        r#"SELECT candidate.payload #>> '{callback_query,id}'
             FROM conversation_events AS candidate
            WHERE candidate.id = $2
              AND candidate.conversation_id = $1
              AND candidate.event_type = 'callback_query'
              AND candidate.payload #>> '{callback_query,id}' IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1
                    FROM conversation_events AS answer
                   WHERE answer.conversation_id = candidate.conversation_id
                     AND answer.event_type = 'answerCallbackQuery'
                     AND answer.payload #>> '{request,callback_query_id}' =
                         candidate.payload #>> '{callback_query,id}'
              )"#,
    )
    .bind(conversation_id)
    .bind(generation)
    .fetch_optional(&state.db)
    .await?;
    let Some(callback_query_id) = callback_query_id else {
        return Err(AppError::Validation(
            "This callback query is no longer actionable; refresh the conversation".into(),
        ));
    };
    params.insert("callback_query_id".into(), Value::String(callback_query_id));
    Ok(())
}

async fn validate_ephemeral_generation(
    state: &AppState,
    conversation_id: Uuid,
    generation: i64,
    expected_ephemeral_message_id: Option<i64>,
) -> Result<i64> {
    let ephemeral_message_id = sqlx::query_scalar::<_, i64>(
        r#"SELECT candidate.ephemeral_message_id
             FROM conversation_events AS candidate
            WHERE candidate.id = $2
              AND candidate.conversation_id = $1
              AND candidate.receiver_user_id IS NOT NULL
              AND candidate.ephemeral_message_id IS NOT NULL
              AND (
                  candidate.direction = 'outgoing'
                  OR candidate.event_type IN (
                      'message', 'edited_message', 'channel_post',
                      'edited_channel_post', 'business_message',
                      'edited_business_message', 'guest_message'
                  )
              )
              AND NOT EXISTS (
                  SELECT 1
                    FROM conversation_events AS newer
                   WHERE newer.conversation_id = candidate.conversation_id
                     AND newer.id > candidate.id
                     AND newer.receiver_user_id = candidate.receiver_user_id
                     AND newer.ephemeral_message_id = candidate.ephemeral_message_id
                     AND (
                         newer.direction = 'outgoing'
                         OR newer.event_type IN (
                             'message', 'edited_message', 'channel_post',
                             'edited_channel_post', 'business_message',
                             'edited_business_message', 'guest_message'
                         )
                     )
              )
              AND NOT EXISTS (
                  SELECT 1
                    FROM conversation_events AS tombstone
                   WHERE tombstone.conversation_id = candidate.conversation_id
                     AND tombstone.id > candidate.id
                     AND tombstone.direction = 'action'
                     AND tombstone.status = 'deleted'
                     AND tombstone.receiver_user_id = candidate.receiver_user_id
                     AND tombstone.ephemeral_message_id = candidate.ephemeral_message_id
              )"#,
    )
    .bind(conversation_id)
    .bind(generation)
    .fetch_optional(&state.db)
    .await?;
    let Some(ephemeral_message_id) = ephemeral_message_id else {
        return Err(AppError::Validation(
            "This ephemeral message is no longer actionable; refresh the conversation".into(),
        ));
    };
    if expected_ephemeral_message_id.is_some_and(|expected| expected != ephemeral_message_id) {
        return Err(AppError::Validation(
            "The ephemeral message generation does not match this action".into(),
        ));
    }
    Ok(ephemeral_message_id)
}

fn validate_json_media_sources(params: &Map<String, Value>) -> Result<()> {
    reject_local_media_value(&Value::Object(params.clone()))
}

fn reject_local_media_value(value: &Value) -> Result<()> {
    match value {
        Value::Array(values) => {
            for value in values {
                reject_local_media_value(value)?;
            }
        }
        Value::Object(values) => {
            for (key, value) in values {
                if is_input_file_field(key) && value.as_str().is_some_and(is_local_media_source) {
                    return Err(AppError::Validation(
                        "Local filesystem media paths are not allowed".into(),
                    ));
                }
                reject_local_media_value(value)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn is_input_file_field(key: &str) -> bool {
    matches!(
        key,
        "photo"
            | "audio"
            | "document"
            | "video"
            | "animation"
            | "voice"
            | "video_note"
            | "sticker"
            | "thumbnail"
            | "thumb"
            | "cover"
            | "media"
            | "live_photo"
    )
}

fn is_local_media_source(value: &str) -> bool {
    value.starts_with("file:")
        || value.starts_with('/')
        || value.starts_with("../")
        || value.contains("\\..\\")
}

fn sanitized_request_summary(params: &Map<String, Value>) -> Value {
    sanitize_telegram_payload(&Value::Object(params.clone()))
}

fn trusted_multipart_body(
    mut multipart: Multipart,
    conversation: &ConversationSummary,
    method: &str,
    local_pool: bool,
    allowed_ephemeral_target: Option<i64>,
) -> Result<(reqwest::Body, HeaderValue)> {
    let boundary = format!("phenogram-{}", Uuid::new_v4().simple());
    let content_type = HeaderValue::from_str(&format!("multipart/form-data; boundary={boundary}"))
        .map_err(|_| AppError::Internal)?;
    let context = trusted_context_fields(conversation, method);
    let upload_method = method.to_owned();
    let stream_boundary = boundary.clone();
    let stream: Pin<Box<dyn Stream<Item = std::result::Result<bytes::Bytes, io::Error>> + Send>> =
        Box::pin(async_stream::try_stream! {
            let mut uploaded = HashSet::<String>::new();
            let mut required_attachments = HashSet::<String>::new();
            let mut attachment_limits = HashMap::<String, u64>::new();
            let mut total_bytes = 0_u64;
            let total_limit = if local_pool { 20_000_000_000_u64 } else { 500_000_000_u64 };

            for (name, value) in context {
                yield bytes::Bytes::from(multipart_text_part(&stream_boundary, &name, &value));
            }
            while let Some(mut field) = multipart
                .next_field()
                .await
                .map_err(|error| io::Error::other(error.to_string()))?
            {
                let name = field
                    .name()
                    .filter(|value| valid_multipart_name(value))
                    .ok_or_else(|| io::Error::other("invalid multipart field name"))?
                    .to_owned();
                if RESERVED_CONTEXT_FIELDS.contains(&name.as_str()) {
                    Err(io::Error::other("multipart request overrides trusted conversation context"))?;
                }
                let filename = field.file_name().map(safe_filename);
                let mime = field.content_type().map(ToString::to_string);
            if let Some(filename) = filename {
                let file_limit = if local_pool {
                    2_000_000_000
                } else if let Some(limit) = attachment_limits.get(&name) {
                    *limit
                } else if is_input_file_field(&name) {
                    multipart_file_limit(false, &upload_method, &name)
                } else {
                    Err(io::Error::other(
                        "multipart attachment descriptor must precede its file part",
                    ))?
                };
                    uploaded.insert(name.clone());
                    let mut header = format!(
                        "--{stream_boundary}\r\nContent-Disposition: form-data; name=\"{name}\"; filename=\"{filename}\"\r\n"
                    );
                    if let Some(mime) = mime.filter(|value| valid_mime(value)) {
                        header.push_str(&format!("Content-Type: {mime}\r\n"));
                    }
                    header.push_str("\r\n");
                    yield bytes::Bytes::from(header);
                    let mut file_bytes = 0_u64;
                    while let Some(chunk) = field
                        .chunk()
                        .await
                        .map_err(|error| io::Error::other(error.to_string()))?
                    {
                        file_bytes = file_bytes.saturating_add(chunk.len() as u64);
                        total_bytes = total_bytes.saturating_add(chunk.len() as u64);
                        if file_bytes > file_limit || total_bytes > total_limit {
                            Err(io::Error::other("multipart upload exceeds the Bot API size limit"))?;
                        }
                        yield chunk;
                    }
                    yield bytes::Bytes::from_static(b"\r\n");
                } else {
                    let mut value = Vec::new();
                    while let Some(chunk) = field
                        .chunk()
                        .await
                        .map_err(|error| io::Error::other(error.to_string()))?
                    {
                        if value.len().saturating_add(chunk.len()) > 512 * 1024 {
                            Err(io::Error::other("multipart text field is too large"))?;
                        }
                        value.extend_from_slice(&chunk);
                    }
                    let value = String::from_utf8(value)
                        .map_err(|_| io::Error::other("multipart text field is not UTF-8"))?;
                validate_multipart_ephemeral_target(
                    &name,
                    &value,
                    allowed_ephemeral_target,
                )?;
                if is_input_file_field(&name) {
                    if is_local_media_source(&value) {
                        Err(io::Error::other("local filesystem media paths are not allowed"))?;
                    }
                    collect_attach_descriptor(
                        &value,
                        multipart_file_limit(local_pool, &upload_method, &name),
                        &mut required_attachments,
                        &mut attachment_limits,
                    );
                }
                if matches!(
                    name.as_str(),
                    "media" | "rich_message" | "options" | "explanation_media"
                ) && let Ok(json) = serde_json::from_str::<Value>(&value)
                {
                    reject_local_media_value(&json)
                        .map_err(|error| io::Error::other(error.to_string()))?;
                    collect_attach_descriptors_from_value(
                        &json,
                        None,
                        local_pool,
                        &mut required_attachments,
                        &mut attachment_limits,
                    );
                }
                    yield bytes::Bytes::from(multipart_text_part(&stream_boundary, &name, &value));
                }
            }
            if !required_attachments.is_subset(&uploaded) {
                Err(io::Error::other("an attach:// reference has no uploaded multipart part"))?;
            }
            yield bytes::Bytes::from(format!("--{stream_boundary}--\r\n"));
        });
    Ok((reqwest::Body::wrap_stream(stream), content_type))
}

fn validate_multipart_ephemeral_target(
    name: &str,
    value: &str,
    allowed_ephemeral_target: Option<i64>,
) -> std::result::Result<(), io::Error> {
    let target = if name == "ephemeral_message_id" {
        value.parse::<i64>().ok()
    } else if name == "reply_parameters" {
        serde_json::from_str::<Value>(value)
            .ok()
            .and_then(|value| value.get("ephemeral_message_id").and_then(Value::as_i64))
    } else {
        None
    };
    if let Some(target) = target
        && allowed_ephemeral_target != Some(target)
    {
        return Err(io::Error::other(
            "ephemeral message generation is missing, stale, or mismatched",
        ));
    }
    Ok(())
}

fn multipart_file_limit(local_pool: bool, method: &str, field: &str) -> u64 {
    if local_pool {
        return 2_000_000_000;
    }
    if matches!(field, "thumbnail" | "thumb") {
        return 200_000;
    }
    if matches!(method, "sendPhoto" | "sendLivePhoto") || field == "photo" {
        return 10_000_000;
    }
    50_000_000
}

fn trusted_context_fields(
    conversation: &ConversationSummary,
    method: &str,
) -> Vec<(String, String)> {
    if method == "answerGuestQuery" {
        return conversation
            .guest_query_id
            .iter()
            .map(|value| ("guest_query_id".into(), value.clone()))
            .collect();
    }
    let mut fields = Vec::new();
    if action_accepts_chat_id(method) {
        fields.push(("chat_id".into(), conversation.chat_id.to_string()));
    }
    if let Some(value) = &conversation.business_connection_id
        && action_accepts_business_connection(method)
    {
        fields.push(("business_connection_id".into(), value.clone()));
    }
    if action_accepts_message_thread(method)
        && let Some(value) = conversation.message_thread_id
    {
        fields.push(("message_thread_id".into(), value.to_string()));
    }
    if action_accepts_direct_messages_topic(method)
        && let Some(value) = conversation.direct_messages_topic_id
    {
        fields.push(("direct_messages_topic_id".into(), value.to_string()));
    }
    if action_uses_receiver_user(method)
        && let Some(value) = conversation.receiver_user_id
    {
        fields.push(("receiver_user_id".into(), value.to_string()));
    }
    fields
}

fn action_uses_receiver_user(method: &str) -> bool {
    method.contains("Ephemeral")
        || matches!(
            method,
            "sendMessage"
                | "sendAnimation"
                | "sendAudio"
                | "sendDocument"
                | "sendLivePhoto"
                | "sendPhoto"
                | "sendSticker"
                | "sendVideo"
                | "sendVideoNote"
                | "sendVoice"
                | "sendContact"
                | "sendLocation"
                | "sendVenue"
        )
}

fn action_accepts_chat_id(method: &str) -> bool {
    !matches!(
        method,
        "answerGuestQuery" | "answerCallbackQuery" | "deleteBusinessMessages"
    )
}

fn action_accepts_business_connection(method: &str) -> bool {
    matches!(
        method,
        "sendMessage"
            | "sendPhoto"
            | "sendAudio"
            | "sendDocument"
            | "sendVideo"
            | "sendAnimation"
            | "sendVoice"
            | "sendVideoNote"
            | "sendLivePhoto"
            | "sendSticker"
            | "sendLocation"
            | "sendVenue"
            | "sendContact"
            | "sendMediaGroup"
            | "sendPoll"
            | "sendDice"
            | "sendRichMessage"
            | "sendChecklist"
            | "sendChatAction"
            | "editMessageText"
            | "editMessageCaption"
            | "editMessageMedia"
            | "editMessageReplyMarkup"
            | "editMessageLiveLocation"
            | "stopMessageLiveLocation"
            | "stopPoll"
            | "editMessageChecklist"
            | "deleteBusinessMessages"
            | "readBusinessMessage"
    )
}

fn action_accepts_message_thread(method: &str) -> bool {
    matches!(
        method,
        "sendMessage"
            | "sendPhoto"
            | "sendAudio"
            | "sendDocument"
            | "sendVideo"
            | "sendAnimation"
            | "sendVoice"
            | "sendVideoNote"
            | "sendLivePhoto"
            | "sendSticker"
            | "sendLocation"
            | "sendVenue"
            | "sendContact"
            | "sendMediaGroup"
            | "sendPoll"
            | "sendDice"
            | "sendRichMessage"
            | "sendChatAction"
            | "forwardMessage"
            | "forwardMessages"
            | "copyMessage"
            | "copyMessages"
    )
}

fn action_accepts_direct_messages_topic(method: &str) -> bool {
    matches!(
        method,
        "sendMessage"
            | "sendPhoto"
            | "sendAudio"
            | "sendDocument"
            | "sendVideo"
            | "sendAnimation"
            | "sendVoice"
            | "sendVideoNote"
            | "sendLivePhoto"
            | "sendSticker"
            | "sendLocation"
            | "sendVenue"
            | "sendContact"
            | "sendMediaGroup"
            | "sendDice"
            | "sendRichMessage"
            | "forwardMessage"
            | "forwardMessages"
            | "copyMessage"
            | "copyMessages"
    )
}

fn valid_multipart_name(value: &str) -> bool {
    (1..=64).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

fn safe_filename(value: &str) -> String {
    let value = value
        .chars()
        .filter(|character| !character.is_control() && !matches!(character, '"' | '\\'))
        .take(180)
        .collect::<String>();
    if value.is_empty() {
        "upload.bin".into()
    } else {
        value
    }
}

fn valid_mime(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'+' | b'-' | b'.'))
}

fn multipart_text_part(boundary: &str, name: &str, value: &str) -> String {
    format!("--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}\r\n")
}

fn collect_attach_descriptors_from_value(
    value: &Value,
    parent_media_type: Option<&str>,
    local_pool: bool,
    names: &mut HashSet<String>,
    limits: &mut HashMap<String, u64>,
) {
    match value {
        Value::Object(values) => {
            let media_type = values
                .get("type")
                .and_then(Value::as_str)
                .or(parent_media_type);
            for (key, value) in values {
                if is_input_file_field(key)
                    && let Some(value) = value.as_str()
                {
                    let limit = if local_pool {
                        2_000_000_000
                    } else if matches!(key.as_str(), "thumbnail" | "thumb") {
                        200_000
                    } else if key == "photo"
                        || key == "live_photo"
                        || (key == "media" && matches!(media_type, Some("photo" | "live_photo")))
                    {
                        10_000_000
                    } else {
                        50_000_000
                    };
                    collect_attach_descriptor(value, limit, names, limits);
                }
                collect_attach_descriptors_from_value(value, media_type, local_pool, names, limits);
            }
        }
        Value::Array(values) => {
            for value in values {
                collect_attach_descriptors_from_value(
                    value,
                    parent_media_type,
                    local_pool,
                    names,
                    limits,
                );
            }
        }
        _ => {}
    }
}

fn collect_attach_descriptor(
    value: &str,
    limit: u64,
    names: &mut HashSet<String>,
    limits: &mut HashMap<String, u64>,
) {
    let Some(name) = value.strip_prefix("attach://") else {
        return;
    };
    if valid_multipart_name(name) {
        names.insert(name.to_owned());
        limits
            .entry(name.to_owned())
            .and_modify(|current| *current = (*current).min(limit))
            .or_insert(limit);
    }
}

fn action_timeline_preview(
    bot_id: Uuid,
    conversation: &ConversationSummary,
    method: &str,
    result: Option<&Value>,
    request_summary: &Value,
) -> Vec<Value> {
    let null_result = Value::Null;
    let results = match result {
        Some(Value::Array(values)) if !values.is_empty() => values.iter().collect::<Vec<_>>(),
        Some(value) => vec![value],
        None => vec![&null_result],
    };
    results
        .into_iter()
        .map(|result| {
            let is_message = result.get("chat").is_some()
                || (result.get("ephemeral_message_id").is_some()
                    && result.get("receiver_user").is_some());
            let creates_placeholder = action_creates_message_placeholder(method);
            let payload = if is_message {
                sanitize_telegram_payload(result)
            } else {
                json!({
                    "telegram_result": sanitize_telegram_payload(result),
                    "request": request_summary,
                    "action": method,
                })
            };
            let telegram_message_id = result
                .get("message_id")
                .and_then(Value::as_i64)
                .or_else(|| request_summary.get("message_id").and_then(Value::as_i64));
            let ephemeral_message_id = result
                .get("ephemeral_message_id")
                .and_then(Value::as_i64)
                .or_else(|| {
                    request_summary
                        .get("ephemeral_message_id")
                        .and_then(Value::as_i64)
                });
            let text = result
                .get("text")
                .or_else(|| result.get("caption"))
                .and_then(Value::as_str)
                .or_else(|| request_summary.get("text").and_then(Value::as_str))
                .or_else(|| request_summary.get("caption").and_then(Value::as_str))
                .or_else(|| {
                    request_summary
                        .pointer("/result/input_message_content/message_text")
                        .and_then(Value::as_str)
                });
            let placeholder_text = (!is_message && creates_placeholder)
                .then(|| action_placeholder_text(method, telegram_message_id, text));
            let text = text.or(placeholder_text.as_deref());
            let created_at = result
                .get("date")
                .and_then(Value::as_i64)
                .and_then(|timestamp| DateTime::<Utc>::from_timestamp(timestamp, 0))
                .unwrap_or_else(Utc::now);
            let id = (is_message || creates_placeholder)
                .then_some(telegram_message_id)
                .flatten()
                .filter(|message_id| *message_id != 0)
                .map(|message_id| format!("outgoing-message-{message_id}"));
            json!({
                "id": id,
                "event_type": method,
                "direction": if is_message || creates_placeholder { "outgoing" } else { "action" },
                "telegram_message_id": telegram_message_id,
                "receiver_user_id": conversation.receiver_user_id,
                "ephemeral_message_id": ephemeral_message_id,
                "text": text,
                "status": "sent",
                "created_at": created_at,
                "payload": payload,
                "content": normalized_message_content(bot_id, Some(result), text),
            })
        })
        .collect()
}

async fn persist_action_result(
    state: &AppState,
    bot: &BotRecord,
    conversation: &ConversationSummary,
    user_id: Uuid,
    method: &str,
    result: Option<&Value>,
    request_summary: &Value,
) -> Vec<Value> {
    let null_result = Value::Null;
    let results = match result {
        Some(Value::Array(values)) => {
            if values.is_empty() {
                vec![&null_result]
            } else {
                values.iter().collect::<Vec<_>>()
            }
        }
        Some(value) => vec![value],
        None => vec![&null_result],
    };
    let mut timeline = Vec::with_capacity(results.len());
    for result in results {
        let is_message = result.get("chat").is_some()
            || (result.get("ephemeral_message_id").is_some()
                && result.get("receiver_user").is_some());
        let creates_placeholder = action_creates_message_placeholder(method);
        let payload = if is_message {
            sanitize_telegram_payload(result)
        } else {
            json!({
                "telegram_result": sanitize_telegram_payload(result),
                "request": request_summary,
                "action": method,
            })
        };
        let telegram_message_id = result
            .get("message_id")
            .and_then(Value::as_i64)
            .or_else(|| request_summary.get("message_id").and_then(Value::as_i64));
        let ephemeral_message_id = result
            .get("ephemeral_message_id")
            .and_then(Value::as_i64)
            .or_else(|| {
                request_summary
                    .get("ephemeral_message_id")
                    .and_then(Value::as_i64)
            });
        let receiver_user_id = result
            .pointer("/receiver_user/id")
            .and_then(Value::as_i64)
            .or(conversation.receiver_user_id);
        let text = result
            .get("text")
            .or_else(|| result.get("caption"))
            .and_then(Value::as_str)
            .or_else(|| request_summary.get("text").and_then(Value::as_str))
            .or_else(|| {
                request_summary
                    .pointer("/result/input_message_content/message_text")
                    .and_then(Value::as_str)
            });
        let placeholder_text = (!is_message && creates_placeholder)
            .then(|| action_placeholder_text(method, telegram_message_id, text));
        let text = text.or(placeholder_text.as_deref());
        if is_message || creates_placeholder {
            if let Err(error) = record_outbound_message(
                state,
                OutboundMessageRecord {
                    bot_id: bot.id,
                    user_id: Some(user_id),
                    conversation_id: Some(conversation.id),
                    chat_id: conversation.chat_id,
                    telegram_message_id,
                    receiver_user_id,
                    ephemeral_message_id,
                    observation_key: None,
                    business_connection_id: conversation.business_connection_id.as_deref(),
                    guest_query_id: conversation.guest_query_id.as_deref(),
                    message_thread_id: conversation.message_thread_id,
                    direct_messages_topic_id: conversation.direct_messages_topic_id,
                    method,
                    source: "bot_view",
                    text,
                    payload: Some(&payload),
                    status: "sent",
                    response_status: Some(200),
                    error_summary: None,
                    created_at: None,
                },
            )
            .await
            {
                tracing::warn!(bot_id = %bot.id, %method, error = ?error, "could not store Bot View action result");
            }
        } else if let Err(error) = persist_non_message_action(
            state,
            bot.id,
            conversation.id,
            method,
            telegram_message_id,
            receiver_user_id,
            ephemeral_message_id,
            &payload,
        )
        .await
        {
            tracing::warn!(bot_id = %bot.id, %method, error = ?error, "could not store Bot View action event");
        }
        timeline.push(json!({
            "event_type": method,
            "telegram_message_id": telegram_message_id,
            "ephemeral_message_id": ephemeral_message_id,
            "text": text,
            "payload": payload,
            "content": normalized_message_content(bot.id, Some(result), text),
        }));
    }
    timeline
}

fn action_creates_message_placeholder(method: &str) -> bool {
    matches!(
        method,
        "copyMessage" | "copyMessages" | "forwardMessages" | "answerGuestQuery"
    )
}

fn action_placeholder_text(method: &str, message_id: Option<i64>, text: Option<&str>) -> String {
    if let Some(text) = text.filter(|text| !text.is_empty()) {
        return text.to_owned();
    }
    let noun = match method {
        "copyMessage" | "copyMessages" => "Copied message",
        "forwardMessages" => "Forwarded message",
        "answerGuestQuery" => return "Guest reply sent".into(),
        _ => "Bot action",
    };
    message_id.map_or_else(|| noun.to_owned(), |id| format!("{noun} #{id}"))
}

#[allow(clippy::too_many_arguments)]
async fn persist_non_message_action(
    state: &AppState,
    bot_id: Uuid,
    conversation_id: Uuid,
    method: &str,
    telegram_message_id: Option<i64>,
    receiver_user_id: Option<i64>,
    ephemeral_message_id: Option<i64>,
    payload: &Value,
) -> Result<()> {
    let mut message_ids = payload
        .pointer("/request/message_ids")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_i64)
        .collect::<Vec<_>>();
    if message_ids.is_empty()
        && let Some(message_id) = telegram_message_id
    {
        message_ids.push(message_id);
    }
    if message_ids.is_empty() {
        message_ids.push(0);
    }
    let deleted = matches!(
        method,
        "deleteMessage" | "deleteMessages" | "deleteBusinessMessages" | "deleteEphemeralMessage"
    );
    for message_id in message_ids {
        sqlx::query(
            r#"INSERT INTO conversation_events
                   (bot_id, conversation_id, direction, event_type, source_table,
                    telegram_message_id, receiver_user_id, ephemeral_message_id,
                    text, status, payload, created_at, expires_at)
               SELECT bots.id, $2, 'action', $3, 'bot_view_action',
                      NULLIF($4, 0), $5, $6, NULL, $7, $8, now(),
                      now() + make_interval(days => bot_effective_retention_days(bots.id))
                 FROM bots WHERE bots.id = $1"#,
        )
        .bind(bot_id)
        .bind(conversation_id)
        .bind(method)
        .bind(message_id)
        .bind(receiver_user_id)
        .bind(ephemeral_message_id)
        .bind(if deleted { "deleted" } else { "sent" })
        .bind(payload)
        .execute(&state.db)
        .await?;
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
pub struct CreateStreamKeyRequest {
    name: Option<String>,
}

pub async fn create_stream_key(
    State(state): State<AppState>,
    user: AuthUser,
    Path(bot_id): Path<Uuid>,
    Json(input): Json<CreateStreamKeyRequest>,
) -> Result<(StatusCode, Json<Value>)> {
    active_membership(&state, user.id).await?;
    let bot = get_bot_summary(&state, user.id, bot_id).await?;
    let name = input.name.unwrap_or_else(|| "Default stream".into());
    if name.trim().is_empty() || name.len() > 80 {
        return Err(AppError::Validation(
            "Stream key name must contain 1 to 80 characters".into(),
        ));
    }
    let key = Crypto::random_token(32)?;
    let id = sqlx::query_scalar::<_, Uuid>(
        "INSERT INTO event_stream_keys (bot_id, name, secret_hash) VALUES ($1,$2,$3) RETURNING id",
    )
    .bind(bot_id)
    .bind(name.trim())
    .bind(Crypto::digest_secret(key.as_bytes()))
    .fetch_one(&state.db)
    .await?;
    let url = format!(
        "{}/events/{}/{}",
        state.config.api_base_url, bot.public_id, key
    );
    sqlx::query(
        r#"INSERT INTO audit_log (user_id, bot_id, action, metadata, expires_at)
           SELECT $1, bots.id, 'stream_key.created', $3,
                  now() + make_interval(days => bot_effective_retention_days(bots.id))
             FROM bots
            WHERE bots.id = $2"#,
    )
    .bind(user.id)
    .bind(bot_id)
    .bind(json!({"stream_key_id": id}))
    .execute(&state.db)
    .await?;
    Ok((
        StatusCode::CREATED,
        Json(json!({
            "id": id,
            "url": url,
            "notice": "This URL contains a revocable stream secret. It is shown once and is not your Telegram bot token."
        })),
    ))
}

pub async fn list_stream_keys(
    State(state): State<AppState>,
    user: AuthUser,
    Path(bot_id): Path<Uuid>,
) -> Result<Json<Value>> {
    assert_bot_owner(&state, user.id, bot_id).await?;
    let rows = sqlx::query_as::<_, StreamKeySummary>(
        "SELECT id, name, last_used_at, revoked_at, created_at FROM event_stream_keys WHERE bot_id = $1 ORDER BY created_at DESC",
    )
    .bind(bot_id).fetch_all(&state.db).await?;
    Ok(Json(json!({"stream_keys": rows})))
}

#[derive(Debug, Serialize, FromRow)]
struct StreamKeySummary {
    id: Uuid,
    name: String,
    last_used_at: Option<chrono::DateTime<Utc>>,
    revoked_at: Option<chrono::DateTime<Utc>>,
    created_at: chrono::DateTime<Utc>,
}

pub async fn revoke_stream_key(
    State(state): State<AppState>,
    user: AuthUser,
    Path((bot_id, key_id)): Path<(Uuid, Uuid)>,
) -> Result<Json<Value>> {
    assert_bot_owner(&state, user.id, bot_id).await?;
    let result = sqlx::query("UPDATE event_stream_keys SET revoked_at = now() WHERE id = $1 AND bot_id = $2 AND revoked_at IS NULL")
        .bind(key_id).bind(bot_id).execute(&state.db).await?;
    if result.rows_affected() == 0 {
        return Err(AppError::NotFound);
    }
    Ok(Json(json!({"ok": true})))
}

#[derive(Debug, Deserialize)]
pub struct FileLinkRequest {
    file_path: String,
    expires_in_seconds: Option<i64>,
}

pub async fn create_file_link(
    State(state): State<AppState>,
    user: AuthUser,
    Path(bot_id): Path<Uuid>,
    Json(input): Json<FileLinkRequest>,
) -> Result<Json<Value>> {
    active_membership(&state, user.id).await?;
    let bot = get_bot_record(&state, user.id, bot_id).await?;
    let file_path = prepare_file_link_path(&state, &bot, &input.file_path)?;
    let ttl = input.expires_in_seconds.unwrap_or(3600).clamp(60, 604_800);
    let expires = Utc::now().timestamp() + ttl;
    let sig = state
        .crypto
        .sign_file_link(&bot.public_id, &file_path, expires);
    let mut url = url::Url::parse(&state.config.api_base_url).map_err(|_| AppError::Internal)?;
    {
        let mut segments = url.path_segments_mut().map_err(|_| AppError::Internal)?;
        segments
            .clear()
            .push("public")
            .push(&bot.public_id)
            .push("files");
        for segment in file_path.split('/') {
            segments.push(segment);
        }
    }
    url.query_pairs_mut()
        .append_pair("expires", &expires.to_string())
        .append_pair("sig", &sig);
    Ok(Json(json!({
        "url": url.as_str(),
        "expires_at": chrono::DateTime::from_timestamp(expires, 0)
    })))
}

#[derive(Deserialize)]
pub struct RoutingRequest {
    mode: String,
    confirm_migration: bool,
}

pub async fn change_routing(
    State(state): State<AppState>,
    user: AuthUser,
    Path(bot_id): Path<Uuid>,
    Json(input): Json<RoutingRequest>,
) -> Result<Json<Value>> {
    if !input.confirm_migration {
        return Err(AppError::Validation(
            "Bot API routing migrations require explicit confirmation".into(),
        ));
    }
    if !matches!(input.mode.as_str(), "cloud" | "local") {
        return Err(AppError::Validation(
            "Routing mode must be cloud or local".into(),
        ));
    }
    if state.config.data_plane_enabled {
        let bot = get_bot_record(&state, user.id, bot_id).await?;
        let target = DataPlanePool::from_routing_mode(&input.mode)?;
        if has_operation(&state, bot_id).await? {
            return Err(AppError::Conflict(
                "The bot is still being connected to its official Bot API pool".into(),
            ));
        }
        if bot.data_plane_pool.as_deref() == Some(target.as_str()) {
            return Ok(Json(
                json!({"bot": get_bot_summary(&state, user.id, bot_id).await?, "warnings": []}),
            ));
        }
        return Err(AppError::Conflict(
            "Moving a bot between official Bot API pools is not available yet. Phenogram left the current route unchanged."
                .into(),
        ));
    }
    // Serialize the externally visible logOut/login saga per bot. Transaction-
    // scoped advisory locks are automatically released on errors or crashes.
    let mut migration_lock = state.db.begin().await?;
    let locked = sqlx::query_scalar::<_, bool>(
        "SELECT pg_try_advisory_xact_lock(hashtextextended($1::text, 0))",
    )
    .bind(bot_id)
    .fetch_one(&mut *migration_lock)
    .await?;
    if !locked {
        return Err(AppError::Conflict(
            "A routing migration is already in progress for this bot".into(),
        ));
    }
    let mut bot = get_bot_record(&state, user.id, bot_id).await?;
    if bot.routing_mode == input.mode {
        migration_lock.commit().await?;
        return Ok(Json(
            json!({"bot": get_bot_summary(&state, user.id, bot_id).await?, "warnings": []}),
        ));
    }
    let membership = active_membership(&state, user.id).await?;
    if input.mode == "local" && !membership.local_bot_api {
        return Err(AppError::PlanLimit(
            "Local Telegram Bot API routing requires Pro or Scale".into(),
        ));
    }
    let local_base = state
        .config
        .telegram_local_api_url
        .as_deref()
        .ok_or_else(|| {
            AppError::Conflict("Local Bot API routing is not configured on this deployment".into())
        })?;
    let token = decrypt_token(&state, &bot)?;
    let token = std::str::from_utf8(&token).map_err(|_| AppError::Internal)?;
    let mut warnings: Vec<String> = Vec::new();
    if input.mode == "local" {
        let (_, logout) = raw_telegram_json_for_dc(
            &state.telegram,
            &state.config.telegram_cloud_api_url,
            token,
            bot.telegram_test_dc,
            "logOut",
            &json!({}),
        )
        .await?;
        if logout.get("ok").and_then(Value::as_bool) != Some(true) {
            return Err(AppError::Upstream(
                "Telegram cloud logout failed; routing was not changed".into(),
            ));
        }
        bot.routing_mode = "local".into();
    } else {
        let (_, logout) = raw_telegram_json_for_dc(
            &state.telegram,
            local_base,
            token,
            bot.telegram_test_dc,
            "logOut",
            &json!({}),
        )
        .await?;
        if logout.get("ok").and_then(Value::as_bool) != Some(true) {
            return Err(AppError::Upstream(
                "Telegram local logout failed; routing was not changed".into(),
            ));
        }
        bot.routing_mode = "cloud".into();
        warnings.push(
            "Telegram may enforce a short login cooldown while cloud routing resumes.".into(),
        );
    }
    sqlx::query("UPDATE bots SET routing_mode = $2, status = 'provisioning', updated_at = now() WHERE id = $1")
        .bind(bot_id).bind(&bot.routing_mode).execute(&state.db).await?;
    let provisioned = install_managed_webhook(&state, &bot).await.unwrap_or(false);
    sqlx::query("UPDATE bots SET status = $2, updated_at = now() WHERE id = $1")
        .bind(bot_id)
        .bind(if provisioned { "healthy" } else { "degraded" })
        .execute(&state.db)
        .await?;
    if !provisioned {
        warnings.push(
            "The target Telegram API is not ready yet. Phenogram kept the new routing mode; retry provisioning from bot settings."
                .into(),
        );
    }
    sqlx::query(
        r#"INSERT INTO audit_log (user_id, bot_id, action, metadata, expires_at)
           SELECT $1, bots.id, 'bot.routing_changed', $3,
                  now() + make_interval(days => bot_effective_retention_days(bots.id))
             FROM bots
            WHERE bots.id = $2"#,
    )
    .bind(user.id)
    .bind(bot_id)
    .bind(json!({"mode": bot.routing_mode}))
    .execute(&state.db)
    .await?;
    migration_lock.commit().await?;
    Ok(Json(
        json!({"bot": get_bot_summary(&state, user.id, bot_id).await?, "warnings": warnings}),
    ))
}

pub async fn delete_bot(
    State(state): State<AppState>,
    user: AuthUser,
    Path(bot_id): Path<Uuid>,
) -> Result<Json<Value>> {
    let bot = get_bot_record(&state, user.id, bot_id).await?;
    let hierarchy = sqlx::query_as::<_, (String, Option<Uuid>)>(
        "SELECT bot_kind, manager_bot_id FROM bots WHERE id = $1 AND user_id = $2",
    )
    .bind(bot_id)
    .bind(user.id)
    .fetch_one(&state.db)
    .await?;
    if hierarchy.0 == "managed" && hierarchy.1.is_some() {
        return Err(AppError::Conflict(
            "This bot is managed automatically. Disconnect its manager before deleting it.".into(),
        ));
    }
    if state.config.data_plane_enabled && has_operation(&state, bot_id).await? {
        return Err(AppError::Conflict(
            "Finish the current Bot API migration before disconnecting this bot".into(),
        ));
    }
    if state.config.data_plane_enabled && bot.data_plane_pool.is_some() {
        return Err(AppError::Conflict(
            "Disconnecting a bot from the official Phenogram data plane is temporarily unavailable because Telegram requires logOut before cloud ownership can resume and may enforce a ten-minute login delay. Phenogram left the route and webhook unchanged."
                .into(),
        ));
    }
    let token = decrypt_token(&state, &bot)?;
    let base = if bot.routing_mode == "local" {
        state
            .config
            .telegram_local_api_url
            .as_deref()
            .unwrap_or(&state.config.telegram_cloud_api_url)
    } else {
        &state.config.telegram_cloud_api_url
    };
    let cleanup = raw_telegram_json_for_dc(
        &state.telegram,
        base,
        std::str::from_utf8(&token).unwrap_or(""),
        bot.telegram_test_dc,
        "deleteWebhook",
        &json!({"drop_pending_updates": false}),
    )
    .await?;
    let cleanup_ok = cleanup.1.get("ok").and_then(Value::as_bool) == Some(true);
    let token_invalid = cleanup.0 == reqwest::StatusCode::UNAUTHORIZED
        || cleanup.1.get("error_code").and_then(Value::as_i64) == Some(401);
    if !cleanup_ok && !token_invalid {
        tracing::warn!(
            bot_id = %bot.id,
            status = %cleanup.0,
            "refusing to delete bot before Telegram webhook cleanup succeeds"
        );
        return Err(AppError::Upstream(
            "Telegram did not confirm webhook cleanup".into(),
        ));
    }
    sqlx::query("DELETE FROM bots WHERE id = $1 AND user_id = $2")
        .bind(bot_id)
        .bind(user.id)
        .execute(&state.db)
        .await?;
    Ok(Json(json!({"ok": true})))
}

async fn get_bot_summary(state: &AppState, user_id: Uuid, bot_id: Uuid) -> Result<BotSummary> {
    sqlx::query_as::<_, BotSummary>(
        r#"SELECT bots.id, bots.telegram_bot_id, bots.telegram_test_dc,
                  bots.username, bots.display_name,
                  bots.public_id, bots.status, bots.routing_mode, bots.data_plane_pool,
                  bots.update_mode,
                  bots.last_update_at, bots.last_api_call_at, bots.created_at,
                  bots.bot_kind, bots.bot_kind = 'managed' AS is_managed,
                  bots.manager_bot_id, bots.manager_telegram_bot_id,
                  manager.username AS manager_username,
                  manager.display_name AS manager_display_name,
                  bots.managed_owner_telegram_user_id,
                  bot_plan_covered(bots.id) AS plan_covered,
                  bot_effective_retention_days(bots.id) AS effective_retention_days,
                  bot_retention_warning(bots.id) AS retention_warning,
                  EXISTS (
                      SELECT 1 FROM managed_bot_sync_jobs jobs
                       WHERE jobs.manager_bot_id = bots.manager_bot_id
                         AND jobs.managed_telegram_bot_id = bots.telegram_bot_id
                         AND jobs.state = 'conflict'
                         AND jobs.error_summary = 'webhook_secret_required'
                  ) AS webhook_secret_required
             FROM bots
             LEFT JOIN bots manager
               ON manager.id = bots.manager_bot_id AND manager.user_id = bots.user_id
            WHERE bots.id = $1 AND bots.user_id = $2"#,
    )
    .bind(bot_id)
    .bind(user_id)
    .fetch_optional(&state.db)
    .await?
    .ok_or(AppError::NotFound)
}

async fn get_bot_record(state: &AppState, user_id: Uuid, bot_id: Uuid) -> Result<BotRecord> {
    sqlx::query_as::<_, BotRecord>(
        r#"SELECT id, user_id, telegram_bot_id, telegram_test_dc, username, display_name,
                  token_ciphertext, token_nonce, token_fingerprint, public_id,
                  ingress_secret_ciphertext, ingress_secret_nonce, status,
                  routing_mode, data_plane_pool, update_mode, last_update_at,
                  last_api_call_at, created_at
             FROM bots WHERE id = $1 AND user_id = $2"#,
    )
    .bind(bot_id)
    .bind(user_id)
    .fetch_optional(&state.db)
    .await?
    .ok_or(AppError::NotFound)
}

async fn assert_bot_owner(state: &AppState, user_id: Uuid, bot_id: Uuid) -> Result<()> {
    let exists = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM bots WHERE id = $1 AND user_id = $2)",
    )
    .bind(bot_id)
    .bind(user_id)
    .fetch_one(&state.db)
    .await?;
    if exists {
        Ok(())
    } else {
        Err(AppError::NotFound)
    }
}

fn connect_target_pool(requested: Option<&str>) -> Result<DataPlanePool> {
    match requested.unwrap_or("standard") {
        "standard" => Ok(DataPlanePool::Standard),
        "local" => Ok(DataPlanePool::Local),
        _ => Err(AppError::Validation(
            "Bot API pool must be standard or local".into(),
        )),
    }
}

fn validate_bot_token(token: &str) -> Result<()> {
    let valid = token.len() <= 256
        && token.split_once(':').is_some_and(|(id, secret)| {
            !id.is_empty()
                && id.bytes().all(|byte| byte.is_ascii_digit())
                && secret.len() >= 20
                && secret
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        });
    if valid {
        Ok(())
    } else {
        Err(AppError::Validation(
            "Enter a valid Telegram bot token".into(),
        ))
    }
}

fn validate_get_me_response(status: StatusCode, response: &Value) -> Result<()> {
    if response.get("ok").and_then(Value::as_bool) == Some(true) {
        return Ok(());
    }

    let message = if status == StatusCode::UNAUTHORIZED
        || response.get("error_code").and_then(Value::as_u64) == Some(401)
    {
        "Telegram rejected this token. Check that it is complete and still active in @BotFather."
    } else {
        response
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or("Telegram rejected this bot token")
    };
    Err(AppError::Validation(message.to_owned()))
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use chrono::Utc;
    use serde_json::json;
    use uuid::Uuid;

    use super::{
        ConnectBotRequest, ManagedWebhookRecoveryRequest, connect_target_pool, existing_webhook,
        health_payload, inject_trusted_context, normalized_message_content,
        validate_get_me_response, validate_json_media_sources,
    };
    use crate::error::AppError;
    use crate::lifecycle::DataPlanePool;
    use crate::models::ConversationSummary;
    use crate::telegram::ExistingWebhookPolicy;

    const API_BASE_URL: &str = "https://api.phenogram.io";

    fn conversation() -> ConversationSummary {
        ConversationSummary {
            id: Uuid::new_v4(),
            chat_id: 42,
            business_connection_id: Some("business-1".into()),
            guest_query_id: None,
            message_thread_id: Some(7),
            direct_messages_topic_id: Some(9),
            receiver_user_id: Some(11),
            chat_type: Some("private".into()),
            title: None,
            username: Some("ada".into()),
            display_name: Some("Ada".into()),
            last_message_preview: None,
            last_update_at: Utc::now(),
        }
    }

    #[test]
    fn bot_view_action_overwrites_all_client_supplied_scope() {
        let mut params = json!({
            "chat_id": 999,
            "business_connection_id": "hostile",
            "message_thread_id": 999,
            "direct_messages_topic_id": 999,
            "receiver_user_id": 999,
            "text": "hello"
        })
        .as_object()
        .expect("object")
        .clone();

        inject_trusted_context(&mut params, &conversation(), "sendMessage");

        assert_eq!(params["chat_id"], 42);
        assert_eq!(params["business_connection_id"], "business-1");
        assert_eq!(params["message_thread_id"], 7);
        assert_eq!(params["direct_messages_topic_id"], 9);
        assert_eq!(params["receiver_user_id"], 11);
    }

    #[test]
    fn bot_view_action_injects_only_method_supported_scope() {
        let mut reaction = json!({
            "chat_id": 999,
            "business_connection_id": "hostile",
            "message_thread_id": 999,
            "direct_messages_topic_id": 999,
            "receiver_user_id": 999,
            "inline_message_id": "hostile",
            "message_id": 5,
            "user_id": 77
        })
        .as_object()
        .expect("object")
        .clone();
        inject_trusted_context(&mut reaction, &conversation(), "deleteMessageReaction");
        assert_eq!(reaction["chat_id"], 42);
        assert_eq!(reaction["user_id"], 77);
        for unsupported in [
            "business_connection_id",
            "message_thread_id",
            "direct_messages_topic_id",
            "receiver_user_id",
            "inline_message_id",
        ] {
            assert!(reaction.get(unsupported).is_none(), "{unsupported}");
        }

        let mut business_delete = json!({"chat_id": 999, "message_ids": [5]})
            .as_object()
            .expect("object")
            .clone();
        inject_trusted_context(
            &mut business_delete,
            &conversation(),
            "deleteBusinessMessages",
        );
        assert_eq!(business_delete["business_connection_id"], "business-1");
        assert!(business_delete.get("chat_id").is_none());
    }

    #[test]
    fn nested_local_media_sources_are_rejected() {
        let params = json!({
            "rich_message": {
                "blocks": [{"media": "/etc/passwd"}]
            }
        });
        assert!(validate_json_media_sources(params.as_object().expect("object")).is_err());
    }

    #[test]
    fn text_that_looks_like_a_path_or_attachment_is_not_treated_as_media() {
        let params = json!({
            "text": "file: notes",
            "caption": "../notes and attach://not-a-file",
            "rich_message": {
                "markdown": "/start\nattach://also-text",
                "blocks": [{"text": "/etc/passwd"}]
            },
            "options": [{"text": "../poll option"}]
        });
        assert!(validate_json_media_sources(params.as_object().expect("object")).is_ok());
    }

    #[test]
    fn rich_media_is_exposed_only_through_the_authenticated_proxy() {
        let bot_id = Uuid::new_v4();
        let content = normalized_message_content(
            bot_id,
            Some(&json!({
                "photo": [{"file_id": "Abc_123-xyz", "file_unique_id": "unique"}]
            })),
            None,
        );
        assert_eq!(content["media"][0]["type"], "photo");
        assert_eq!(
            content["media"][0]["url"],
            format!("/api/bots/{bot_id}/media/Abc_123-xyz")
        );
        assert!(content.to_string().find("bot_token").is_none());
    }

    #[test]
    fn telegram_photo_sizes_normalize_to_one_largest_attachment() {
        let bot_id = Uuid::new_v4();
        let content = normalized_message_content(
            bot_id,
            Some(&json!({
                "photo": [
                    {"file_id": "small", "file_unique_id": "photo", "width": 90, "height": 90},
                    {"file_id": "large", "file_unique_id": "photo", "width": 1280, "height": 1280}
                ],
                "reply_to_message": {
                    "document": {"file_id": "replied-document", "thumbnail": {"file_id": "thumb"}}
                }
            })),
            None,
        );
        let media = content["media"].as_array().expect("media array");
        assert_eq!(media.len(), 1);
        assert_eq!(media[0]["file_id"], "large");
    }

    #[test]
    fn health_payload_exposes_the_running_deployment_revision() {
        let payload = health_payload(true, "sha-abc123");

        assert_eq!(payload["status"], "ok");
        assert_eq!(payload["database"], true);
        assert_eq!(payload["version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(payload["deployment_revision"], "sha-abc123");
    }

    #[test]
    fn connect_request_needs_only_the_bot_token() {
        let request: ConnectBotRequest = serde_json::from_value(json!({
            "token": "123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdef"
        }))
        .expect("token-only connect request should deserialize");
        assert_eq!(request.token, "123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdef");
        assert!(request.pool.is_none());
        assert!(!request.test_dc);
        assert!(request.existing_webhook_secret.is_none());
        assert!(!request.existing_webhook_has_no_secret);
        assert!(request.existing_webhook_ip_address.is_none());
        assert!(!request.existing_webhook_has_no_ip_address);
        assert_eq!(
            connect_target_pool(request.pool.as_deref()).unwrap(),
            DataPlanePool::Standard
        );
    }

    #[test]
    fn connect_request_accepts_the_telegram_test_environment() {
        let request: ConnectBotRequest = serde_json::from_value(json!({
            "token": "123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdef",
            "test_dc": true
        }))
        .expect("test-environment connect request should deserialize");

        assert!(request.test_dc);
    }

    #[test]
    fn managed_webhook_recovery_keeps_secret_input_explicit() {
        let with_secret: ManagedWebhookRecoveryRequest = serde_json::from_value(json!({
            "existing_webhook_secret": "Current_secret-1"
        }))
        .expect("secret recovery request should deserialize");
        assert_eq!(
            with_secret.existing_webhook_secret.as_deref(),
            Some("Current_secret-1")
        );
        assert!(!with_secret.existing_webhook_has_no_secret);
        assert!(with_secret.existing_webhook_ip_address.is_none());
        assert!(!with_secret.existing_webhook_has_no_ip_address);

        let without_secret: ManagedWebhookRecoveryRequest = serde_json::from_value(json!({
            "existing_webhook_has_no_secret": true
        }))
        .expect("no-secret recovery request should deserialize");
        assert!(without_secret.existing_webhook_secret.is_none());
        assert!(without_secret.existing_webhook_has_no_secret);

        let fixed_ip: ManagedWebhookRecoveryRequest = serde_json::from_value(json!({
            "existing_webhook_secret": "Current_secret-1",
            "existing_webhook_ip_address": "203.0.113.9"
        }))
        .expect("fixed-IP recovery request should deserialize");
        assert_eq!(
            fixed_ip.existing_webhook_ip_address.as_deref(),
            Some("203.0.113.9")
        );
        assert!(!fixed_ip.existing_webhook_has_no_ip_address);
        assert!(without_secret.existing_webhook_has_no_secret);
    }

    #[test]
    fn connect_pool_supports_initial_premium_local_placement() {
        let request: ConnectBotRequest = serde_json::from_value(json!({
            "token": "123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdef",
            "pool": "local"
        }))
        .expect("local connect request should deserialize");

        assert_eq!(
            connect_target_pool(request.pool.as_deref()).unwrap(),
            DataPlanePool::Local
        );
        assert!(connect_target_pool(Some("legacy")).is_err());
    }

    #[test]
    fn telegram_unauthorized_token_has_actionable_connect_error() {
        let error = validate_get_me_response(
            StatusCode::UNAUTHORIZED,
            &json!({
                "ok": false,
                "error_code": 401,
                "description": "Unauthorized"
            }),
        )
        .expect_err("Telegram 401 must reject the token");

        assert!(matches!(
            error,
            AppError::Validation(message)
                if message == "Telegram rejected this token. Check that it is complete and still active in @BotFather."
        ));
    }

    #[test]
    fn other_telegram_connect_errors_keep_their_description() {
        let error = validate_get_me_response(
            StatusCode::BAD_REQUEST,
            &json!({
                "ok": false,
                "error_code": 400,
                "description": "Bad Request: test rejection"
            }),
        )
        .expect_err("Telegram rejection must fail the connection");

        assert!(matches!(
            error,
            AppError::Validation(message) if message == "Bad Request: test rejection"
        ));
    }

    #[test]
    fn imports_supported_existing_webhook_delivery_settings() {
        let webhook = existing_webhook(
            &json!({
                "ok": true,
                "result": {
                    "url": "https://receiver.example/telegram",
                    "has_custom_certificate": false,
                    "allowed_updates": ["message", "callback_query"],
                    "max_connections": 73,
                    "ip_address": "203.0.113.7"
                }
            }),
            API_BASE_URL,
            ExistingWebhookPolicy::Cloud {
                allow_insecure_development: false,
            },
        )
        .expect("valid webhook information should be accepted")
        .expect("non-empty webhook should be imported");

        assert_eq!(webhook.url, "https://receiver.example/telegram");
        assert_eq!(
            webhook.allowed_updates,
            Some(json!(["message", "callback_query"]))
        );
        assert_eq!(webhook.max_connections, 73);
        assert_eq!(webhook.reported_ip_address.as_deref(), Some("203.0.113.7"));
    }

    #[test]
    fn refuses_unsafe_existing_webhook_without_exposing_its_url() {
        let secret_url = "https://127.0.0.1/private-token-path";
        let error = existing_webhook(
            &json!({
                "ok": true,
                "result": {
                    "url": secret_url,
                    "has_custom_certificate": false
                }
            }),
            API_BASE_URL,
            ExistingWebhookPolicy::Cloud {
                allow_insecure_development: false,
            },
        )
        .expect_err("private webhook targets must not be imported");

        assert!(!error.to_string().contains(secret_url));
    }

    #[test]
    fn imports_telegram_default_filter_when_allowed_updates_are_omitted() {
        let webhook = existing_webhook(
            &json!({
                "ok": true,
                "result": {
                    "url": "https://receiver.example/telegram",
                    "has_custom_certificate": false
                }
            }),
            API_BASE_URL,
            ExistingWebhookPolicy::Cloud {
                allow_insecure_development: false,
            },
        )
        .expect("valid webhook information should be accepted")
        .expect("non-empty webhook should be imported");

        assert_eq!(webhook.allowed_updates, None);
    }

    #[test]
    fn refuses_webhook_with_an_unrecoverable_custom_certificate() {
        let error = existing_webhook(
            &json!({
                "ok": true,
                "result": {
                    "url": "https://receiver.example/telegram",
                    "has_custom_certificate": true
                }
            }),
            API_BASE_URL,
            ExistingWebhookPolicy::Cloud {
                allow_insecure_development: false,
            },
        )
        .expect_err("custom certificate cannot be recovered from Telegram");

        assert!(error.to_string().contains("custom certificate"));
    }

    #[test]
    fn refuses_an_invalid_reported_webhook_ipv4_before_transfer() {
        let error = existing_webhook(
            &json!({
                "ok": true,
                "result": {
                    "url": "https://receiver.example/telegram",
                    "has_custom_certificate": false,
                    "ip_address": "2001:db8::1"
                }
            }),
            API_BASE_URL,
            ExistingWebhookPolicy::Cloud {
                allow_insecure_development: false,
            },
        )
        .expect_err("Telegram webhook transfer accepts only a canonical IPv4 address");

        assert!(error.to_string().contains("invalid IPv4 address"));
    }

    #[test]
    fn does_not_import_a_stale_managed_ingress_as_downstream() {
        let webhook = existing_webhook(
            &json!({
                "ok": true,
                "result": {
                    "url": "https://api.phenogram.io/telegram/webhook/phg_stale",
                    "has_custom_certificate": false,
                    "allowed_updates": ["message"],
                    "max_connections": 40
                }
            }),
            API_BASE_URL,
            ExistingWebhookPolicy::Cloud {
                allow_insecure_development: false,
            },
        )
        .expect("stale managed ingress should be handled");

        assert!(webhook.is_none());
    }
}
