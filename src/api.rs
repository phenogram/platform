use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::FromRow;
use uuid::Uuid;

use crate::{
    auth::{AuthUser, active_membership, membership},
    crypto::{Ciphertext, Crypto},
    error::{AppError, Result},
    models::{ActivitySummary, BotRecord, BotSummary, ConversationSummary, UpdateSummary},
    state::AppState,
    telegram::{
        ALL_UPDATE_TYPES, OutboundMessageRecord, decrypt_token, raw_telegram_json,
        record_outbound_message, telegram_json_for_bot, validate_webhook_url,
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
        Json(json!({
            "status": if database { "ok" } else { "degraded" },
            "database": database,
            "version": env!("CARGO_PKG_VERSION")
        })),
    )
        .into_response()
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

#[derive(Debug, Deserialize)]
pub struct ConnectBotRequest {
    token: String,
}

#[derive(Debug)]
struct ExistingWebhook {
    url: String,
    allowed_updates: Value,
    max_connections: i32,
}

#[derive(Debug, Serialize)]
pub struct ConnectBotResponse {
    bot: BotSummary,
    warnings: Vec<String>,
}

pub async fn connect_bot(
    State(state): State<AppState>,
    user: AuthUser,
    Json(input): Json<ConnectBotRequest>,
) -> Result<(StatusCode, Json<ConnectBotResponse>)> {
    let token = input.token.trim().to_owned();
    validate_bot_token(&token)?;
    let membership = active_membership(&state, user.id).await?;
    let bot_count = sqlx::query_scalar::<_, i64>("SELECT count(*) FROM bots WHERE user_id = $1")
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

    let (_, me) = raw_telegram_json(
        &state.telegram,
        &state.config.telegram_cloud_api_url,
        &token,
        "getMe",
        &json!({}),
    )
    .await?;
    if me.get("ok").and_then(Value::as_bool) != Some(true) {
        return Err(AppError::Validation(
            me.get("description")
                .and_then(Value::as_str)
                .unwrap_or("Telegram rejected this bot token")
                .to_owned(),
        ));
    }
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

    if let Some(existing_owner) =
        sqlx::query_scalar::<_, Uuid>("SELECT user_id FROM bots WHERE telegram_bot_id = $1")
            .bind(telegram_bot_id)
            .fetch_optional(&state.db)
            .await?
    {
        return Err(if existing_owner == user.id {
            AppError::Conflict("This bot is already connected to your account".into())
        } else {
            AppError::Conflict("This bot is already connected to another Phenogram account".into())
        });
    }

    let (_, webhook_info) = raw_telegram_json(
        &state.telegram,
        &state.config.telegram_cloud_api_url,
        &token,
        "getWebhookInfo",
        &json!({}),
    )
    .await?;
    let previous_webhook = existing_webhook(
        &webhook_info,
        &state.config.api_base_url,
        state.config.app_env != "production",
    )?;

    let bot_id = Uuid::new_v4();
    let public_id = state.crypto.bot_public_id(&token);
    let token_fingerprint = Crypto::token_fingerprint(&token);
    let token_encrypted = state
        .crypto
        .encrypt(token.as_bytes(), format!("bot:{bot_id}:token").as_bytes())?;
    let ingress_secret = Crypto::random_token(32)?;
    let ingress_encrypted = state.crypto.encrypt(
        ingress_secret.as_bytes(),
        format!("bot:{bot_id}:ingress-secret").as_bytes(),
    )?;

    let mut tx = state.db.begin().await?;
    let insert = sqlx::query(
        r#"INSERT INTO bots
               (id, user_id, telegram_bot_id, username, display_name, token_ciphertext, token_nonce,
                token_fingerprint, public_id, ingress_secret_ciphertext, ingress_secret_nonce)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)"#,
    )
    .bind(bot_id)
    .bind(user.id)
    .bind(telegram_bot_id)
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
               (bot_id, allowed_updates, downstream_webhook_url, max_connections)
           VALUES ($1, $2, $3, $4)"#,
    )
    .bind(bot_id)
    .bind(
        previous_webhook
            .as_ref()
            .map(|webhook| &webhook.allowed_updates),
    )
    .bind(
        previous_webhook
            .as_ref()
            .map(|webhook| webhook.url.as_str()),
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
                  now() + make_interval(days => plans.retention_days)
             FROM bots
             JOIN memberships ON memberships.user_id = bots.user_id
             JOIN plan_definitions plans ON plans.id = memberships.plan_id
            WHERE bots.id = $2"#,
    )
    .bind(user.id)
    .bind(bot_id)
    .bind(json!({"telegram_bot_id": telegram_bot_id, "username": username, "migrated_webhook": previous_webhook.is_some()}))
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;

    let webhook_url = format!(
        "{}/telegram/webhook/{}",
        state.config.api_base_url, public_id
    );
    let webhook_result = raw_telegram_json(
        &state.telegram,
        &state.config.telegram_cloud_api_url,
        &token,
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
    if previous_webhook.is_some() && provisioned {
        // Telegram's getWebhookInfo response deliberately omits secret_token,
        // so the previous downstream authentication header cannot be recovered.
        warnings.push(
            "Webhook transferred automatically. If the existing receiver validates Telegram's secret-token header, configure that secret again with setWebhook through Phenogram."
                .into(),
        );
    }
    if !provisioned {
        warnings.push("The bot was saved, but Telegram did not accept the Phenogram webhook. Retry setup from bot settings.".into());
    }
    let bot = get_bot_summary(&state, user.id, bot_id).await?;
    Ok((
        StatusCode::CREATED,
        Json(ConnectBotResponse { bot, warnings }),
    ))
}

fn existing_webhook(
    webhook_info: &Value,
    api_base_url: &str,
    allow_insecure_development: bool,
) -> Result<Option<ExistingWebhook>> {
    if webhook_info.get("ok").and_then(Value::as_bool) != Some(true) {
        return Err(AppError::Upstream(
            webhook_info
                .get("description")
                .and_then(Value::as_str)
                .unwrap_or("Telegram did not return webhook information")
                .to_owned(),
        ));
    }
    let result = webhook_info
        .get("result")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            AppError::Upstream("Telegram returned invalid webhook information".into())
        })?;
    let Some(url) = result
        .get("url")
        .and_then(Value::as_str)
        .filter(|url| !url.is_empty())
    else {
        return Ok(None);
    };
    if is_managed_ingress_url(url, api_base_url) {
        return Ok(None);
    }
    if result
        .get("has_custom_certificate")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return Err(AppError::Validation(
            "The bot's existing webhook uses a custom certificate that Telegram cannot transfer. Switch it to a publicly trusted certificate and try again."
                .into(),
        ));
    }
    validate_webhook_url(url, allow_insecure_development).map_err(|_| {
        AppError::Validation(
            "The bot's existing webhook cannot be transferred safely. Update it in Telegram and try again."
                .into(),
        )
    })?;
    let allowed_updates = result
        .get("allowed_updates")
        .filter(|value| {
            value
                .as_array()
                .is_some_and(|items| items.iter().all(Value::is_string))
        })
        .cloned()
        .unwrap_or_else(|| json!([]));
    let max_connections = result
        .get("max_connections")
        .and_then(Value::as_i64)
        .and_then(|value| i32::try_from(value).ok())
        .filter(|value| (1..=100).contains(value))
        .unwrap_or(40);
    Ok(Some(ExistingWebhook {
        url: url.to_owned(),
        allowed_updates,
        max_connections,
    }))
}

fn is_managed_ingress_url(candidate: &str, api_base_url: &str) -> bool {
    let (Ok(candidate), Ok(api_base)) = (url::Url::parse(candidate), url::Url::parse(api_base_url))
    else {
        return false;
    };
    candidate.scheme() == api_base.scheme()
        && candidate
            .host_str()
            .zip(api_base.host_str())
            .is_some_and(|(candidate, api)| candidate.eq_ignore_ascii_case(api))
        && candidate.port_or_known_default() == api_base.port_or_known_default()
        && candidate
            .path()
            .strip_prefix("/telegram/webhook/")
            .is_some_and(|public_id| !public_id.is_empty() && !public_id.contains('/'))
}

pub async fn provision_bot(
    State(state): State<AppState>,
    user: AuthUser,
    Path(bot_id): Path<Uuid>,
) -> Result<Json<Value>> {
    active_membership(&state, user.id).await?;
    let bot = get_bot_record(&state, user.id, bot_id).await?;
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
                  now() + make_interval(days => plans.retention_days)
             FROM bots
             JOIN memberships ON memberships.user_id = bots.user_id
             JOIN plan_definitions plans ON plans.id = memberships.plan_id
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

pub async fn list_bots(State(state): State<AppState>, user: AuthUser) -> Result<Json<Value>> {
    let bots = sqlx::query_as::<_, BotSummary>(
        r#"SELECT id, telegram_bot_id, username, display_name, public_id,
                  status, routing_mode, update_mode, last_update_at, last_api_call_at, created_at
             FROM bots WHERE user_id = $1 ORDER BY created_at DESC"#,
    )
    .bind(user.id)
    .fetch_all(&state.db)
    .await?;
    Ok(Json(json!({"bots": bots})))
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
            "api_base": format!("{}/bot${{BOT_TOKEN}}", state.config.api_base_url),
            "public_id": bot.public_id,
            "retention_days": membership.retention_days
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
    let search = query
        .query
        .as_deref()
        .map(|value| format!("%{}%", value.chars().take(120).collect::<String>()));
    let updates = sqlx::query_as::<_, UpdateSummary>(
        r#"SELECT id, update_id, event_type, chat_id, telegram_user_id, payload, received_at, expires_at
             FROM updates
            WHERE bot_id = $1
              AND ($2::text IS NULL OR event_type = $2)
              AND ($3::text IS NULL OR payload::text ILIKE $3)
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
    .fetch_all(&state.db)
    .await?;
    Ok(Json(
        json!({"updates": updates, "next_before": updates.last().map(|update| update.id)}),
    ))
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
        r#"SELECT chat_id, chat_type, title, username, display_name, last_message_preview, last_update_at
             FROM conversations WHERE bot_id = $1 ORDER BY last_update_at DESC LIMIT 250"#,
    )
    .bind(bot_id)
    .fetch_all(&state.db)
    .await?;
    Ok(Json(json!({"conversations": conversations})))
}

#[derive(Debug, Serialize)]
struct TimelineMessage {
    id: String,
    event_type: String,
    direction: &'static str,
    text: Option<String>,
    status: String,
    created_at: DateTime<Utc>,
    payload: Option<Value>,
}

pub async fn conversation_messages(
    State(state): State<AppState>,
    user: AuthUser,
    Path((bot_id, chat_id)): Path<(Uuid, i64)>,
) -> Result<Json<Value>> {
    assert_bot_owner(&state, user.id, bot_id).await?;
    let incoming = sqlx::query_as::<_, (i64, String, Value, DateTime<Utc>)>(
        r#"SELECT id, event_type, payload, received_at
             FROM updates
            WHERE bot_id = $1 AND chat_id = $2
            ORDER BY received_at DESC LIMIT 250"#,
    )
    .bind(bot_id)
    .bind(chat_id)
    .fetch_all(&state.db)
    .await?;
    let outgoing = sqlx::query_as::<_, (i64, String, Option<String>, String, DateTime<Utc>)>(
        r#"SELECT id, method, text, status, created_at
             FROM outbound_messages
            WHERE bot_id = $1 AND chat_id = $2
            ORDER BY created_at DESC LIMIT 250"#,
    )
    .bind(bot_id)
    .bind(chat_id)
    .fetch_all(&state.db)
    .await?;
    let mut messages = Vec::with_capacity(incoming.len() + outgoing.len());
    messages.extend(
        incoming
            .into_iter()
            .map(|(id, event_type, payload, created_at)| TimelineMessage {
                id: format!("in-{id}"),
                text: telegram_message_text(&payload),
                event_type,
                direction: "incoming",
                status: "received".into(),
                created_at,
                payload: Some(payload),
            }),
    );
    messages.extend(
        outgoing
            .into_iter()
            .map(|(id, method, text, status, created_at)| TimelineMessage {
                id: format!("out-{id}"),
                text,
                event_type: method,
                direction: "outgoing",
                status,
                created_at,
                payload: None,
            }),
    );
    messages.sort_by_key(|message| message.created_at);
    Ok(Json(json!({"messages": messages})))
}

#[derive(Debug, Deserialize)]
pub struct SendMessageRequest {
    chat_id: i64,
    text: String,
}

pub async fn send_message(
    State(state): State<AppState>,
    user: AuthUser,
    Path(bot_id): Path<Uuid>,
    Json(input): Json<SendMessageRequest>,
) -> Result<Json<Value>> {
    active_membership(&state, user.id).await?;
    if input.text.trim().is_empty() || input.text.chars().count() > 4096 {
        return Err(AppError::Validation(
            "Message text must contain 1 to 4096 characters".into(),
        ));
    }
    let bot = get_bot_record(&state, user.id, bot_id).await?;
    let text = input.text.trim().to_owned();
    let response = telegram_json_for_bot(
        &state,
        &bot,
        "sendMessage",
        &json!({"chat_id": input.chat_id, "text": text}),
        "bot_view",
    )
    .await;
    let (status, telegram_message_id, error_summary) = match &response {
        Ok(body) => (
            "sent",
            body.pointer("/result/message_id").and_then(Value::as_i64),
            None,
        ),
        Err(error) => ("failed", None, Some(error.to_string())),
    };
    record_outbound_message(
        &state,
        OutboundMessageRecord {
            bot_id,
            user_id: Some(user.id),
            chat_id: input.chat_id,
            telegram_message_id,
            method: "sendMessage",
            source: "bot_view",
            text: Some(&text),
            status,
            response_status: None,
            error_summary: error_summary.as_deref(),
        },
    )
    .await?;
    let response = response?;
    sqlx::query(
        r#"INSERT INTO audit_log (user_id, bot_id, action, metadata, expires_at)
           SELECT $1, bots.id, 'bot_view.message_sent', $3,
                  now() + make_interval(days => plans.retention_days)
             FROM bots
             JOIN memberships ON memberships.user_id = bots.user_id
             JOIN plan_definitions plans ON plans.id = memberships.plan_id
            WHERE bots.id = $2"#,
    )
    .bind(user.id)
    .bind(bot_id)
    .bind(json!({"chat_id": input.chat_id}))
    .execute(&state.db)
    .await?;
    Ok(Json(response))
}

fn telegram_message_text(payload: &Value) -> Option<String> {
    let event = payload
        .as_object()?
        .iter()
        .find(|(key, _)| key.as_str() != "update_id")?
        .1;
    let message = event
        .get("message")
        .or_else(|| event.get("edited_message"))
        .unwrap_or(event);
    message
        .get("text")
        .or_else(|| message.get("caption"))
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or_else(|| {
            event
                .get("data")
                .and_then(Value::as_str)
                .map(|data| format!("Button: {data}"))
        })
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
                  now() + make_interval(days => plans.retention_days)
             FROM bots
             JOIN memberships ON memberships.user_id = bots.user_id
             JOIN plan_definitions plans ON plans.id = memberships.plan_id
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
    let bot = get_bot_summary(&state, user.id, bot_id).await?;
    if input.file_path.is_empty()
        || input.file_path.starts_with('/')
        || input.file_path.contains("..")
        || input.file_path.contains(['?', '#', '\\'])
    {
        return Err(AppError::Validation("Invalid Telegram file path".into()));
    }
    let ttl = input.expires_in_seconds.unwrap_or(3600).clamp(60, 604_800);
    let expires = Utc::now().timestamp() + ttl;
    let sig = state
        .crypto
        .sign_file_link(&bot.public_id, &input.file_path, expires);
    let mut url = url::Url::parse(&state.config.api_base_url).map_err(|_| AppError::Internal)?;
    {
        let mut segments = url.path_segments_mut().map_err(|_| AppError::Internal)?;
        segments
            .clear()
            .push("public")
            .push(&bot.public_id)
            .push("files");
        for segment in input.file_path.split('/') {
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

#[derive(Debug, Deserialize)]
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
        let (_, logout) = raw_telegram_json(
            &state.telegram,
            &state.config.telegram_cloud_api_url,
            token,
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
        let (_, logout) =
            raw_telegram_json(&state.telegram, local_base, token, "logOut", &json!({})).await?;
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
                  now() + make_interval(days => plans.retention_days)
             FROM bots
             JOIN memberships ON memberships.user_id = bots.user_id
             JOIN plan_definitions plans ON plans.id = memberships.plan_id
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

async fn install_managed_webhook(state: &AppState, bot: &BotRecord) -> Result<bool> {
    let token = decrypt_token(state, bot)?;
    let token = std::str::from_utf8(&token).map_err(|_| AppError::Internal)?;
    let ingress_secret = state.crypto.decrypt(
        &Ciphertext {
            data: bot.ingress_secret_ciphertext.clone(),
            nonce: bot.ingress_secret_nonce.clone(),
        },
        format!("bot:{}:ingress-secret", bot.id).as_bytes(),
    )?;
    let ingress_secret = std::str::from_utf8(&ingress_secret).map_err(|_| AppError::Internal)?;
    let base = if bot.routing_mode == "local" {
        state
            .config
            .telegram_local_api_url
            .as_deref()
            .ok_or_else(|| AppError::Conflict("Local Bot API routing is not configured".into()))?
    } else {
        &state.config.telegram_cloud_api_url
    };
    let webhook_url = format!(
        "{}/telegram/webhook/{}",
        state.config.api_base_url, bot.public_id
    );
    let (_, response) = raw_telegram_json(
        &state.telegram,
        base,
        token,
        "setWebhook",
        &json!({
            "url": webhook_url,
            "secret_token": ingress_secret,
            "allowed_updates": ALL_UPDATE_TYPES,
            "drop_pending_updates": false
        }),
    )
    .await?;
    Ok(response.get("ok").and_then(Value::as_bool) == Some(true))
}

pub async fn delete_bot(
    State(state): State<AppState>,
    user: AuthUser,
    Path(bot_id): Path<Uuid>,
) -> Result<Json<Value>> {
    let bot = get_bot_record(&state, user.id, bot_id).await?;
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
    let cleanup = raw_telegram_json(
        &state.telegram,
        base,
        std::str::from_utf8(&token).unwrap_or(""),
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
        r#"SELECT id, telegram_bot_id, username, display_name, public_id,
                  status, routing_mode, update_mode, last_update_at, last_api_call_at, created_at
             FROM bots WHERE id = $1 AND user_id = $2"#,
    )
    .bind(bot_id)
    .bind(user_id)
    .fetch_optional(&state.db)
    .await?
    .ok_or(AppError::NotFound)
}

async fn get_bot_record(state: &AppState, user_id: Uuid, bot_id: Uuid) -> Result<BotRecord> {
    sqlx::query_as::<_, BotRecord>(
        r#"SELECT id, user_id, telegram_bot_id, username, display_name,
                  token_ciphertext, token_nonce, token_fingerprint, public_id,
                  ingress_secret_ciphertext, ingress_secret_nonce, status,
                  routing_mode, update_mode, last_update_at, last_api_call_at, created_at
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{ConnectBotRequest, existing_webhook};

    const API_BASE_URL: &str = "https://api.phenogram.io";

    #[test]
    fn connect_request_needs_only_the_bot_token() {
        let request: ConnectBotRequest = serde_json::from_value(json!({
            "token": "123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdef"
        }))
        .expect("token-only connect request should deserialize");
        assert_eq!(request.token, "123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdef");
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
                    "max_connections": 73
                }
            }),
            API_BASE_URL,
            false,
        )
        .expect("valid webhook information should be accepted")
        .expect("non-empty webhook should be imported");

        assert_eq!(webhook.url, "https://receiver.example/telegram");
        assert_eq!(
            webhook.allowed_updates,
            json!(["message", "callback_query"])
        );
        assert_eq!(webhook.max_connections, 73);
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
            false,
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
            false,
        )
        .expect("valid webhook information should be accepted")
        .expect("non-empty webhook should be imported");

        assert_eq!(webhook.allowed_updates, json!([]));
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
            false,
        )
        .expect_err("custom certificate cannot be recovered from Telegram");

        assert!(error.to_string().contains("custom certificate"));
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
            false,
        )
        .expect("stale managed ingress should be handled");

        assert!(webhook.is_none());
    }
}
