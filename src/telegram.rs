use std::{
    convert::Infallible,
    io::SeekFrom,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    time::{Duration, Instant},
};

use axum::{
    Json,
    body::{Body, to_bytes},
    extract::{Path, Query, Request, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{
        IntoResponse, Response,
        sse::{Event, Sse},
    },
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::Utc;
use futures_util::{StreamExt, TryStreamExt};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use sqlx::FromRow;
use subtle::ConstantTimeEq;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::io::ReaderStream;
use uuid::Uuid;
use zeroize::Zeroizing;

use crate::{
    crypto::{Ciphertext, Crypto},
    error::{AppError, Result},
    models::BotRecord,
    state::{AppState, StoredUpdate},
};

const SPECIAL_BODY_LIMIT: usize = 2 * 1024 * 1024;
const SSE_REPLAY_ROW_LIMIT: usize = 5_000;
const SSE_REPLAY_BYTE_LIMIT: usize = 8 * 1024 * 1024;
const SSE_REPLAY_EVENT_BYTE_LIMIT: usize = SPECIAL_BODY_LIMIT + 64 * 1024;
pub const ALL_UPDATE_TYPES: &[&str] = &[
    "message",
    "edited_message",
    "channel_post",
    "edited_channel_post",
    "business_connection",
    "business_message",
    "edited_business_message",
    "deleted_business_messages",
    "guest_message",
    "message_reaction",
    "message_reaction_count",
    "inline_query",
    "chosen_inline_result",
    "callback_query",
    "shipping_query",
    "pre_checkout_query",
    "purchased_paid_media",
    "poll",
    "poll_answer",
    "my_chat_member",
    "chat_member",
    "chat_join_request",
    "chat_boost",
    "removed_chat_boost",
    "managed_bot",
    "subscription",
];

pub async fn proxy_method(
    State(state): State<AppState>,
    Path((token, method)): Path<(String, String)>,
    request: Request,
) -> Response {
    if !valid_method_name(&method) {
        return telegram_error(
            StatusCode::BAD_REQUEST,
            400,
            "Bad Request: invalid method name",
        );
    }
    let bot = match resolve_bot_by_token(&state, &token).await {
        Ok(bot) => bot,
        Err(_) => return telegram_error(StatusCode::UNAUTHORIZED, 401, "Unauthorized"),
    };
    match method.to_ascii_lowercase().as_str() {
        "getupdates" => virtual_get_updates(state, bot, request).await,
        "setwebhook" => virtual_set_webhook(state, bot, request).await,
        "deletewebhook" => virtual_delete_webhook(state, bot, request).await,
        "getwebhookinfo" => virtual_get_webhook_info(state, bot).await,
        "getfile" if bot.routing_mode == "local" => {
            virtual_local_get_file(state, bot, request).await
        }
        "logout" | "close" if bot.routing_mode == "local" => telegram_error(
            StatusCode::CONFLICT,
            409,
            "Managed local Bot API lifecycle methods must be performed from Phenogram settings",
        ),
        _ => match forward_method(&state, &bot, &token, &method, request).await {
            Ok(response) => response,
            Err(error) => {
                tracing::warn!(bot_id = %bot.id, method, error = ?error, "Telegram proxy failed");
                telegram_error(
                    StatusCode::BAD_GATEWAY,
                    502,
                    "Bad Gateway: Telegram is unavailable",
                )
            }
        },
    }
}

fn valid_method_name(method: &str) -> bool {
    !method.is_empty()
        && method.len() <= 128
        && method
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

pub async fn proxy_file(
    State(state): State<AppState>,
    Path((token, file_path)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let bot = match resolve_bot_by_token(&state, &token).await {
        Ok(bot) => bot,
        Err(_) => return telegram_error(StatusCode::UNAUTHORIZED, 401, "Unauthorized"),
    };
    match forward_file(
        &state,
        &bot,
        &token,
        &file_path,
        headers.get(header::RANGE).cloned(),
    )
    .await
    {
        Ok(response) => response,
        Err(error) => {
            tracing::warn!(bot_id = %bot.id, error = ?error, "Telegram file proxy failed");
            telegram_error(
                StatusCode::BAD_GATEWAY,
                502,
                "Bad Gateway: Telegram is unavailable",
            )
        }
    }
}

#[derive(Deserialize)]
pub struct PublicFileQuery {
    expires: i64,
    sig: String,
}

pub async fn public_file(
    State(state): State<AppState>,
    Path((public_id, file_path)): Path<(String, String)>,
    Query(query): Query<PublicFileQuery>,
    headers: HeaderMap,
) -> Response {
    if !state.crypto.verify_file_link(
        &public_id,
        &file_path,
        query.expires,
        &query.sig,
        Utc::now().timestamp(),
    ) {
        return (StatusCode::FORBIDDEN, Json(json!({"error": {"code": "invalid_link", "message": "This file link is invalid or expired"}}))).into_response();
    }
    let bot = match find_bot_by_public_id(&state, &public_id).await {
        Ok(Some(bot)) => bot,
        _ => return AppError::NotFound.into_response(),
    };
    let token = match decrypt_token(&state, &bot) {
        Ok(token) => token,
        Err(error) => return error.into_response(),
    };
    match forward_file(
        &state,
        &bot,
        std::str::from_utf8(&token).unwrap_or(""),
        &file_path,
        headers.get(header::RANGE).cloned(),
    )
    .await
    {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}

async fn forward_method(
    state: &AppState,
    bot: &BotRecord,
    token: &str,
    method_name: &str,
    request: Request,
) -> Result<Response> {
    let started = Instant::now();
    let query_raw = request.uri().query().unwrap_or("").to_owned();
    let query = if query_raw.is_empty() {
        String::new()
    } else {
        format!("?{query_raw}")
    };
    let url = format!(
        "{}/bot{}/{}{}",
        bot_api_base(state, bot)?,
        token,
        method_name,
        query
    );
    let method = request.method().clone();
    let content_type = request.headers().get(header::CONTENT_TYPE).cloned();
    let content_length = request.headers().get(header::CONTENT_LENGTH).cloned();
    let capture_candidate = method_name.eq_ignore_ascii_case("sendMessage")
        && !content_type
            .as_ref()
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("multipart/form-data"));
    let body = request.into_body();
    let (upstream_body, capture) = if capture_candidate {
        let bytes = to_bytes(body, 65_536)
            .await
            .map_err(|_| AppError::Validation("sendMessage body is too large to inspect".into()))?;
        let capture = outbound_capture(
            &query_raw,
            content_type.as_ref().and_then(|value| value.to_str().ok()),
            &bytes,
        );
        (reqwest::Body::from(bytes), capture)
    } else {
        (reqwest::Body::wrap_stream(body.into_data_stream()), None)
    };
    let mut upstream = state.telegram.request(method, url).body(upstream_body);
    if let Some(value) = content_type {
        upstream = upstream.header(header::CONTENT_TYPE, value);
    }
    if let Some(value) = content_length {
        upstream = upstream.header(header::CONTENT_LENGTH, value);
    }
    let response = match upstream.send().await {
        Ok(response) => response,
        Err(error) => {
            let error = error.without_url().to_string();
            if let Some(capture) = &capture {
                let _ = record_outbound_message(
                    state,
                    OutboundMessageRecord {
                        bot_id: bot.id,
                        user_id: None,
                        chat_id: capture.chat_id,
                        telegram_message_id: None,
                        method: method_name,
                        source: "proxy",
                        text: capture.text.as_deref(),
                        status: "failed",
                        response_status: None,
                        error_summary: Some(&error),
                    },
                )
                .await;
            }
            return Err(AppError::Upstream(error));
        }
    };
    let status = response.status();
    let response_headers = response.headers().clone();
    record_api_call(
        state,
        bot.id,
        method_name,
        "proxy",
        Some(status.as_u16() as i32),
        None,
        started.elapsed(),
        None,
    )
    .await;
    if let Some(capture) = &capture
        && let Err(error) = record_outbound_message(
            state,
            OutboundMessageRecord {
                bot_id: bot.id,
                user_id: None,
                chat_id: capture.chat_id,
                telegram_message_id: None,
                method: method_name,
                source: "proxy",
                text: capture.text.as_deref(),
                status: if status.is_success() {
                    "sent"
                } else {
                    "failed"
                },
                response_status: Some(status.as_u16() as i32),
                error_summary: None,
            },
        )
        .await
    {
        tracing::warn!(bot_id = %bot.id, error = ?error, "could not record proxied outbound message");
    }
    stream_response(status, response_headers, response)
}

struct OutboundCapture {
    chat_id: i64,
    text: Option<String>,
}

fn outbound_capture(
    query: &str,
    content_type: Option<&str>,
    body: &[u8],
) -> Option<OutboundCapture> {
    let mut params = Map::new();
    for (key, value) in serde_urlencoded::from_str::<Vec<(String, String)>>(query).ok()? {
        params.insert(key, Value::String(value));
    }
    if !body.is_empty() {
        if content_type.is_some_and(|value| value.starts_with("application/json")) {
            params.extend(
                serde_json::from_slice::<Value>(body)
                    .ok()?
                    .as_object()?
                    .clone(),
            );
        } else if content_type
            .unwrap_or("application/x-www-form-urlencoded")
            .starts_with("application/x-www-form-urlencoded")
        {
            for (key, value) in serde_urlencoded::from_bytes::<Vec<(String, String)>>(body).ok()? {
                params.insert(key, Value::String(value));
            }
        }
    }
    Some(OutboundCapture {
        chat_id: params
            .get("chat_id")
            .and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()))?,
        text: params
            .get("text")
            .and_then(Value::as_str)
            .map(str::to_owned),
    })
}

async fn forward_file(
    state: &AppState,
    bot: &BotRecord,
    token: &str,
    file_path: &str,
    range: Option<HeaderValue>,
) -> Result<Response> {
    if bot.routing_mode == "local"
        && let Some(encoded) = file_path.strip_prefix("__phenogram_local__/")
    {
        let path = decode_local_file_path(state, bot, encoded)?;
        return stream_local_file(state, &path, range.as_ref()).await;
    }
    if file_path.contains("..")
        || file_path.starts_with('/')
        || file_path.contains(['?', '#', '\\'])
    {
        return Err(AppError::Validation("Invalid Telegram file path".into()));
    }
    let url = format!(
        "{}/file/bot{}/{}",
        bot_api_base(state, bot)?,
        token,
        file_path
    );
    let mut request = state.telegram.get(url);
    if let Some(range) = range {
        request = request.header(header::RANGE, range);
    }
    let response = request
        .send()
        .await
        .map_err(|error| AppError::Upstream(error.without_url().to_string()))?;
    stream_response(response.status(), response.headers().clone(), response)
}

async fn virtual_local_get_file(state: AppState, bot: BotRecord, request: Request) -> Response {
    let params = match params_from_request(request).await {
        Ok(params) => Value::Object(params),
        Err(message) => return telegram_error(StatusCode::BAD_REQUEST, 400, &message),
    };
    let token = match decrypt_token(&state, &bot) {
        Ok(token) => token,
        Err(_) => {
            return telegram_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                500,
                "Internal Server Error",
            );
        }
    };
    let token = match std::str::from_utf8(&token) {
        Ok(token) => token,
        Err(_) => {
            return telegram_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                500,
                "Internal Server Error",
            );
        }
    };
    let base = match bot_api_base(&state, &bot) {
        Ok(base) => base,
        Err(_) => {
            return telegram_error(
                StatusCode::BAD_GATEWAY,
                502,
                "Bad Gateway: local Telegram API is unavailable",
            );
        }
    };
    let started = Instant::now();
    let (status, mut body) =
        match raw_telegram_json(&state.telegram, base, token, "getFile", &params).await {
            Ok(response) => response,
            Err(_) => {
                return telegram_error(
                    StatusCode::BAD_GATEWAY,
                    502,
                    "Bad Gateway: local Telegram API is unavailable",
                );
            }
        };
    record_api_call(
        &state,
        bot.id,
        "getFile",
        "proxy",
        Some(status.as_u16() as i32),
        body.get("ok").and_then(Value::as_bool),
        started.elapsed(),
        body.get("description")
            .and_then(Value::as_str)
            .map(truncate_error),
    )
    .await;
    if let Some(path) = body
        .pointer("/result/file_path")
        .and_then(Value::as_str)
        .filter(|path| path.starts_with('/'))
    {
        let encoded = match encode_local_file_path(&state, &bot, path) {
            Ok(encoded) => encoded,
            Err(_) => {
                return telegram_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    500,
                    "Internal Server Error",
                );
            }
        };
        if let Some(result) = body.get_mut("result").and_then(Value::as_object_mut) {
            result.insert(
                "file_path".into(),
                Value::String(format!("__phenogram_local__/{encoded}")),
            );
        }
    }
    (status, Json(body)).into_response()
}

fn encode_local_file_path(state: &AppState, bot: &BotRecord, path: &str) -> Result<String> {
    let encrypted = state.crypto.encrypt(
        path.as_bytes(),
        format!("bot:{}:local-file", bot.id).as_bytes(),
    )?;
    let mut value = encrypted.nonce;
    value.extend_from_slice(&encrypted.data);
    Ok(URL_SAFE_NO_PAD.encode(value))
}

fn decode_local_file_path(state: &AppState, bot: &BotRecord, encoded: &str) -> Result<String> {
    let value = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| AppError::Validation("Invalid local Telegram file path".into()))?;
    if value.len() <= 24 {
        return Err(AppError::Validation(
            "Invalid local Telegram file path".into(),
        ));
    }
    let decrypted = state.crypto.decrypt(
        &Ciphertext {
            nonce: value[..24].to_vec(),
            data: value[24..].to_vec(),
        },
        format!("bot:{}:local-file", bot.id).as_bytes(),
    )?;
    String::from_utf8(decrypted.to_vec())
        .map_err(|_| AppError::Validation("Invalid local Telegram file path".into()))
}

async fn stream_local_file(
    state: &AppState,
    path: &str,
    range: Option<&HeaderValue>,
) -> Result<Response> {
    let configured_root = state
        .config
        .telegram_local_data_dir
        .as_ref()
        .ok_or_else(|| AppError::Upstream("Local Telegram file storage is not mounted".into()))?;
    let root = tokio::fs::canonicalize(configured_root)
        .await
        .map_err(|_| AppError::Upstream("Local Telegram file storage is unavailable".into()))?;
    let canonical = tokio::fs::canonicalize(path)
        .await
        .map_err(|_| AppError::NotFound)?;
    if !canonical.starts_with(&root) {
        return Err(AppError::Forbidden);
    }
    let mut file = tokio::fs::File::open(&canonical)
        .await
        .map_err(|_| AppError::NotFound)?;
    let total = file.metadata().await.map_err(|_| AppError::NotFound)?.len();
    let requested = match range {
        Some(value) => match value
            .to_str()
            .ok()
            .and_then(|value| byte_range(value, total))
        {
            Some(range) => Some(range),
            None => {
                return Response::builder()
                    .status(StatusCode::RANGE_NOT_SATISFIABLE)
                    .header(header::CONTENT_RANGE, format!("bytes */{total}"))
                    .body(Body::empty())
                    .map_err(|_| AppError::Internal);
            }
        },
        None => None,
    };
    let (status, start, end) = requested
        .map(|(start, end)| (StatusCode::PARTIAL_CONTENT, start, end))
        .unwrap_or((StatusCode::OK, 0, total.saturating_sub(1)));
    let length = if total == 0 { 0 } else { end - start + 1 };
    if start > 0 {
        file.seek(SeekFrom::Start(start))
            .await
            .map_err(|_| AppError::NotFound)?;
    }
    let mut builder = Response::builder()
        .status(status)
        .header(header::CONTENT_LENGTH, length)
        .header(header::ACCEPT_RANGES, "bytes")
        .header(
            header::CONTENT_TYPE,
            mime_guess::from_path(&canonical)
                .first_or_octet_stream()
                .as_ref(),
        )
        .header(header::CACHE_CONTROL, "private, no-store");
    if status == StatusCode::PARTIAL_CONTENT {
        builder = builder.header(
            header::CONTENT_RANGE,
            format!("bytes {start}-{end}/{total}"),
        );
    }
    builder
        .body(Body::from_stream(ReaderStream::new(file.take(length))))
        .map_err(|_| AppError::Internal)
}

fn byte_range(value: &str, total: u64) -> Option<(u64, u64)> {
    let value = value.strip_prefix("bytes=")?;
    if total == 0 || value.contains(',') {
        return None;
    }
    let (start, end) = value.split_once('-')?;
    if start.is_empty() {
        let suffix = end.parse::<u64>().ok()?.min(total);
        return (suffix > 0).then_some((total - suffix, total - 1));
    }
    let start = start.parse::<u64>().ok()?;
    if start >= total {
        return None;
    }
    let end = if end.is_empty() {
        total - 1
    } else {
        end.parse::<u64>().ok()?.min(total - 1)
    };
    (start <= end).then_some((start, end))
}

fn stream_response(
    status: StatusCode,
    headers: HeaderMap,
    response: reqwest::Response,
) -> Result<Response> {
    let mut builder = Response::builder().status(status);
    for name in [
        header::CONTENT_TYPE,
        header::CONTENT_LENGTH,
        header::CONTENT_DISPOSITION,
        header::CONTENT_RANGE,
        header::ACCEPT_RANGES,
        header::CACHE_CONTROL,
        header::ETAG,
        header::LAST_MODIFIED,
        header::RETRY_AFTER,
    ] {
        if let Some(value) = headers.get(&name) {
            builder = builder.header(name, value);
        }
    }
    let stream = response.bytes_stream().map_err(std::io::Error::other);
    builder
        .body(Body::from_stream(stream))
        .map_err(|_| AppError::Internal)
}

pub async fn resolve_bot_by_token(state: &AppState, token: &str) -> Result<BotRecord> {
    if token.len() < 8 || token.len() > 256 {
        return Err(AppError::Unauthorized);
    }
    let token_lookup_hash = state.crypto.bot_public_id(token);
    let bot = find_bot_by_token_lookup(state, &token_lookup_hash)
        .await?
        .ok_or(AppError::Unauthorized)?;
    let stored = decrypt_token(state, &bot)?;
    if !bool::from(stored.as_slice().ct_eq(token.as_bytes())) {
        return Err(AppError::Unauthorized);
    }
    Ok(bot)
}

async fn find_bot_by_token_lookup(
    state: &AppState,
    token_lookup_hash: &str,
) -> Result<Option<BotRecord>> {
    sqlx::query_as::<_, BotRecord>(
        r#"SELECT bots.id, bots.user_id, bots.telegram_bot_id, bots.username, bots.display_name,
                  bots.token_ciphertext, bots.token_nonce, bots.token_fingerprint, bots.public_id,
                  bots.ingress_secret_ciphertext, bots.ingress_secret_nonce, bots.status,
                  bots.routing_mode, bots.update_mode, bots.last_update_at, bots.last_api_call_at,
                  bots.created_at
             FROM bots
             JOIN memberships ON memberships.user_id = bots.user_id
            WHERE bots.token_lookup_hash = $1
              AND (memberships.status IN ('active', 'trialing') OR
                   (memberships.status IN ('past_due', 'canceled') AND
                    memberships.current_period_ends_at > now()))"#,
    )
    .bind(token_lookup_hash)
    .fetch_optional(&state.db)
    .await
    .map_err(Into::into)
}

async fn find_active_bot_by_id(state: &AppState, bot_id: Uuid) -> Result<Option<BotRecord>> {
    sqlx::query_as::<_, BotRecord>(
        r#"SELECT bots.id, bots.user_id, bots.telegram_bot_id, bots.username, bots.display_name,
                  bots.token_ciphertext, bots.token_nonce, bots.token_fingerprint, bots.public_id,
                  bots.ingress_secret_ciphertext, bots.ingress_secret_nonce, bots.status,
                  bots.routing_mode, bots.update_mode, bots.last_update_at, bots.last_api_call_at,
                  bots.created_at
             FROM bots
             JOIN memberships ON memberships.user_id = bots.user_id
            WHERE bots.id = $1
              AND (memberships.status IN ('active', 'trialing') OR
                   (memberships.status IN ('past_due', 'canceled') AND
                    memberships.current_period_ends_at > now()))"#,
    )
    .bind(bot_id)
    .fetch_optional(&state.db)
    .await
    .map_err(Into::into)
}

pub async fn find_bot_by_public_id(state: &AppState, public_id: &str) -> Result<Option<BotRecord>> {
    sqlx::query_as::<_, BotRecord>(
        r#"SELECT bots.id, bots.user_id, bots.telegram_bot_id, bots.username, bots.display_name,
                  bots.token_ciphertext, bots.token_nonce, bots.token_fingerprint, bots.public_id,
                  bots.ingress_secret_ciphertext, bots.ingress_secret_nonce, bots.status,
                  bots.routing_mode, bots.update_mode, bots.last_update_at, bots.last_api_call_at,
                  bots.created_at
             FROM bots
             JOIN memberships ON memberships.user_id = bots.user_id
            WHERE bots.public_id = $1
              AND (memberships.status IN ('active', 'trialing') OR
                   (memberships.status IN ('past_due', 'canceled') AND
                    memberships.current_period_ends_at > now()))"#,
    )
    .bind(public_id)
    .fetch_optional(&state.db)
    .await
    .map_err(Into::into)
}

pub fn decrypt_token(state: &AppState, bot: &BotRecord) -> Result<zeroize::Zeroizing<Vec<u8>>> {
    state.crypto.decrypt(
        &Ciphertext {
            nonce: bot.token_nonce.clone(),
            data: bot.token_ciphertext.clone(),
        },
        format!("bot:{}:token", bot.id).as_bytes(),
    )
}

pub(crate) async fn install_managed_webhook(state: &AppState, bot: &BotRecord) -> Result<bool> {
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
    let webhook_url = format!(
        "{}/telegram/webhook/{}",
        state.config.api_base_url, bot.public_id
    );
    let (_, response) = raw_telegram_json(
        &state.telegram,
        bot_api_base(state, bot)?,
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

fn bot_api_base<'a>(state: &'a AppState, bot: &BotRecord) -> Result<&'a str> {
    bot_api_base_for_routing(state, &bot.routing_mode)
}

fn bot_api_base_for_routing<'a>(state: &'a AppState, routing_mode: &str) -> Result<&'a str> {
    match routing_mode {
        "local" => state
            .config
            .telegram_local_api_url
            .as_deref()
            .ok_or_else(|| AppError::Upstream("Local Bot API routing is not configured".into())),
        "cloud" => Ok(&state.config.telegram_cloud_api_url),
        _ => Err(AppError::Internal),
    }
}

fn managed_child_routing<'a>(existing: Option<&'a str>, manager: &'a str) -> &'a str {
    existing.unwrap_or(manager)
}

pub async fn raw_telegram_json(
    client: &reqwest::Client,
    base: &str,
    token: &str,
    method: &str,
    payload: &Value,
) -> Result<(StatusCode, Value)> {
    if !valid_method_name(method) {
        return Err(AppError::Validation("Invalid Telegram method name".into()));
    }
    let url = format!("{}/bot{}/{}", base.trim_end_matches('/'), token, method);
    let response = client
        .post(url)
        .json(payload)
        .send()
        .await
        .map_err(|error| AppError::Upstream(error.without_url().to_string()))?;
    let status = response.status();
    let body = response.json::<Value>().await.map_err(|error| {
        AppError::Upstream(format!(
            "Telegram returned an invalid response: {}",
            error.without_url()
        ))
    })?;
    Ok((status, body))
}

pub async fn telegram_json_for_bot(
    state: &AppState,
    bot: &BotRecord,
    method: &str,
    payload: &Value,
    source: &str,
) -> Result<Value> {
    let started = Instant::now();
    let token = decrypt_token(state, bot)?;
    let (status, body) = raw_telegram_json(
        &state.telegram,
        bot_api_base(state, bot)?,
        std::str::from_utf8(&token)
            .map_err(|_| AppError::Crypto("invalid token encoding".into()))?,
        method,
        payload,
    )
    .await?;
    let ok = body.get("ok").and_then(Value::as_bool);
    let error = body
        .get("description")
        .and_then(Value::as_str)
        .map(truncate_error);
    record_api_call(
        state,
        bot.id,
        method,
        source,
        Some(status.as_u16() as i32),
        ok,
        started.elapsed(),
        error,
    )
    .await;
    if !status.is_success() || ok == Some(false) {
        return Err(AppError::Upstream(
            body.get("description")
                .and_then(Value::as_str)
                .unwrap_or("Telegram rejected the request")
                .to_owned(),
        ));
    }
    Ok(body)
}

#[allow(clippy::too_many_arguments)]
async fn record_api_call(
    state: &AppState,
    bot_id: Uuid,
    method: &str,
    source: &str,
    http_status: Option<i32>,
    telegram_ok: Option<bool>,
    elapsed: Duration,
    error: Option<String>,
) {
    let result = sqlx::query(
        r#"WITH inserted AS (
               INSERT INTO api_calls
                      (bot_id, method, source, http_status, telegram_ok, latency_ms, error_summary, expires_at)
               SELECT bots.id, $2, $3, $4, $5, $6, $7,
                      now() + make_interval(days => bot_effective_retention_days(bots.id))
                 FROM bots
                WHERE bots.id = $1
           )
           UPDATE bots SET last_api_call_at = now(), updated_at = now() WHERE id = $1"#,
    )
    .bind(bot_id)
    .bind(method)
    .bind(source)
    .bind(http_status)
    .bind(telegram_ok)
    .bind(elapsed.as_millis().min(i32::MAX as u128) as i32)
    .bind(error)
    .execute(&state.db)
    .await;
    if let Err(error) = result {
        tracing::warn!(bot_id = %bot_id, error = ?error, "could not record API call");
    }
}

pub struct OutboundMessageRecord<'a> {
    pub bot_id: Uuid,
    pub user_id: Option<Uuid>,
    pub chat_id: i64,
    pub telegram_message_id: Option<i64>,
    pub method: &'a str,
    pub source: &'a str,
    pub text: Option<&'a str>,
    pub status: &'a str,
    pub response_status: Option<i32>,
    pub error_summary: Option<&'a str>,
}

pub async fn record_outbound_message(
    state: &AppState,
    message: OutboundMessageRecord<'_>,
) -> Result<()> {
    let mut tx = state.db.begin().await?;
    sqlx::query(
        r#"INSERT INTO outbound_messages
               (bot_id, user_id, chat_id, telegram_message_id, method, source, text, status,
                response_status, error_summary, expires_at)
           SELECT bots.id, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                  now() + make_interval(days => bot_effective_retention_days(bots.id))
             FROM bots
            WHERE bots.id = $1"#,
    )
    .bind(message.bot_id)
    .bind(message.user_id)
    .bind(message.chat_id)
    .bind(message.telegram_message_id)
    .bind(message.method)
    .bind(message.source)
    .bind(message.text)
    .bind(message.status)
    .bind(message.response_status)
    .bind(message.error_summary.map(truncate_error))
    .execute(&mut *tx)
    .await?;
    if let Some(text) = message.text {
        let preview = format!("You: {}", text.chars().take(170).collect::<String>());
        sqlx::query(
            r#"INSERT INTO conversations
                   (bot_id, chat_id, display_name, last_message_preview, last_update_at, expires_at)
               SELECT bots.id, $2, $3, $4, now(),
                      now() + make_interval(days => bot_effective_retention_days(bots.id))
                 FROM bots
                WHERE bots.id = $1
               ON CONFLICT (bot_id, chat_id) DO UPDATE SET
                   last_message_preview = EXCLUDED.last_message_preview,
                   last_update_at = EXCLUDED.last_update_at,
                   expires_at = EXCLUDED.expires_at"#,
        )
        .bind(message.bot_id)
        .bind(message.chat_id)
        .bind(format!("Chat {}", message.chat_id))
        .bind(preview)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

fn truncate_error(value: &str) -> String {
    value.chars().take(500).collect()
}

fn telegram_ok(result: Value) -> Response {
    (StatusCode::OK, Json(json!({"ok": true, "result": result}))).into_response()
}

fn telegram_error(status: StatusCode, error_code: u16, description: &str) -> Response {
    (
        status,
        Json(json!({"ok": false, "error_code": error_code, "description": description})),
    )
        .into_response()
}

async fn virtual_get_updates(state: AppState, bot: BotRecord, request: Request) -> Response {
    if bot.update_mode == "webhook" {
        return telegram_error(
            StatusCode::CONFLICT,
            409,
            "Conflict: can't use getUpdates method while webhook is active; use deleteWebhook to delete the webhook first",
        );
    }
    let params = match params_from_request(request).await {
        Ok(params) => params,
        Err(message) => return telegram_error(StatusCode::BAD_REQUEST, 400, &message),
    };
    let offset = param_i64(&params, "offset").unwrap_or(0);
    let limit = param_i64(&params, "limit").unwrap_or(100).clamp(1, 100);
    let timeout = param_i64(&params, "timeout").unwrap_or(0).clamp(0, 50);
    let requested_allowed = params.get("allowed_updates").and_then(parse_string_array);

    if params.contains_key("allowed_updates")
        && let Err(error) = sqlx::query(
            "UPDATE bot_update_state SET allowed_updates = $2, updated_at = now() WHERE bot_id = $1",
        )
        .bind(bot.id)
        .bind(requested_allowed.as_ref().map(|types| json!(types)))
        .execute(&state.db)
        .await
    {
        tracing::error!(bot_id = %bot.id, error = ?error, "could not store update filter");
        return telegram_error(StatusCode::INTERNAL_SERVER_ERROR, 500, "Internal Server Error");
    }

    if offset > 0 {
        let cursor_updated = sqlx::query(
            r#"UPDATE bot_update_state
                  SET confirmed_through = GREATEST(COALESCE(confirmed_through, $2 - 1), $2 - 1),
                      updated_at = now()
                WHERE bot_id = $1"#,
        )
        .bind(bot.id)
        .bind(offset)
        .execute(&state.db)
        .await;
        let updates_consumed = sqlx::query(
            "UPDATE updates SET consumed_at = now() WHERE bot_id = $1 AND update_id < $2 AND consumed_at IS NULL",
        )
        .bind(bot.id)
        .bind(offset)
        .execute(&state.db)
        .await;
        if cursor_updated.is_err() || updates_consumed.is_err() {
            return telegram_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                500,
                "Internal Server Error",
            );
        }
    }

    // Subscribe before the first query so an update committed between the
    // initial read and the wait cannot be missed until the long-poll timeout.
    let mut receiver = if timeout > 0 {
        Some(state.events.subscribe(bot.id).await)
    } else {
        None
    };
    let mut updates = match fetch_poll_updates(&state, bot.id, offset, limit).await {
        Ok(updates) => updates,
        Err(error) => {
            tracing::error!(bot_id = %bot.id, error = ?error, "getUpdates query failed");
            return telegram_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                500,
                "Internal Server Error",
            );
        }
    };
    if updates.is_empty()
        && let Some(receiver) = receiver.as_mut()
    {
        let _ = tokio::time::timeout(Duration::from_secs(timeout as u64), receiver.recv()).await;
        updates = match fetch_poll_updates(&state, bot.id, offset, limit).await {
            Ok(updates) => updates,
            Err(error) => {
                tracing::error!(bot_id = %bot.id, error = ?error, "getUpdates query after wait failed");
                return telegram_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    500,
                    "Internal Server Error",
                );
            }
        };
    }
    telegram_ok(Value::Array(updates))
}

async fn fetch_poll_updates(
    state: &AppState,
    bot_id: Uuid,
    offset: i64,
    limit: i64,
) -> Result<Vec<Value>> {
    if offset < 0 {
        let count = (-offset).clamp(1, 100);
        let mut rows = sqlx::query_as::<_, (i64, Value)>(
            "SELECT update_id, payload FROM updates WHERE bot_id = $1 AND consumed_at IS NULL ORDER BY update_id DESC LIMIT $2",
        )
        .bind(bot_id)
        .bind(count)
        .fetch_all(&state.db)
        .await?;
        rows.reverse();
        if let Some((first, _)) = rows.first() {
            sqlx::query(
                "UPDATE updates SET consumed_at = now() WHERE bot_id = $1 AND update_id < $2 AND consumed_at IS NULL",
            )
            .bind(bot_id)
            .bind(*first)
            .execute(&state.db)
            .await?;
        }
        return Ok(rows.into_iter().map(|(_, payload)| payload).collect());
    }

    let floor = if offset > 0 { offset - 1 } else { i64::MIN };
    let rows = sqlx::query_scalar::<_, Value>(
        "SELECT payload FROM updates WHERE bot_id = $1 AND consumed_at IS NULL AND update_id > $2 ORDER BY update_id ASC LIMIT $3",
    )
    .bind(bot_id)
    .bind(floor)
    .bind(limit)
    .fetch_all(&state.db)
    .await?;
    Ok(rows)
}

async fn virtual_set_webhook(state: AppState, bot: BotRecord, request: Request) -> Response {
    let params = match params_from_request(request).await {
        Ok(params) => params,
        Err(message) => return telegram_error(StatusCode::BAD_REQUEST, 400, &message),
    };
    let url = params
        .get("url")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if let Err(message) = validate_webhook_url(url, state.config.app_env != "production") {
        return telegram_error(
            StatusCode::BAD_REQUEST,
            400,
            &format!("Bad Request: {message}"),
        );
    }
    let secret = params.get("secret_token").and_then(Value::as_str);
    if secret.is_some_and(|value| {
        value.is_empty()
            || value.len() > 256
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    }) {
        return telegram_error(
            StatusCode::BAD_REQUEST,
            400,
            "Bad Request: invalid secret token",
        );
    }
    let max_connections = param_i64(&params, "max_connections")
        .unwrap_or(40)
        .clamp(1, 100) as i32;
    let allowed = params.get("allowed_updates").and_then(parse_string_array);
    let drop_pending = param_bool(&params, "drop_pending_updates").unwrap_or(false);
    let encrypted = match secret {
        Some(secret) => match state.crypto.encrypt(
            secret.as_bytes(),
            format!("bot:{}:downstream-secret", bot.id).as_bytes(),
        ) {
            Ok(value) => Some(value),
            Err(_) => {
                return telegram_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    500,
                    "Internal Server Error",
                );
            }
        },
        None => None,
    };

    let mut tx = match state.db.begin().await {
        Ok(tx) => tx,
        Err(_) => {
            return telegram_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                500,
                "Internal Server Error",
            );
        }
    };
    let result = sqlx::query(
        r#"UPDATE bot_update_state
              SET downstream_webhook_url = $2,
                  downstream_secret_ciphertext = $3,
                  downstream_secret_nonce = $4,
                  max_connections = $5,
                  allowed_updates = COALESCE($6, allowed_updates),
                  updated_at = now()
            WHERE bot_id = $1"#,
    )
    .bind(bot.id)
    .bind(url)
    .bind(encrypted.as_ref().map(|value| value.data.as_slice()))
    .bind(encrypted.as_ref().map(|value| value.nonce.as_slice()))
    .bind(max_connections)
    .bind(allowed.as_ref().map(|value| json!(value)))
    .execute(&mut *tx)
    .await;
    if result.is_err() {
        return telegram_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            500,
            "Internal Server Error",
        );
    }
    if sqlx::query("UPDATE bots SET update_mode = 'webhook', updated_at = now() WHERE id = $1")
        .bind(bot.id)
        .execute(&mut *tx)
        .await
        .is_err()
    {
        return telegram_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            500,
            "Internal Server Error",
        );
    }
    if drop_pending {
        let confirmed = sqlx::query(
            "UPDATE bot_update_state SET confirmed_through = (SELECT max(update_id) FROM updates WHERE bot_id = $1) WHERE bot_id = $1",
        )
        .bind(bot.id)
        .execute(&mut *tx)
        .await;
        let consumed = sqlx::query(
            "UPDATE updates SET consumed_at = now() WHERE bot_id = $1 AND consumed_at IS NULL",
        )
        .bind(bot.id)
        .execute(&mut *tx)
        .await;
        let discarded = sqlx::query(
            "UPDATE webhook_deliveries SET state = 'discarded', updated_at = now() WHERE bot_id = $1 AND state <> 'delivered'",
        )
        .bind(bot.id)
        .execute(&mut *tx)
        .await;
        if confirmed.is_err() || consumed.is_err() || discarded.is_err() {
            return telegram_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                500,
                "Internal Server Error",
            );
        }
    } else {
        let queued = sqlx::query(
            r#"INSERT INTO webhook_deliveries (bot_id, update_row_id)
               SELECT $1, updates.id FROM updates
                WHERE updates.bot_id = $1
                  AND updates.consumed_at IS NULL
               ON CONFLICT DO NOTHING"#,
        )
        .bind(bot.id)
        .execute(&mut *tx)
        .await;
        if queued.is_err() {
            return telegram_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                500,
                "Internal Server Error",
            );
        }
    }
    if tx.commit().await.is_err() {
        return telegram_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            500,
            "Internal Server Error",
        );
    }
    telegram_ok(Value::Bool(true))
}

async fn virtual_delete_webhook(state: AppState, bot: BotRecord, request: Request) -> Response {
    let params = params_from_request(request).await.unwrap_or_default();
    let drop_pending = param_bool(&params, "drop_pending_updates").unwrap_or(false);
    let mut tx = match state.db.begin().await {
        Ok(tx) => tx,
        Err(_) => {
            return telegram_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                500,
                "Internal Server Error",
            );
        }
    };
    let state_updated = sqlx::query(
        r#"UPDATE bot_update_state
              SET downstream_webhook_url = NULL,
                  downstream_secret_ciphertext = NULL,
                  downstream_secret_nonce = NULL,
                  updated_at = now()
            WHERE bot_id = $1"#,
    )
    .bind(bot.id)
    .execute(&mut *tx)
    .await;
    let bot_updated =
        sqlx::query("UPDATE bots SET update_mode = 'polling', updated_at = now() WHERE id = $1")
            .bind(bot.id)
            .execute(&mut *tx)
            .await;
    let deliveries_discarded = sqlx::query(
        "UPDATE webhook_deliveries SET state = 'discarded', updated_at = now() WHERE bot_id = $1 AND state <> 'delivered'",
    )
    .bind(bot.id)
    .execute(&mut *tx)
    .await;
    if state_updated.is_err() || bot_updated.is_err() || deliveries_discarded.is_err() {
        return telegram_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            500,
            "Internal Server Error",
        );
    }
    if drop_pending {
        let cursor_updated = sqlx::query(
            "UPDATE bot_update_state SET confirmed_through = (SELECT max(update_id) FROM updates WHERE bot_id = $1) WHERE bot_id = $1",
        )
        .bind(bot.id)
        .execute(&mut *tx)
        .await;
        let consumed = sqlx::query(
            "UPDATE updates SET consumed_at = now() WHERE bot_id = $1 AND consumed_at IS NULL",
        )
        .bind(bot.id)
        .execute(&mut *tx)
        .await;
        if cursor_updated.is_err() || consumed.is_err() {
            return telegram_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                500,
                "Internal Server Error",
            );
        }
    }
    if tx.commit().await.is_err() {
        return telegram_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            500,
            "Internal Server Error",
        );
    }
    telegram_ok(Value::Bool(true))
}

async fn virtual_get_webhook_info(state: AppState, bot: BotRecord) -> Response {
    let row = sqlx::query_as::<_, (Option<String>, i64, Option<i64>, Option<String>, Option<i32>, Option<Value>, i32)>(
        r#"SELECT state.downstream_webhook_url,
                  (SELECT count(*) FROM webhook_deliveries deliveries WHERE deliveries.bot_id = state.bot_id AND deliveries.state IN ('pending', 'failed', 'delivering')),
                  (SELECT extract(epoch FROM max(updated_at))::bigint FROM webhook_deliveries deliveries WHERE deliveries.bot_id = state.bot_id AND deliveries.state = 'failed'),
                  (SELECT error_summary FROM webhook_deliveries deliveries WHERE deliveries.bot_id = state.bot_id AND deliveries.state = 'failed' ORDER BY updated_at DESC LIMIT 1),
                  NULL::integer,
                  state.allowed_updates,
                  state.max_connections
             FROM bot_update_state state WHERE state.bot_id = $1"#,
    )
    .bind(bot.id)
    .fetch_optional(&state.db)
    .await;
    let Ok(Some((
        url,
        pending,
        last_error_date,
        last_error_message,
        last_synchronization_error_date,
        allowed_updates,
        max_connections,
    ))) = row
    else {
        return telegram_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            500,
            "Internal Server Error",
        );
    };
    telegram_ok(json!({
        "url": url.unwrap_or_default(),
        "has_custom_certificate": false,
        "pending_update_count": pending,
        "last_error_date": last_error_date,
        "last_error_message": last_error_message,
        "last_synchronization_error_date": last_synchronization_error_date,
        "max_connections": max_connections,
        "allowed_updates": allowed_updates.unwrap_or_else(|| json!([]))
    }))
}

async fn params_from_request(request: Request) -> std::result::Result<Map<String, Value>, String> {
    let mut params = Map::new();
    if let Some(query) = request.uri().query() {
        for (key, value) in serde_urlencoded::from_str::<Vec<(String, String)>>(query)
            .map_err(|error| format!("Bad Request: invalid query parameters: {error}"))?
        {
            params.insert(key, Value::String(value));
        }
    }
    let content_type = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_owned();
    let body = to_bytes(request.into_body(), SPECIAL_BODY_LIMIT)
        .await
        .map_err(|_| "Bad Request: request body is too large".to_owned())?;
    if body.is_empty() {
        return Ok(params);
    }
    if content_type.starts_with("application/json") {
        let body = serde_json::from_slice::<Value>(&body)
            .map_err(|error| format!("Bad Request: invalid JSON: {error}"))?;
        let object = body
            .as_object()
            .ok_or_else(|| "Bad Request: JSON parameters must be an object".to_owned())?;
        params.extend(object.clone());
    } else if content_type.starts_with("application/x-www-form-urlencoded")
        || content_type.is_empty()
    {
        for (key, value) in serde_urlencoded::from_bytes::<Vec<(String, String)>>(&body)
            .map_err(|error| format!("Bad Request: invalid form body: {error}"))?
        {
            params.insert(key, Value::String(value));
        }
    } else if content_type.starts_with("multipart/form-data") {
        return Err("Bad Request: multipart is not supported for managed update methods".into());
    }
    Ok(params)
}

fn param_i64(params: &Map<String, Value>, name: &str) -> Option<i64> {
    params
        .get(name)
        .and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()))
}

fn param_bool(params: &Map<String, Value>, name: &str) -> Option<bool> {
    params.get(name).and_then(|value| {
        value.as_bool().or_else(|| match value.as_str()? {
            "true" | "1" => Some(true),
            "false" | "0" => Some(false),
            _ => None,
        })
    })
}

fn parse_string_array(value: &Value) -> Option<Vec<String>> {
    match value {
        Value::Array(values) => Some(
            values
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect(),
        ),
        Value::String(value) => serde_json::from_str(value).ok(),
        _ => None,
    }
}

#[derive(Debug)]
pub(crate) struct ExistingWebhook {
    pub(crate) url: String,
    pub(crate) allowed_updates: Value,
    pub(crate) max_connections: i32,
}

pub(crate) fn existing_webhook(
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

pub fn validate_webhook_url(
    value: &str,
    allow_insecure_development: bool,
) -> std::result::Result<(), String> {
    let url = url::Url::parse(value).map_err(|_| "invalid webhook URL")?;
    if url.scheme() != "https" && !(allow_insecure_development && url.scheme() == "http") {
        return Err("webhook URL must use HTTPS".into());
    }
    if !url.username().is_empty() || url.password().is_some() || url.fragment().is_some() {
        return Err("webhook URL must not include credentials or a fragment".into());
    }
    if let Some(port) = url.port_or_known_default()
        && !allow_insecure_development
        && !matches!(port, 443 | 80 | 88 | 8443)
    {
        return Err("webhook port must be 443, 80, 88, or 8443".into());
    }
    let host = url.host_str().ok_or("webhook URL must include a host")?;
    if !allow_insecure_development
        && (host.eq_ignore_ascii_case("localhost")
            || host.ends_with(".localhost")
            || host.ends_with(".local"))
    {
        return Err("local webhook hosts are not allowed".into());
    }
    if !allow_insecure_development {
        match url.host() {
            Some(url::Host::Ipv4(ip)) if !is_globally_routable(IpAddr::V4(ip)) => {
                return Err("private or local webhook addresses are not allowed".into());
            }
            Some(url::Host::Ipv6(ip)) if !is_globally_routable(IpAddr::V6(ip)) => {
                return Err("private or local webhook addresses are not allowed".into());
            }
            _ => {}
        }
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct ManagedBotIdentity {
    telegram_bot_id: i64,
    owner_telegram_user_id: i64,
    username: String,
    display_name: String,
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

async fn queue_managed_bot_sync(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    manager: &BotRecord,
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

pub async fn webhook_ingress(
    State(state): State<AppState>,
    Path(public_id): Path<String>,
    headers: HeaderMap,
    Json(payload): Json<Value>,
) -> Response {
    let bot = match find_bot_by_public_id(&state, &public_id).await {
        Ok(Some(bot)) => bot,
        _ => return StatusCode::NOT_FOUND.into_response(),
    };
    let supplied = match headers
        .get("x-telegram-bot-api-secret-token")
        .and_then(|value| value.to_str().ok())
    {
        Some(value) => value,
        None => return StatusCode::UNAUTHORIZED.into_response(),
    };
    let expected = match state.crypto.decrypt(
        &Ciphertext {
            nonce: bot.ingress_secret_nonce.clone(),
            data: bot.ingress_secret_ciphertext.clone(),
        },
        format!("bot:{}:ingress-secret", bot.id).as_bytes(),
    ) {
        Ok(value) => value,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    if !bool::from(expected.as_slice().ct_eq(supplied.as_bytes())) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let update_id = match payload.get("update_id").and_then(Value::as_i64) {
        Some(value) => value,
        None => return (StatusCode::BAD_REQUEST, "missing update_id").into_response(),
    };
    let event_type = event_type(&payload).to_owned();
    let managed_identity = match managed_bot_identity(&payload) {
        Ok(identity) => identity,
        Err(error) => return (StatusCode::BAD_REQUEST, error).into_response(),
    };
    let projection = conversation_projection(&payload);

    let mut tx = match state.db.begin().await {
        Ok(tx) => tx,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let inserted = sqlx::query_scalar::<_, i64>(
        r#"INSERT INTO updates (bot_id, update_id, event_type, chat_id, telegram_user_id, payload, expires_at)
           SELECT bots.id, $2, $3, $4, $5, $6,
                  now() + make_interval(days => bot_effective_retention_days(bots.id))
             FROM bots
            WHERE bots.id = $1
           ON CONFLICT (bot_id, update_id) DO NOTHING
           RETURNING id"#,
    )
    .bind(bot.id)
    .bind(update_id)
    .bind(&event_type)
    .bind(projection.as_ref().map(|value| value.chat_id))
    .bind(projection.as_ref().and_then(|value| value.user_id))
    .bind(&payload)
    .fetch_optional(&mut *tx)
    .await;
    let row_id = match inserted {
        Ok(Some(value)) => value,
        Ok(None) => {
            if let Some(identity) = &managed_identity {
                let existing_row_id = match sqlx::query_scalar::<_, i64>(
                    "SELECT id FROM updates WHERE bot_id = $1 AND update_id = $2",
                )
                .bind(bot.id)
                .bind(update_id)
                .fetch_optional(&mut *tx)
                .await
                {
                    Ok(Some(row_id)) => row_id,
                    _ => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
                };
                if let Err(error) =
                    queue_managed_bot_sync(&mut tx, &bot, update_id, existing_row_id, identity)
                        .await
                {
                    tracing::error!(manager_bot_id = %bot.id, error = ?error, "could not queue managed bot sync");
                    return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                }
            }
            if tx.commit().await.is_err() {
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }
            return (StatusCode::OK, Json(json!({"ok": true}))).into_response();
        }
        Err(error) => {
            tracing::error!(bot_id = %bot.id, error = ?error, "could not persist update");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };
    if let Some(identity) = &managed_identity
        && let Err(error) = queue_managed_bot_sync(&mut tx, &bot, update_id, row_id, identity).await
    {
        tracing::error!(manager_bot_id = %bot.id, error = ?error, "could not queue managed bot sync");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    if let Some(projection) = projection {
        let projected = sqlx::query(
            r#"INSERT INTO conversations
                   (bot_id, chat_id, chat_type, title, username, display_name,
                    last_message_preview, last_update_at, expires_at)
               SELECT bots.id, $2, $3, $4, $5, $6, $7, now(),
                      now() + make_interval(days => bot_effective_retention_days(bots.id))
                 FROM bots
                WHERE bots.id = $1
               ON CONFLICT (bot_id, chat_id) DO UPDATE SET
                   chat_type = COALESCE(EXCLUDED.chat_type, conversations.chat_type),
                   title = COALESCE(EXCLUDED.title, conversations.title),
                   username = COALESCE(EXCLUDED.username, conversations.username),
                   display_name = COALESCE(EXCLUDED.display_name, conversations.display_name),
                   last_message_preview = COALESCE(EXCLUDED.last_message_preview, conversations.last_message_preview),
                   last_update_at = EXCLUDED.last_update_at,
                   expires_at = EXCLUDED.expires_at"#,
        )
        .bind(bot.id)
        .bind(projection.chat_id)
        .bind(projection.chat_type)
        .bind(projection.title)
        .bind(projection.username)
        .bind(projection.display_name)
        .bind(projection.preview)
        .execute(&mut *tx)
        .await;
        if let Err(error) = projected {
            tracing::error!(bot_id = %bot.id, error = ?error, "could not update conversation projection");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    }
    let webhook_state = match sqlx::query_as::<_, (bool, Option<Value>)>(
        "SELECT downstream_webhook_url IS NOT NULL, allowed_updates FROM bot_update_state WHERE bot_id = $1",
    )
    .bind(bot.id)
    .fetch_one(&mut *tx)
    .await
    {
        Ok(webhook_state) => webhook_state,
        Err(error) => {
            tracing::error!(bot_id = %bot.id, error = ?error, "could not load managed update state");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };
    let allowed = webhook_state.1.as_ref().and_then(parse_string_array);
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
    if webhook_state.0 && deliver_event {
        let queued = sqlx::query(
            "INSERT INTO webhook_deliveries (bot_id, update_row_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
        )
        .bind(bot.id)
        .bind(row_id)
        .execute(&mut *tx)
        .await;
        if let Err(error) = queued {
            tracing::error!(bot_id = %bot.id, update_id, error = ?error, "could not queue webhook delivery");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    } else if !deliver_event {
        // Telegram's allowed_updates applies when an update is created, not
        // retroactively at fetch time. Keep the journal copy for the console,
        // while excluding it from both polling and webhook delivery.
        let consumed = sqlx::query("UPDATE updates SET consumed_at = now() WHERE id = $1")
            .bind(row_id)
            .execute(&mut *tx)
            .await;
        if let Err(error) = consumed {
            tracing::error!(bot_id = %bot.id, update_id, error = ?error, "could not mark filtered update consumed");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    }
    let bot_updated = sqlx::query(
        "UPDATE bots SET last_update_at = now(), status = 'healthy', updated_at = now() WHERE id = $1",
    )
    .bind(bot.id)
    .execute(&mut *tx)
    .await;
    if let Err(error) = bot_updated {
        tracing::error!(bot_id = %bot.id, error = ?error, "could not update bot health after ingress");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    if tx.commit().await.is_err() {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    state
        .events
        .publish(
            bot.id,
            StoredUpdate {
                row_id,
                update_id,
                event_type,
                payload,
            },
        )
        .await;
    (StatusCode::OK, Json(json!({"ok": true}))).into_response()
}

#[derive(Debug, Deserialize)]
pub struct StreamQuery {
    after: Option<i64>,
}

pub async fn event_stream(
    State(state): State<AppState>,
    Path((public_id, stream_key)): Path<(String, String)>,
    Query(query): Query<StreamQuery>,
    headers: HeaderMap,
) -> Response {
    let digest = crate::crypto::Crypto::digest_secret(stream_key.as_bytes());
    let bot_id = sqlx::query_scalar::<_, Uuid>(
        r#"SELECT bots.id
             FROM event_stream_keys keys
             JOIN bots ON bots.id = keys.bot_id
             JOIN memberships ON memberships.user_id = bots.user_id
            WHERE bots.public_id = $1 AND keys.secret_hash = $2 AND keys.revoked_at IS NULL
              AND (memberships.status IN ('active', 'trialing') OR
                   (memberships.status IN ('past_due', 'canceled') AND
                    memberships.current_period_ends_at > now()))"#,
    )
    .bind(&public_id)
    .bind(&digest)
    .fetch_optional(&state.db)
    .await;
    let Ok(Some(bot_id)) = bot_id else {
        return AppError::Unauthorized.into_response();
    };
    let permit = match state.stream_limiter.try_acquire(&digest) {
        Ok(permit) => permit,
        Err(error) => return error.into_response(),
    };
    let _ = sqlx::query("UPDATE event_stream_keys SET last_used_at = now() WHERE secret_hash = $1")
        .bind(&digest)
        .execute(&state.db)
        .await;
    // Subscribe before querying replay so updates committed during the query are
    // held by the live receiver and then de-duplicated by row id.
    let mut receiver = state.events.subscribe(bot_id).await;
    let header_after = headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok());
    let after = query.after.or(header_after).unwrap_or(0);
    let key_digest = digest;
    let database = state.db.clone();
    let stream = async_stream::stream! {
        let _permit = permit;
        let mut replay_last_id = after;
        let mut replay_truncated = false;
        {
            let replay = sqlx::query_as::<_, (i64, i64, String, Value)>(
                "SELECT id, update_id, event_type, payload FROM updates WHERE bot_id = $1 AND id > $2 ORDER BY id ASC LIMIT $3",
            )
            .bind(bot_id)
            .bind(after)
            .bind((SSE_REPLAY_ROW_LIMIT + 1) as i64)
            .fetch(&database);
            futures_util::pin_mut!(replay);
            let mut replay_rows = 0_usize;
            let mut replay_bytes = 0_usize;
            loop {
                let row = match replay.try_next().await {
                    Ok(row) => row,
                    Err(_) => {
                        yield Ok::<Event, Infallible>(Event::default().event("error").data("replay storage is temporarily unavailable"));
                        return;
                    }
                };
                let Some((row_id, update_id, event_type, payload)) = row else {
                    break;
                };
                let data = json!({"row_id": row_id, "update_id": update_id, "event_type": event_type, "payload": payload});
                let serialized = match serde_json::to_string(&data) {
                    Ok(serialized) => serialized,
                    Err(_) => {
                        yield Ok::<Event, Infallible>(Event::default().event("error").data("update serialization failed"));
                        return;
                    }
                };
                if serialized.len() > SSE_REPLAY_EVENT_BYTE_LIMIT {
                    yield Ok::<Event, Infallible>(Event::default().event("error").data("stored update exceeds the replay event limit"));
                    return;
                }
                if replay_rows >= SSE_REPLAY_ROW_LIMIT
                    || replay_bytes.saturating_add(serialized.len()) > SSE_REPLAY_BYTE_LIMIT
                {
                    replay_truncated = true;
                    break;
                }
                replay_rows += 1;
                replay_bytes = replay_bytes.saturating_add(serialized.len());
                replay_last_id = row_id;
                yield Ok::<Event, Infallible>(Event::default().id(row_id.to_string()).event("update").data(serialized));
            }
        }
        if replay_truncated {
            yield Ok::<Event, Infallible>(Event::default().id(replay_last_id.to_string()).event("resync").data("reconnect with this Last-Event-ID to continue replay"));
            return;
        }
        let mut key_check = tokio::time::interval(Duration::from_secs(15));
        key_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        key_check.tick().await;
        loop {
            tokio::select! {
                next = receiver.recv() => match next {
                Ok(update) => {
                    if update.row_id <= replay_last_id {
                        continue;
                    }
                    let row_id = update.row_id;
                    yield Ok::<Event, Infallible>(Event::default().id(row_id.to_string()).event("update").json_data(update).unwrap_or_else(|_| Event::default().event("error").data("serialization failed")));
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    yield Ok::<Event, Infallible>(Event::default().event("resync").data("consumer lagged; reconnect with Last-Event-ID"));
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                },
                _ = key_check.tick() => {
                    let active = sqlx::query_scalar::<_, bool>(
                        r#"SELECT EXISTS(
                               SELECT 1
                                 FROM event_stream_keys keys
                                 JOIN bots ON bots.id = keys.bot_id
                                 JOIN memberships ON memberships.user_id = bots.user_id
                                WHERE keys.secret_hash = $1
                                  AND keys.revoked_at IS NULL
                                  AND (memberships.status IN ('active', 'trialing') OR
                                       (memberships.status IN ('past_due', 'canceled') AND
                                        memberships.current_period_ends_at > now()))
                           )"#,
                    )
                    .bind(&key_digest)
                    .fetch_one(&database)
                    .await
                    .unwrap_or(false);
                    if !active {
                        yield Ok::<Event, Infallible>(Event::default().event("revoked").data("stream key revoked"));
                        break;
                    }
                    yield Ok::<Event, Infallible>(Event::default().comment("keepalive"));
                }
            }
        }
    };
    Sse::new(stream).into_response()
}

#[derive(Debug)]
struct ConversationProjection {
    chat_id: i64,
    user_id: Option<i64>,
    chat_type: Option<String>,
    title: Option<String>,
    username: Option<String>,
    display_name: Option<String>,
    preview: Option<String>,
}

fn event_type(payload: &Value) -> &str {
    ALL_UPDATE_TYPES
        .iter()
        .find(|kind| payload.get(**kind).is_some())
        .copied()
        .unwrap_or("unknown")
}

fn conversation_projection(payload: &Value) -> Option<ConversationProjection> {
    let event = ALL_UPDATE_TYPES.iter().find_map(|kind| payload.get(*kind));
    let message = event.and_then(|value| value.get("message").or(Some(value)))?;
    let chat = message.get("chat")?;
    let chat_id = chat.get("id")?.as_i64()?;
    let from = message
        .get("from")
        .or_else(|| event.and_then(|value| value.get("from")));
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

#[derive(Debug, FromRow)]
struct ManagedBotSyncJob {
    id: Uuid,
    manager_bot_id: Uuid,
    managed_telegram_bot_id: i64,
    managed_owner_telegram_user_id: i64,
    username: String,
    display_name: String,
    source_update_id: i64,
    source_update_row_id: i64,
    attempt: i32,
}

#[derive(Debug, FromRow)]
struct StoredManagedBot {
    id: Uuid,
    user_id: Uuid,
    token_lookup_hash: String,
    bot_kind: String,
    manager_bot_id: Option<Uuid>,
    manager_telegram_bot_id: Option<i64>,
    managed_owner_telegram_user_id: Option<i64>,
    routing_mode: String,
}

pub async fn run_managed_bot_sync_worker(state: AppState) {
    loop {
        match claim_managed_bot_sync(&state).await {
            Ok(Some(job)) => process_managed_bot_sync(&state, job).await,
            Ok(None) => tokio::time::sleep(Duration::from_millis(250)).await,
            Err(_) => {
                tracing::error!("managed bot sync worker failed");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
}

async fn claim_managed_bot_sync(state: &AppState) -> Result<Option<ManagedBotSyncJob>> {
    let mut tx = state.db.begin().await?;
    sqlx::query(
        r#"UPDATE managed_bot_sync_jobs
              SET state = 'retry', error_summary = 'worker_lease_expired',
                  next_attempt_at = now(), locked_at = NULL, updated_at = now()
            WHERE state = 'processing'
              AND locked_at < now() - interval '5 minutes'"#,
    )
    .execute(&mut *tx)
    .await?;
    let job = sqlx::query_as::<_, ManagedBotSyncJob>(
        r#"WITH candidate AS (
               SELECT jobs.id
                 FROM managed_bot_sync_jobs jobs
                 JOIN bots manager ON manager.id = jobs.manager_bot_id
                 JOIN memberships ON memberships.user_id = manager.user_id
                WHERE jobs.state IN ('pending', 'retry')
                  AND jobs.next_attempt_at <= now()
                  AND (
                      memberships.status IN ('active', 'trialing')
                      OR (
                          memberships.status IN ('past_due', 'canceled')
                          AND memberships.current_period_ends_at > now()
                      )
                  )
                ORDER BY jobs.next_attempt_at, jobs.id
                FOR UPDATE OF jobs SKIP LOCKED
                LIMIT 1
           )
           UPDATE managed_bot_sync_jobs jobs
              SET state = 'processing', attempt = attempt + 1,
                  locked_at = now(), updated_at = now()
             FROM candidate
            WHERE jobs.id = candidate.id
        RETURNING jobs.id, jobs.manager_bot_id, jobs.managed_telegram_bot_id,
                  jobs.managed_owner_telegram_user_id, jobs.username,
                  jobs.display_name, jobs.source_update_id,
                  jobs.source_update_row_id, jobs.attempt"#,
    )
    .fetch_optional(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(job)
}

async fn process_managed_bot_sync(state: &AppState, job: ManagedBotSyncJob) {
    match sync_managed_bot(state, &job).await {
        Ok(()) => {
            if let Err(error) = sqlx::query(
                r#"UPDATE managed_bot_sync_jobs
                      SET state = 'completed', error_summary = NULL,
                          locked_at = NULL, completed_at = now(), updated_at = now()
                    WHERE id = $1 AND source_update_row_id = $2 AND state = 'processing'"#,
            )
            .bind(job.id)
            .bind(job.source_update_row_id)
            .execute(&state.db)
            .await
            {
                tracing::error!(job_id = %job.id, error = ?error, "could not complete managed bot sync job");
            }
        }
        Err(error) => {
            let error_code = managed_sync_error_code(&error);
            let terminal = matches!(&error, AppError::Conflict(_));
            let update = if terminal {
                sqlx::query(
                    r#"UPDATE managed_bot_sync_jobs
                          SET state = 'conflict', error_summary = $3,
                              locked_at = NULL, updated_at = now()
                        WHERE id = $1 AND source_update_row_id = $2 AND state = 'processing'"#,
                )
                .bind(job.id)
                .bind(job.source_update_row_id)
                .bind(error_code)
                .execute(&state.db)
                .await
            } else {
                let seconds = 2_i64.pow(job.attempt.clamp(1, 11) as u32).min(3600);
                sqlx::query(
                    r#"UPDATE managed_bot_sync_jobs
                          SET state = 'retry', error_summary = $3,
                              next_attempt_at = now() + make_interval(secs => $4),
                              locked_at = NULL, updated_at = now()
                        WHERE id = $1 AND source_update_row_id = $2 AND state = 'processing'"#,
                )
                .bind(job.id)
                .bind(job.source_update_row_id)
                .bind(error_code)
                .bind(seconds as f64)
                .execute(&state.db)
                .await
            };
            if let Err(database_error) = update {
                tracing::error!(job_id = %job.id, error = ?database_error, "could not reschedule managed bot sync job");
            }
            tracing::warn!(
                job_id = %job.id,
                manager_bot_id = %job.manager_bot_id,
                managed_telegram_bot_id = job.managed_telegram_bot_id,
                error_code,
                terminal,
                "managed bot sync did not complete"
            );
        }
    }
}

fn managed_sync_error_code(error: &AppError) -> &'static str {
    match error {
        AppError::Conflict(_) => "ownership_conflict",
        AppError::Validation(_) => "invalid_managed_bot",
        AppError::Crypto(_) => "credential_encryption_failed",
        AppError::Database(_) => "database_unavailable",
        AppError::Unauthorized | AppError::Forbidden | AppError::NotFound => "manager_unavailable",
        AppError::Config(_) | AppError::Internal => "internal_error",
        AppError::Upstream(_) | AppError::RateLimited | AppError::PlanLimit(_) => {
            "telegram_unavailable"
        }
    }
}

fn managed_token_is_valid(token: &str) -> bool {
    token.len() <= 256
        && token.split_once(':').is_some_and(|(id, secret)| {
            !id.is_empty()
                && id.bytes().all(|byte| byte.is_ascii_digit())
                && secret.len() >= 20
                && secret
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        })
}

fn take_managed_bot_token(response: &mut Value) -> Result<Zeroizing<String>> {
    let token = match response.get_mut("result").map(Value::take) {
        Some(Value::String(token)) => Zeroizing::new(token),
        _ => {
            return Err(AppError::Upstream(
                "managed bot credential was unavailable".into(),
            ));
        }
    };
    if !managed_token_is_valid(&token) {
        return Err(AppError::Upstream(
            "managed bot credential was invalid".into(),
        ));
    }
    Ok(token)
}

async fn sync_managed_bot(state: &AppState, job: &ManagedBotSyncJob) -> Result<()> {
    let manager = find_active_bot_by_id(state, job.manager_bot_id)
        .await?
        .ok_or(AppError::NotFound)?;

    let mut token_response = telegram_json_for_bot(
        state,
        &manager,
        "getManagedBotToken",
        &json!({"user_id": job.managed_telegram_bot_id}),
        "system",
    )
    .await?;
    let child_token = take_managed_bot_token(&mut token_response)?;

    let observed_bot_id =
        sqlx::query_scalar::<_, Uuid>("SELECT id FROM bots WHERE telegram_bot_id = $1")
            .bind(job.managed_telegram_bot_id)
            .fetch_optional(&state.db)
            .await?;

    let mut tx = state.db.begin().await?;
    if let Some(bot_id) = observed_bot_id {
        // Use the same lock as an explicit cloud/local migration. A token
        // refresh must never silently move an existing child to its manager's
        // backend or inspect it while logOut/login is in progress.
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1::text, 0))")
            .bind(bot_id)
            .execute(&mut *tx)
            .await?;
    }
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(job.managed_telegram_bot_id)
        .execute(&mut *tx)
        .await?;
    let stored = sqlx::query_as::<_, StoredManagedBot>(
        r#"SELECT id, user_id, token_lookup_hash, bot_kind, manager_bot_id,
                  manager_telegram_bot_id, managed_owner_telegram_user_id,
                  routing_mode
             FROM bots
            WHERE telegram_bot_id = $1
            FOR UPDATE"#,
    )
    .bind(job.managed_telegram_bot_id)
    .fetch_optional(&mut *tx)
    .await?;
    if stored.as_ref().map(|bot| bot.id) != observed_bot_id {
        return Err(AppError::Upstream(
            "managed bot changed while synchronization started".into(),
        ));
    }
    if let Some(stored) = &stored
        && stored.user_id != manager.user_id
    {
        return Err(AppError::Conflict(
            "managed bot belongs to another workspace".into(),
        ));
    }

    let bot_id = stored.as_ref().map_or_else(Uuid::new_v4, |bot| bot.id);
    if observed_bot_id.is_none() {
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1::text, 0))")
            .bind(bot_id)
            .execute(&mut *tx)
            .await?;
    }
    let child_routing_mode = managed_child_routing(
        stored.as_ref().map(|bot| bot.routing_mode.as_str()),
        &manager.routing_mode,
    );
    let backend = bot_api_base_for_routing(state, child_routing_mode)?.to_owned();
    let (_, me) =
        raw_telegram_json(&state.telegram, &backend, &child_token, "getMe", &json!({})).await?;
    let identity = me
        .get("result")
        .filter(|_| me.get("ok").and_then(Value::as_bool) == Some(true))
        .ok_or_else(|| AppError::Upstream("managed bot identity verification failed".into()))?;
    if identity.get("id").and_then(Value::as_i64) != Some(job.managed_telegram_bot_id)
        || identity.get("is_bot").and_then(Value::as_bool) != Some(true)
    {
        return Err(AppError::Conflict(
            "managed bot identity did not match".into(),
        ));
    }
    let username = identity
        .get("username")
        .and_then(Value::as_str)
        .filter(|username| !username.is_empty())
        .unwrap_or(&job.username)
        .to_owned();
    let display_name = identity
        .get("first_name")
        .and_then(Value::as_str)
        .filter(|name| !name.is_empty())
        .unwrap_or(&job.display_name)
        .to_owned();

    let (_, webhook_info) = raw_telegram_json(
        &state.telegram,
        &backend,
        &child_token,
        "getWebhookInfo",
        &json!({}),
    )
    .await?;
    let previous_webhook = existing_webhook(
        &webhook_info,
        &state.config.api_base_url,
        state.config.app_env != "production",
    )
    .map_err(|_| AppError::Upstream("managed bot webhook inspection failed".into()))?;
    let token_lookup_hash = state.crypto.bot_public_id(&child_token);
    let token_fingerprint = Crypto::token_fingerprint(&child_token);
    let encrypted_token = state.crypto.encrypt(
        child_token.as_bytes(),
        format!("bot:{bot_id}:token").as_bytes(),
    )?;
    let changed = stored.as_ref().is_none_or(|bot| {
        bot.token_lookup_hash != token_lookup_hash
            || bot.bot_kind != "managed"
            || bot.manager_bot_id != Some(manager.id)
            || bot.manager_telegram_bot_id != Some(manager.telegram_bot_id)
            || bot.managed_owner_telegram_user_id != Some(job.managed_owner_telegram_user_id)
    });

    if stored.is_some() {
        sqlx::query(
            r#"UPDATE bots
                  SET username = $2, display_name = $3,
                      token_ciphertext = $4, token_nonce = $5,
                      token_fingerprint = $6, token_lookup_hash = $7,
                      bot_kind = 'managed', manager_bot_id = $8,
                      manager_telegram_bot_id = $9,
                      managed_owner_telegram_user_id = $10,
                      update_mode = CASE WHEN $11 THEN 'webhook' ELSE update_mode END,
                      status = 'provisioning', updated_at = now()
                WHERE id = $1"#,
        )
        .bind(bot_id)
        .bind(&username)
        .bind(&display_name)
        .bind(&encrypted_token.data)
        .bind(&encrypted_token.nonce)
        .bind(&token_fingerprint)
        .bind(&token_lookup_hash)
        .bind(manager.id)
        .bind(manager.telegram_bot_id)
        .bind(job.managed_owner_telegram_user_id)
        .bind(previous_webhook.is_some())
        .execute(&mut *tx)
        .await?;
    } else {
        let public_id = format!("phg_{}", Crypto::random_token(18)?);
        let ingress_secret = Crypto::random_token(32)?;
        let encrypted_ingress = state.crypto.encrypt(
            ingress_secret.as_bytes(),
            format!("bot:{bot_id}:ingress-secret").as_bytes(),
        )?;
        sqlx::query(
            r#"INSERT INTO bots
                   (id, user_id, telegram_bot_id, username, display_name,
                    token_ciphertext, token_nonce, token_fingerprint, public_id,
                    token_lookup_hash, ingress_secret_ciphertext, ingress_secret_nonce,
                    status, routing_mode, update_mode, bot_kind, manager_bot_id,
                    manager_telegram_bot_id, managed_owner_telegram_user_id)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,
                       'provisioning',$13,$14,'managed',$15,$16,$17)"#,
        )
        .bind(bot_id)
        .bind(manager.user_id)
        .bind(job.managed_telegram_bot_id)
        .bind(&username)
        .bind(&display_name)
        .bind(&encrypted_token.data)
        .bind(&encrypted_token.nonce)
        .bind(&token_fingerprint)
        .bind(public_id)
        .bind(&token_lookup_hash)
        .bind(&encrypted_ingress.data)
        .bind(&encrypted_ingress.nonce)
        .bind(&manager.routing_mode)
        .bind(if previous_webhook.is_some() {
            "webhook"
        } else {
            "polling"
        })
        .bind(manager.id)
        .bind(manager.telegram_bot_id)
        .bind(job.managed_owner_telegram_user_id)
        .execute(&mut *tx)
        .await?;
    }

    sqlx::query(
        r#"INSERT INTO bot_update_state
               (bot_id, allowed_updates, downstream_webhook_url, max_connections)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT (bot_id) DO UPDATE SET
               allowed_updates = CASE
                   WHEN bot_update_state.downstream_webhook_url IS NULL
                       THEN EXCLUDED.allowed_updates
                   ELSE bot_update_state.allowed_updates
               END,
               downstream_webhook_url = COALESCE(
                   bot_update_state.downstream_webhook_url,
                   EXCLUDED.downstream_webhook_url
               ),
               max_connections = CASE
                   WHEN bot_update_state.downstream_webhook_url IS NULL
                       THEN EXCLUDED.max_connections
                   ELSE bot_update_state.max_connections
               END,
               updated_at = now()"#,
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

    if changed {
        sqlx::query(
            r#"INSERT INTO audit_log (user_id, bot_id, action, metadata, expires_at)
               SELECT bots.user_id, bots.id, $2, $3,
                      now() + make_interval(days => bot_effective_retention_days(bots.id))
                 FROM bots
                WHERE bots.id = $1"#,
        )
        .bind(bot_id)
        .bind(if stored.is_some() {
            "bot.managed_refreshed"
        } else {
            "bot.managed_discovered"
        })
        .bind(json!({
            "manager_bot_id": manager.id,
            "manager_telegram_bot_id": manager.telegram_bot_id,
            "telegram_bot_id": job.managed_telegram_bot_id,
            "managed_owner_telegram_user_id": job.managed_owner_telegram_user_id,
            "source_update_id": job.source_update_id
        }))
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;

    let mut provision_lock = state.db.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1::text, 0))")
        .bind(bot_id)
        .execute(&mut *provision_lock)
        .await?;
    let child = find_active_bot_by_id(state, bot_id)
        .await?
        .ok_or(AppError::NotFound)?;
    let provisioning = install_managed_webhook(state, &child).await;
    let provisioned = provisioning.as_ref().is_ok_and(|accepted| *accepted);
    sqlx::query("UPDATE bots SET status = $2, updated_at = now() WHERE id = $1")
        .bind(bot_id)
        .bind(if provisioned { "healthy" } else { "degraded" })
        .execute(&mut *provision_lock)
        .await?;
    provision_lock.commit().await?;
    provisioning?;
    if !provisioned {
        return Err(AppError::Upstream(
            "managed bot webhook provisioning failed".into(),
        ));
    }
    Ok(())
}

#[derive(Debug, FromRow)]
struct DeliveryJob {
    delivery_id: i64,
    attempt: i32,
    bot_id: Uuid,
    payload: Value,
    downstream_webhook_url: String,
    downstream_secret_ciphertext: Option<Vec<u8>>,
    downstream_secret_nonce: Option<Vec<u8>>,
    token_ciphertext: Vec<u8>,
    token_nonce: Vec<u8>,
    routing_mode: String,
}

pub async fn run_delivery_worker(state: AppState) {
    loop {
        match claim_delivery(&state).await {
            Ok(Some(job)) => deliver(&state, job).await,
            Ok(None) => tokio::time::sleep(Duration::from_millis(250)).await,
            Err(error) => {
                tracing::error!(error = ?error, "webhook delivery worker failed");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
}

async fn claim_delivery(state: &AppState) -> Result<Option<DeliveryJob>> {
    let mut tx = state.db.begin().await?;
    let id = sqlx::query_scalar::<_, i64>(
        r#"SELECT deliveries.id
             FROM webhook_deliveries deliveries
             JOIN bot_update_state state ON state.bot_id = deliveries.bot_id
             JOIN bots ON bots.id = deliveries.bot_id
             JOIN memberships ON memberships.user_id = bots.user_id
            WHERE deliveries.state IN ('pending', 'failed')
              AND deliveries.next_attempt_at <= now()
              AND state.downstream_webhook_url IS NOT NULL
              AND (memberships.status IN ('active', 'trialing') OR
                   (memberships.status IN ('past_due', 'canceled') AND
                    memberships.current_period_ends_at > now()))
            ORDER BY deliveries.next_attempt_at, deliveries.id
            FOR UPDATE SKIP LOCKED LIMIT 1"#,
    )
    .fetch_optional(&mut *tx)
    .await?;
    let Some(id) = id else {
        tx.rollback().await?;
        return Ok(None);
    };
    sqlx::query(
        "UPDATE webhook_deliveries SET state = 'delivering', attempt = attempt + 1, locked_at = now(), updated_at = now() WHERE id = $1",
    )
    .bind(id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    let job = sqlx::query_as::<_, DeliveryJob>(
        r#"SELECT deliveries.id AS delivery_id, deliveries.attempt, deliveries.bot_id,
                  updates.payload, state.downstream_webhook_url,
                  state.downstream_secret_ciphertext, state.downstream_secret_nonce,
                  bots.token_ciphertext, bots.token_nonce, bots.routing_mode
             FROM webhook_deliveries deliveries
             JOIN updates ON updates.id = deliveries.update_row_id
             JOIN bot_update_state state ON state.bot_id = deliveries.bot_id
             JOIN bots ON bots.id = deliveries.bot_id
            WHERE deliveries.id = $1 AND state.downstream_webhook_url IS NOT NULL"#,
    )
    .bind(id)
    .fetch_optional(&state.db)
    .await?;
    if job.is_none() {
        sqlx::query(
            "UPDATE webhook_deliveries SET state = 'discarded', locked_at = NULL, updated_at = now() WHERE id = $1 AND state = 'delivering'",
        )
        .bind(id)
        .execute(&state.db)
        .await?;
    }
    Ok(job)
}

async fn deliver(state: &AppState, job: DeliveryJob) {
    let delivery = match pinned_delivery_client(
        &job.downstream_webhook_url,
        state.config.app_env != "production",
    )
    .await
    {
        Ok(delivery) => delivery,
        Err(error) => {
            fail_delivery(state, &job, &error).await;
            return;
        }
    };
    let mut request = delivery
        .post(&job.downstream_webhook_url)
        .json(&job.payload);
    if let (Some(data), Some(nonce)) = (
        &job.downstream_secret_ciphertext,
        &job.downstream_secret_nonce,
    ) {
        match state.crypto.decrypt(
            &Ciphertext {
                data: data.clone(),
                nonce: nonce.clone(),
            },
            format!("bot:{}:downstream-secret", job.bot_id).as_bytes(),
        ) {
            Ok(secret) => {
                if let Ok(value) = HeaderValue::from_bytes(&secret) {
                    request = request.header("x-telegram-bot-api-secret-token", value);
                }
            }
            Err(error) => {
                fail_delivery(state, &job, &error.to_string()).await;
                return;
            }
        }
    }
    match request.send().await {
        Ok(response) if response.status().is_success() => {
            let response_body = read_bounded_response(response, 65_536).await;
            let _ = sqlx::query(
                r#"WITH delivered AS (
                       UPDATE webhook_deliveries
                          SET state = 'delivered', response_status = $2, delivered_at = now(),
                              locked_at = NULL, updated_at = now()
                        WHERE id = $1
                    RETURNING update_row_id
                   )
                   UPDATE updates SET consumed_at = now()
                    WHERE id = (SELECT update_row_id FROM delivered)"#,
            )
            .bind(job.delivery_id)
            .bind(response_body.0)
            .execute(&state.db)
            .await;
            if let Some(body) = response_body.1 {
                execute_webhook_response_method(state, &job, &body).await;
            }
        }
        Ok(response) => {
            let status = response.status().as_u16() as i32;
            fail_delivery_with_status(
                state,
                &job,
                status,
                &format!("downstream returned HTTP {status}"),
            )
            .await;
        }
        Err(error) => fail_delivery(state, &job, &error.without_url().to_string()).await,
    }
}

async fn read_bounded_response(response: reqwest::Response, limit: usize) -> (i32, Option<Value>) {
    let status = response.status().as_u16() as i32;
    let mut stream = response.bytes_stream();
    let mut buffer = Vec::new();
    while let Some(chunk) = stream.next().await {
        let Ok(chunk) = chunk else {
            return (status, None);
        };
        if buffer.len() + chunk.len() > limit {
            return (status, None);
        }
        buffer.extend_from_slice(&chunk);
    }
    (status, serde_json::from_slice(&buffer).ok())
}

async fn execute_webhook_response_method(state: &AppState, job: &DeliveryJob, body: &Value) {
    let Some(object) = body.as_object() else {
        return;
    };
    let Some(method) = object.get("method").and_then(Value::as_str) else {
        return;
    };
    if matches!(
        method.to_ascii_lowercase().as_str(),
        "getupdates" | "setwebhook" | "deletewebhook" | "getwebhookinfo" | "logout" | "close"
    ) {
        tracing::warn!(
            bot_id = %job.bot_id,
            method,
            "ignored webhook response method managed by Phenogram"
        );
        return;
    }
    let mut payload = object.clone();
    payload.remove("method");
    let token = match state.crypto.decrypt(
        &Ciphertext {
            data: job.token_ciphertext.clone(),
            nonce: job.token_nonce.clone(),
        },
        format!("bot:{}:token", job.bot_id).as_bytes(),
    ) {
        Ok(token) => token,
        Err(_) => return,
    };
    let base = if job.routing_mode == "local" {
        state.config.telegram_local_api_url.as_deref()
    } else {
        Some(state.config.telegram_cloud_api_url.as_str())
    };
    let Some(base) = base else {
        return;
    };
    let started = Instant::now();
    if let Ok((status, response)) = raw_telegram_json(
        &state.telegram,
        base,
        std::str::from_utf8(&token).unwrap_or(""),
        method,
        &Value::Object(payload),
    )
    .await
    {
        record_api_call(
            state,
            job.bot_id,
            method,
            "webhook_response",
            Some(status.as_u16() as i32),
            response.get("ok").and_then(Value::as_bool),
            started.elapsed(),
            response
                .get("description")
                .and_then(Value::as_str)
                .map(truncate_error),
        )
        .await;
    }
}

async fn fail_delivery(state: &AppState, job: &DeliveryJob, error: &str) {
    fail_delivery_with_status(state, job, 0, error).await;
}

async fn fail_delivery_with_status(state: &AppState, job: &DeliveryJob, status: i32, error: &str) {
    let seconds = 2_i64.pow(job.attempt.clamp(1, 11) as u32).min(3600);
    let _ = sqlx::query(
        r#"UPDATE webhook_deliveries
              SET state = 'failed', response_status = NULLIF($2, 0), error_summary = $3,
                  next_attempt_at = now() + make_interval(secs => $4), locked_at = NULL, updated_at = now()
            WHERE id = $1"#,
    )
    .bind(job.delivery_id)
    .bind(status)
    .bind(truncate_error(error))
    .bind(seconds as f64)
    .execute(&state.db)
    .await;
}

async fn pinned_delivery_client(
    value: &str,
    allow_development: bool,
) -> std::result::Result<reqwest::Client, String> {
    validate_webhook_url(value, allow_development)?;
    let url = url::Url::parse(value).map_err(|_| "invalid webhook URL")?;
    let port = url.port_or_known_default().ok_or("missing webhook port")?;
    let (dns_name, mut addresses): (Option<&str>, Vec<SocketAddr>) = match url.host() {
        Some(url::Host::Domain(host)) => (
            Some(host),
            tokio::net::lookup_host((host, port))
                .await
                .map_err(|_| "webhook host could not be resolved")?
                .collect(),
        ),
        Some(url::Host::Ipv4(ip)) => (None, vec![SocketAddr::new(IpAddr::V4(ip), port)]),
        Some(url::Host::Ipv6(ip)) => (None, vec![SocketAddr::new(IpAddr::V6(ip), port)]),
        None => return Err("missing webhook host".into()),
    };
    addresses.sort_unstable();
    addresses.dedup();
    if addresses.is_empty() {
        return Err("webhook host did not resolve".into());
    }
    if !allow_development {
        for address in &addresses {
            if !is_globally_routable(address.ip()) {
                return Err("webhook resolved to a non-global address".into());
            }
        }
    }

    let mut builder = reqwest::Client::builder()
        .no_proxy()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(15))
        .redirect(reqwest::redirect::Policy::none())
        .user_agent(concat!("Phenogram-Webhook/", env!("CARGO_PKG_VERSION")));
    if let Some(host) = dns_name {
        builder = builder.resolve_to_addrs(host, &addresses);
    }
    builder
        .build()
        .map_err(|_| "webhook delivery client could not be created".into())
}

fn is_globally_routable(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => is_globally_routable_v4(ip),
        IpAddr::V6(ip) => is_globally_routable_v6(ip),
    }
}

fn is_globally_routable_v4(ip: Ipv4Addr) -> bool {
    let [a, b, c, _] = ip.octets();
    !matches!(
        (a, b, c),
        (0, _, _)
            | (10, _, _)
            | (100, 64..=127, _)
            | (127, _, _)
            | (169, 254, _)
            | (172, 16..=31, _)
            | (192, 0, 0)
            | (192, 0, 2)
            | (192, 88, 99)
            | (192, 168, _)
            | (198, 18..=19, _)
            | (198, 51, 100)
            | (203, 0, 113)
            | (224..=255, _, _)
    )
}

fn is_globally_routable_v6(ip: Ipv6Addr) -> bool {
    if let Some(ipv4) = ip.to_ipv4_mapped() {
        return is_globally_routable_v4(ipv4);
    }
    let segments = ip.segments();
    let is_global_unicast = (segments[0] & 0xe000) == 0x2000;
    let is_ietf_protocol_assignment = segments[0] == 0x2001 && segments[1] <= 0x01ff;
    let is_documentation = segments[0] == 0x2001 && segments[1] == 0x0db8;
    let is_six_to_four = segments[0] == 0x2002;
    let is_documentation_3fff = segments[0] == 0x3fff && (segments[1] & 0xf000) == 0;
    is_global_unicast
        && !is_ietf_protocol_assignment
        && !is_documentation
        && !is_six_to_four
        && !is_documentation_3fff
}

#[cfg(test)]
mod destination_tests {
    use super::{
        ALL_UPDATE_TYPES, byte_range, is_globally_routable, managed_bot_identity,
        managed_child_routing, take_managed_bot_token, validate_webhook_url,
    };
    use serde_json::{Value, json};
    use std::net::IpAddr;

    fn ip(value: &str) -> IpAddr {
        value.parse().expect("valid test address")
    }

    #[test]
    fn accepts_public_unicast_destinations() {
        assert!(is_globally_routable(ip("8.8.8.8")));
        assert!(is_globally_routable(ip("1.1.1.1")));
        assert!(is_globally_routable(ip("2606:4700:4700::1111")));
    }

    #[test]
    fn rejects_non_global_and_ipv4_mapped_destinations() {
        for value in [
            "0.0.0.1",
            "10.0.0.1",
            "100.64.0.1",
            "127.0.0.1",
            "169.254.169.254",
            "192.0.2.1",
            "198.18.0.1",
            "203.0.113.1",
            "240.0.0.1",
            "::1",
            "::ffff:127.0.0.1",
            "2001:db8::1",
            "2002:7f00:1::",
            "3fff::1",
            "fc00::1",
            "fe80::1",
        ] {
            assert!(!is_globally_routable(ip(value)), "accepted {value}");
        }
    }

    #[test]
    fn validates_literal_ipv6_webhook_hosts() {
        assert!(validate_webhook_url("https://[::1]/hook", false).is_err());
        assert!(validate_webhook_url("https://[2606:4700:4700::1111]/hook", false).is_ok());
    }

    #[test]
    fn parses_single_http_byte_ranges() {
        assert_eq!(byte_range("bytes=2-5", 10), Some((2, 5)));
        assert_eq!(byte_range("bytes=7-", 10), Some((7, 9)));
        assert_eq!(byte_range("bytes=-3", 10), Some((7, 9)));
        assert_eq!(byte_range("bytes=20-", 10), None);
        assert_eq!(byte_range("bytes=1-2,4-5", 10), None);
        assert_eq!(byte_range("items=1-2", 10), None);
    }

    #[test]
    fn recognizes_current_control_plane_update_types() {
        for update_type in ["guest_message", "managed_bot", "subscription"] {
            assert!(ALL_UPDATE_TYPES.contains(&update_type));
        }
    }

    #[test]
    fn managed_child_keeps_an_existing_independent_route() {
        assert_eq!(managed_child_routing(Some("local"), "cloud"), "local");
        assert_eq!(managed_child_routing(Some("cloud"), "local"), "cloud");
        assert_eq!(managed_child_routing(None, "local"), "local");
    }

    #[test]
    fn parses_managed_bot_identity_without_a_token() {
        let identity = managed_bot_identity(&json!({
            "update_id": 44,
            "managed_bot": {
                "user": {"id": 777, "is_bot": false, "first_name": "Owner"},
                "bot": {
                    "id": 987654321,
                    "is_bot": true,
                    "first_name": "Nested Agent",
                    "username": "nested_agent_bot"
                }
            }
        }))
        .expect("valid managed update")
        .expect("managed identity");

        assert_eq!(identity.telegram_bot_id, 987654321);
        assert_eq!(identity.owner_telegram_user_id, 777);
        assert_eq!(identity.username, "nested_agent_bot");
        assert_eq!(identity.display_name, "Nested Agent");
    }

    #[test]
    fn rejects_malformed_managed_bot_identity() {
        assert!(
            managed_bot_identity(&json!({
                "managed_bot": {
                    "user": {"id": 777},
                    "bot": {"id": 987654321, "is_bot": false}
                }
            }))
            .is_err()
        );
    }

    #[test]
    fn takes_the_managed_token_out_of_the_response_before_use() {
        let mut response = json!({
            "ok": true,
            "result": "987654321:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdef"
        });
        let token = take_managed_bot_token(&mut response).expect("valid managed token");

        assert_eq!(&**token, "987654321:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdef");
        assert!(response.get("result").is_some_and(Value::is_null));
        assert!(!response.to_string().contains("ABCDEFGHIJKLMNOPQRSTUVWXYZ"));
    }
}
