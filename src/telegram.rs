use std::{
    convert::Infallible,
    io::SeekFrom,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    path::{Component, Path as FsPath},
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
use chrono::{DateTime, Utc};
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
    ingestion::{IngestionBot, IngestionSource, ingest_update},
    models::BotRecord,
    state::{AppState, StoredUpdate},
};

const SPECIAL_BODY_LIMIT: usize = 2 * 1024 * 1024;
const SSE_REPLAY_ROW_LIMIT: usize = 5_000;
const SSE_REPLAY_BYTE_LIMIT: usize = 8 * 1024 * 1024;
const SSE_REPLAY_EVENT_BYTE_LIMIT: usize = SPECIAL_BODY_LIMIT + 64 * 1024;
const TELEGRAM_MEDIA_DIRECTORIES: &[&str] = &[
    "thumbnails",
    "profile_photos",
    "photos",
    "voice",
    "videos",
    "documents",
    "secret",
    "temp",
    "stickers",
    "music",
    "animations",
    "secret_thumbnails",
    "video_notes",
    "passport",
    "wallpapers",
    "notification_sounds",
    "stories",
];
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
    proxy_method_for_environment(state, token, method, false, request).await
}

pub async fn proxy_test_method(
    State(state): State<AppState>,
    Path((token, method)): Path<(String, String)>,
    request: Request,
) -> Response {
    proxy_method_for_environment(state, token, method, true, request).await
}

async fn proxy_method_for_environment(
    state: AppState,
    token: String,
    method: String,
    is_test_dc: bool,
    request: Request,
) -> Response {
    if !valid_method_name(&method) {
        return telegram_error(
            StatusCode::BAD_REQUEST,
            400,
            "Bad Request: invalid method name",
        );
    }
    let bot = match resolve_bot_by_token(&state, &token, is_test_dc).await {
        Ok(bot) if bot.telegram_test_dc == is_test_dc => bot,
        Err(_) => return telegram_error(StatusCode::UNAUTHORIZED, 401, "Unauthorized"),
        Ok(_) => return telegram_error(StatusCode::UNAUTHORIZED, 401, "Unauthorized"),
    };
    if state.config.data_plane_enabled && bot.data_plane_pool.is_some() {
        return telegram_error(
            StatusCode::SERVICE_UNAVAILABLE,
            503,
            "Service Unavailable: this bot is served by the official data-plane gateway",
        );
    }
    if data_plane_request_fenced(&state, bot.id).await {
        return telegram_error(
            StatusCode::SERVICE_UNAVAILABLE,
            503,
            "Service Unavailable: Bot API migration is in progress or requires recovery",
        );
    }
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
    request: Request,
) -> Response {
    proxy_file_for_environment(state, token, file_path, false, request).await
}

pub async fn proxy_test_file(
    State(state): State<AppState>,
    Path((token, file_path)): Path<(String, String)>,
    request: Request,
) -> Response {
    proxy_file_for_environment(state, token, file_path, true, request).await
}

async fn proxy_file_for_environment(
    state: AppState,
    token: String,
    file_path: String,
    is_test_dc: bool,
    request: Request,
) -> Response {
    let bot = match resolve_bot_by_token(&state, &token, is_test_dc).await {
        Ok(bot) if bot.telegram_test_dc == is_test_dc => bot,
        Err(_) => return telegram_error(StatusCode::UNAUTHORIZED, 401, "Unauthorized"),
        Ok(_) => return telegram_error(StatusCode::UNAUTHORIZED, 401, "Unauthorized"),
    };
    if state.config.data_plane_enabled && bot.data_plane_pool.is_some() {
        return telegram_error(
            StatusCode::SERVICE_UNAVAILABLE,
            503,
            "Service Unavailable: this bot is served by the official data-plane gateway",
        );
    }
    if data_plane_request_fenced(&state, bot.id).await {
        return telegram_error(
            StatusCode::SERVICE_UNAVAILABLE,
            503,
            "Service Unavailable: Bot API migration is in progress or requires recovery",
        );
    }
    let range = request.headers().get(header::RANGE).cloned();
    match forward_file(&state, &bot, &token, &file_path, range).await {
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

async fn data_plane_request_fenced(state: &AppState, bot_id: Uuid) -> bool {
    if !state.config.data_plane_enabled {
        return false;
    }
    match crate::lifecycle::has_operation(state, bot_id).await {
        Ok(fenced) => fenced,
        Err(error) => {
            tracing::error!(%bot_id, error = ?error, "could not verify Bot API lifecycle fence");
            true
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
        "{}/bot{}/{}{}{}",
        bot_api_base(state, bot)?,
        token,
        telegram_environment_segment(bot.telegram_test_dc),
        method_name,
        query
    );
    let method = request.method().clone();
    let content_type = request.headers().get(header::CONTENT_TYPE).cloned();
    let content_length = request.headers().get(header::CONTENT_LENGTH).cloned();
    let capture_candidate = bot.data_plane_pool.is_none()
        && method_name.eq_ignore_ascii_case("sendMessage")
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
    if let Some(encoded) = file_path.strip_prefix("__phenogram_local__/") {
        let path = decode_local_file_path(state, bot, encoded)?;
        if bot.data_plane_pool.as_deref() == Some("local") {
            validate_data_plane_local_file_path(state, bot, &path)?;
            return forward_data_plane_local_file(state, bot, token, &path, range).await;
        }
        if bot.data_plane_pool.is_none() && bot.routing_mode == "local" {
            return stream_local_file(state, &path, range.as_ref()).await;
        }
        return Err(AppError::Validation(
            "Invalid local Telegram file path".into(),
        ));
    }
    if file_path.contains("..")
        || file_path.starts_with('/')
        || file_path.contains(['?', '#', '\\'])
    {
        return Err(AppError::Validation("Invalid Telegram file path".into()));
    }
    let url = format!(
        "{}/file/bot{}/{}{}",
        bot_api_base(state, bot)?,
        token,
        telegram_environment_segment(bot.telegram_test_dc),
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

async fn forward_data_plane_local_file(
    state: &AppState,
    bot: &BotRecord,
    token: &str,
    absolute_path: &str,
    range: Option<HeaderValue>,
) -> Result<Response> {
    let url = format!(
        "{}/file/bot{}/{}{}",
        bot_api_base(state, bot)?,
        token,
        telegram_environment_segment(bot.telegram_test_dc),
        absolute_path
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
    let (status, mut body) = match raw_telegram_json_for_dc(
        &state.telegram,
        base,
        token,
        bot.telegram_test_dc,
        "getFile",
        &params,
    )
    .await
    {
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
    encode_bot_bound_local_file_path(&state.crypto, bot.id, path)
}

fn encode_bot_bound_local_file_path(crypto: &Crypto, bot_id: Uuid, path: &str) -> Result<String> {
    let encrypted = crypto.encrypt(
        path.as_bytes(),
        format!("bot:{bot_id}:local-file").as_bytes(),
    )?;
    let mut value = encrypted.nonce;
    value.extend_from_slice(&encrypted.data);
    Ok(URL_SAFE_NO_PAD.encode(value))
}

fn decode_local_file_path(state: &AppState, bot: &BotRecord, encoded: &str) -> Result<String> {
    decode_bot_bound_local_file_path(&state.crypto, bot.id, encoded)
}

fn decode_bot_bound_local_file_path(
    crypto: &Crypto,
    bot_id: Uuid,
    encoded: &str,
) -> Result<String> {
    let value = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| AppError::Validation("Invalid local Telegram file path".into()))?;
    if value.len() <= 24 {
        return Err(AppError::Validation(
            "Invalid local Telegram file path".into(),
        ));
    }
    let decrypted = crypto
        .decrypt(
            &Ciphertext {
                nonce: value[..24].to_vec(),
                data: value[24..].to_vec(),
            },
            format!("bot:{bot_id}:local-file").as_bytes(),
        )
        .map_err(|_| AppError::Validation("Invalid local Telegram file path".into()))?;
    String::from_utf8(decrypted.to_vec())
        .map_err(|_| AppError::Validation("Invalid local Telegram file path".into()))
}

pub(crate) fn prepare_file_link_path(
    state: &AppState,
    bot: &BotRecord,
    file_path: &str,
) -> Result<String> {
    if bot.data_plane_pool.as_deref() != Some("local") {
        validate_relative_telegram_file_path(file_path)?;
        return Ok(file_path.to_owned());
    }

    if let Some(encoded) = file_path.strip_prefix("__phenogram_local__/") {
        let absolute_path = decode_local_file_path(state, bot, encoded)?;
        validate_data_plane_local_file_path(state, bot, &absolute_path)?;
        return Ok(file_path.to_owned());
    }

    validate_data_plane_local_file_path(state, bot, file_path)?;
    Ok(format!(
        "__phenogram_local__/{}",
        encode_local_file_path(state, bot, file_path)?
    ))
}

fn validate_relative_telegram_file_path(file_path: &str) -> Result<()> {
    if file_path.is_empty()
        || file_path.starts_with('/')
        || file_path.contains("..")
        || file_path.contains(['?', '#', '\\'])
    {
        return Err(AppError::Validation("Invalid Telegram file path".into()));
    }
    Ok(())
}

fn validate_data_plane_local_file_path(
    state: &AppState,
    bot: &BotRecord,
    file_path: &str,
) -> Result<()> {
    let token = decrypt_token(state, bot)?;
    let token = std::str::from_utf8(&token).map_err(|_| AppError::Internal)?;
    let root = state
        .config
        .data_plane_official_data_dir
        .as_deref()
        .ok_or_else(|| AppError::Upstream("Local Bot API file storage is not configured".into()))?;
    validate_native_local_file_path(file_path, token, bot.telegram_test_dc, root)
}

fn validate_native_local_file_path(
    file_path: &str,
    token: &str,
    test_dc: bool,
    root: &FsPath,
) -> Result<()> {
    if file_path.contains(['?', '#', '\\']) || file_path.as_bytes().contains(&0) {
        return Err(AppError::Validation(
            "Invalid local Telegram file path".into(),
        ));
    }
    let relative = FsPath::new(file_path).strip_prefix(root).map_err(|_| {
        AppError::Validation(
            "Local Bot API file path must be the exact path returned by getFile".into(),
        )
    })?;
    let mut components = relative.components();
    let bot_directory = normal_path_component(components.next())?;
    let media_directory = normal_path_component(components.next())?;
    normal_path_component(components.next())?;
    if components
        .any(|component| !matches!(component, Component::Normal(value) if !value.is_empty()))
    {
        return Err(AppError::Validation(
            "Invalid local Telegram file path".into(),
        ));
    }

    let mut native_directory = token.as_bytes().to_vec();
    if test_dc {
        native_directory.extend_from_slice(b":T");
    }
    let fallback_directory = native_directory
        .iter()
        .map(|byte| if *byte == b':' { b'~' } else { *byte })
        .collect::<Vec<_>>();
    if bot_directory.as_encoded_bytes() != native_directory
        && bot_directory.as_encoded_bytes() != fallback_directory
    {
        return Err(AppError::Validation(
            "Local Bot API file path does not belong to this bot".into(),
        ));
    }
    if !media_directory
        .to_str()
        .is_some_and(|name| TELEGRAM_MEDIA_DIRECTORIES.contains(&name))
    {
        return Err(AppError::Validation(
            "Invalid local Telegram media path".into(),
        ));
    }
    Ok(())
}

fn normal_path_component(component: Option<Component<'_>>) -> Result<&std::ffi::OsStr> {
    match component {
        Some(Component::Normal(value)) if !value.is_empty() => Ok(value),
        _ => Err(AppError::Validation(
            "Invalid local Telegram file path".into(),
        )),
    }
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

pub async fn resolve_bot_by_token(
    state: &AppState,
    token: &str,
    telegram_test_dc: bool,
) -> Result<BotRecord> {
    if token.len() < 8 || token.len() > 256 {
        return Err(AppError::Unauthorized);
    }
    let token_lookup_hash = state.crypto.bot_public_id(token, telegram_test_dc);
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
        r#"SELECT bots.id, bots.user_id, bots.telegram_bot_id, bots.telegram_test_dc,
                  bots.username, bots.display_name,
                  bots.token_ciphertext, bots.token_nonce, bots.token_fingerprint, bots.public_id,
                  bots.ingress_secret_ciphertext, bots.ingress_secret_nonce, bots.status,
                  bots.routing_mode, bots.data_plane_pool, bots.update_mode,
                  bots.last_update_at, bots.last_api_call_at, bots.created_at
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
        r#"SELECT bots.id, bots.user_id, bots.telegram_bot_id, bots.telegram_test_dc,
                  bots.username, bots.display_name,
                  bots.token_ciphertext, bots.token_nonce, bots.token_fingerprint, bots.public_id,
                  bots.ingress_secret_ciphertext, bots.ingress_secret_nonce, bots.status,
                  bots.routing_mode, bots.data_plane_pool, bots.update_mode,
                  bots.last_update_at, bots.last_api_call_at, bots.created_at
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
        r#"SELECT bots.id, bots.user_id, bots.telegram_bot_id, bots.telegram_test_dc,
                  bots.username, bots.display_name,
                  bots.token_ciphertext, bots.token_nonce, bots.token_fingerprint, bots.public_id,
                  bots.ingress_secret_ciphertext, bots.ingress_secret_nonce, bots.status,
                  bots.routing_mode, bots.data_plane_pool, bots.update_mode,
                  bots.last_update_at, bots.last_api_call_at, bots.created_at
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
    let (_, response) = raw_telegram_json_for_dc(
        &state.telegram,
        bot_api_base(state, bot)?,
        token,
        bot.telegram_test_dc,
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
    if state.config.data_plane_enabled && bot.data_plane_pool.is_some() {
        crate::lifecycle::gateway_base(state)
    } else {
        bot_api_base_for_routing(state, &bot.routing_mode)
    }
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
    raw_telegram_json_for_dc(client, base, token, false, method, payload).await
}

pub async fn raw_telegram_json_for_dc(
    client: &reqwest::Client,
    base: &str,
    token: &str,
    is_test_dc: bool,
    method: &str,
    payload: &Value,
) -> Result<(StatusCode, Value)> {
    if !valid_method_name(method) {
        return Err(AppError::Validation("Invalid Telegram method name".into()));
    }
    let url = format!(
        "{}/bot{}/{}{}",
        base.trim_end_matches('/'),
        token,
        telegram_environment_segment(is_test_dc),
        method
    );
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
    if bot.data_plane_pool.is_none() && data_plane_request_fenced(state, bot.id).await {
        return Err(AppError::Conflict(
            "Bot API migration is in progress or requires recovery".into(),
        ));
    }
    let started = Instant::now();
    let token = decrypt_token(state, bot)?;
    let (status, body) = raw_telegram_json_for_dc(
        &state.telegram,
        bot_api_base(state, bot)?,
        std::str::from_utf8(&token)
            .map_err(|_| AppError::Crypto("invalid token encoding".into()))?,
        bot.telegram_test_dc,
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

fn telegram_environment_segment(is_test_dc: bool) -> &'static str {
    if is_test_dc { "test/" } else { "" }
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
    let stored = sqlx::query_as::<_, (String, Option<String>, DateTime<Utc>)>(
        r#"INSERT INTO outbound_messages
               (bot_id, user_id, chat_id, telegram_message_id, method, source, text, status,
                response_status, error_summary, expires_at)
           SELECT bots.id, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                  now() + make_interval(days => bot_effective_retention_days(bots.id))
             FROM bots
            WHERE bots.id = $1
           ON CONFLICT (bot_id, chat_id, telegram_message_id)
               WHERE telegram_message_id IS NOT NULL
           DO UPDATE SET
               user_id = COALESCE(EXCLUDED.user_id, outbound_messages.user_id),
               source = CASE
                   WHEN outbound_messages.source = 'bot_view' OR EXCLUDED.source <> 'bot_view'
                   THEN outbound_messages.source
                   ELSE EXCLUDED.source
               END,
               method = CASE
                   WHEN EXCLUDED.created_at > outbound_messages.created_at
                   THEN EXCLUDED.method ELSE outbound_messages.method
               END,
               text = CASE
                   WHEN EXCLUDED.created_at > outbound_messages.created_at
                   THEN COALESCE(EXCLUDED.text, outbound_messages.text)
                   ELSE outbound_messages.text
               END,
               status = CASE
                   WHEN EXCLUDED.created_at > outbound_messages.created_at
                   THEN EXCLUDED.status ELSE outbound_messages.status
               END,
               response_status = CASE
                   WHEN EXCLUDED.created_at > outbound_messages.created_at
                   THEN COALESCE(EXCLUDED.response_status, outbound_messages.response_status)
                   ELSE outbound_messages.response_status
               END,
               error_summary = CASE
                   WHEN EXCLUDED.created_at > outbound_messages.created_at
                   THEN EXCLUDED.error_summary ELSE outbound_messages.error_summary
               END,
               created_at = GREATEST(outbound_messages.created_at, EXCLUDED.created_at),
               expires_at = GREATEST(outbound_messages.expires_at, EXCLUDED.expires_at)
           RETURNING source, text, created_at"#,
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
    .fetch_optional(&mut *tx)
    .await?;
    if let Some((source, Some(text), created_at)) = stored {
        let actor = if source == "bot_view" { "You" } else { "Bot" };
        let preview = format!("{actor}: {}", text.chars().take(170).collect::<String>());
        sqlx::query(
            r#"INSERT INTO conversations
                   (bot_id, chat_id, display_name, last_message_preview, last_update_at, expires_at)
               SELECT bots.id, $2, $3, $4, $5,
                      $5 + make_interval(days => bot_effective_retention_days(bots.id))
                 FROM bots
                WHERE bots.id = $1
               ON CONFLICT (bot_id, chat_id) DO UPDATE SET
                   last_message_preview = CASE
                       WHEN EXCLUDED.last_update_at >= conversations.last_update_at
                       THEN EXCLUDED.last_message_preview
                       ELSE conversations.last_message_preview
                   END,
                   last_update_at = GREATEST(conversations.last_update_at, EXCLUDED.last_update_at),
                   expires_at = GREATEST(conversations.expires_at, EXCLUDED.expires_at)"#,
        )
        .bind(message.bot_id)
        .bind(message.chat_id)
        .bind(format!("Chat {}", message.chat_id))
        .bind(preview)
        .bind(created_at)
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
    /// `None` means Telegram omitted the field. Do not turn that into an
    /// explicit empty list during migration because those requests have
    /// different filter semantics.
    pub(crate) allowed_updates: Option<Value>,
    pub(crate) max_connections: i32,
    /// Telegram does not say whether this address was explicitly pinned or
    /// resolved from DNS. It is reported to the operator, who must explicitly
    /// choose fixed-IP continuity or DNS before a controlled transfer.
    pub(crate) reported_ip_address: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExistingWebhookPolicy {
    Cloud { allow_insecure_development: bool },
    Local,
}

pub(crate) fn existing_webhook(
    webhook_info: &Value,
    api_base_url: &str,
    policy: ExistingWebhookPolicy,
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
    validate_webhook_url_for_policy(url, policy).map_err(|_| {
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
        .cloned();
    let max_connections = result
        .get("max_connections")
        .and_then(Value::as_i64)
        .and_then(|value| i32::try_from(value).ok())
        // A local Bot API server accepts a substantially larger webhook
        // concurrency value than the cloud endpoint.
        .filter(|value| (1..=100_000).contains(value))
        .unwrap_or(40);
    // getWebhookInfo does not distinguish an explicitly pinned address from
    // the address Telegram resolved from DNS. Report the current address to
    // the operator, but never infer fixed-IP intent from it.
    let reported_ip_address = match result
        .get("ip_address")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
    {
        Some(value) => Some(
            value
                .parse::<Ipv4Addr>()
                .map_err(|_| {
                    AppError::Validation(
                        "Telegram reported an invalid IPv4 address for the existing webhook".into(),
                    )
                })?
                .to_string(),
        ),
        None => None,
    };
    Ok(Some(ExistingWebhook {
        url: url.to_owned(),
        allowed_updates,
        max_connections,
        reported_ip_address,
    }))
}

pub(crate) fn is_managed_ingress_url(candidate: &str, api_base_url: &str) -> bool {
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
    validate_webhook_url_for_policy(
        value,
        ExistingWebhookPolicy::Cloud {
            allow_insecure_development,
        },
    )
}

fn validate_webhook_url_for_policy(
    value: &str,
    policy: ExistingWebhookPolicy,
) -> std::result::Result<(), String> {
    let url = url::Url::parse(value).map_err(|_| "invalid webhook URL")?;
    let allow_insecure_development = matches!(
        policy,
        ExistingWebhookPolicy::Cloud {
            allow_insecure_development: true
        }
    );
    let local = policy == ExistingWebhookPolicy::Local;
    if url.scheme() != "https" && !((allow_insecure_development || local) && url.scheme() == "http")
    {
        return Err("webhook URL must use HTTPS".into());
    }
    if !url.username().is_empty() || url.password().is_some() || url.fragment().is_some() {
        return Err("webhook URL must not include credentials or a fragment".into());
    }
    if url.port() == Some(0) {
        return Err("webhook port must be a valid TCP port".into());
    }
    if let Some(port) = url.port_or_known_default()
        && !allow_insecure_development
        && !local
        && !matches!(port, 443 | 80 | 88 | 8443)
    {
        return Err("webhook port must be 443, 80, 88, or 8443".into());
    }
    let host = url.host_str().ok_or("webhook URL must include a host")?;
    if !allow_insecure_development
        && !local
        && (host.eq_ignore_ascii_case("localhost")
            || host.ends_with(".localhost")
            || host.ends_with(".local"))
    {
        return Err("local webhook hosts are not allowed".into());
    }
    if !allow_insecure_development && !local {
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
    // Sequence values are allocated before commit. Keep per-bot ingestion
    // serialized so DB cursors cannot become observable out of commit order.
    let _ingestion_guard = state.events.lock_ingestion(bot.id).await;
    let ingestion_bot = IngestionBot {
        id: bot.id,
        telegram_bot_id: bot.telegram_bot_id,
    };
    match ingest_update(
        &state.db,
        ingestion_bot,
        payload,
        IngestionSource::ManagedWebhook,
        None,
    )
    .await
    {
        Ok(_) => (StatusCode::OK, Json(json!({"ok": true}))).into_response(),
        Err(AppError::Validation(message)) => (StatusCode::BAD_REQUEST, message).into_response(),
        Err(error) => {
            tracing::error!(bot_id = %bot.id, error = ?error, "could not ingest update");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct StreamQuery {
    pub(crate) after: Option<i64>,
}

#[derive(Clone, Copy)]
enum StreamEventShape {
    Public,
    Console,
}

enum StreamAccess {
    Key(Vec<u8>),
    Console { user_id: Uuid, session_id: Uuid },
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
    let after = stream_after(&headers, query.after);
    update_stream_response(
        state,
        bot_id,
        after,
        permit,
        StreamAccess::Key(digest),
        StreamEventShape::Public,
    )
    .await
}

pub async fn console_event_stream(
    state: AppState,
    user_id: Uuid,
    session_id: Uuid,
    bot_id: Uuid,
    query: StreamQuery,
    headers: HeaderMap,
) -> Result<Response> {
    let limiter_key =
        crate::crypto::Crypto::digest_secret(format!("console:{session_id}:{bot_id}").as_bytes());
    let permit = state.console_stream_limiter.try_acquire(&limiter_key)?;
    let after = stream_after(&headers, query.after);
    Ok(update_stream_response(
        state,
        bot_id,
        after,
        permit,
        StreamAccess::Console {
            user_id,
            session_id,
        },
        StreamEventShape::Console,
    )
    .await)
}

async fn update_stream_response(
    state: AppState,
    bot_id: Uuid,
    after: i64,
    permit: crate::state::StreamPermit,
    access: StreamAccess,
    shape: StreamEventShape,
) -> Response {
    // Subscribe before querying replay so updates committed during the query are
    // held by the live receiver and then de-duplicated by the monotonic DB row id.
    let mut receiver = state.events.subscribe(bot_id).await;
    let database = state.db.clone();
    let stream = async_stream::stream! {
        let _permit = permit;
        let mut last_seen_id = after;
        let mut replay_truncated = false;
        let mut access_check = tokio::time::interval(Duration::from_secs(15));
        access_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        access_check.tick().await;
        {
            let replay = sqlx::query_as::<_, (i64, i64, String, Option<i64>, Option<i64>, Value, DateTime<Utc>, DateTime<Utc>)>(
                r#"SELECT id, update_id, event_type, chat_id, telegram_user_id,
                          payload, received_at, expires_at
                     FROM updates
                    WHERE bot_id = $1 AND id > $2 AND expires_at > now()
                    ORDER BY id ASC LIMIT $3"#,
            )
            .bind(bot_id)
            .bind(after)
            .bind((SSE_REPLAY_ROW_LIMIT + 1) as i64)
            .fetch(&database);
            futures_util::pin_mut!(replay);
            let mut replay_rows = 0_usize;
            let mut replay_bytes = 0_usize;
            loop {
                let row = loop {
                    tokio::select! {
                        row = replay.try_next() => break row,
                        _ = access_check.tick() => {
                            if !stream_access_active(&database, bot_id, &access).await {
                                yield Ok::<Event, Infallible>(Event::default().event("revoked").data(stream_revocation_message(&access)));
                                return;
                            }
                            yield Ok::<Event, Infallible>(Event::default().comment("keepalive"));
                        }
                    }
                };
                let row = match row {
                    Ok(row) => row,
                    Err(_) => {
                        yield Ok::<Event, Infallible>(Event::default().event("error").data("replay storage is temporarily unavailable"));
                        return;
                    }
                };
                let Some((row_id, update_id, event_type, chat_id, telegram_user_id, payload, received_at, expires_at)) = row else {
                    break;
                };
                let update = StoredUpdate {
                    row_id,
                    update_id,
                    event_type,
                    chat_id,
                    telegram_user_id,
                    payload,
                    received_at,
                    expires_at,
                };
                let serialized = match serialize_stream_update(&update, shape) {
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
                last_seen_id = row_id;
                yield Ok::<Event, Infallible>(Event::default().id(row_id.to_string()).event("update").data(serialized));
            }
        }
        if replay_truncated {
            yield Ok::<Event, Infallible>(Event::default().id(last_seen_id.to_string()).event("resync").data("reconnect with this Last-Event-ID to continue replay"));
            return;
        }
        loop {
            tokio::select! {
                next = receiver.recv() => match next {
                Ok(update) => {
                    if update.row_id <= last_seen_id {
                        continue;
                    }
                    let row_id = update.row_id;
                    last_seen_id = row_id;
                    if update.expires_at <= Utc::now() {
                        continue;
                    }
                    let serialized = match serialize_stream_update(&update, shape) {
                        Ok(serialized) if serialized.len() <= SSE_REPLAY_EVENT_BYTE_LIMIT => serialized,
                        Ok(_) => {
                            yield Ok::<Event, Infallible>(Event::default().event("error").data("stored update exceeds the replay event limit"));
                            return;
                        }
                        Err(_) => {
                            yield Ok::<Event, Infallible>(Event::default().event("error").data("update serialization failed"));
                            return;
                        }
                    };
                    yield Ok::<Event, Infallible>(Event::default().id(row_id.to_string()).event("update").data(serialized));
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    yield Ok::<Event, Infallible>(Event::default().id(last_seen_id.to_string()).event("resync").data("consumer lagged; reconnect with Last-Event-ID"));
                    return;
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                },
                _ = access_check.tick() => {
                    let active = stream_access_active(&database, bot_id, &access).await;
                    if !active {
                        yield Ok::<Event, Infallible>(Event::default().event("revoked").data(stream_revocation_message(&access)));
                        break;
                    }
                    yield Ok::<Event, Infallible>(Event::default().comment("keepalive"));
                }
            }
        }
    };
    let mut response = Sse::new(stream).into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-cache, no-store"),
    );
    response
        .headers_mut()
        .insert("x-accel-buffering", HeaderValue::from_static("no"));
    response
}

fn stream_revocation_message(access: &StreamAccess) -> &'static str {
    match access {
        StreamAccess::Key(_) => "stream key revoked",
        StreamAccess::Console { .. } => "console access revoked",
    }
}

async fn stream_access_active(
    database: &sqlx::PgPool,
    bot_id: Uuid,
    access: &StreamAccess,
) -> bool {
    match access {
        StreamAccess::Key(digest) => sqlx::query_scalar::<_, bool>(
            r#"SELECT EXISTS(
                   SELECT 1
                     FROM event_stream_keys keys
                     JOIN bots ON bots.id = keys.bot_id
                     JOIN memberships ON memberships.user_id = bots.user_id
                    WHERE keys.bot_id = $1 AND keys.secret_hash = $2
                      AND keys.revoked_at IS NULL
                      AND (memberships.status IN ('active', 'trialing') OR
                           (memberships.status IN ('past_due', 'canceled') AND
                            memberships.current_period_ends_at > now()))
               )"#,
        )
        .bind(bot_id)
        .bind(digest)
        .fetch_one(database)
        .await
        .unwrap_or(false),
        StreamAccess::Console {
            user_id,
            session_id,
        } => sqlx::query_scalar::<_, bool>(
            r#"SELECT EXISTS(
                   SELECT 1
                     FROM sessions
                     JOIN bots ON bots.user_id = sessions.user_id
                    WHERE sessions.id = $1 AND sessions.user_id = $2
                      AND sessions.expires_at > now() AND bots.id = $3
               )"#,
        )
        .bind(session_id)
        .bind(user_id)
        .bind(bot_id)
        .fetch_one(database)
        .await
        .unwrap_or(false),
    }
}

fn stream_after(headers: &HeaderMap, query_after: Option<i64>) -> i64 {
    headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<i64>().ok())
        .or(query_after)
        .unwrap_or(0)
        .max(0)
}

fn serialize_stream_update(
    update: &StoredUpdate,
    shape: StreamEventShape,
) -> serde_json::Result<String> {
    let value = match shape {
        StreamEventShape::Public => json!({
            "row_id": update.row_id,
            "update_id": update.update_id,
            "event_type": update.event_type,
            "payload": update.payload,
        }),
        StreamEventShape::Console => json!({
            "id": update.row_id,
            "update_id": update.update_id,
            "event_type": update.event_type,
            "chat_id": update.chat_id,
            "telegram_user_id": update.telegram_user_id,
            "payload": update.payload,
            "received_at": update.received_at,
            "expires_at": update.expires_at,
        }),
    };
    serde_json::to_string(&value)
}

pub(crate) fn search_pattern(value: &str) -> String {
    let value = value.chars().take(120).collect::<String>();
    let mut escaped = String::with_capacity(value.len() + 2);
    escaped.push('%');
    for character in value.chars() {
        if matches!(character, '\\' | '%' | '_') {
            escaped.push('\\');
        }
        escaped.push(character);
    }
    escaped.push('%');
    escaped
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
    source_generation: i64,
    attempt: i32,
}

#[derive(Clone, Copy, Default)]
struct ManagedWebhookSecretResolution<'a> {
    secret: Option<&'a str>,
    confirmed_absent: bool,
    ip_address: Option<&'a str>,
    confirmed_no_ip_address: bool,
}

#[derive(Clone, Debug, Eq, FromRow, PartialEq)]
struct ManagedBotPlacementSnapshot {
    id: Uuid,
    data_plane_pool: Option<String>,
    data_plane_target_pool: Option<String>,
    token_fingerprint: String,
    status: String,
    bot_kind: String,
    manager_bot_id: Option<Uuid>,
    manager_telegram_bot_id: Option<i64>,
}

fn is_staged_managed_initial_placement(
    bot: &ManagedBotPlacementSnapshot,
    manager_id: Uuid,
    manager_telegram_bot_id: i64,
    target_pool: &str,
) -> bool {
    bot.data_plane_pool.is_none()
        && bot.data_plane_target_pool.as_deref() == Some(target_pool)
        && bot.status == "degraded"
        && bot.bot_kind == "managed"
        && bot.manager_bot_id == Some(manager_id)
        && bot.manager_telegram_bot_id == Some(manager_telegram_bot_id)
}

fn managed_sync_claim_is_current(
    current_generation: Option<i64>,
    expected_generation: i64,
) -> bool {
    current_generation == Some(expected_generation)
}

async fn lock_managed_sync_claim(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    job: &ManagedBotSyncJob,
) -> Result<()> {
    let current_generation = sqlx::query_scalar::<_, i64>(
        r#"SELECT source_generation
             FROM managed_bot_sync_jobs
            WHERE id = $1 AND state = 'processing'
            FOR UPDATE"#,
    )
    .bind(job.id)
    .fetch_optional(&mut **tx)
    .await?;
    if !managed_sync_claim_is_current(current_generation, job.source_generation) {
        return Err(AppError::Upstream(
            "A newer managed-bot event superseded this synchronization".into(),
        ));
    }
    Ok(())
}

async fn mark_managed_webhook_blocked(
    state: &AppState,
    job: &ManagedBotSyncJob,
    bot_id: Uuid,
) -> Result<()> {
    let mut tx = state.db.begin().await?;
    lock_managed_sync_claim(&mut tx, job).await?;
    sqlx::query(
        r#"UPDATE managed_bot_sync_jobs
              SET state = 'conflict', error_summary = 'webhook_secret_required',
                  locked_at = NULL, updated_at = now()
            WHERE id = $1 AND source_generation = $2 AND state = 'processing'"#,
    )
    .bind(job.id)
    .bind(job.source_generation)
    .execute(&mut *tx)
    .await?;
    sqlx::query("UPDATE bots SET status = 'degraded', updated_at = now() WHERE id = $1")
        .bind(bot_id)
        .execute(&mut *tx)
        .await?;
    sqlx::query(
        r#"INSERT INTO audit_log (user_id, bot_id, action, metadata, expires_at)
           SELECT bots.user_id, bots.id, 'bot.managed_webhook_blocked',
                  '{"reason":"webhook_secret_required"}'::jsonb,
                  now() + make_interval(days => bot_effective_retention_days(bots.id))
             FROM bots WHERE bots.id = $1"#,
    )
    .bind(bot_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

async fn mark_managed_webhook_blocked_if_waiting(
    state: &AppState,
    job: &ManagedBotSyncJob,
    bot_id: Uuid,
    error: &AppError,
) -> Result<()> {
    if !managed_webhook_preflight_requires_operator(error) {
        return Ok(());
    }
    let waiting = sqlx::query_scalar::<_, bool>(
        r#"SELECT EXISTS(
               SELECT 1 FROM bot_data_plane_operations
                WHERE bot_id = $1 AND phase = 'webhook_resolution_required'
           )"#,
    )
    .bind(bot_id)
    .fetch_one(&state.db)
    .await?;
    if waiting {
        mark_managed_webhook_blocked(state, job, bot_id).await?;
    }
    Ok(())
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
    telegram_test_dc: bool,
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
                  jobs.source_generation, jobs.attempt"#,
    )
    .fetch_optional(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(job)
}

async fn process_managed_bot_sync(state: &AppState, job: ManagedBotSyncJob) {
    match sync_managed_bot(state, &job, None).await {
        Ok(()) => {
            if let Err(error) = sqlx::query(
                r#"UPDATE managed_bot_sync_jobs
                      SET state = 'completed', error_summary = NULL,
                          locked_at = NULL, completed_at = now(), updated_at = now()
                    WHERE id = $1 AND source_generation = $2 AND state = 'processing'"#,
            )
            .bind(job.id)
            .bind(job.source_generation)
            .execute(&state.db)
            .await
            {
                tracing::error!(job_id = %job.id, error = ?error, "could not complete managed bot sync job");
            }
        }
        Err(error) => {
            let error_code = managed_sync_error_code(&error);
            let terminal = matches!(
                &error,
                AppError::Conflict(_)
                    | AppError::WebhookSecretRequired { .. }
                    | AppError::WebhookIpAddressResolutionRequired { .. }
            );
            let update = if terminal {
                sqlx::query(
                    r#"UPDATE managed_bot_sync_jobs
                          SET state = 'conflict', error_summary = $3,
                              locked_at = NULL, updated_at = now()
                        WHERE id = $1 AND source_generation = $2 AND state = 'processing'"#,
                )
                .bind(job.id)
                .bind(job.source_generation)
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
                        WHERE id = $1 AND source_generation = $2 AND state = 'processing'"#,
                )
                .bind(job.id)
                .bind(job.source_generation)
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

/// Continue managed-bot onboarding or token rotation after it was deliberately
/// fenced because native webhook intent is not observable through getWebhookInfo. The
/// operator input stays in request memory; once preflight succeeds, the exact
/// webhook and both rotation credentials live only in the encrypted lifecycle
/// operation so a crash can resume safely.
pub async fn recover_managed_bot_rotation(
    state: &AppState,
    user_id: Uuid,
    bot_id: Uuid,
    existing_webhook_secret: Option<&str>,
    existing_webhook_has_no_secret: bool,
    existing_webhook_ip_address: Option<&str>,
    existing_webhook_has_no_ip_address: bool,
) -> Result<()> {
    use crate::lifecycle::{has_operation, resolve_connect_webhook_secret};

    // Validate mutually-exclusive and format constraints before claiming the
    // durable job. The actual current webhook is re-read below by the same
    // fail-closed preflight used by Connect.
    resolve_connect_webhook_secret(
        None,
        existing_webhook_secret,
        existing_webhook_has_no_secret,
    )?;

    let job = sqlx::query_as::<_, ManagedBotSyncJob>(
        r#"WITH candidate AS (
               SELECT jobs.id
                 FROM managed_bot_sync_jobs jobs
                 JOIN bots child
                   ON child.manager_bot_id = jobs.manager_bot_id
                  AND child.telegram_bot_id = jobs.managed_telegram_bot_id
                WHERE child.id = $1
                  AND child.user_id = $2
                  AND child.bot_kind = 'managed'
                  AND (child.data_plane_pool IS NOT NULL
                       OR child.data_plane_target_pool IS NOT NULL)
                  AND jobs.state = 'conflict'
                  AND jobs.error_summary = 'webhook_secret_required'
                FOR UPDATE OF jobs
           )
           UPDATE managed_bot_sync_jobs jobs
              SET state = 'processing', attempt = attempt + 1,
                  source_generation = nextval('managed_bot_sync_source_generation_seq'),
                  locked_at = now(), completed_at = NULL, updated_at = now()
             FROM candidate
            WHERE jobs.id = candidate.id
        RETURNING jobs.id, jobs.manager_bot_id, jobs.managed_telegram_bot_id,
                  jobs.managed_owner_telegram_user_id, jobs.username,
                  jobs.display_name, jobs.source_update_id,
                  jobs.source_generation, jobs.attempt"#,
    )
    .bind(bot_id)
    .bind(user_id)
    .fetch_optional(&state.db)
    .await?
    .ok_or_else(|| {
        AppError::Conflict("This managed bot is not waiting for webhook recovery".into())
    })?;

    let resolution = ManagedWebhookSecretResolution {
        secret: existing_webhook_secret,
        confirmed_absent: existing_webhook_has_no_secret,
        ip_address: existing_webhook_ip_address,
        confirmed_no_ip_address: existing_webhook_has_no_ip_address,
    };
    let result = sync_managed_bot(state, &job, Some(resolution)).await;
    match result {
        Ok(()) => {
            let completed = sqlx::query(
                r#"UPDATE managed_bot_sync_jobs
                      SET state = 'completed', error_summary = NULL,
                          locked_at = NULL, completed_at = now(), updated_at = now()
                    WHERE id = $1 AND source_generation = $2 AND state = 'processing'"#,
            )
            .bind(job.id)
            .bind(job.source_generation)
            .execute(&state.db)
            .await?;
            if completed.rows_affected() != 1 {
                return Err(AppError::Conflict(
                    "A newer managed-bot change superseded this recovery request".into(),
                ));
            }
            Ok(())
        }
        Err(error) => {
            // Once an operation exists, the supplied secret and replacement
            // token are durably encrypted there; the ordinary lifecycle worker
            // may resume it. Before that checkpoint, keep the job explicitly
            // recoverable and require the operator input again.
            let operation_exists = has_operation(state, bot_id).await.unwrap_or(false);
            let (job_state, error_code) = if operation_exists {
                ("retry", managed_sync_error_code(&error))
            } else {
                ("conflict", "webhook_secret_required")
            };
            if let Err(database_error) = sqlx::query(
                r#"UPDATE managed_bot_sync_jobs
                      SET state = $3, error_summary = $4,
                          next_attempt_at = CASE WHEN $3 = 'retry' THEN now() ELSE next_attempt_at END,
                          locked_at = NULL, updated_at = now()
                    WHERE id = $1 AND source_generation = $2 AND state = 'processing'"#,
            )
            .bind(job.id)
            .bind(job.source_generation)
            .bind(job_state)
            .bind(error_code)
            .execute(&state.db)
            .await
            {
                tracing::error!(job_id = %job.id, error = ?database_error, "could not preserve managed rotation recovery state");
            }
            Err(error)
        }
    }
}

fn managed_sync_error_code(error: &AppError) -> &'static str {
    match error {
        AppError::Conflict(_) => "ownership_conflict",
        AppError::WebhookSecretRequired { .. } => "webhook_secret_required",
        AppError::WebhookIpAddressResolutionRequired { .. } => "webhook_secret_required",
        AppError::Validation(_) => "invalid_managed_bot",
        AppError::Crypto(_) => "credential_encryption_failed",
        AppError::Database(_) => "database_unavailable",
        AppError::GatewayDrainPending => "gateway_draining",
        AppError::Unauthorized | AppError::Forbidden | AppError::NotFound => "manager_unavailable",
        AppError::Config(_) | AppError::Internal => "internal_error",
        AppError::Upstream(_) | AppError::RateLimited | AppError::PlanLimit(_) => {
            "telegram_unavailable"
        }
    }
}

fn managed_webhook_preflight_requires_operator(error: &AppError) -> bool {
    matches!(
        error,
        AppError::WebhookSecretRequired { .. }
            | AppError::WebhookIpAddressResolutionRequired { .. }
            | AppError::Validation(_)
    )
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

async fn sync_managed_bot_data_plane(
    state: &AppState,
    job: &ManagedBotSyncJob,
    manager: &BotRecord,
    child_token: &str,
    webhook_secret_resolution: Option<ManagedWebhookSecretResolution<'_>>,
) -> Result<()> {
    use crate::lifecycle::{
        DataPlanePool, ExistingWebhookResolution, LifecycleOperation, LifecycleOutcome, SourcePool,
        create_operation, create_rotation_operation, has_operation,
        prepare_managed_rotation_webhook_transfer, prepare_token_rotation,
        run_bot_operation_with_webhook_resolution, source_base, source_for_bot,
        validate_migration_path,
    };

    let target = DataPlanePool::parse(
        manager
            .data_plane_pool
            .as_deref()
            .ok_or_else(|| AppError::Upstream("manager route is not active".into()))?,
    )?;
    let observed_bot_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT id FROM bots WHERE telegram_bot_id = $1 AND telegram_test_dc = $2",
    )
    .bind(job.managed_telegram_bot_id)
    .bind(manager.telegram_test_dc)
    .fetch_optional(&state.db)
    .await?;

    if let Some(bot_id) = observed_bot_id
        && has_operation(state, bot_id).await?
    {
        let resolution = webhook_secret_resolution.unwrap_or_default();
        let operation = run_bot_operation_with_webhook_resolution(
            state,
            bot_id,
            ExistingWebhookResolution {
                secret: resolution.secret,
                confirmed_no_secret: resolution.confirmed_absent,
                ip_address: resolution.ip_address,
                confirmed_no_ip_address: resolution.confirmed_no_ip_address,
            },
        )
        .await;
        let outcome = match operation {
            Ok(outcome) => outcome,
            Err(error) => {
                mark_managed_webhook_blocked_if_waiting(state, job, bot_id, &error).await?;
                return Err(error);
            }
        };
        match outcome {
            LifecycleOutcome::Active { .. } => {}
            LifecycleOutcome::RolledBack => {
                return Err(AppError::Upstream(
                    "managed bot token rotation was safely rolled back and will retry".into(),
                ));
            }
            LifecycleOutcome::Busy => {
                return Err(AppError::Upstream(
                    "managed bot lifecycle operation is already running".into(),
                ));
            }
        }
    }

    let existing = match observed_bot_id {
        Some(bot_id) => find_active_bot_by_id(state, bot_id).await?,
        None => None,
    };
    let placement_snapshot = match observed_bot_id {
        Some(bot_id) => {
            sqlx::query_as::<_, ManagedBotPlacementSnapshot>(
                r#"SELECT id, data_plane_pool, data_plane_target_pool, token_fingerprint,
                      status, bot_kind, manager_bot_id, manager_telegram_bot_id
                 FROM bots WHERE id = $1"#,
            )
            .bind(bot_id)
            .fetch_optional(&state.db)
            .await?
        }
        None => None,
    };
    if placement_snapshot.as_ref().map(|bot| bot.id) != observed_bot_id {
        return Err(AppError::Upstream(
            "managed bot changed while synchronization started".into(),
        ));
    }
    if existing.as_ref().is_some_and(|bot| {
        bot.user_id != manager.user_id || bot.telegram_test_dc != manager.telegram_test_dc
    }) {
        return Err(AppError::Conflict(
            "managed bot belongs to another workspace".into(),
        ));
    }
    let source = existing
        .as_ref()
        .map(source_for_bot)
        .transpose()?
        .unwrap_or(SourcePool::Cloud);
    let staged_initial = placement_snapshot.as_ref().is_some_and(|bot| {
        is_staged_managed_initial_placement(
            bot,
            manager.id,
            manager.telegram_bot_id,
            target.as_str(),
        )
    });
    if existing
        .as_ref()
        .is_some_and(|bot| bot.data_plane_pool.is_none())
        && !staged_initial
    {
        return Err(AppError::Conflict(
            "The clean-state data-plane release cannot adopt a legacy managed bot record".into(),
        ));
    }
    let token_lookup_hash = state
        .crypto
        .bot_public_id(child_token, manager.telegram_test_dc);
    let token_fingerprint = Crypto::token_fingerprint(child_token, manager.telegram_test_dc);
    let same_pool = existing
        .as_ref()
        .is_some_and(|bot| bot.data_plane_pool.as_deref() == Some(target.as_str()));
    if existing.is_some() && !same_pool {
        validate_migration_path(source, target, LifecycleOperation::ManagedSync)?;
    }
    let initial_placement = existing.is_none() || staged_initial;
    let rotating_token = same_pool
        && existing
            .as_ref()
            .is_some_and(|bot| bot.token_fingerprint != token_fingerprint);

    // Probing the replacement token before closing the old official Client
    // would create two Clients for the same numeric bot ID. For a rotation,
    // defer identity verification until after confirmed close. New children
    // and unchanged tokens can be verified immediately.
    let (username, display_name) = if rotating_token {
        (job.username.clone(), job.display_name.clone())
    } else {
        let (_, me) = raw_telegram_json_for_dc(
            &state.telegram,
            source_base(state, source)?,
            child_token,
            manager.telegram_test_dc,
            "getMe",
            &json!({}),
        )
        .await?;
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
        (
            identity
                .get("username")
                .and_then(Value::as_str)
                .filter(|username| !username.is_empty())
                .unwrap_or(&job.username)
                .to_owned(),
            identity
                .get("first_name")
                .and_then(Value::as_str)
                .filter(|name| !name.is_empty())
                .unwrap_or(&job.display_name)
                .to_owned(),
        )
    };

    let bot_id = existing.as_ref().map_or_else(Uuid::new_v4, |bot| bot.id);
    let encrypted_token = state.crypto.encrypt(
        child_token.as_bytes(),
        format!("bot:{bot_id}:token").as_bytes(),
    )?;
    let resolution = webhook_secret_resolution.unwrap_or_default();
    let mut blocked_webhook_error = None;
    let (prepared_webhook, prepared_rotation) = if rotating_token {
        let old_token = decrypt_token(state, existing.as_ref().ok_or(AppError::Internal)?)?;
        let old_token = std::str::from_utf8(&old_token).map_err(|_| AppError::Internal)?;
        (
            None,
            Some(prepare_token_rotation(
                state,
                bot_id,
                old_token,
                child_token,
            )?),
        )
    } else if initial_placement {
        validate_migration_path(source, target, LifecycleOperation::ManagedSync)?;
        match prepare_managed_rotation_webhook_transfer(
            state,
            bot_id,
            source,
            child_token,
            manager.telegram_test_dc,
            ExistingWebhookResolution {
                secret: resolution.secret,
                confirmed_no_secret: resolution.confirmed_absent,
                ip_address: resolution.ip_address,
                confirmed_no_ip_address: resolution.confirmed_no_ip_address,
            },
        )
        .await
        {
            Ok(prepared) => (Some(prepared), None),
            Err(error) if managed_webhook_preflight_requires_operator(&error) => {
                if existing.is_some() {
                    mark_managed_webhook_blocked(state, job, bot_id).await?;
                    return Err(error);
                }
                // Persist a route-less degraded child so the authenticated
                // recovery UI can collect the missing intent. The native
                // webhook and cloud ownership remain completely untouched.
                blocked_webhook_error = Some(error);
                (None, None)
            }
            Err(error) => return Err(error),
        }
    } else {
        (None, None)
    };

    let mut tx = state.db.begin().await?;
    if let Some(existing) = &existing {
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1::text, 0))")
            .bind(existing.id)
            .execute(&mut *tx)
            .await?;
    }
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(job.managed_telegram_bot_id)
        .execute(&mut *tx)
        .await?;
    // External identity/webhook reads happen before this transaction. Fence
    // their result against the exact claimed generation before mutating the
    // child or creating an operation, and hold the row lock through commit so
    // a newer lifecycle upsert waits and then wins cleanly.
    lock_managed_sync_claim(&mut tx, job).await?;
    let still_observed = sqlx::query_as::<_, ManagedBotPlacementSnapshot>(
        r#"SELECT id, data_plane_pool, data_plane_target_pool, token_fingerprint,
                  status, bot_kind, manager_bot_id, manager_telegram_bot_id
             FROM bots
            WHERE telegram_bot_id = $1 AND telegram_test_dc = $2
            FOR UPDATE"#,
    )
    .bind(job.managed_telegram_bot_id)
    .bind(manager.telegram_test_dc)
    .fetch_optional(&mut *tx)
    .await?;
    if still_observed != placement_snapshot {
        return Err(AppError::Upstream(
            "managed bot changed while synchronization started".into(),
        ));
    }
    if existing.is_none() {
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1::text, 0))")
            .bind(bot_id)
            .execute(&mut *tx)
            .await?;
    }

    if rotating_token {
        sqlx::query(
            r#"UPDATE bots
                  SET username = $2, display_name = $3,
                      bot_kind = 'managed', manager_bot_id = $4,
                      manager_telegram_bot_id = $5,
                      managed_owner_telegram_user_id = $6,
                      data_plane_pool = NULL, data_plane_target_pool = $7,
                      status = 'provisioning', updated_at = now()
                WHERE id = $1"#,
        )
        .bind(bot_id)
        .bind(&username)
        .bind(&display_name)
        .bind(manager.id)
        .bind(manager.telegram_bot_id)
        .bind(job.managed_owner_telegram_user_id)
        .bind(target.as_str())
        .execute(&mut *tx)
        .await?;
    } else if initial_placement && existing.is_some() {
        sqlx::query(
            r#"UPDATE bots
                  SET username = $2, display_name = $3,
                      token_ciphertext = $4, token_nonce = $5,
                      token_fingerprint = $6, token_lookup_hash = $7,
                      bot_kind = 'managed', manager_bot_id = $8,
                      manager_telegram_bot_id = $9,
                      managed_owner_telegram_user_id = $10,
                      data_plane_pool = NULL, data_plane_target_pool = $11,
                      routing_mode = $12, status = 'provisioning',
                      updated_at = now()
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
        .bind(target.as_str())
        .bind(target.routing_mode())
        .execute(&mut *tx)
        .await?;
    } else if existing.is_some() {
        sqlx::query(
            r#"UPDATE bots
                  SET username = $2, display_name = $3,
                      token_ciphertext = $4, token_nonce = $5,
                      token_fingerprint = $6, token_lookup_hash = $7,
                      bot_kind = 'managed', manager_bot_id = $8,
                      manager_telegram_bot_id = $9,
                      managed_owner_telegram_user_id = $10,
                      data_plane_target_pool = NULL,
                      routing_mode = $11, status = 'healthy',
                      updated_at = now()
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
        .bind(target.routing_mode())
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
                   (id, user_id, telegram_bot_id, telegram_test_dc, username, display_name,
                    token_ciphertext, token_nonce, token_fingerprint, public_id,
                    token_lookup_hash, ingress_secret_ciphertext, ingress_secret_nonce,
                    status, routing_mode, bot_kind, manager_bot_id,
                    manager_telegram_bot_id, managed_owner_telegram_user_id,
                    data_plane_target_pool)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,
                       $14,$15,'managed',$16,$17,$18,$19)"#,
        )
        .bind(bot_id)
        .bind(manager.user_id)
        .bind(job.managed_telegram_bot_id)
        .bind(manager.telegram_test_dc)
        .bind(&username)
        .bind(&display_name)
        .bind(&encrypted_token.data)
        .bind(&encrypted_token.nonce)
        .bind(&token_fingerprint)
        .bind(public_id)
        .bind(&token_lookup_hash)
        .bind(&encrypted_ingress.data)
        .bind(&encrypted_ingress.nonce)
        .bind(if blocked_webhook_error.is_some() {
            "degraded"
        } else {
            "provisioning"
        })
        .bind(target.routing_mode())
        .bind(manager.id)
        .bind(manager.telegram_bot_id)
        .bind(job.managed_owner_telegram_user_id)
        .bind(target.as_str())
        .execute(&mut *tx)
        .await?;
    }
    sqlx::query(
        "INSERT INTO bot_update_state (bot_id) VALUES ($1) ON CONFLICT (bot_id) DO NOTHING",
    )
    .bind(bot_id)
    .execute(&mut *tx)
    .await?;

    if rotating_token {
        let generation = sqlx::query_scalar::<_, i64>(
            "SELECT generation FROM data_plane_route_state WHERE singleton = TRUE",
        )
        .fetch_one(&mut *tx)
        .await?;
        create_rotation_operation(
            &mut tx,
            bot_id,
            source,
            target,
            prepared_rotation.as_ref().ok_or(AppError::Internal)?,
        )
        .await?;
        sqlx::query(
            "UPDATE bot_data_plane_operations SET withdraw_generation = $2 WHERE bot_id = $1",
        )
        .bind(bot_id)
        .bind(generation)
        .execute(&mut *tx)
        .await?;
    } else if initial_placement && blocked_webhook_error.is_none() {
        create_operation(
            &mut tx,
            bot_id,
            LifecycleOperation::ManagedSync,
            source,
            target,
            prepared_webhook.as_ref().ok_or(AppError::Internal)?,
        )
        .await?;
    }
    if blocked_webhook_error.is_some() {
        sqlx::query(
            r#"UPDATE managed_bot_sync_jobs
                  SET state = 'conflict', error_summary = 'webhook_secret_required',
                      locked_at = NULL, updated_at = now()
                WHERE id = $1 AND source_generation = $2 AND state = 'processing'"#,
        )
        .bind(job.id)
        .bind(job.source_generation)
        .execute(&mut *tx)
        .await?;
    }
    sqlx::query(
        r#"INSERT INTO audit_log (user_id, bot_id, action, metadata, expires_at)
           SELECT bots.user_id, bots.id, $2, $3,
                  now() + make_interval(days => bot_effective_retention_days(bots.id))
             FROM bots WHERE bots.id = $1"#,
    )
    .bind(bot_id)
    .bind(if blocked_webhook_error.is_some() {
        "bot.managed_webhook_blocked"
    } else if initial_placement {
        "bot.managed_discovered"
    } else if existing.is_some() {
        "bot.managed_refreshed"
    } else {
        "bot.managed_discovered"
    })
    .bind(json!({
        "manager_bot_id": manager.id,
        "manager_telegram_bot_id": manager.telegram_bot_id,
        "telegram_bot_id": job.managed_telegram_bot_id,
        "managed_owner_telegram_user_id": job.managed_owner_telegram_user_id,
        "source_update_id": job.source_update_id,
        "data_plane_target": target.as_str(),
        "token_rotation": rotating_token,
    }))
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;

    if let Some(error) = blocked_webhook_error {
        return Err(error);
    }

    if existing.is_some() && !rotating_token && !initial_placement {
        return Ok(());
    }
    let operation = run_bot_operation_with_webhook_resolution(
        state,
        bot_id,
        ExistingWebhookResolution {
            secret: resolution.secret,
            confirmed_no_secret: resolution.confirmed_absent,
            ip_address: resolution.ip_address,
            confirmed_no_ip_address: resolution.confirmed_no_ip_address,
        },
    )
    .await;
    let outcome = match operation {
        Ok(outcome) => outcome,
        Err(error) => {
            mark_managed_webhook_blocked_if_waiting(state, job, bot_id, &error).await?;
            return Err(error);
        }
    };
    match outcome {
        LifecycleOutcome::Active { .. } => Ok(()),
        LifecycleOutcome::RolledBack => Err(AppError::Upstream(
            "managed bot token rotation was safely rolled back and will retry".into(),
        )),
        LifecycleOutcome::Busy => Err(AppError::Upstream(
            "managed bot lifecycle operation is already running".into(),
        )),
    }
}

async fn sync_managed_bot(
    state: &AppState,
    job: &ManagedBotSyncJob,
    webhook_secret_resolution: Option<ManagedWebhookSecretResolution<'_>>,
) -> Result<()> {
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

    if state.config.data_plane_enabled {
        if manager.data_plane_pool.is_none() {
            return Err(AppError::Upstream(
                "manager Bot API migration is not ready".into(),
            ));
        }
        return sync_managed_bot_data_plane(
            state,
            job,
            &manager,
            &child_token,
            webhook_secret_resolution,
        )
        .await;
    }

    let observed_bot_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT id FROM bots WHERE telegram_bot_id = $1 AND telegram_test_dc = $2",
    )
    .bind(job.managed_telegram_bot_id)
    .bind(manager.telegram_test_dc)
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
                  routing_mode, telegram_test_dc
             FROM bots
            WHERE telegram_bot_id = $1 AND telegram_test_dc = $2
            FOR UPDATE"#,
    )
    .bind(job.managed_telegram_bot_id)
    .bind(manager.telegram_test_dc)
    .fetch_optional(&mut *tx)
    .await?;
    if stored.as_ref().map(|bot| bot.id) != observed_bot_id {
        return Err(AppError::Upstream(
            "managed bot changed while synchronization started".into(),
        ));
    }
    if let Some(stored) = &stored
        && (stored.user_id != manager.user_id
            || stored.telegram_test_dc != manager.telegram_test_dc)
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
    let (_, me) = raw_telegram_json_for_dc(
        &state.telegram,
        &backend,
        &child_token,
        manager.telegram_test_dc,
        "getMe",
        &json!({}),
    )
    .await?;
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

    let (_, webhook_info) = raw_telegram_json_for_dc(
        &state.telegram,
        &backend,
        &child_token,
        manager.telegram_test_dc,
        "getWebhookInfo",
        &json!({}),
    )
    .await?;
    let previous_webhook = existing_webhook(
        &webhook_info,
        &state.config.api_base_url,
        if child_routing_mode == "local" {
            ExistingWebhookPolicy::Local
        } else {
            ExistingWebhookPolicy::Cloud {
                allow_insecure_development: state.config.app_env != "production",
            }
        },
    )
    .map_err(|_| AppError::Upstream("managed bot webhook inspection failed".into()))?;
    let token_lookup_hash = state
        .crypto
        .bot_public_id(&child_token, manager.telegram_test_dc);
    let token_fingerprint = Crypto::token_fingerprint(&child_token, manager.telegram_test_dc);
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
                   (id, user_id, telegram_bot_id, telegram_test_dc, username, display_name,
                    token_ciphertext, token_nonce, token_fingerprint, public_id,
                    token_lookup_hash, ingress_secret_ciphertext, ingress_secret_nonce,
                    status, routing_mode, update_mode, bot_kind, manager_bot_id,
                    manager_telegram_bot_id, managed_owner_telegram_user_id)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,
                       'provisioning',$14,$15,'managed',$16,$17,$18)"#,
        )
        .bind(bot_id)
        .bind(manager.user_id)
        .bind(job.managed_telegram_bot_id)
        .bind(manager.telegram_test_dc)
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
            .and_then(|webhook| webhook.allowed_updates.as_ref()),
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
    data_plane_pool: Option<String>,
    telegram_test_dc: bool,
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
                  bots.token_ciphertext, bots.token_nonce, bots.routing_mode,
                  bots.data_plane_pool, bots.telegram_test_dc
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
    let base = if job.data_plane_pool.is_some() {
        crate::lifecycle::gateway_base(state).ok()
    } else if job.routing_mode == "local" {
        state.config.telegram_local_api_url.as_deref()
    } else {
        Some(state.config.telegram_cloud_api_url.as_str())
    };
    let Some(base) = base else {
        return;
    };
    let started = Instant::now();
    if let Ok((status, response)) = raw_telegram_json_for_dc(
        &state.telegram,
        base,
        std::str::from_utf8(&token).unwrap_or(""),
        job.telegram_test_dc,
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
        ALL_UPDATE_TYPES, ExistingWebhookPolicy, ManagedBotPlacementSnapshot, StreamEventShape,
        byte_range, decode_bot_bound_local_file_path, encode_bot_bound_local_file_path,
        existing_webhook, is_globally_routable, is_staged_managed_initial_placement,
        managed_child_routing, managed_sync_claim_is_current, search_pattern,
        serialize_stream_update, stream_after, take_managed_bot_token,
        validate_native_local_file_path, validate_webhook_url,
    };
    use crate::crypto::Crypto;
    use crate::state::StoredUpdate;
    use axum::http::{HeaderMap, HeaderValue};
    use chrono::{TimeZone, Utc};
    use serde_json::{Value, json};
    use std::net::IpAddr;
    use uuid::Uuid;

    fn ip(value: &str) -> IpAddr {
        value.parse().expect("valid test address")
    }

    #[test]
    fn last_event_id_takes_precedence_over_query_cursor() {
        let mut headers = HeaderMap::new();
        headers.insert("last-event-id", HeaderValue::from_static("42"));

        assert_eq!(stream_after(&headers, Some(7)), 42);
        assert_eq!(stream_after(&HeaderMap::new(), Some(7)), 7);
    }

    #[test]
    fn console_stream_update_matches_update_summary_shape() {
        let received_at = Utc
            .with_ymd_and_hms(2026, 8, 14, 9, 30, 0)
            .single()
            .expect("valid received timestamp");
        let expires_at = Utc
            .with_ymd_and_hms(2026, 9, 13, 9, 30, 0)
            .single()
            .expect("valid expiry timestamp");
        let update = StoredUpdate {
            row_id: 51,
            update_id: 7001,
            event_type: "message".into(),
            chat_id: Some(99),
            telegram_user_id: Some(100),
            payload: json!({"update_id": 7001, "message": {"text": "hello"}}),
            received_at,
            expires_at,
        };

        let serialized = serialize_stream_update(&update, StreamEventShape::Console)
            .expect("console update serializes");
        let value: Value = serde_json::from_str(&serialized).expect("valid event JSON");

        assert_eq!(value["id"], 51);
        assert_eq!(value["update_id"], 7001);
        assert_eq!(value["event_type"], "message");
        assert_eq!(value["chat_id"], 99);
        assert_eq!(value["telegram_user_id"], 100);
        assert_eq!(value["received_at"], "2026-08-14T09:30:00Z");
        assert_eq!(value["expires_at"], "2026-09-13T09:30:00Z");
        assert!(value.get("row_id").is_none());
    }

    #[test]
    fn list_search_escapes_like_wildcards() {
        assert_eq!(search_pattern(r"100%_ready\now"), r"%100\%\_ready\\now%");
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
    fn local_existing_webhooks_allow_native_local_mode_destinations() {
        for url in [
            "http://10.42.0.18:9123/hook",
            "https://receiver.default.svc.cluster.local:54321/hook",
            "http://localhost:3000/hook",
        ] {
            let webhook = existing_webhook(
                &json!({
                    "ok": true,
                    "result": {
                        "url": url,
                        "has_custom_certificate": false
                    }
                }),
                "https://api.phenogram.io",
                ExistingWebhookPolicy::Local,
            )
            .expect("local Bot API webhook semantics should be accepted")
            .expect("non-empty webhook should be retained");
            assert_eq!(webhook.url, url);
        }
    }

    #[test]
    fn cloud_existing_webhooks_keep_public_https_constraints() {
        for url in [
            "http://10.42.0.18:9123/hook",
            "https://10.42.0.18:443/hook",
            "https://receiver.example:9123/hook",
        ] {
            assert!(
                existing_webhook(
                    &json!({
                        "ok": true,
                        "result": {
                            "url": url,
                            "has_custom_certificate": false
                        }
                    }),
                    "https://api.phenogram.io",
                    ExistingWebhookPolicy::Cloud {
                        allow_insecure_development: false,
                    },
                )
                .is_err(),
                "cloud policy accepted {url}"
            );
        }
    }

    #[test]
    fn local_existing_webhooks_still_reject_unsafe_url_syntax() {
        for url in [
            "ftp://receiver.local/hook",
            "http://user:password@receiver.local/hook",
            "http://receiver.local/hook#fragment",
            "http://receiver.local:0/hook",
        ] {
            assert!(
                existing_webhook(
                    &json!({
                        "ok": true,
                        "result": {
                            "url": url,
                            "has_custom_certificate": false
                        }
                    }),
                    "https://api.phenogram.io",
                    ExistingWebhookPolicy::Local,
                )
                .is_err(),
                "local policy accepted {url}"
            );
        }
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
    fn native_local_file_paths_are_scoped_to_the_bot_and_media_tree() {
        let token = "123:secret";
        let root = std::path::Path::new("/var/lib/telegram-bot-api");
        for path in [
            "/var/lib/telegram-bot-api/123:secret/documents/file.bin",
            "/var/lib/telegram-bot-api/123~secret/videos/nested/file.bin",
        ] {
            validate_native_local_file_path(path, token, false, root)
                .expect("valid prod file path");
        }
        for path in [
            "/var/lib/telegram-bot-api/123:secret:T/photos/file.jpg",
            "/var/lib/telegram-bot-api/123~secret~T/voice/file.ogg",
        ] {
            validate_native_local_file_path(path, token, true, root).expect("valid test file path");
        }
        for path in [
            "/etc/passwd",
            "/var/lib/telegram-bot-api/456:other/documents/file.bin",
            "/var/lib/telegram-bot-api/123:secret/tdlib/session.db",
            "/var/lib/telegram-bot-api/123:secret/documents/../session.db",
            "/var/lib/telegram-bot-api/123:secret/documents",
        ] {
            assert!(
                validate_native_local_file_path(path, token, false, root).is_err(),
                "accepted {path}"
            );
        }
    }

    #[test]
    fn opaque_local_file_handles_reject_wrong_bot_and_tampering() {
        let crypto = Crypto::new(&"a".repeat(32), &"b".repeat(32), &"c".repeat(32));
        let owner = Uuid::from_u128(1);
        let other = Uuid::from_u128(2);
        let path = "/var/lib/telegram-bot-api/123:secret/documents/file.bin";
        let encoded =
            encode_bot_bound_local_file_path(&crypto, owner, path).expect("encoded handle");

        assert_eq!(
            decode_bot_bound_local_file_path(&crypto, owner, &encoded).expect("owner decodes"),
            path
        );
        assert!(decode_bot_bound_local_file_path(&crypto, other, &encoded).is_err());

        let mut tampered = encoded.into_bytes();
        let last = tampered.last_mut().expect("nonempty handle");
        *last = if *last == b'A' { b'B' } else { b'A' };
        let tampered = String::from_utf8(tampered).expect("base64url remains UTF-8");
        assert!(decode_bot_bound_local_file_path(&crypto, owner, &tampered).is_err());
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
    fn stale_managed_sync_generation_cannot_mutate_the_child() {
        assert!(managed_sync_claim_is_current(Some(42), 42));
        assert!(!managed_sync_claim_is_current(Some(43), 42));
        assert!(!managed_sync_claim_is_current(None, 42));
    }

    #[test]
    fn only_the_exact_degraded_route_less_child_is_recoverable_as_initial_placement() {
        let manager_id = Uuid::from_u128(10);
        let mut child = ManagedBotPlacementSnapshot {
            id: Uuid::from_u128(11),
            data_plane_pool: None,
            data_plane_target_pool: Some("standard".into()),
            token_fingerprint: "fingerprint".into(),
            status: "degraded".into(),
            bot_kind: "managed".into(),
            manager_bot_id: Some(manager_id),
            manager_telegram_bot_id: Some(100),
        };

        assert!(is_staged_managed_initial_placement(
            &child, manager_id, 100, "standard"
        ));
        child.data_plane_pool = Some("standard".into());
        assert!(!is_staged_managed_initial_placement(
            &child, manager_id, 100, "standard"
        ));
        child.data_plane_pool = None;
        child.status = "provisioning".into();
        assert!(!is_staged_managed_initial_placement(
            &child, manager_id, 100, "standard"
        ));
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
