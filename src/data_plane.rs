use axum::{
    Json,
    body::to_bytes,
    extract::{Request, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;

use crate::{
    config::PublicSurface,
    state::AppState,
    telegram::{OutboundMessageRecord, record_outbound_message},
};

pub const ROUTES_PATH: &str = "/api/internal/data-plane/routes";
pub const TELEMETRY_PATH: &str = "/api/internal/data-plane/telemetry";
/// Must match the gateway's bounded batch envelope. The gateway never queues
/// more than this many serialized bytes, while this receiver still applies
/// per-event and per-payload validation below.
pub const TELEMETRY_BODY_LIMIT: usize = 576 * 1024;
const OUTBOUND_PAYLOAD_LIMIT: usize = 448 * 1024;

pub fn is_internal_path(path: &str) -> bool {
    matches!(
        path,
        ROUTES_PATH
            | TELEMETRY_PATH
            | "/internal/data-plane/routes"
            | "/internal/data-plane/telemetry"
    )
}

#[derive(Debug, Serialize)]
struct RouteSnapshot {
    schema_version: u8,
    generation: u64,
    routes: Vec<DataPlaneRoute>,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
struct DataPlaneRoute {
    token_lookup_hash: String,
    pool: String,
}

pub async fn routes(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if let Some(response) = internal_request_rejection(&state, &headers) {
        return response;
    }

    match load_snapshot(&state).await {
        Ok(snapshot) => {
            let mut response = Json(snapshot).into_response();
            response.headers_mut().insert(
                header::CACHE_CONTROL,
                "no-store".parse().expect("static header"),
            );
            response
        }
        Err(error) => {
            tracing::error!(error = ?error, "could not build data-plane route snapshot");
            json_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "route_snapshot_unavailable",
                "Route snapshot is temporarily unavailable",
            )
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TelemetryBatch {
    schema_version: u8,
    events: Vec<TelemetryEvent>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum TelemetryEvent {
    OutboundMessage(Box<OutboundMessageTelemetryEvent>),
    ApiCall(ApiCallTelemetryEvent),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ApiCallTelemetryEvent {
    schema_version: u8,
    token_lookup_hash: String,
    pool: DataPlanePool,
    method: String,
    upstream_status: u16,
    latency_ms: u32,
    observed_at_unix_ms: i64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OutboundMessageTelemetryEvent {
    schema_version: u8,
    kind: OutboundTelemetryKind,
    token_lookup_hash: String,
    pool: DataPlanePool,
    method: String,
    upstream_status: u16,
    observed_at_unix_us: i64,
    message: OutboundTelegramMessage,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum OutboundTelemetryKind {
    OutboundMessage,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OutboundTelegramMessage {
    chat_id: i64,
    #[serde(default)]
    telegram_message_id: Option<i64>,
    #[serde(default)]
    receiver_user_id: Option<i64>,
    #[serde(default)]
    ephemeral_message_id: Option<i64>,
    #[serde(default)]
    business_connection_id: Option<String>,
    #[serde(default)]
    guest_query_id: Option<String>,
    #[serde(default)]
    message_thread_id: Option<i64>,
    #[serde(default)]
    direct_messages_topic_id: Option<i64>,
    #[serde(default)]
    payload: Option<serde_json::Value>,
    text: Option<String>,
    chat_type: Option<String>,
    title: Option<String>,
    username: Option<String>,
    display_name: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum DataPlanePool {
    Standard,
    Local,
}

impl DataPlanePool {
    fn as_str(self) -> &'static str {
        match self {
            Self::Standard => "standard",
            Self::Local => "local",
        }
    }
}

#[derive(Serialize)]
struct TelemetryAccepted {
    ok: bool,
    accepted: usize,
    unknown: usize,
}

pub async fn telemetry(State(state): State<AppState>, request: Request) -> Response {
    if let Some(response) = internal_request_rejection(&state, request.headers()) {
        return response;
    }
    let body = match to_bytes(request.into_body(), TELEMETRY_BODY_LIMIT).await {
        Ok(body) => body,
        Err(_) => {
            return json_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "telemetry_too_large",
                "Telemetry request exceeds the size limit",
            );
        }
    };
    let batch = match serde_json::from_slice::<TelemetryBatch>(&body) {
        Ok(batch) => batch,
        Err(_) => {
            return json_error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_telemetry",
                "Telemetry body is not valid JSON",
            );
        }
    };
    let events = match validate_telemetry(batch) {
        Ok(events) => events,
        Err(message) => {
            return json_error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_telemetry",
                message,
            );
        }
    };
    let total = events.len();
    match insert_telemetry(&state, events).await {
        Ok(accepted) => {
            let mut response = Json(TelemetryAccepted {
                ok: true,
                accepted,
                unknown: total - accepted,
            })
            .into_response();
            response.headers_mut().insert(
                header::CACHE_CONTROL,
                "no-store".parse().expect("static header"),
            );
            response
        }
        Err(error) => {
            tracing::warn!(error = ?error, "could not store data-plane telemetry");
            json_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "telemetry_unavailable",
                "Telemetry is temporarily unavailable",
            )
        }
    }
}

struct ValidApiCallTelemetryEvent {
    token_lookup_hash: String,
    pool: &'static str,
    method: String,
    upstream_status: i32,
    latency_ms: i32,
    observed_at: DateTime<Utc>,
}

struct ValidOutboundMessageTelemetryEvent {
    token_lookup_hash: String,
    pool: &'static str,
    method: String,
    upstream_status: i32,
    observed_at: DateTime<Utc>,
    message: OutboundTelegramMessage,
}

enum ValidTelemetryEvent {
    ApiCall(ValidApiCallTelemetryEvent),
    OutboundMessage(Box<ValidOutboundMessageTelemetryEvent>),
}

fn validate_telemetry(
    batch: TelemetryBatch,
) -> std::result::Result<Vec<ValidTelemetryEvent>, &'static str> {
    if batch.schema_version != 1 {
        return Err("unsupported telemetry schema");
    }
    if batch.events.is_empty() || batch.events.len() > 100 {
        return Err("events must contain between 1 and 100 records");
    }
    let now = Utc::now();
    let earliest = now - Duration::hours(24);
    let latest = now + Duration::minutes(5);
    batch
        .events
        .into_iter()
        .map(|event| match event {
            TelemetryEvent::ApiCall(event) => {
                let observed_at = DateTime::from_timestamp_millis(event.observed_at_unix_ms)
                    .ok_or("invalid observation timestamp")?;
                validate_common_event(
                    event.schema_version,
                    &event.token_lookup_hash,
                    &event.method,
                    event.upstream_status,
                    observed_at,
                    earliest,
                    latest,
                )?;
                if event.latency_ms > 120_000 {
                    return Err("invalid upstream latency");
                }
                Ok(ValidTelemetryEvent::ApiCall(ValidApiCallTelemetryEvent {
                    token_lookup_hash: event.token_lookup_hash,
                    pool: event.pool.as_str(),
                    method: event.method,
                    upstream_status: i32::from(event.upstream_status),
                    latency_ms: event.latency_ms as i32,
                    observed_at,
                }))
            }
            TelemetryEvent::OutboundMessage(event) => {
                let observed_at = DateTime::from_timestamp_micros(event.observed_at_unix_us)
                    .ok_or("invalid observation timestamp")?;
                validate_common_event(
                    event.schema_version,
                    &event.token_lookup_hash,
                    &event.method,
                    event.upstream_status,
                    observed_at,
                    earliest,
                    latest,
                )?;
                if !(200..=299).contains(&event.upstream_status)
                    || event.message.chat_id == 0
                    || (event.message.telegram_message_id.is_none_or(|id| id <= 0)
                        && event.message.ephemeral_message_id.is_none())
                    || event.message.ephemeral_message_id.is_some_and(|id| id <= 0)
                    || (event.message.ephemeral_message_id.is_some()
                        && event.message.receiver_user_id.is_none_or(|id| id <= 0))
                    || event.message.payload.as_ref().is_some_and(|payload| {
                        serde_json::to_vec(payload)
                            .map_or(true, |value| value.len() > OUTBOUND_PAYLOAD_LIMIT)
                            || contains_sensitive_payload_key(payload)
                    })
                    || !valid_optional_string(
                        event.message.business_connection_id.as_deref(),
                        512,
                        2 * 1024,
                    )
                    || !valid_optional_string(
                        event.message.guest_query_id.as_deref(),
                        512,
                        2 * 1024,
                    )
                    || event.message.message_thread_id.is_some_and(|id| id <= 0)
                    || event
                        .message
                        .direct_messages_topic_id
                        .is_some_and(|id| id <= 0)
                    || !valid_optional_string(event.message.text.as_deref(), 4_096, 16 * 1024)
                    || !valid_optional_string(event.message.chat_type.as_deref(), 64, 256)
                    || !valid_optional_string(event.message.title.as_deref(), 512, 2 * 1024)
                    || !valid_optional_string(event.message.username.as_deref(), 128, 512)
                    || !valid_optional_string(event.message.display_name.as_deref(), 512, 2 * 1024)
                {
                    return Err("invalid outbound message");
                }
                let _ = event.kind;
                Ok(ValidTelemetryEvent::OutboundMessage(Box::new(
                    ValidOutboundMessageTelemetryEvent {
                        token_lookup_hash: event.token_lookup_hash,
                        pool: event.pool.as_str(),
                        method: event.method,
                        upstream_status: i32::from(event.upstream_status),
                        observed_at,
                        message: event.message,
                    },
                )))
            }
        })
        .collect()
}

fn contains_sensitive_payload_key(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Object(values) => values.iter().any(|(key, value)| {
            let key = key.to_ascii_lowercase();
            key == "file_path"
                || key == "authorization"
                || key.contains("token")
                || contains_sensitive_payload_key(value)
        }),
        serde_json::Value::Array(values) => values.iter().any(contains_sensitive_payload_key),
        _ => false,
    }
}

#[allow(clippy::too_many_arguments)]
fn validate_common_event(
    schema_version: u8,
    token_lookup_hash: &str,
    method: &str,
    upstream_status: u16,
    observed_at: DateTime<Utc>,
    earliest: DateTime<Utc>,
    latest: DateTime<Utc>,
) -> std::result::Result<(), &'static str> {
    if schema_version != 1 {
        return Err("unsupported event schema");
    }
    if !valid_lookup_hash(token_lookup_hash) {
        return Err("invalid token lookup hash");
    }
    if !valid_method(method) {
        return Err("invalid Telegram method");
    }
    if !(100..=599).contains(&upstream_status) {
        return Err("invalid upstream status");
    }
    if observed_at < earliest || observed_at > latest {
        return Err("observation timestamp is outside the accepted window");
    }
    Ok(())
}

fn valid_optional_string(value: Option<&str>, max_chars: usize, max_bytes: usize) -> bool {
    value.is_none_or(|value| {
        !value.contains('\0') && value.len() <= max_bytes && value.chars().count() <= max_chars
    })
}

fn valid_lookup_hash(value: &str) -> bool {
    value.starts_with("phg_")
        && (20..=128).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

fn valid_method(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

async fn insert_telemetry(
    state: &AppState,
    events: Vec<ValidTelemetryEvent>,
) -> crate::error::Result<usize> {
    let mut api_calls = Vec::with_capacity(events.len());
    let mut outbound_messages = Vec::new();
    for event in events {
        match event {
            ValidTelemetryEvent::ApiCall(event) => api_calls.push(event),
            ValidTelemetryEvent::OutboundMessage(event) => outbound_messages.push(*event),
        }
    }
    let mut accepted = 0_i64;
    if !api_calls.is_empty() {
        let mut tx = state.db.begin().await?;
        let mut hashes = Vec::with_capacity(api_calls.len());
        let mut pools = Vec::with_capacity(api_calls.len());
        let mut methods = Vec::with_capacity(api_calls.len());
        let mut statuses = Vec::with_capacity(api_calls.len());
        let mut latencies = Vec::with_capacity(api_calls.len());
        let mut observed = Vec::with_capacity(api_calls.len());
        for event in api_calls {
            hashes.push(event.token_lookup_hash);
            pools.push(event.pool.to_owned());
            methods.push(event.method);
            statuses.push(event.upstream_status);
            latencies.push(event.latency_ms);
            observed.push(event.observed_at);
        }
        accepted += sqlx::query_scalar::<_, i64>(
            r#"WITH input AS (
                   SELECT *
                     FROM unnest($1::text[], $2::text[], $3::text[], $4::int4[],
                                 $5::int4[], $6::timestamptz[])
                          AS value(token_lookup_hash, pool, method, upstream_status,
                                   latency_ms, observed_at)
               ), inserted AS (
                   INSERT INTO api_calls
                          (bot_id, method, source, http_status, latency_ms,
                           data_plane_pool, created_at, expires_at)
                   SELECT bots.id, input.method, 'data_plane', input.upstream_status,
                          input.latency_ms, input.pool, input.observed_at,
                          input.observed_at
                              + make_interval(days => bot_effective_retention_days(bots.id))
                     FROM input
                     JOIN bots ON bots.token_lookup_hash = input.token_lookup_hash
                RETURNING 1
               )
               SELECT count(*) FROM inserted"#,
        )
        .bind(hashes)
        .bind(pools)
        .bind(methods)
        .bind(statuses)
        .bind(latencies)
        .bind(observed)
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;
    }
    for event in outbound_messages {
        let message = event.message;
        let observation_key = format!(
            "v1:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}",
            event.token_lookup_hash,
            event.pool,
            event.method,
            event.observed_at.timestamp_micros(),
            message.chat_id,
            message.telegram_message_id.unwrap_or(0),
            message.receiver_user_id.unwrap_or(0),
            message.ephemeral_message_id.unwrap_or(0),
            message.business_connection_id.as_deref().unwrap_or(""),
            message.guest_query_id.as_deref().unwrap_or(""),
            message.message_thread_id.unwrap_or(0),
            message.direct_messages_topic_id.unwrap_or(0),
        );
        let bot_id = sqlx::query_scalar::<_, uuid::Uuid>(
            r#"SELECT bots.id FROM bots
                WHERE bots.token_lookup_hash = $1 AND bots.data_plane_pool = $2"#,
        )
        .bind(&event.token_lookup_hash)
        .bind(event.pool)
        .fetch_optional(&state.db)
        .await?;
        let Some(bot_id) = bot_id else {
            continue;
        };
        record_outbound_message(
            state,
            OutboundMessageRecord {
                bot_id,
                user_id: None,
                conversation_id: None,
                chat_id: message.chat_id,
                telegram_message_id: message.telegram_message_id,
                receiver_user_id: message.receiver_user_id,
                ephemeral_message_id: message.ephemeral_message_id,
                observation_key: Some(&observation_key),
                business_connection_id: message.business_connection_id.as_deref(),
                guest_query_id: message.guest_query_id.as_deref(),
                message_thread_id: message.message_thread_id,
                direct_messages_topic_id: message.direct_messages_topic_id,
                method: &event.method,
                source: "proxy",
                text: message.text.as_deref(),
                payload: message.payload.as_ref(),
                status: "sent",
                response_status: Some(event.upstream_status),
                error_summary: None,
                created_at: Some(event.observed_at),
            },
        )
        .await?;
        accepted += 1;
    }
    usize::try_from(accepted).map_err(|_| crate::error::AppError::Internal)
}

async fn load_snapshot(state: &AppState) -> crate::error::Result<RouteSnapshot> {
    let mut tx = state.db.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *tx)
        .await?;
    let generation = sqlx::query_scalar::<_, i64>(
        "SELECT generation FROM data_plane_route_state WHERE singleton = TRUE",
    )
    .fetch_one(&mut *tx)
    .await?;
    let generation = u64::try_from(generation).map_err(|_| crate::error::AppError::Internal)?;
    let routes = sqlx::query_as::<_, DataPlaneRoute>(
        r#"SELECT bots.token_lookup_hash, bots.data_plane_pool AS pool
             FROM bots
            WHERE bots.data_plane_pool IS NOT NULL
            ORDER BY bots.token_lookup_hash"#,
    )
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(RouteSnapshot {
        schema_version: 1,
        generation,
        routes,
    })
}

fn authorized(headers: &HeaderMap, expected: &str) -> bool {
    let supplied = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    supplied.is_some_and(|supplied| bool::from(supplied.as_bytes().ct_eq(expected.as_bytes())))
}

fn internal_request_rejection(state: &AppState, headers: &HeaderMap) -> Option<Response> {
    let host = headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok());
    if !matches!(state.config.public_surface(host), PublicSurface::Unknown)
        || !state.config.data_plane_enabled
    {
        return Some(hidden());
    }
    let Some(expected) = state.config.data_plane_sync_token.as_deref() else {
        return Some(hidden());
    };
    if !authorized(headers, expected) {
        return Some(json_error(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "Authentication required",
        ));
    }
    None
}

fn hidden() -> Response {
    json_error(
        StatusCode::NOT_FOUND,
        "not_found",
        "The requested resource was not found",
    )
}

fn json_error(status: StatusCode, code: &'static str, message: &str) -> Response {
    let mut response = (
        status,
        Json(serde_json::json!({"error": {"code": code, "message": message}})),
    )
        .into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        "no-store".parse().expect("static header"),
    );
    response
}

#[cfg(test)]
mod tests {
    use axum::http::{HeaderMap, HeaderValue, header};
    use chrono::Utc;
    use serde_json::json;

    use super::{TelemetryBatch, authorized, validate_telemetry};

    #[test]
    fn bearer_auth_requires_an_exact_case_sensitive_secret() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        );
        assert!(authorized(&headers, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
        assert!(!authorized(&headers, "baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
        assert!(!authorized(&headers, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
    }

    #[test]
    fn bearer_auth_rejects_missing_or_non_bearer_headers() {
        assert!(!authorized(&HeaderMap::new(), "secret"));
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Basic c2VjcmV0"),
        );
        assert!(!authorized(&headers, "secret"));
    }

    #[test]
    fn telemetry_validation_accepts_only_bounded_exact_v1_events() {
        let valid: TelemetryBatch = serde_json::from_value(json!({
            "schema_version": 1,
            "events": [{
                "schema_version": 1,
                "token_lookup_hash": "phg_abcdefghijklmnopqrstuvwx",
                "pool": "standard",
                "method": "sendMessage",
                "upstream_status": 200,
                "latency_ms": 12,
                "observed_at_unix_ms": Utc::now().timestamp_millis()
            }]
        }))
        .unwrap();
        assert_eq!(validate_telemetry(valid).unwrap().len(), 1);

        for mutation in [
            json!({"schema_version": 2, "events": []}),
            json!({"schema_version": 1, "events": []}),
            json!({
                "schema_version": 1,
                "events": [{
                    "schema_version": 1,
                    "token_lookup_hash": "not-a-hash",
                    "pool": "standard",
                    "method": "sendMessage",
                    "upstream_status": 200,
                    "latency_ms": 12,
                    "observed_at_unix_ms": Utc::now().timestamp_millis()
                }]
            }),
            json!({
                "schema_version": 1,
                "events": [{
                    "schema_version": 1,
                    "token_lookup_hash": "phg_abcdefghijklmnopqrstuvwx",
                    "pool": "standard",
                    "method": "bad/method",
                    "upstream_status": 99,
                    "latency_ms": 120001,
                    "observed_at_unix_ms": Utc::now().timestamp_millis()
                }]
            }),
        ] {
            let batch: TelemetryBatch = serde_json::from_value(mutation).unwrap();
            assert!(validate_telemetry(batch).is_err());
        }
    }

    #[test]
    fn telemetry_validation_accepts_bounded_outbound_messages_without_weakening_v1() {
        let now = Utc::now().timestamp_micros();
        let batch: TelemetryBatch = serde_json::from_value(json!({
            "schema_version": 1,
            "events": [{
                "schema_version": 1,
                "kind": "outbound_message",
                "token_lookup_hash": "phg_abcdefghijklmnopqrstuvwx",
                "pool": "standard",
                "method": "sendMessage",
                "upstream_status": 200,
                "observed_at_unix_us": now,
                "message": {
                    "chat_id": 99,
                    "telegram_message_id": 9001,
                    "text": "hello",
                    "chat_type": "private",
                    "title": null,
                    "username": "ada",
                    "display_name": "Ada"
                }
            }, {
                "schema_version": 1,
                "kind": "outbound_message",
                "token_lookup_hash": "phg_abcdefghijklmnopqrstuvwx",
                "pool": "local",
                "method": "sendPhoto",
                "upstream_status": 200,
                "observed_at_unix_us": now,
                "message": {
                    "chat_id": -1007,
                    "telegram_message_id": 9002,
                    "business_connection_id": "business-1",
                    "message_thread_id": 7,
                    "direct_messages_topic_id": 9,
                    "text": "caption",
                    "chat_type": "supergroup",
                    "title": "Launch",
                    "username": null,
                    "display_name": null
                }
            }]
        }))
        .unwrap();
        let events = validate_telemetry(batch).expect("valid outbound telemetry");
        assert_eq!(events.len(), 2);
        assert!(matches!(
            events[0],
            super::ValidTelemetryEvent::OutboundMessage(_)
        ));
        assert!(matches!(
            events[1],
            super::ValidTelemetryEvent::OutboundMessage(_)
        ));

        for invalid_message in [
            json!({
                "chat_id": 0,
                "telegram_message_id": 1,
                "text": "hello",
                "chat_type": "private",
                "title": null,
                "username": null,
                "display_name": null
            }),
            json!({
                "chat_id": 1,
                "telegram_message_id": 0,
                "text": "hello",
                "chat_type": "private",
                "title": null,
                "username": null,
                "display_name": null
            }),
            json!({
                "chat_id": 1,
                "telegram_message_id": 1,
                "text": "x".repeat(4_097),
                "chat_type": "private",
                "title": null,
                "username": null,
                "display_name": null
            }),
        ] {
            let batch: TelemetryBatch = serde_json::from_value(json!({
                "schema_version": 1,
                "events": [{
                    "schema_version": 1,
                    "kind": "outbound_message",
                    "token_lookup_hash": "phg_abcdefghijklmnopqrstuvwx",
                    "pool": "standard",
                    "method": "sendMessage",
                    "upstream_status": 200,
                    "observed_at_unix_us": now,
                    "message": invalid_message
                }]
            }))
            .unwrap();
            assert!(validate_telemetry(batch).is_err());
        }

        let oversized_payload = json!({"text": "x".repeat(super::OUTBOUND_PAYLOAD_LIMIT)});
        let batch: TelemetryBatch = serde_json::from_value(json!({
            "schema_version": 1,
            "events": [{
                "schema_version": 1,
                "kind": "outbound_message",
                "token_lookup_hash": "phg_abcdefghijklmnopqrstuvwx",
                "pool": "standard",
                "method": "sendRichMessage",
                "upstream_status": 200,
                "observed_at_unix_us": now,
                "message": {
                    "chat_id": 99,
                    "telegram_message_id": 9003,
                    "payload": oversized_payload
                }
            }]
        }))
        .unwrap();
        assert!(validate_telemetry(batch).is_err());
    }
}
