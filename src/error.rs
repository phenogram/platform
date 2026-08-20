use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;

pub type Result<T, E = AppError> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("configuration error: {0}")]
    Config(String),
    #[error("cryptography error: {0}")]
    Crypto(String),
    #[error("database error")]
    Database(#[from] sqlx::Error),
    #[error("upstream service error: {0}")]
    Upstream(String),
    #[error("Telegram rejected the request: {0}")]
    TelegramRejected(String),
    #[error("the data-plane gateway is still draining admitted requests")]
    GatewayDrainPending,
    #[error("invalid request: {0}")]
    Validation(String),
    #[error("authentication required")]
    Unauthorized,
    #[error("access denied")]
    Forbidden,
    #[error("resource not found")]
    NotFound,
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("existing webhook secret required for {destination_host}")]
    WebhookSecretRequired { destination_host: String },
    #[error("existing webhook IP-address intent required for {destination_host}")]
    WebhookIpAddressResolutionRequired {
        destination_host: String,
        reported_ip_address: String,
    },
    #[error("plan limit reached: {0}")]
    PlanLimit(String),
    #[error("rate limit exceeded")]
    RateLimited,
    #[error("internal server error")]
    Internal,
}

#[derive(Serialize)]
struct ErrorBody {
    error: ErrorDetail,
}

#[derive(Serialize)]
struct ErrorDetail {
    code: &'static str,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    requires_webhook_secret: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    requires_webhook_ip_address_resolution: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reported_ip_address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    destination_host: Option<String>,
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let rate_limited = matches!(self, Self::RateLimited);
        let (status, code, public_message) = match &self {
            Self::Config(_) | Self::Crypto(_) | Self::Database(_) | Self::Internal => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                "The request could not be completed".to_owned(),
            ),
            Self::Upstream(_) => (
                StatusCode::BAD_GATEWAY,
                "telegram_unavailable",
                "Telegram is temporarily unavailable".to_owned(),
            ),
            Self::TelegramRejected(message) => (
                StatusCode::UNPROCESSABLE_ENTITY,
                "telegram_rejected",
                message.clone(),
            ),
            Self::GatewayDrainPending => (
                StatusCode::SERVICE_UNAVAILABLE,
                "data_plane_draining",
                "Phenogram is waiting for admitted Bot API requests to finish before continuing safely."
                    .to_owned(),
            ),
            Self::Validation(message) => (
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_request",
                message.clone(),
            ),
            Self::Unauthorized => (
                StatusCode::UNAUTHORIZED,
                "unauthorized",
                "Sign in to continue".to_owned(),
            ),
            Self::Forbidden => (
                StatusCode::FORBIDDEN,
                "forbidden",
                "You do not have access to this resource".to_owned(),
            ),
            Self::NotFound => (
                StatusCode::NOT_FOUND,
                "not_found",
                "The requested resource was not found".to_owned(),
            ),
            Self::Conflict(message) => (StatusCode::CONFLICT, "conflict", message.clone()),
            Self::WebhookSecretRequired { .. } => (
                StatusCode::CONFLICT,
                "webhook_secret_required",
                "This bot already has a webhook. Enter its current secret token, or declare that it does not use one, so Phenogram can transfer it without breaking delivery."
                    .to_owned(),
            ),
            Self::WebhookIpAddressResolutionRequired { .. } => (
                StatusCode::CONFLICT,
                "webhook_ip_address_resolution_required",
                "Telegram reports a current webhook IPv4 address but does not reveal whether it was explicitly pinned. Choose whether to preserve that exact address or continue with DNS resolution before Phenogram transfers the webhook."
                    .to_owned(),
            ),
            Self::PlanLimit(message) => {
                (StatusCode::PAYMENT_REQUIRED, "plan_limit", message.clone())
            }
            Self::RateLimited => (
                StatusCode::TOO_MANY_REQUESTS,
                "rate_limited",
                "Too many authentication attempts. Try again shortly.".to_owned(),
            ),
        };
        if status.is_server_error() {
            tracing::error!(error = ?self, "request failed");
        }
        let (
            requires_webhook_secret,
            requires_webhook_ip_address_resolution,
            destination_host,
            reported_ip_address,
        ) = match &self {
            Self::WebhookSecretRequired { destination_host } => {
                (Some(true), None, Some(destination_host.clone()), None)
            }
            Self::WebhookIpAddressResolutionRequired {
                destination_host,
                reported_ip_address,
            } => (
                None,
                Some(true),
                Some(destination_host.clone()),
                Some(reported_ip_address.clone()),
            ),
            _ => (None, None, None, None),
        };
        let mut response = (
            status,
            Json(ErrorBody {
                error: ErrorDetail {
                    code,
                    message: public_message,
                    requires_webhook_secret,
                    requires_webhook_ip_address_resolution,
                    reported_ip_address,
                    destination_host,
                },
            }),
        )
            .into_response();
        if rate_limited {
            response.headers_mut().insert(
                axum::http::header::RETRY_AFTER,
                axum::http::HeaderValue::from_static("600"),
            );
        }
        response
    }
}
