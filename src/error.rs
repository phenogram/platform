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
        let mut response = (
            status,
            Json(ErrorBody {
                error: ErrorDetail {
                    code,
                    message: public_message,
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
