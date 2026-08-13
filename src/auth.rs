use std::time::Duration;

use argon2::{Argon2, PasswordHash, PasswordHasher, PasswordVerifier, password_hash::SaltString};
use axum::{
    Json,
    extract::{FromRequestParts, Request, State},
    http::{HeaderMap, HeaderValue, Method, header, request::Parts},
    middleware::Next,
    response::{IntoResponse, Response},
};
use chrono::{Duration as ChronoDuration, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use uuid::Uuid;

use crate::{
    crypto::Crypto,
    error::{AppError, Result},
    models::PlanMembership,
    state::AppState,
};

pub const SESSION_COOKIE: &str = "phg_session";

#[derive(Clone, Debug)]
pub struct AuthUser {
    pub id: Uuid,
    pub email: String,
    pub session_id: Uuid,
    pub csrf_token: String,
}

impl FromRequestParts<AppState> for AuthUser {
    type Rejection = AppError;

    async fn from_request_parts(parts: &mut Parts, state: &AppState) -> Result<Self> {
        let token = cookie_value(&parts.headers, SESSION_COOKIE).ok_or(AppError::Unauthorized)?;
        let digest = Crypto::digest_secret(token.as_bytes());
        let row = sqlx::query_as::<_, (Uuid, Uuid, String)>(
            r#"SELECT sessions.id, users.id, users.email
                 FROM sessions
                 JOIN users ON users.id = sessions.user_id
                WHERE sessions.token_hash = $1 AND sessions.expires_at > now()"#,
        )
        .bind(digest)
        .fetch_optional(&state.db)
        .await?
        .ok_or(AppError::Unauthorized)?;

        Ok(Self {
            session_id: row.0,
            id: row.1,
            email: row.2,
            csrf_token: state.crypto.csrf_token(&token),
        })
    }
}

#[derive(Debug, Deserialize)]
pub struct Credentials {
    pub email: String,
    pub password: String,
}

#[derive(Debug, Serialize)]
pub struct SessionResponse {
    pub user: UserResponse,
    pub membership: PlanMembership,
    pub csrf_token: String,
}

#[derive(Debug, Serialize)]
pub struct UserResponse {
    pub id: Uuid,
    pub email: String,
}

pub async fn register(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(input): Json<Credentials>,
) -> Result<Response> {
    let email = normalize_email(&input.email)?;
    state
        .auth_limiter
        .check(&request_source(&headers), &email)
        .await?;
    validate_password(&input.password)?;
    if sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM users WHERE email = $1)")
        .bind(&email)
        .fetch_one(&state.db)
        .await?
    {
        return Err(AppError::Conflict(
            "An account with this email already exists".into(),
        ));
    }
    let _hashing_slot = state.auth_limiter.hashing_slot()?;
    let password = input.password;
    let password_hash = tokio::task::spawn_blocking(move || hash_password(&password))
        .await
        .map_err(|_| AppError::Internal)??;
    drop(_hashing_slot);

    let mut tx = state.db.begin().await?;
    let user_id = match sqlx::query_scalar::<_, Uuid>(
        "INSERT INTO users (email, password_hash) VALUES ($1, $2) RETURNING id",
    )
    .bind(&email)
    .bind(&password_hash)
    .fetch_one(&mut *tx)
    .await
    {
        Ok(id) => id,
        Err(error) if is_unique_violation(&error) => {
            return Err(AppError::Conflict(
                "An account with this email already exists".into(),
            ));
        }
        Err(error) => return Err(error.into()),
    };
    sqlx::query("INSERT INTO memberships (user_id, plan_id) VALUES ($1, 'free')")
        .bind(user_id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;

    create_session_response(&state, user_id, email, headers.get(header::USER_AGENT)).await
}

pub async fn login(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(input): Json<Credentials>,
) -> Result<Response> {
    let email = normalize_email(&input.email)?;
    state
        .auth_limiter
        .check(&request_source(&headers), &email)
        .await?;
    let row =
        sqlx::query_as::<_, (Uuid, String)>("SELECT id, password_hash FROM users WHERE email = $1")
            .bind(&email)
            .fetch_optional(&state.db)
            .await?;

    // Always do Argon2 work so unknown accounts do not become a timing oracle.
    let (user_id, stored_hash) = row.unwrap_or_else(|| {
        (
            Uuid::nil(),
            "$argon2id$v=19$m=19456,t=2,p=1$VGhpc0lzQUZha2VTYWx0$jlBa7S1FW7SsOIyqkDeL2Q".into(),
        )
    });
    let _hashing_slot = state.auth_limiter.hashing_slot()?;
    let password = input.password;
    let valid = tokio::task::spawn_blocking(move || verify_password(&password, &stored_hash))
        .await
        .map_err(|_| AppError::Internal)?;
    drop(_hashing_slot);
    if !valid || user_id.is_nil() {
        return Err(AppError::Validation(
            "Email or password is incorrect".into(),
        ));
    }

    create_session_response(&state, user_id, email, headers.get(header::USER_AGENT)).await
}

pub async fn logout(State(state): State<AppState>, user: AuthUser) -> Result<Response> {
    sqlx::query("DELETE FROM sessions WHERE id = $1 AND user_id = $2")
        .bind(user.session_id)
        .bind(user.id)
        .execute(&state.db)
        .await?;
    let mut headers = HeaderMap::new();
    headers.insert(
        header::SET_COOKIE,
        HeaderValue::from_str(&expired_cookie(state.config.secure_cookies()))
            .map_err(|_| AppError::Internal)?,
    );
    Ok((headers, Json(serde_json::json!({"ok": true}))).into_response())
}

pub async fn me(State(state): State<AppState>, user: AuthUser) -> Result<Json<SessionResponse>> {
    let membership = membership(&state, user.id).await?;
    Ok(Json(SessionResponse {
        user: UserResponse {
            id: user.id,
            email: user.email,
        },
        membership,
        csrf_token: user.csrf_token,
    }))
}

pub async fn csrf_guard(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Result<Response> {
    if matches!(
        *request.method(),
        Method::GET | Method::HEAD | Method::OPTIONS
    ) {
        return Ok(next.run(request).await);
    }

    let origin_valid = request
        .headers()
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        .map(|origin| origin.trim_end_matches('/') == state.config.public_base_url)
        .unwrap_or(state.config.app_env != "production");
    if !origin_valid {
        return Err(AppError::Forbidden);
    }

    let path = request.uri().path();
    if path.ends_with("/auth/register") || path.ends_with("/auth/login") {
        return Ok(next.run(request).await);
    }

    let session = cookie_value(request.headers(), SESSION_COOKIE).ok_or(AppError::Unauthorized)?;
    let expected = state.crypto.csrf_token(&session);
    let supplied = request
        .headers()
        .get("x-phenogram-csrf")
        .and_then(|value| value.to_str().ok())
        .ok_or(AppError::Forbidden)?;
    if !bool::from(expected.as_bytes().ct_eq(supplied.as_bytes())) {
        return Err(AppError::Forbidden);
    }
    Ok(next.run(request).await)
}

pub async fn membership(state: &AppState, user_id: Uuid) -> Result<PlanMembership> {
    sqlx::query_as::<_, PlanMembership>(
        r#"SELECT memberships.plan_id,
                  plans.name AS plan_name,
                  memberships.status,
                  plans.bot_limit,
                  plans.retention_days,
                  plans.local_bot_api,
                  plans.monthly_price_cents,
                  memberships.current_period_ends_at,
                  (memberships.status IN ('active', 'trialing') OR
                   (memberships.status IN ('past_due', 'canceled') AND
                    memberships.current_period_ends_at > now())) AS entitlements_active
             FROM memberships
             JOIN plan_definitions plans ON plans.id = memberships.plan_id
            WHERE memberships.user_id = $1"#,
    )
    .bind(user_id)
    .fetch_one(&state.db)
    .await
    .map_err(Into::into)
}

pub async fn active_membership(state: &AppState, user_id: Uuid) -> Result<PlanMembership> {
    let membership = membership(state, user_id).await?;
    if membership.entitlements_active {
        Ok(membership)
    } else {
        Err(AppError::Forbidden)
    }
}

async fn create_session_response(
    state: &AppState,
    user_id: Uuid,
    email: String,
    user_agent: Option<&HeaderValue>,
) -> Result<Response> {
    let token = Crypto::random_token(32)?;
    let csrf_token = state.crypto.csrf_token(&token);
    let expires = Utc::now()
        + ChronoDuration::from_std(state.config.session_ttl)
            .unwrap_or_else(|_| ChronoDuration::days(30));
    let user_agent_hash = user_agent
        .and_then(|value| value.to_str().ok())
        .map(|value| Sha256::digest(value.as_bytes()).to_vec());
    sqlx::query(
        "INSERT INTO sessions (user_id, token_hash, expires_at, user_agent_hash) VALUES ($1, $2, $3, $4)",
    )
    .bind(user_id)
    .bind(Crypto::digest_secret(token.as_bytes()))
    .bind(expires)
    .bind(user_agent_hash)
    .execute(&state.db)
    .await?;
    let membership = membership(state, user_id).await?;
    let cookie = session_cookie(
        &token,
        state.config.session_ttl,
        state.config.secure_cookies(),
    );
    let mut headers = HeaderMap::new();
    headers.insert(
        header::SET_COOKIE,
        HeaderValue::from_str(&cookie).map_err(|_| AppError::Internal)?,
    );
    Ok((
        headers,
        Json(SessionResponse {
            user: UserResponse { id: user_id, email },
            membership,
            csrf_token,
        }),
    )
        .into_response())
}

fn normalize_email(value: &str) -> Result<String> {
    let email = value.trim().to_lowercase();
    let valid = email.len() <= 254
        && email.split_once('@').is_some_and(|(local, domain)| {
            !local.is_empty() && !domain.is_empty() && domain.contains('.') && !domain.contains(' ')
        });
    if !valid {
        return Err(AppError::Validation("Enter a valid email address".into()));
    }
    Ok(email)
}

fn validate_password(value: &str) -> Result<()> {
    if value.len() < 12 || value.len() > 256 {
        return Err(AppError::Validation(
            "Password must be between 12 and 256 characters".into(),
        ));
    }
    Ok(())
}

fn hash_password(password: &str) -> Result<String> {
    let mut salt = [0_u8; 16];
    getrandom::fill(&mut salt).map_err(|e| AppError::Crypto(e.to_string()))?;
    let salt = SaltString::encode_b64(&salt).map_err(|e| AppError::Crypto(e.to_string()))?;
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|hash| hash.to_string())
        .map_err(|e| AppError::Crypto(e.to_string()))
}

fn verify_password(password: &str, encoded: &str) -> bool {
    PasswordHash::new(encoded).ok().is_some_and(|hash| {
        Argon2::default()
            .verify_password(password.as_bytes(), &hash)
            .is_ok()
    })
}

fn session_cookie(token: &str, ttl: Duration, secure: bool) -> String {
    let secure = if secure { "; Secure" } else { "" };
    format!(
        "{SESSION_COOKIE}={token}; Path=/; HttpOnly; SameSite=Strict; Max-Age={}{}",
        ttl.as_secs(),
        secure
    )
}

fn expired_cookie(secure: bool) -> String {
    let secure = if secure { "; Secure" } else { "" };
    format!("{SESSION_COOKIE}=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0{secure}")
}

pub fn cookie_value(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(header::COOKIE)?
        .to_str()
        .ok()?
        .split(';')
        .filter_map(|part| part.trim().split_once('='))
        .find_map(|(key, value)| (key == name).then(|| value.to_owned()))
}

fn is_unique_violation(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .and_then(|error| error.code())
        .is_some_and(|code| code == "23505")
}

fn request_source(headers: &HeaderMap) -> String {
    headers
        .get("x-forwarded-for")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(',').next())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .or_else(|| {
            headers
                .get("x-real-ip")
                .and_then(|value| value.to_str().ok())
        })
        .unwrap_or("direct")
        .chars()
        .take(64)
        .collect()
}
