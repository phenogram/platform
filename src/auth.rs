use std::{collections::BTreeSet, time::Duration};

use axum::{
    Json,
    extract::{FromRequestParts, Path, Query, Request, State},
    http::{HeaderMap, HeaderValue, Method, header, request::Parts},
    middleware::Next,
    response::{IntoResponse, Redirect, Response},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{Duration as ChronoDuration, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use url::Url;
use uuid::Uuid;
use zeroize::Zeroizing;

use crate::{
    crypto::{Ciphertext, Crypto},
    error::{AppError, Result},
    models::PlanMembership,
    state::AppState,
};

pub const SESSION_COOKIE: &str = "phg_session";
const OAUTH_COOKIE: &str = "phg_oauth";
const OAUTH_TTL: Duration = Duration::from_secs(10 * 60);
const OAUTH_AAD: &[u8] = b"oauth-pkce:v1";

#[derive(Clone, Debug)]
pub struct AuthUser {
    pub id: Uuid,
    pub identity_id: Uuid,
    pub session_id: Uuid,
    pub csrf_token: String,
}

impl FromRequestParts<AppState> for AuthUser {
    type Rejection = AppError;

    async fn from_request_parts(parts: &mut Parts, state: &AppState) -> Result<Self> {
        let token = cookie_value(&parts.headers, SESSION_COOKIE).ok_or(AppError::Unauthorized)?;
        let digest = Crypto::digest_secret(token.as_bytes());
        let row = sqlx::query_as::<_, (Uuid, Uuid, Uuid)>(
            r#"SELECT sessions.id, sessions.user_id, sessions.identity_id
                 FROM sessions
                 JOIN oauth_identities ON oauth_identities.id = sessions.identity_id
                    AND oauth_identities.user_id = sessions.user_id
                WHERE sessions.token_hash = $1 AND sessions.expires_at > now()"#,
        )
        .bind(digest)
        .fetch_optional(&state.db)
        .await?
        .ok_or(AppError::Unauthorized)?;

        Ok(Self {
            session_id: row.0,
            id: row.1,
            identity_id: row.2,
            csrf_token: state.crypto.csrf_token(&token),
        })
    }
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
    pub provider: String,
    pub display_name: Option<String>,
    pub provider_login: Option<String>,
    pub avatar_url: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Provider {
    Google,
    Github,
}

impl Provider {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "google" => Ok(Self::Google),
            "github" => Ok(Self::Github),
            _ => Err(AppError::NotFound),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Google => "google",
            Self::Github => "github",
        }
    }

    fn credentials(self, state: &AppState) -> Result<(&str, &str)> {
        let (id, secret) = match self {
            Self::Google => (
                state.config.google_oauth_client_id.as_deref(),
                state.config.google_oauth_client_secret.as_deref(),
            ),
            Self::Github => (
                state.config.github_oauth_client_id.as_deref(),
                state.config.github_oauth_client_secret.as_deref(),
            ),
        };
        id.zip(secret)
            .ok_or_else(|| AppError::Config(format!("{} OAuth is not configured", self.as_str())))
    }
}

#[derive(Debug, Deserialize)]
pub struct OAuthCallback {
    code: Option<String>,
    state: Option<String>,
    error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawOAuthToken {
    access_token: String,
    #[serde(default)]
    scope: Option<String>,
}

#[derive(Debug)]
struct OAuthToken {
    access_token: Zeroizing<String>,
    scope: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GoogleProfile {
    sub: String,
    name: Option<String>,
    picture: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GithubGraphql {
    data: Option<GithubData>,
    #[serde(default)]
    errors: Vec<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct GithubData {
    viewer: GithubProfile,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GithubProfile {
    database_id: i64,
    login: String,
    name: Option<String>,
    avatar_url: String,
}

#[derive(Debug)]
struct ProviderIdentity {
    subject: String,
    display_name: Option<String>,
    login: Option<String>,
    avatar_url: Option<String>,
}

pub async fn oauth_start(
    State(state): State<AppState>,
    Path(provider): Path<String>,
    headers: HeaderMap,
) -> Result<Response> {
    let provider = Provider::parse(&provider)?;
    let (client_id, _) = provider.credentials(&state)?;
    state
        .auth_limiter
        .check(&request_source(&headers), provider.as_str())
        .await?;

    let oauth_state = Crypto::random_token(32)?;
    let browser_secret = Crypto::random_token(32)?;
    let pkce_verifier = Zeroizing::new(Crypto::random_token(48)?);
    let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(pkce_verifier.as_bytes()));
    let encrypted = state.crypto.encrypt(pkce_verifier.as_bytes(), OAUTH_AAD)?;
    let expires = Utc::now() + ChronoDuration::seconds(OAUTH_TTL.as_secs() as i64);

    let mut tx = state.db.begin().await?;
    sqlx::query("DELETE FROM oauth_login_attempts WHERE expires_at <= now()")
        .execute(&mut *tx)
        .await?;
    sqlx::query(
        r#"INSERT INTO oauth_login_attempts
           (state_hash, browser_secret_hash, provider, pkce_verifier_ciphertext,
            pkce_verifier_nonce, expires_at)
           VALUES ($1, $2, $3, $4, $5, $6)"#,
    )
    .bind(Crypto::digest_secret(oauth_state.as_bytes()))
    .bind(Crypto::digest_secret(browser_secret.as_bytes()))
    .bind(provider.as_str())
    .bind(encrypted.data)
    .bind(encrypted.nonce)
    .bind(expires)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;

    let redirect_uri = callback_url(&state, provider);
    let mut authorize = match provider {
        Provider::Google => Url::parse("https://accounts.google.com/o/oauth2/v2/auth")
            .expect("static Google authorization URL"),
        Provider::Github => Url::parse("https://github.com/login/oauth/authorize")
            .expect("static GitHub authorization URL"),
    };
    {
        let mut query = authorize.query_pairs_mut();
        query
            .append_pair("client_id", client_id)
            .append_pair("redirect_uri", &redirect_uri)
            .append_pair("response_type", "code")
            .append_pair("state", &oauth_state)
            .append_pair("code_challenge", &challenge)
            .append_pair("code_challenge_method", "S256");
        if provider == Provider::Google {
            query.append_pair("scope", "openid profile");
        }
    }

    let cookie = oauth_cookie(&browser_secret, OAUTH_TTL, state.config.secure_cookies());
    let mut response = Redirect::temporary(authorize.as_str()).into_response();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response.headers_mut().insert(
        header::SET_COOKIE,
        HeaderValue::from_str(&cookie).map_err(|_| AppError::Internal)?,
    );
    Ok(response)
}

pub async fn oauth_callback(
    State(state): State<AppState>,
    Path(provider): Path<String>,
    Query(callback): Query<OAuthCallback>,
    headers: HeaderMap,
) -> Response {
    let provider = match Provider::parse(&provider) {
        Ok(provider) => provider,
        Err(error) => return error.into_response(),
    };
    if let Err(error) = state
        .auth_limiter
        .check(
            &request_source(&headers),
            &format!("callback:{}", provider.as_str()),
        )
        .await
    {
        return error.into_response();
    }
    let access_denied = callback.error.as_deref() == Some("access_denied");
    let result = complete_oauth(&state, provider, callback, &headers).await;
    match result {
        Ok((user_id, identity_id)) => {
            match create_session_redirect(
                &state,
                user_id,
                identity_id,
                headers.get(header::USER_AGENT),
            )
            .await
            {
                Ok(response) => response,
                Err(error) => {
                    tracing::error!(provider = provider.as_str(), error = ?error, "OAuth session creation failed");
                    oauth_error_redirect(&state, "temporarily_unavailable")
                }
            }
        }
        Err(error) => {
            if matches!(error, AppError::Database(_) | AppError::Internal) {
                tracing::error!(provider = provider.as_str(), error = ?error, "OAuth callback failed");
            } else {
                tracing::warn!(provider = provider.as_str(), "OAuth callback rejected");
            }
            oauth_error_redirect(
                &state,
                if matches!(error, AppError::Unauthorized) {
                    "invalid_state"
                } else if access_denied {
                    "access_denied"
                } else {
                    "provider_error"
                },
            )
        }
    }
}

async fn complete_oauth(
    state: &AppState,
    provider: Provider,
    callback: OAuthCallback,
    headers: &HeaderMap,
) -> Result<(Uuid, Uuid)> {
    if callback.error.is_some() {
        return Err(AppError::Validation("provider rejected sign-in".into()));
    }
    let code = callback
        .code
        .filter(|value| !value.is_empty())
        .ok_or(AppError::Unauthorized)?;
    let oauth_state = callback
        .state
        .filter(|value| !value.is_empty())
        .ok_or(AppError::Unauthorized)?;
    let browser_secret = cookie_value(headers, OAUTH_COOKIE).ok_or(AppError::Unauthorized)?;

    let mut tx = state.db.begin().await?;
    let attempt = sqlx::query_as::<_, (String, Vec<u8>, Vec<u8>)>(
        r#"DELETE FROM oauth_login_attempts
            WHERE state_hash = $1
              AND browser_secret_hash = $2
              AND provider = $3
              AND expires_at > now()
        RETURNING provider, pkce_verifier_ciphertext, pkce_verifier_nonce"#,
    )
    .bind(Crypto::digest_secret(oauth_state.as_bytes()))
    .bind(Crypto::digest_secret(browser_secret.as_bytes()))
    .bind(provider.as_str())
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(AppError::Unauthorized)?;
    tx.commit().await?;

    let verifier = state.crypto.decrypt(
        &Ciphertext {
            data: attempt.1,
            nonce: attempt.2,
        },
        OAUTH_AAD,
    )?;
    let token = exchange_code(state, provider, code, &verifier).await?;
    let identity = fetch_identity(state, provider, &token.access_token).await?;
    persist_identity(state, provider, identity).await
}

async fn exchange_code(
    state: &AppState,
    provider: Provider,
    code: String,
    verifier: &[u8],
) -> Result<OAuthToken> {
    let (client_id, client_secret) = provider.credentials(state)?;
    let endpoint = match provider {
        Provider::Google => "https://oauth2.googleapis.com/token",
        Provider::Github => "https://github.com/login/oauth/access_token",
    };
    let redirect_uri = callback_url(state, provider);
    let mut form = vec![
        ("client_id", client_id),
        ("client_secret", client_secret),
        ("code", code.as_str()),
        ("redirect_uri", redirect_uri.as_str()),
        (
            "code_verifier",
            std::str::from_utf8(verifier).map_err(|_| AppError::Internal)?,
        ),
    ];
    if provider == Provider::Google {
        form.push(("grant_type", "authorization_code"));
    }
    let response = state
        .oauth
        .post(endpoint)
        .header(header::ACCEPT, "application/json")
        .form(&form)
        .send()
        .await
        .map_err(redacted_oauth_error)?;
    if !response.status().is_success() {
        return Err(AppError::Upstream(
            "OAuth provider rejected token exchange".into(),
        ));
    }
    let raw = response
        .json::<RawOAuthToken>()
        .await
        .map_err(redacted_oauth_error)?;
    let token = OAuthToken {
        access_token: Zeroizing::new(raw.access_token),
        scope: raw.scope,
    };
    validate_granted_scope(provider, token.scope.as_deref())?;
    Ok(token)
}

fn validate_granted_scope(provider: Provider, scope: Option<&str>) -> Result<()> {
    let scopes = scope
        .unwrap_or_default()
        .split(|character: char| character.is_ascii_whitespace() || character == ',')
        .filter(|scope| !scope.is_empty())
        .collect::<BTreeSet<_>>();
    let valid = match provider {
        Provider::Google => {
            scopes.is_empty()
                || scopes == BTreeSet::from(["openid", "profile"])
                || scopes
                    == BTreeSet::from([
                        "https://www.googleapis.com/auth/userinfo.profile",
                        "openid",
                    ])
        }
        Provider::Github => scopes.is_empty(),
    };
    valid.then_some(()).ok_or_else(|| AppError::Forbidden)
}

async fn fetch_identity(
    state: &AppState,
    provider: Provider,
    access_token: &str,
) -> Result<ProviderIdentity> {
    match provider {
        Provider::Google => {
            let response = state
                .oauth
                .get("https://openidconnect.googleapis.com/v1/userinfo")
                .bearer_auth(access_token)
                .send()
                .await
                .map_err(redacted_oauth_error)?;
            if !response.status().is_success() {
                return Err(AppError::Upstream("Google identity lookup failed".into()));
            }
            let profile = response
                .json::<GoogleProfile>()
                .await
                .map_err(redacted_oauth_error)?;
            Ok(ProviderIdentity {
                subject: validate_subject(profile.sub)?,
                display_name: public_text(profile.name, 200),
                login: None,
                avatar_url: safe_avatar(profile.picture),
            })
        }
        Provider::Github => {
            let response = state
                .oauth
                .post("https://api.github.com/graphql")
                .bearer_auth(access_token)
                .json(&serde_json::json!({
                    "query": "query PhenogramIdentity { viewer { databaseId login name avatarUrl } }"
                }))
                .send()
                .await
                .map_err(redacted_oauth_error)?;
            if !response.status().is_success() {
                return Err(AppError::Upstream("GitHub identity lookup failed".into()));
            }
            let response = response
                .json::<GithubGraphql>()
                .await
                .map_err(redacted_oauth_error)?;
            if !response.errors.is_empty() {
                return Err(AppError::Upstream("GitHub identity lookup failed".into()));
            }
            let profile = response
                .data
                .ok_or_else(|| AppError::Upstream("GitHub identity lookup failed".into()))?
                .viewer;
            Ok(ProviderIdentity {
                subject: validate_subject(profile.database_id.to_string())?,
                display_name: public_text(profile.name, 200),
                login: public_text(Some(profile.login), 100),
                avatar_url: safe_avatar(Some(profile.avatar_url)),
            })
        }
    }
}

async fn persist_identity(
    state: &AppState,
    provider: Provider,
    identity: ProviderIdentity,
) -> Result<(Uuid, Uuid)> {
    let mut tx = state.db.begin().await?;
    let existing = sqlx::query_as::<_, (Uuid, Uuid)>(
        "SELECT id, user_id FROM oauth_identities WHERE provider = $1 AND provider_subject = $2 FOR UPDATE",
    )
    .bind(provider.as_str())
    .bind(&identity.subject)
    .fetch_optional(&mut *tx)
    .await?;
    let (identity_id, user_id) = if let Some(existing) = existing {
        sqlx::query(
            r#"UPDATE oauth_identities
                  SET display_name = $1, provider_login = $2, avatar_url = $3,
                      last_login_at = now(), updated_at = now()
                WHERE id = $4"#,
        )
        .bind(&identity.display_name)
        .bind(&identity.login)
        .bind(&identity.avatar_url)
        .bind(existing.0)
        .execute(&mut *tx)
        .await?;
        existing
    } else {
        let user_id =
            sqlx::query_scalar::<_, Uuid>("INSERT INTO users DEFAULT VALUES RETURNING id")
                .fetch_one(&mut *tx)
                .await?;
        let insert = sqlx::query_scalar::<_, Uuid>(
            r#"INSERT INTO oauth_identities
               (user_id, provider, provider_subject, display_name, provider_login, avatar_url)
               VALUES ($1, $2, $3, $4, $5, $6)
               ON CONFLICT (provider, provider_subject) DO NOTHING
               RETURNING id"#,
        )
        .bind(user_id)
        .bind(provider.as_str())
        .bind(&identity.subject)
        .bind(&identity.display_name)
        .bind(&identity.login)
        .bind(&identity.avatar_url)
        .fetch_optional(&mut *tx)
        .await?;
        if let Some(identity_id) = insert {
            sqlx::query("INSERT INTO memberships (user_id, plan_id) VALUES ($1, 'free')")
                .bind(user_id)
                .execute(&mut *tx)
                .await?;
            (identity_id, user_id)
        } else {
            sqlx::query("DELETE FROM users WHERE id = $1")
                .bind(user_id)
                .execute(&mut *tx)
                .await?;
            sqlx::query_as::<_, (Uuid, Uuid)>(
                "SELECT id, user_id FROM oauth_identities WHERE provider = $1 AND provider_subject = $2",
            )
            .bind(provider.as_str())
            .bind(&identity.subject)
            .fetch_one(&mut *tx)
            .await?
        }
    };
    tx.commit().await?;
    Ok((user_id, identity_id))
}

pub async fn logout(State(state): State<AppState>, user: AuthUser) -> Result<Response> {
    sqlx::query("DELETE FROM sessions WHERE id = $1 AND user_id = $2")
        .bind(user.session_id)
        .bind(user.id)
        .execute(&state.db)
        .await?;
    let mut headers = HeaderMap::new();
    headers.append(
        header::SET_COOKIE,
        HeaderValue::from_str(&expired_cookie(
            SESSION_COOKIE,
            true,
            state.config.secure_cookies(),
        ))
        .map_err(|_| AppError::Internal)?,
    );
    Ok((headers, Json(serde_json::json!({"ok": true}))).into_response())
}

pub async fn me(State(state): State<AppState>, user: AuthUser) -> Result<Json<SessionResponse>> {
    let membership = membership(&state, user.id).await?;
    let identity = user_response(&state, user.identity_id).await?;
    Ok(Json(SessionResponse {
        user: identity,
        membership,
        csrf_token: user.csrf_token,
    }))
}

async fn user_response(state: &AppState, identity_id: Uuid) -> Result<UserResponse> {
    sqlx::query_as::<_, (Uuid, String, Option<String>, Option<String>, Option<String>)>(
        r#"SELECT user_id, provider, display_name, provider_login, avatar_url
             FROM oauth_identities WHERE id = $1"#,
    )
    .bind(identity_id)
    .fetch_optional(&state.db)
    .await?
    .map(|row| UserResponse {
        id: row.0,
        provider: row.1,
        display_name: row.2,
        provider_login: row.3,
        avatar_url: row.4,
    })
    .ok_or(AppError::Unauthorized)
}

async fn create_session_redirect(
    state: &AppState,
    user_id: Uuid,
    identity_id: Uuid,
    user_agent: Option<&HeaderValue>,
) -> Result<Response> {
    let token = Zeroizing::new(Crypto::random_token(32)?);
    let expires = Utc::now()
        + ChronoDuration::from_std(state.config.session_ttl)
            .unwrap_or_else(|_| ChronoDuration::days(30));
    let user_agent_hash = user_agent
        .and_then(|value| value.to_str().ok())
        .map(|value| Sha256::digest(value.as_bytes()).to_vec());
    sqlx::query(
        r#"INSERT INTO sessions
           (user_id, identity_id, token_hash, expires_at, user_agent_hash)
           VALUES ($1, $2, $3, $4, $5)"#,
    )
    .bind(user_id)
    .bind(identity_id)
    .bind(Crypto::digest_secret(token.as_bytes()))
    .bind(expires)
    .bind(user_agent_hash)
    .execute(&state.db)
    .await?;

    let mut response =
        Redirect::to(&format!("{}/#/overview", state.config.app_base_url)).into_response();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response.headers_mut().append(
        header::SET_COOKIE,
        HeaderValue::from_str(&session_cookie(
            &token,
            state.config.session_ttl,
            state.config.secure_cookies(),
        ))
        .map_err(|_| AppError::Internal)?,
    );
    response.headers_mut().append(
        header::SET_COOKIE,
        HeaderValue::from_str(&expired_cookie(
            OAUTH_COOKIE,
            false,
            state.config.secure_cookies(),
        ))
        .map_err(|_| AppError::Internal)?,
    );
    Ok(response)
}

fn oauth_error_redirect(state: &AppState, code: &'static str) -> Response {
    let mut response = Redirect::to(&format!(
        "{}/#/login?oauth_error={code}",
        state.config.app_base_url
    ))
    .into_response();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    if let Ok(cookie) = HeaderValue::from_str(&expired_cookie(
        OAUTH_COOKIE,
        false,
        state.config.secure_cookies(),
    )) {
        response.headers_mut().append(header::SET_COOKIE, cookie);
    }
    response
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
        .map(|origin| origin.trim_end_matches('/') == state.config.app_base_url)
        .unwrap_or(state.config.app_env != "production");
    if !origin_valid {
        return Err(AppError::Forbidden);
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
        r#"SELECT memberships.plan_id, plans.name AS plan_name,
                  memberships.status, plans.bot_limit, plans.retention_days,
                  plans.local_bot_api, plans.monthly_price_cents,
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

fn callback_url(state: &AppState, provider: Provider) -> String {
    format!(
        "{}/api/auth/oauth/{}/callback",
        state.config.app_base_url,
        provider.as_str()
    )
}

fn validate_subject(subject: String) -> Result<String> {
    let subject = subject.trim().to_owned();
    if subject.is_empty() || subject.len() > 255 || subject.contains('@') {
        return Err(AppError::Forbidden);
    }
    Ok(subject)
}

fn public_text(value: Option<String>, max: usize) -> Option<String> {
    value
        .map(|value| value.trim().chars().take(max).collect::<String>())
        .filter(|value| {
            !value.is_empty()
                && !value.contains('@')
                && !value.chars().any(|character| {
                    character.is_control()
                        || matches!(
                            character,
                            '\u{061c}'
                                | '\u{200e}'
                                | '\u{200f}'
                                | '\u{202a}'..='\u{202e}'
                                | '\u{2066}'..='\u{2069}'
                        )
                })
        })
}

fn safe_avatar(value: Option<String>) -> Option<String> {
    value
        .filter(|value| {
            value.len() <= 2048
                && !value.contains('@')
                && !value.to_ascii_lowercase().contains("%40")
        })
        .filter(|value| {
            Url::parse(value).is_ok_and(|url| {
                url.scheme() == "https"
                    && url.host_str().is_some()
                    && url.username().is_empty()
                    && url.password().is_none()
            })
        })
}

fn redacted_oauth_error(error: reqwest::Error) -> AppError {
    AppError::Upstream(error.without_url().to_string())
}

fn session_cookie(token: &str, ttl: Duration, secure: bool) -> String {
    let secure = if secure { "; Secure" } else { "" };
    format!(
        "{SESSION_COOKIE}={token}; Path=/; HttpOnly; SameSite=Strict; Max-Age={}{}",
        ttl.as_secs(),
        secure
    )
}

fn oauth_cookie(token: &str, ttl: Duration, secure: bool) -> String {
    let secure = if secure { "; Secure" } else { "" };
    format!(
        "{OAUTH_COOKIE}={token}; Path=/api/auth/oauth; HttpOnly; SameSite=Lax; Max-Age={}{}",
        ttl.as_secs(),
        secure
    )
}

fn expired_cookie(name: &str, strict: bool, secure: bool) -> String {
    let secure = if secure { "; Secure" } else { "" };
    let same_site = if strict { "Strict" } else { "Lax" };
    let path = if name == OAUTH_COOKIE {
        "/api/auth/oauth"
    } else {
        "/"
    };
    format!("{name}=; Path={path}; HttpOnly; SameSite={same_site}; Max-Age=0{secure}")
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_and_scope_contract_is_email_free() {
        assert_eq!(Provider::parse("google").unwrap(), Provider::Google);
        assert_eq!(Provider::parse("github").unwrap(), Provider::Github);
        assert!(Provider::parse("other").is_err());
        assert!(validate_granted_scope(Provider::Google, Some("openid profile")).is_ok());
        assert!(validate_granted_scope(Provider::Google, Some("profile openid")).is_ok());
        assert!(validate_granted_scope(Provider::Google, Some("openid profile email")).is_err());
        assert!(validate_granted_scope(Provider::Github, None).is_ok());
        assert!(validate_granted_scope(Provider::Github, Some("read:user")).is_err());
    }

    #[test]
    fn profile_text_never_persists_an_email_shape() {
        assert_eq!(
            public_text(Some("Ada Lovelace".into()), 200).as_deref(),
            Some("Ada Lovelace")
        );
        assert!(public_text(Some("ada@example.com".into()), 200).is_none());
        assert!(public_text(Some("safe\u{202e}spoof".into()), 200).is_none());
        assert!(validate_subject("123456".into()).is_ok());
        assert!(validate_subject("person@example.com".into()).is_err());
    }

    #[test]
    fn oauth_cookie_is_browser_bound_and_short_lived() {
        let cookie = oauth_cookie("secret", OAUTH_TTL, true);
        assert!(cookie.contains("HttpOnly"));
        assert!(cookie.contains("SameSite=Lax"));
        assert!(cookie.contains("Secure"));
        assert!(cookie.contains("Max-Age=600"));
        assert!(cookie.contains("Path=/api/auth/oauth"));
    }
}
