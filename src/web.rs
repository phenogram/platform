use axum::{
    body::Body,
    extract::{Request, State},
    http::{HeaderValue, StatusCode, header},
    middleware::Next,
    response::Response,
};

use crate::{
    config::{PublicRequestAccess, PublicSurface},
    state::AppState,
};

const INDEX: &str = include_str!("../assets/index.html");
const CSS: &str = include_str!("../assets/app.css");
const JS: &str = include_str!("../assets/app.js");

pub async fn index() -> Response {
    asset(INDEX, "text/html; charset=utf-8", "no-cache")
}

pub async fn css() -> Response {
    asset(CSS, "text/css; charset=utf-8", "no-cache")
}

pub async fn js() -> Response {
    asset(JS, "text/javascript; charset=utf-8", "no-cache")
}

pub async fn runtime_js(State(state): State<AppState>, headers: axum::http::HeaderMap) -> Response {
    let host = headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok());
    let surface = match state.config.public_surface(host) {
        PublicSurface::Landing => "landing",
        PublicSurface::App => "app",
        PublicSurface::Combined => "combined",
        PublicSurface::Api | PublicSurface::Unknown => "unknown",
    };
    let runtime = serde_json::json!({
        "surface": surface,
        "landingBaseUrl": state.config.landing_base_url,
        "appBaseUrl": state.config.app_base_url,
        "apiBaseUrl": state.config.api_base_url,
    });
    asset_owned(
        format!("window.PHENOGRAM_RUNTIME = {runtime};\n"),
        "text/javascript; charset=utf-8",
        "no-store",
    )
}

pub async fn fallback(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    uri: axum::http::Uri,
) -> Response {
    let host = headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok());
    match state.config.public_request_access(host, uri.path()) {
        PublicRequestAccess::UnknownHost => {
            return json_error(
                StatusCode::MISDIRECTED_REQUEST,
                "misdirected_request",
                "The request host is not served here",
            );
        }
        PublicRequestAccess::WrongSurface => {
            return json_error(
                StatusCode::NOT_FOUND,
                "not_found",
                "The requested resource was not found",
            );
        }
        PublicRequestAccess::Allowed => {}
    }
    if uri.path() == "/api"
        || uri.path().starts_with("/api/")
        || uri.path().starts_with("/bot")
        || uri.path() == "/file"
        || uri.path().starts_with("/file/")
        || uri.path() == "/telegram"
        || uri.path().starts_with("/telegram/")
        || uri.path() == "/events"
        || uri.path().starts_with("/events/")
        || uri.path() == "/public"
        || uri.path().starts_with("/public/")
    {
        return json_error(
            StatusCode::NOT_FOUND,
            "not_found",
            "The requested resource was not found",
        );
    }
    index().await
}

pub async fn public_host_guard(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    let host = request
        .headers()
        .get(header::HOST)
        .and_then(|value| value.to_str().ok());
    match state
        .config
        .public_request_access(host, request.uri().path())
    {
        PublicRequestAccess::Allowed => next.run(request).await,
        PublicRequestAccess::WrongSurface => json_error(
            StatusCode::NOT_FOUND,
            "not_found",
            "The requested resource was not found",
        ),
        PublicRequestAccess::UnknownHost => json_error(
            StatusCode::MISDIRECTED_REQUEST,
            "misdirected_request",
            "The request host is not served here",
        ),
    }
}

fn json_error(status: StatusCode, code: &'static str, message: &'static str) -> Response {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CACHE_CONTROL, "no-store")
        .body(Body::from(format!(
            r#"{{"error":{{"code":"{code}","message":"{message}"}}}}"#
        )))
        .expect("valid response")
}

fn asset(body: &'static str, content_type: &'static str, cache_control: &'static str) -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CACHE_CONTROL, cache_control)
        .header(header::X_CONTENT_TYPE_OPTIONS, "nosniff")
        .body(Body::from(body))
        .expect("valid static response")
}

fn asset_owned(body: String, content_type: &'static str, cache_control: &'static str) -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CACHE_CONTROL, cache_control)
        .header(header::X_CONTENT_TYPE_OPTIONS, "nosniff")
        .body(Body::from(body))
        .expect("valid dynamic asset response")
}

pub async fn security_headers(
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let mut response = next.run(request).await;
    let headers = response.headers_mut();
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("strict-origin-when-cross-origin"),
    );
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static("default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'"),
    );
    headers.insert(
        "permissions-policy",
        HeaderValue::from_static("camera=(), microphone=(), geolocation=()"),
    );
    response
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use axum::{body::Body, http::Request};
    use sqlx::postgres::PgPoolOptions;
    use tower::ServiceExt;

    use crate::{config::Config, state::AppState};

    fn test_app() -> axum::Router {
        let config = Config {
            app_env: "production".into(),
            listen_addr: "127.0.0.1:8080".parse().unwrap(),
            landing_base_url: "https://phenogram.io".into(),
            app_base_url: "https://app.phenogram.io".into(),
            api_base_url: "https://api.phenogram.io".into(),
            google_oauth_client_id: Some("google-client-id".into()),
            google_oauth_client_secret: Some("google-client-secret".into()),
            github_oauth_client_id: Some("github-client-id".into()),
            github_oauth_client_secret: Some("github-client-secret".into()),
            database_url: "postgresql://phenogram:password@127.0.0.1/phenogram".into(),
            master_key: "m".repeat(32),
            public_id_key: "p".repeat(32),
            link_signing_key: "l".repeat(32),
            telegram_cloud_api_url: "https://api.telegram.org".into(),
            telegram_local_api_url: None,
            telegram_local_data_dir: None,
            session_ttl: Duration::from_secs(3600),
            retention_sweep: Duration::from_secs(3600),
        };
        let db = PgPoolOptions::new()
            .connect_lazy(&config.database_url)
            .unwrap();
        crate::app(AppState::new(config, db).unwrap())
    }

    async fn response(app: &axum::Router, host: &str, path: &str) -> axum::response::Response {
        app.clone()
            .oneshot(
                Request::builder()
                    .uri(path)
                    .header(axum::http::header::HOST, host)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn router_separates_landing_app_api_and_unknown_hosts() {
        let app = test_app();
        assert_eq!(
            response(&app, "phenogram.io", "/").await.status(),
            axum::http::StatusCode::OK
        );
        assert_eq!(
            response(&app, "phenogram.io", "/client-route")
                .await
                .status(),
            axum::http::StatusCode::NOT_FOUND
        );
        assert_eq!(
            response(&app, "phenogram.io", "/api/plans").await.status(),
            axum::http::StatusCode::NOT_FOUND
        );
        assert_eq!(
            response(&app, "app.phenogram.io", "/client-route")
                .await
                .status(),
            axum::http::StatusCode::OK
        );
        assert_eq!(
            response(&app, "app.phenogram.io", "/botjunk")
                .await
                .status(),
            axum::http::StatusCode::NOT_FOUND
        );
        assert_eq!(
            response(&app, "api.phenogram.io", "/client-route")
                .await
                .status(),
            axum::http::StatusCode::NOT_FOUND
        );
        assert_eq!(
            response(&app, "attacker.example", "/").await.status(),
            axum::http::StatusCode::MISDIRECTED_REQUEST
        );
    }

    #[tokio::test]
    async fn runtime_identifies_each_browser_surface() {
        use http_body_util::BodyExt;

        let app = test_app();
        let landing = response(&app, "phenogram.io", "/assets/runtime.js").await;
        let landing = landing.into_body().collect().await.unwrap().to_bytes();
        let landing = std::str::from_utf8(&landing).unwrap();
        assert!(landing.contains(r#""surface":"landing""#));
        assert!(landing.contains(r#""appBaseUrl":"https://app.phenogram.io""#));

        let app_response = response(&app, "app.phenogram.io", "/assets/runtime.js").await;
        let app_response = app_response.into_body().collect().await.unwrap().to_bytes();
        assert!(
            std::str::from_utf8(&app_response)
                .unwrap()
                .contains(r#""surface":"app""#)
        );
    }
}
