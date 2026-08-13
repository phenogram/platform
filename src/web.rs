use axum::{
    body::Body,
    http::{HeaderValue, StatusCode, header},
    response::Response,
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

pub async fn fallback(uri: axum::http::Uri) -> Response {
    if uri.path().starts_with("/api/")
        || uri.path().starts_with("/bot")
        || uri.path().starts_with("/file/")
        || uri.path().starts_with("/telegram/")
        || uri.path().starts_with("/events/")
        || uri.path().starts_with("/public/")
    {
        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"error":{"code":"not_found","message":"The requested resource was not found"}}"#))
            .expect("valid response");
    }
    index().await
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
