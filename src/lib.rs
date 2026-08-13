pub mod api;
pub mod auth;
pub mod config;
pub mod crypto;
pub mod error;
pub mod models;
pub mod retention;
pub mod state;
pub mod telegram;
pub mod web;

use axum::{
    Router,
    extract::DefaultBodyLimit,
    middleware,
    routing::{any, delete, get, post},
};
use tower_http::{
    catch_panic::CatchPanicLayer,
    compression::CompressionLayer,
    request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer},
};

use state::AppState;

pub fn app(state: AppState) -> Router {
    let api = Router::new()
        .route("/health", get(api::health))
        .route("/plans", get(api::plans))
        .route("/auth/register", post(auth::register))
        .route("/auth/login", post(auth::login))
        .route("/auth/logout", post(auth::logout))
        .route("/me", get(auth::me))
        .route("/bots", get(api::list_bots).post(api::connect_bot))
        .route("/bots/{bot_id}", get(api::get_bot).delete(api::delete_bot))
        .route("/bots/{bot_id}/provision", post(api::provision_bot))
        .route("/bots/{bot_id}/updates", get(api::updates))
        .route("/bots/{bot_id}/activity", get(api::activity))
        .route("/bots/{bot_id}/conversations", get(api::conversations))
        .route(
            "/bots/{bot_id}/conversations/{chat_id}/messages",
            get(api::conversation_messages),
        )
        .route("/bots/{bot_id}/messages", post(api::send_message))
        .route(
            "/bots/{bot_id}/stream-keys",
            get(api::list_stream_keys).post(api::create_stream_key),
        )
        .route(
            "/bots/{bot_id}/stream-keys/{key_id}",
            delete(api::revoke_stream_key),
        )
        .route("/bots/{bot_id}/file-links", post(api::create_file_link))
        .route("/bots/{bot_id}/routing", post(api::change_routing))
        .layer(DefaultBodyLimit::max(2 * 1024 * 1024))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            auth::csrf_guard,
        ));

    Router::new()
        .nest("/api", api)
        .route("/bot{token}/{method}", any(telegram::proxy_method))
        .route("/file/bot{token}/{*file_path}", get(telegram::proxy_file))
        .route(
            "/telegram/webhook/{public_id}",
            post(telegram::webhook_ingress),
        )
        .route(
            "/events/{public_id}/{stream_key}",
            get(telegram::event_stream),
        )
        .route(
            "/public/{public_id}/files/{*file_path}",
            get(telegram::public_file),
        )
        .route("/", get(web::index))
        .route("/assets/app.css", get(web::css))
        .route("/assets/app.js", get(web::js))
        .fallback(web::fallback)
        .layer(middleware::from_fn(web::security_headers))
        .layer(CompressionLayer::new())
        .layer(PropagateRequestIdLayer::x_request_id())
        .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid))
        .layer(CatchPanicLayer::new())
        .with_state(state)
}
