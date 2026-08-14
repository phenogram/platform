use std::error::Error;

use phenogram_platform::{config::Config, state::AppState};
use sqlx::postgres::PgPoolOptions;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("phenogram_platform=info")),
        )
        .json()
        .init();

    let config = Config::from_env()?;
    let pool = PgPoolOptions::new()
        .max_connections(20)
        .min_connections(2)
        .acquire_timeout(std::time::Duration::from_secs(10))
        .connect(&config.database_url)
        .await?;
    sqlx::migrate!().run(&pool).await?;
    let listen_addr = config.listen_addr;
    let state = AppState::new(config, pool)?;

    let _update_notifications =
        phenogram_platform::ingestion::start_update_notification_listener(state.clone()).await?;
    tokio::spawn(phenogram_platform::retention::run(state.clone()));
    tokio::spawn(phenogram_platform::telegram::run_managed_bot_sync_worker(
        state.clone(),
    ));
    tokio::spawn(phenogram_platform::lifecycle::run_worker(state.clone()));
    for _ in 0..4 {
        tokio::spawn(phenogram_platform::telegram::run_delivery_worker(
            state.clone(),
        ));
    }

    let listener = tokio::net::TcpListener::bind(listen_addr).await?;
    tracing::info!(%listen_addr, "Phenogram Platform is ready");
    axum::serve(listener, phenogram_platform::app(state))
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler")
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! { _ = ctrl_c => {}, _ = terminate => {} }
}
