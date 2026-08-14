use std::{sync::Arc, time::Duration};

use phenogram_data_plane_gateway::{
    Config, FileServerConfig, FileServerState, GatewayState, admin_router, file_server_router,
    public_router, serve_public_http1, snapshot_sync_loop, telemetry_delivery_loop,
};
use tokio::sync::watch;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    if let Err(error) = run().await {
        tracing::error!(reason = %error, "data-plane gateway stopped");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    if let Some(config) = FileServerConfig::from_env_if_enabled()? {
        return run_file_server(config).await;
    }
    let config = Config::from_env()?;
    let (state, telemetry_receiver) = GatewayState::new(&config)?;
    if let Err(error) = state.load_last_good_snapshot(&config.snapshot_path).await {
        tracing::warn!(reason = %error, "last-good route snapshot was not loaded");
    }

    let public_listener = tokio::net::TcpListener::bind(config.public_listen_addr)
        .await
        .map_err(|error| format!("failed to bind public listener: {error}"))?;
    let admin_listener = tokio::net::TcpListener::bind(config.admin_listen_addr)
        .await
        .map_err(|error| format!("failed to bind admin listener: {error}"))?;

    let sync_state = state.clone();
    let sync_config = config.clone();
    tokio::spawn(async move { snapshot_sync_loop(sync_state, sync_config).await });
    let telemetry_state = state.clone();
    let telemetry_config = config.clone();
    tokio::spawn(async move {
        telemetry_delivery_loop(telemetry_state, telemetry_config, telemetry_receiver).await;
    });

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let shutdown_tx = Arc::new(shutdown_tx);
    let signal_tx = shutdown_tx.clone();
    tokio::spawn(async move {
        wait_for_termination_signal().await;
        let _ = signal_tx.send(true);
    });

    let public = serve_public_http1(
        public_listener,
        public_router(state.clone()),
        shutdown_rx.clone(),
    );
    let admin_shutdown = wait_for_shutdown(shutdown_rx);
    let admin =
        axum::serve(admin_listener, admin_router(state)).with_graceful_shutdown(admin_shutdown);

    tokio::try_join!(public, admin).map_err(|error| format!("HTTP server failed: {error}"))?;
    tokio::time::sleep(Duration::from_millis(10)).await;
    Ok(())
}

async fn run_file_server(config: FileServerConfig) -> Result<(), String> {
    let listener = tokio::net::TcpListener::bind(config.listen_addr)
        .await
        .map_err(|error| format!("failed to bind file-server listener: {error}"))?;
    let state = FileServerState::from(config);
    axum::serve(listener, file_server_router(state))
        .with_graceful_shutdown(wait_for_termination_signal())
        .await
        .map_err(|error| format!("file-server HTTP server failed: {error}"))
}

async fn wait_for_shutdown(mut receiver: watch::Receiver<bool>) {
    while !*receiver.borrow() && receiver.changed().await.is_ok() {}
}

async fn wait_for_termination_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("SIGTERM handler must be installable");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
