use std::error::Error;

use phenogram_platform::tap::TapConfig;
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

    let config = TapConfig::from_env().map_err(std::io::Error::other)?;
    phenogram_platform::tap::run(config).await
}
