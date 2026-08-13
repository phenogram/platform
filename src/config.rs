use std::{env, net::SocketAddr, path::PathBuf, str::FromStr, time::Duration};

use crate::error::{AppError, Result};

#[derive(Clone, Debug)]
pub struct Config {
    pub app_env: String,
    pub listen_addr: SocketAddr,
    pub public_base_url: String,
    pub database_url: String,
    pub master_key: String,
    pub public_id_key: String,
    pub link_signing_key: String,
    pub telegram_cloud_api_url: String,
    pub telegram_local_api_url: Option<String>,
    pub telegram_local_data_dir: Option<PathBuf>,
    pub session_ttl: Duration,
    pub retention_sweep: Duration,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let _ = dotenvy::dotenv();
        let app_env = value("APP_ENV", "development");
        let config = Self {
            app_env: app_env.clone(),
            listen_addr: SocketAddr::from_str(&value("LISTEN_ADDR", "127.0.0.1:8080"))
                .map_err(|e| AppError::Config(format!("invalid LISTEN_ADDR: {e}")))?,
            public_base_url: required("PUBLIC_BASE_URL")?
                .trim_end_matches('/')
                .to_owned(),
            database_url: required("DATABASE_URL")?,
            master_key: required("MASTER_KEY")?,
            public_id_key: required("PUBLIC_ID_KEY")?,
            link_signing_key: required("LINK_SIGNING_KEY")?,
            telegram_cloud_api_url: value("TELEGRAM_CLOUD_API_URL", "https://api.telegram.org")
                .trim_end_matches('/')
                .to_owned(),
            telegram_local_api_url: optional("TELEGRAM_LOCAL_API_URL")
                .map(|value| value.trim_end_matches('/').to_owned()),
            telegram_local_data_dir: optional("TELEGRAM_LOCAL_DATA_DIR").map(PathBuf::from),
            session_ttl: Duration::from_secs(parse_u64("SESSION_TTL_HOURS", 720)? * 3600),
            retention_sweep: Duration::from_secs(parse_u64("RETENTION_SWEEP_SECONDS", 3600)?),
        };
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        if !matches!(self.app_env.as_str(), "development" | "test" | "production") {
            return Err(AppError::Config(
                "APP_ENV must be development, test, or production".into(),
            ));
        }
        let production = self.app_env == "production";
        if production && !self.public_base_url.starts_with("https://") {
            return Err(AppError::Config(
                "PUBLIC_BASE_URL must use HTTPS in production".into(),
            ));
        }
        for (name, secret) in [
            ("MASTER_KEY", &self.master_key),
            ("PUBLIC_ID_KEY", &self.public_id_key),
            ("LINK_SIGNING_KEY", &self.link_signing_key),
        ] {
            if secret.len() < 32 {
                return Err(AppError::Config(format!(
                    "{name} must contain at least 32 bytes"
                )));
            }
            if production && secret.contains("development") {
                return Err(AppError::Config(format!(
                    "{name} still contains a development value"
                )));
            }
        }
        let parsed = url::Url::parse(&self.public_base_url)
            .map_err(|e| AppError::Config(format!("invalid PUBLIC_BASE_URL: {e}")))?;
        if parsed.host_str().is_none() {
            return Err(AppError::Config("PUBLIC_BASE_URL must have a host".into()));
        }
        if !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.query().is_some()
            || parsed.fragment().is_some()
            || !matches!(parsed.path(), "" | "/")
        {
            return Err(AppError::Config(
                "PUBLIC_BASE_URL must be an origin without credentials, path, query, or fragment"
                    .into(),
            ));
        }
        if self.retention_sweep.is_zero() {
            return Err(AppError::Config(
                "RETENTION_SWEEP_SECONDS must be greater than zero".into(),
            ));
        }
        if self
            .telegram_local_data_dir
            .as_ref()
            .is_some_and(|path| !path.is_absolute())
        {
            return Err(AppError::Config(
                "TELEGRAM_LOCAL_DATA_DIR must be an absolute path".into(),
            ));
        }
        validate_api_origin(
            "TELEGRAM_CLOUD_API_URL",
            &self.telegram_cloud_api_url,
            production,
        )?;
        if let Some(origin) = &self.telegram_local_api_url {
            validate_api_origin("TELEGRAM_LOCAL_API_URL", origin, false)?;
        }
        Ok(())
    }

    pub fn secure_cookies(&self) -> bool {
        self.app_env == "production" || self.public_base_url.starts_with("https://")
    }
}

fn required(name: &str) -> Result<String> {
    env::var(name).map_err(|_| AppError::Config(format!("{name} is required")))
}

fn optional(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.trim().is_empty())
}

fn value(name: &str, fallback: &str) -> String {
    env::var(name).unwrap_or_else(|_| fallback.to_owned())
}

fn parse_u64(name: &str, fallback: u64) -> Result<u64> {
    optional(name)
        .map(|value| {
            value
                .parse()
                .map_err(|e| AppError::Config(format!("invalid {name}: {e}")))
        })
        .unwrap_or(Ok(fallback))
}

fn validate_api_origin(name: &str, value: &str, require_https: bool) -> Result<()> {
    let parsed = url::Url::parse(value)
        .map_err(|error| AppError::Config(format!("invalid {name}: {error}")))?;
    if parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
        || !matches!(parsed.path(), "" | "/")
        || !matches!(parsed.scheme(), "http" | "https")
    {
        return Err(AppError::Config(format!(
            "{name} must be an HTTP(S) origin without credentials, path, query, or fragment"
        )));
    }
    if require_https && parsed.scheme() != "https" {
        return Err(AppError::Config(format!(
            "{name} must use HTTPS in production"
        )));
    }
    Ok(())
}
