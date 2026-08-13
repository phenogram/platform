use std::{env, net::SocketAddr, path::PathBuf, str::FromStr, time::Duration};

use crate::error::{AppError, Result};

#[derive(Clone, Debug)]
pub struct Config {
    pub app_env: String,
    pub listen_addr: SocketAddr,
    /// Browser-facing landing page and management console origin.
    pub web_base_url: String,
    /// Telegram-compatible, webhook, event stream, and public file origin.
    pub api_base_url: String,
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
        // PUBLIC_BASE_URL is retained only as a non-breaking local-development
        // fallback. Production validation below requires the two public hosts
        // to be configured separately.
        let legacy_base_url = optional("PUBLIC_BASE_URL");
        let web_base_url = optional("WEB_BASE_URL")
            .or_else(|| {
                (app_env != "production")
                    .then(|| legacy_base_url.clone())
                    .flatten()
            })
            .ok_or_else(|| AppError::Config("WEB_BASE_URL is required".into()))?
            .trim_end_matches('/')
            .to_owned();
        let api_base_url = optional("API_BASE_URL")
            .or_else(|| {
                (app_env != "production")
                    .then_some(legacy_base_url)
                    .flatten()
            })
            .or_else(|| (app_env != "production").then(|| web_base_url.clone()))
            .ok_or_else(|| AppError::Config("API_BASE_URL is required".into()))?
            .trim_end_matches('/')
            .to_owned();
        let config = Self {
            app_env: app_env.clone(),
            listen_addr: SocketAddr::from_str(&value("LISTEN_ADDR", "127.0.0.1:8080"))
                .map_err(|e| AppError::Config(format!("invalid LISTEN_ADDR: {e}")))?,
            web_base_url,
            api_base_url,
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
        let web_origin = validate_public_origin("WEB_BASE_URL", &self.web_base_url, production)?;
        let api_origin = validate_public_origin("API_BASE_URL", &self.api_base_url, production)?;
        if production
            && web_origin
                .host_str()
                .zip(api_origin.host_str())
                .is_some_and(|(web, api)| web.eq_ignore_ascii_case(api))
        {
            return Err(AppError::Config(
                "WEB_BASE_URL and API_BASE_URL must use different hosts in production".into(),
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
        self.app_env == "production" || self.web_base_url.starts_with("https://")
    }

    pub fn public_request_access(&self, host: Option<&str>, path: &str) -> PublicRequestAccess {
        let console = host.is_some_and(|host| host_matches_origin(host, &self.web_base_url));
        let api = host.is_some_and(|host| host_matches_origin(host, &self.api_base_url));

        // One-host local development remains supported. Production validation
        // requires different hosts, so this cannot weaken the live boundary.
        if console && api {
            return PublicRequestAccess::Allowed;
        }
        if console {
            return if is_machine_path(path) {
                PublicRequestAccess::WrongSurface
            } else {
                PublicRequestAccess::Allowed
            };
        }
        if api {
            return if is_machine_path(path) {
                PublicRequestAccess::Allowed
            } else {
                PublicRequestAccess::WrongSurface
            };
        }
        // Kubernetes and other direct service probes generally use a pod or
        // service IP as Host. Health is deliberately the sole host-independent
        // endpoint and contains no credentials or tenant data. This exception
        // is reached only after excluding both configured public hosts.
        if path == "/api/health" {
            return PublicRequestAccess::Allowed;
        }
        PublicRequestAccess::UnknownHost
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublicRequestAccess {
    Allowed,
    WrongSurface,
    UnknownHost,
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

fn validate_public_origin(name: &str, value: &str, require_https: bool) -> Result<url::Url> {
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
    Ok(parsed)
}

fn host_matches_origin(host: &str, origin: &str) -> bool {
    let Ok(authority) = http::uri::Authority::from_str(host) else {
        return false;
    };
    let Ok(origin) = url::Url::parse(origin) else {
        return false;
    };
    if !origin
        .host_str()
        .is_some_and(|expected| authority.host().eq_ignore_ascii_case(expected))
    {
        return false;
    }
    match authority.port_u16() {
        Some(port) => Some(port) == origin.port_or_known_default(),
        None => origin.port().is_none(),
    }
}

fn is_machine_path(path: &str) -> bool {
    path.starts_with("/bot")
        || path == "/file"
        || path.starts_with("/file/")
        || path == "/telegram"
        || path.starts_with("/telegram/")
        || path == "/events"
        || path.starts_with("/events/")
        || path == "/public"
        || path.starts_with("/public/")
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, path::PathBuf, time::Duration};

    use super::{Config, PublicRequestAccess};

    fn config(app_env: &str, console: &str, api: &str) -> Config {
        Config {
            app_env: app_env.into(),
            listen_addr: "127.0.0.1:8080".parse::<SocketAddr>().unwrap(),
            web_base_url: console.into(),
            api_base_url: api.into(),
            database_url: "postgresql://phenogram:password@localhost/phenogram".into(),
            master_key: "m".repeat(32),
            public_id_key: "p".repeat(32),
            link_signing_key: "l".repeat(32),
            telegram_cloud_api_url: "https://api.telegram.org".into(),
            telegram_local_api_url: None,
            telegram_local_data_dir: None::<PathBuf>,
            session_ttl: Duration::from_secs(3600),
            retention_sweep: Duration::from_secs(3600),
        }
    }

    #[test]
    fn production_requires_separate_https_hosts() {
        assert!(
            config(
                "production",
                "https://phenogram.io",
                "https://api.phenogram.io"
            )
            .validate()
            .is_ok()
        );
        assert!(
            config("production", "https://phenogram.io", "https://phenogram.io")
                .validate()
                .is_err()
        );
        assert!(
            config(
                "production",
                "http://phenogram.io",
                "https://api.phenogram.io"
            )
            .validate()
            .is_err()
        );
    }

    #[test]
    fn separates_console_and_machine_surfaces_by_host() {
        let config = config(
            "production",
            "https://phenogram.io",
            "https://api.phenogram.io",
        );
        assert_eq!(
            config.public_request_access(Some("phenogram.io"), "/settings"),
            PublicRequestAccess::Allowed
        );
        assert_eq!(
            config.public_request_access(Some("phenogram.io:443"), "/api/bots"),
            PublicRequestAccess::Allowed
        );
        assert_eq!(
            config.public_request_access(Some("phenogram.io"), "/bot123:getMe"),
            PublicRequestAccess::WrongSurface
        );
        assert_eq!(
            config.public_request_access(Some("api.phenogram.io"), "/events/id/key"),
            PublicRequestAccess::Allowed
        );
        assert_eq!(
            config.public_request_access(Some("api.phenogram.io"), "/api/me"),
            PublicRequestAccess::WrongSurface
        );
        assert_eq!(
            config.public_request_access(Some("api.phenogram.io"), "/api/health"),
            PublicRequestAccess::WrongSurface
        );
        assert_eq!(
            config.public_request_access(Some("10.42.0.17:8080"), "/api/health"),
            PublicRequestAccess::Allowed
        );
        assert_eq!(
            config.public_request_access(Some("attacker.example"), "/"),
            PublicRequestAccess::UnknownHost
        );
        assert_eq!(
            config.public_request_access(Some("phenogram.io:80"), "/"),
            PublicRequestAccess::UnknownHost
        );
    }

    #[test]
    fn permits_single_origin_local_development() {
        let config = config("test", "http://127.0.0.1:18080", "http://127.0.0.1:18080");
        assert_eq!(
            config.public_request_access(Some("127.0.0.1:18080"), "/api/me"),
            PublicRequestAccess::Allowed
        );
        assert_eq!(
            config.public_request_access(Some("127.0.0.1:18080"), "/bot123/getMe"),
            PublicRequestAccess::Allowed
        );
    }
}
