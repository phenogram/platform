use std::{env, net::SocketAddr, path::PathBuf, str::FromStr, time::Duration};

use crate::error::{AppError, Result};

#[derive(Clone)]
pub struct Config {
    pub app_env: String,
    pub deployment_revision: String,
    pub listen_addr: SocketAddr,
    /// Public marketing and capability overview origin.
    pub landing_base_url: String,
    /// Authenticated browser console and management API origin.
    pub app_base_url: String,
    /// Telegram-compatible, webhook, event stream, and public file origin.
    pub api_base_url: String,
    pub google_oauth_client_id: Option<String>,
    pub google_oauth_client_secret: Option<String>,
    pub github_oauth_client_id: Option<String>,
    pub github_oauth_client_secret: Option<String>,
    pub database_url: String,
    pub master_key: String,
    pub public_id_key: String,
    pub link_signing_key: String,
    pub telegram_cloud_api_url: String,
    pub telegram_local_api_url: Option<String>,
    pub telegram_local_data_dir: Option<PathBuf>,
    pub data_plane_enabled: bool,
    pub data_plane_sync_token: Option<String>,
    /// Private raw Bot API gateway used by control-plane calls after a route is active.
    pub data_plane_gateway_url: Option<String>,
    /// Private gateway admin origin used to fence route withdrawal before logOut.
    pub data_plane_gateway_admin_url: Option<String>,
    /// Private direct origins used only by the serialized login/logOut saga.
    pub data_plane_standard_api_url: Option<String>,
    pub data_plane_local_api_url: Option<String>,
    /// Native official Bot API data root shared with the local-pool file sidecar.
    pub data_plane_official_data_dir: Option<PathBuf>,
    pub session_ttl: Duration,
    pub retention_sweep: Duration,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let _ = dotenvy::dotenv();
        let app_env = value("APP_ENV", "development");
        // The former one-origin variables are retained only for local and test
        // compatibility. Production must name all three origins explicitly.
        let development_origin = optional("PUBLIC_BASE_URL").or_else(|| optional("WEB_BASE_URL"));
        let landing_base_url = optional("LANDING_BASE_URL")
            .or_else(|| {
                (app_env != "production")
                    .then(|| development_origin.clone())
                    .flatten()
            })
            .ok_or_else(|| AppError::Config("LANDING_BASE_URL is required".into()))?
            .trim_end_matches('/')
            .to_owned();
        let app_base_url = optional("APP_BASE_URL")
            .or_else(|| {
                (app_env != "production")
                    .then(|| development_origin.clone())
                    .flatten()
            })
            .or_else(|| (app_env != "production").then(|| landing_base_url.clone()))
            .ok_or_else(|| AppError::Config("APP_BASE_URL is required".into()))?
            .trim_end_matches('/')
            .to_owned();
        let api_base_url = optional("API_BASE_URL")
            .or_else(|| {
                (app_env != "production")
                    .then_some(development_origin)
                    .flatten()
            })
            .or_else(|| (app_env != "production").then(|| app_base_url.clone()))
            .ok_or_else(|| AppError::Config("API_BASE_URL is required".into()))?
            .trim_end_matches('/')
            .to_owned();
        let config = Self {
            app_env: app_env.clone(),
            deployment_revision: deployment_revision(optional("DEPLOYMENT_REVISION")),
            listen_addr: SocketAddr::from_str(&value("LISTEN_ADDR", "127.0.0.1:8080"))
                .map_err(|e| AppError::Config(format!("invalid LISTEN_ADDR: {e}")))?,
            landing_base_url,
            app_base_url,
            api_base_url,
            google_oauth_client_id: optional("GOOGLE_OAUTH_CLIENT_ID"),
            google_oauth_client_secret: optional("GOOGLE_OAUTH_CLIENT_SECRET"),
            github_oauth_client_id: optional("GITHUB_OAUTH_CLIENT_ID"),
            github_oauth_client_secret: optional("GITHUB_OAUTH_CLIENT_SECRET"),
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
            data_plane_enabled: parse_bool("DATA_PLANE_ENABLED", false)?,
            data_plane_sync_token: optional("DATA_PLANE_SYNC_TOKEN"),
            data_plane_gateway_url: optional_origin("DATA_PLANE_GATEWAY_URL"),
            data_plane_gateway_admin_url: optional_origin("DATA_PLANE_GATEWAY_ADMIN_URL"),
            data_plane_standard_api_url: optional_origin("DATA_PLANE_STANDARD_API_URL"),
            data_plane_local_api_url: optional_origin("DATA_PLANE_LOCAL_API_URL"),
            data_plane_official_data_dir: optional("DATA_PLANE_OFFICIAL_DATA_DIR")
                .map(PathBuf::from),
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
        let landing_origin =
            validate_public_origin("LANDING_BASE_URL", &self.landing_base_url, production)?;
        let app_origin = validate_public_origin("APP_BASE_URL", &self.app_base_url, production)?;
        let api_origin = validate_public_origin("API_BASE_URL", &self.api_base_url, production)?;
        if production {
            let hosts = [
                ("LANDING_BASE_URL", landing_origin.host_str()),
                ("APP_BASE_URL", app_origin.host_str()),
                ("API_BASE_URL", api_origin.host_str()),
            ];
            for left in 0..hosts.len() {
                for right in (left + 1)..hosts.len() {
                    if hosts[left]
                        .1
                        .zip(hosts[right].1)
                        .is_some_and(|(left, right)| left.eq_ignore_ascii_case(right))
                    {
                        return Err(AppError::Config(format!(
                            "{} and {} must use different hosts in production",
                            hosts[left].0, hosts[right].0
                        )));
                    }
                }
            }
        }
        validate_oauth_credentials(
            "Google",
            self.google_oauth_client_id.as_deref(),
            self.google_oauth_client_secret.as_deref(),
            production,
        )?;
        validate_oauth_credentials(
            "GitHub",
            self.github_oauth_client_id.as_deref(),
            self.github_oauth_client_secret.as_deref(),
            production,
        )?;
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
        if self
            .data_plane_sync_token
            .as_ref()
            .is_some_and(|token| token.len() < 32)
        {
            return Err(AppError::Config(
                "DATA_PLANE_SYNC_TOKEN must contain at least 32 bytes".into(),
            ));
        }
        if self.data_plane_enabled && self.data_plane_sync_token.is_none() {
            return Err(AppError::Config(
                "DATA_PLANE_SYNC_TOKEN is required when DATA_PLANE_ENABLED is true".into(),
            ));
        }
        for (name, origin) in [
            ("DATA_PLANE_GATEWAY_URL", &self.data_plane_gateway_url),
            (
                "DATA_PLANE_GATEWAY_ADMIN_URL",
                &self.data_plane_gateway_admin_url,
            ),
            (
                "DATA_PLANE_STANDARD_API_URL",
                &self.data_plane_standard_api_url,
            ),
            ("DATA_PLANE_LOCAL_API_URL", &self.data_plane_local_api_url),
        ] {
            if self.data_plane_enabled && origin.is_none() {
                return Err(AppError::Config(format!(
                    "{name} is required when DATA_PLANE_ENABLED is true"
                )));
            }
            if let Some(origin) = origin {
                validate_api_origin(name, origin, false)?;
            }
        }
        if self.data_plane_enabled && self.data_plane_official_data_dir.is_none() {
            return Err(AppError::Config(
                "DATA_PLANE_OFFICIAL_DATA_DIR is required when DATA_PLANE_ENABLED is true".into(),
            ));
        }
        if self
            .data_plane_official_data_dir
            .as_deref()
            .is_some_and(|path| !is_normalized_absolute_directory(path))
        {
            return Err(AppError::Config(
                "DATA_PLANE_OFFICIAL_DATA_DIR must be a normalized absolute path below /".into(),
            ));
        }
        Ok(())
    }

    pub fn secure_cookies(&self) -> bool {
        self.app_env == "production" || self.app_base_url.starts_with("https://")
    }

    pub fn public_request_access(&self, host: Option<&str>, path: &str) -> PublicRequestAccess {
        match self.public_surface(host) {
            PublicSurface::Combined => return PublicRequestAccess::Allowed,
            PublicSurface::Landing => {
                return if is_landing_path(path) {
                    PublicRequestAccess::Allowed
                } else {
                    PublicRequestAccess::WrongSurface
                };
            }
            PublicSurface::App => {
                return if is_machine_path(path) {
                    PublicRequestAccess::WrongSurface
                } else {
                    PublicRequestAccess::Allowed
                };
            }
            PublicSurface::Api => {
                return if is_machine_path(path) {
                    PublicRequestAccess::Allowed
                } else {
                    PublicRequestAccess::WrongSurface
                };
            }
            PublicSurface::Unknown => {}
        }
        // Kubernetes and other direct service probes generally use a pod or
        // service IP as Host. Health is deliberately the sole host-independent
        // endpoint and contains no credentials or tenant data. This exception
        // is reached only after excluding every configured public host.
        if path == "/api/health" {
            return PublicRequestAccess::Allowed;
        }
        PublicRequestAccess::UnknownHost
    }

    pub fn public_surface(&self, host: Option<&str>) -> PublicSurface {
        let Some(host) = host else {
            return PublicSurface::Unknown;
        };
        let landing = host_matches_origin(host, &self.landing_base_url);
        let app = host_matches_origin(host, &self.app_base_url);
        let api = host_matches_origin(host, &self.api_base_url);
        if landing && app && api {
            PublicSurface::Combined
        } else if landing {
            PublicSurface::Landing
        } else if app {
            PublicSurface::App
        } else if api {
            PublicSurface::Api
        } else {
            PublicSurface::Unknown
        }
    }
}

fn optional_origin(name: &str) -> Option<String> {
    optional(name).map(|value| value.trim_end_matches('/').to_owned())
}

fn is_normalized_absolute_directory(path: &std::path::Path) -> bool {
    use std::path::Component;

    let mut components = path.components();
    if !matches!(components.next(), Some(Component::RootDir)) {
        return false;
    }
    let mut normal_components = 0_usize;
    for component in components {
        match component {
            Component::Normal(value) if !value.is_empty() => normal_components += 1,
            _ => return false,
        }
    }
    normal_components > 0
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublicSurface {
    Landing,
    App,
    Api,
    Combined,
    Unknown,
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

fn deployment_revision(revision: Option<String>) -> String {
    revision
        .map(|revision| revision.trim().to_owned())
        .filter(|revision| !revision.is_empty())
        .unwrap_or_else(|| "local".to_owned())
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

fn parse_bool(name: &str, fallback: bool) -> Result<bool> {
    optional(name)
        .map(|value| match value.as_str() {
            "true" | "1" => Ok(true),
            "false" | "0" => Ok(false),
            _ => Err(AppError::Config(format!(
                "{name} must be true, false, 1, or 0"
            ))),
        })
        .unwrap_or(Ok(fallback))
}

fn validate_oauth_credentials(
    provider: &str,
    client_id: Option<&str>,
    client_secret: Option<&str>,
    required_in_environment: bool,
) -> Result<()> {
    if client_id.is_some() != client_secret.is_some() {
        return Err(AppError::Config(format!(
            "{provider} OAuth client ID and secret must be configured together"
        )));
    }
    if required_in_environment && client_id.is_none() {
        return Err(AppError::Config(format!(
            "{provider} OAuth credentials are required in production"
        )));
    }
    Ok(())
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

fn is_landing_path(path: &str) -> bool {
    path == "/"
        || path == "/privacy"
        || matches!(
            path,
            "/assets/app.css" | "/assets/app.js" | "/assets/runtime.js"
        )
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, path::PathBuf, time::Duration};

    use super::{Config, PublicRequestAccess, PublicSurface, deployment_revision};

    fn config(app_env: &str, landing: &str, app: &str, api: &str) -> Config {
        Config {
            app_env: app_env.into(),
            deployment_revision: "local".into(),
            listen_addr: "127.0.0.1:8080".parse::<SocketAddr>().unwrap(),
            landing_base_url: landing.into(),
            app_base_url: app.into(),
            api_base_url: api.into(),
            google_oauth_client_id: Some("google-client-id".into()),
            google_oauth_client_secret: Some("google-client-secret".into()),
            github_oauth_client_id: Some("github-client-id".into()),
            github_oauth_client_secret: Some("github-client-secret".into()),
            database_url: "postgresql://phenogram:password@localhost/phenogram".into(),
            master_key: "m".repeat(32),
            public_id_key: "p".repeat(32),
            link_signing_key: "l".repeat(32),
            telegram_cloud_api_url: "https://api.telegram.org".into(),
            telegram_local_api_url: None,
            telegram_local_data_dir: None::<PathBuf>,
            data_plane_enabled: false,
            data_plane_sync_token: None,
            data_plane_gateway_url: None,
            data_plane_gateway_admin_url: None,
            data_plane_standard_api_url: None,
            data_plane_local_api_url: None,
            data_plane_official_data_dir: None,
            session_ttl: Duration::from_secs(3600),
            retention_sweep: Duration::from_secs(3600),
        }
    }

    #[test]
    fn deployment_revision_defaults_to_local_and_trims_configured_values() {
        assert_eq!(deployment_revision(None), "local");
        assert_eq!(deployment_revision(Some("  ".into())), "local");
        assert_eq!(deployment_revision(Some("  abc123  ".into())), "abc123");
    }

    #[test]
    fn data_plane_secret_is_optional_until_the_data_plane_is_enabled() {
        let mut value = config(
            "development",
            "http://localhost:8080",
            "http://localhost:8080",
            "http://localhost:8080",
        );
        assert!(value.validate().is_ok());
        value.data_plane_enabled = true;
        assert!(value.validate().is_err());
        value.data_plane_sync_token = Some("short".into());
        assert!(value.validate().is_err());
        value.data_plane_sync_token = Some("s".repeat(32));
        value.data_plane_gateway_url = Some("http://gateway:8080".into());
        value.data_plane_gateway_admin_url = Some("http://gateway:9090".into());
        value.data_plane_standard_api_url = Some("http://telegram-standard:8081".into());
        value.data_plane_local_api_url = Some("http://telegram-local:8081".into());
        value.data_plane_official_data_dir = Some("/var/lib/telegram-bot-api".into());
        assert!(value.validate().is_ok());
    }

    #[test]
    fn data_plane_official_data_dir_must_be_normalized_and_absolute() {
        let mut value = config(
            "development",
            "http://localhost:8080",
            "http://localhost:8080",
            "http://localhost:8080",
        );
        value.data_plane_official_data_dir = Some("relative/data".into());
        assert!(value.validate().is_err());
        value.data_plane_official_data_dir = Some("/var/lib/../telegram-bot-api".into());
        assert!(value.validate().is_err());
        value.data_plane_official_data_dir = Some("/var/lib/telegram-bot-api".into());
        assert!(value.validate().is_ok());
    }

    #[test]
    fn production_requires_three_separate_https_hosts() {
        assert!(
            config(
                "production",
                "https://phenogram.io",
                "https://app.phenogram.io",
                "https://api.phenogram.io"
            )
            .validate()
            .is_ok()
        );
        assert!(
            config(
                "production",
                "https://phenogram.io",
                "https://phenogram.io",
                "https://api.phenogram.io"
            )
            .validate()
            .is_err()
        );
        assert!(
            config(
                "production",
                "http://phenogram.io",
                "https://app.phenogram.io",
                "https://api.phenogram.io"
            )
            .validate()
            .is_err()
        );
    }

    #[test]
    fn separates_landing_app_and_machine_surfaces_by_host() {
        let config = config(
            "production",
            "https://phenogram.io",
            "https://app.phenogram.io",
            "https://api.phenogram.io",
        );
        assert_eq!(
            config.public_request_access(Some("phenogram.io"), "/"),
            PublicRequestAccess::Allowed
        );
        assert_eq!(
            config.public_request_access(Some("phenogram.io"), "/api/plans"),
            PublicRequestAccess::WrongSurface
        );
        assert_eq!(
            config.public_request_access(Some("phenogram.io"), "/privacy"),
            PublicRequestAccess::Allowed
        );
        assert_eq!(
            config.public_request_access(Some("phenogram.io"), "/bot123:getMe"),
            PublicRequestAccess::WrongSurface
        );
        assert_eq!(
            config.public_request_access(Some("phenogram.io"), "/client-route"),
            PublicRequestAccess::WrongSurface
        );
        assert_eq!(
            config.public_request_access(Some("app.phenogram.io:443"), "/api/bots"),
            PublicRequestAccess::Allowed
        );
        assert_eq!(
            config.public_request_access(Some("app.phenogram.io"), "/bot123:getMe"),
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
            config.public_request_access(Some("phenogram.io"), "/api/health"),
            PublicRequestAccess::WrongSurface
        );
        assert_eq!(
            config.public_request_access(Some("app.phenogram.io"), "/api/health"),
            PublicRequestAccess::Allowed
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
        assert_eq!(
            config.public_surface(Some("phenogram.io")),
            PublicSurface::Landing
        );
        assert_eq!(
            config.public_surface(Some("app.phenogram.io")),
            PublicSurface::App
        );
        assert_eq!(
            config.public_surface(Some("api.phenogram.io")),
            PublicSurface::Api
        );
    }

    #[test]
    fn permits_single_origin_local_development() {
        let config = config(
            "test",
            "http://127.0.0.1:18080",
            "http://127.0.0.1:18080",
            "http://127.0.0.1:18080",
        );
        assert_eq!(
            config.public_request_access(Some("127.0.0.1:18080"), "/api/me"),
            PublicRequestAccess::Allowed
        );
        assert_eq!(
            config.public_surface(Some("127.0.0.1:18080")),
            PublicSurface::Combined
        );
        assert_eq!(
            config.public_request_access(Some("127.0.0.1:18080"), "/bot123/getMe"),
            PublicRequestAccess::Allowed
        );
    }
}
