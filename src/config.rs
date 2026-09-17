use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};
use serde::Deserialize;

use crate::error::{AppError, AppResult};

const ST_TIMEOUT_RANGE: (u64, u64) = (100, 120_000);
const ST_HARD_TIMEOUT_RANGE: (u64, u64) = (1_000, 3_600_000);
const ST_IDLE_TIMEOUT_RANGE: (u64, u64) = (1_000, 600_000);
const SESSION_TTL_RANGE: (i64, i64) = (1, 168);

#[derive(Debug, Clone, Parser)]
#[command(name = "im-bridge", about = "Telegram-to-SillyTavern sidecar service")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Debug, Clone, Subcommand)]
pub enum Commands {
    Serve {
        #[arg(long, env = "IMBRIDGE_CONFIG")]
        config: Option<PathBuf>,
    },
    Doctor {
        #[arg(long, env = "IMBRIDGE_CONFIG")]
        config: Option<PathBuf>,
    },
    BootstrapAdmin {
        #[arg(long)]
        username: String,
        /// Read the password from this file. Use `-` to read one line from stdin.
        #[arg(long, value_name = "PATH")]
        password_file: Option<PathBuf>,
        #[arg(long, env = "IMBRIDGE_CONFIG")]
        config: Option<PathBuf>,
    },
    ImportSt {
        #[arg(long)]
        data_root: PathBuf,
        #[arg(long)]
        plugin_db: Option<PathBuf>,
        #[arg(long)]
        dry_run: bool,
        #[arg(long, env = "IMBRIDGE_CONFIG")]
        config: Option<PathBuf>,
    },
    ExportSt {
        #[arg(long)]
        workspace: String,
        #[arg(long)]
        output: PathBuf,
        #[arg(long, env = "IMBRIDGE_CONFIG")]
        config: Option<PathBuf>,
    },
    Backup {
        #[arg(long)]
        output: PathBuf,
        #[arg(long, env = "IMBRIDGE_CONFIG")]
        config: Option<PathBuf>,
    },
    RotateMasterKey {
        #[arg(long)]
        dry_run: bool,
        #[arg(long)]
        new_key: PathBuf,
        #[arg(long, env = "IMBRIDGE_CONFIG")]
        config: Option<PathBuf>,
    },
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
    pub listen: SocketAddr,
    pub data_dir: PathBuf,
    pub database_path: PathBuf,
    pub master_key_path: PathBuf,
    pub session_ttl_hours: i64,
    pub cookie_secure: bool,
    #[serde(default)]
    pub st: StClientConfig,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StClientConfig {
    #[serde(default)]
    pub base_url: Option<String>,
    #[serde(default = "default_st_handle")]
    pub handle: String,
    #[serde(default)]
    pub host_header: Option<String>,
    #[serde(default = "default_st_timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default = "default_st_generate_hard_timeout_ms")]
    pub generate_hard_timeout_ms: u64,
    #[serde(default = "default_st_generate_idle_timeout_ms")]
    pub generate_idle_timeout_ms: u64,
    #[serde(default = "default_st_mode")]
    pub mode: String,
    #[serde(default)]
    pub connector_hmac_key: Option<String>,
}

fn default_st_handle() -> String {
    "default-user".into()
}

fn default_st_mode() -> String {
    "disabled".into()
}

fn default_st_timeout_ms() -> u64 {
    15_000
}

fn default_st_generate_hard_timeout_ms() -> u64 {
    900_000
}

fn default_st_generate_idle_timeout_ms() -> u64 {
    90_000
}

impl Default for StClientConfig {
    fn default() -> Self {
        Self {
            base_url: None,
            handle: default_st_handle(),
            host_header: None,
            timeout_ms: default_st_timeout_ms(),
            generate_hard_timeout_ms: default_st_generate_hard_timeout_ms(),
            generate_idle_timeout_ms: default_st_generate_idle_timeout_ms(),
            mode: default_st_mode(),
            connector_hmac_key: None,
        }
    }
}

impl StClientConfig {
    pub fn from_env() -> AppResult<Self> {
        let mut config = Self::default();
        if let Ok(value) = std::env::var("IMBRIDGE_ST_BASE_URL") {
            let trimmed = value.trim().trim_end_matches('/').to_string();
            if !trimmed.is_empty() {
                config.base_url = Some(trimmed);
            }
        }
        if let Ok(value) = std::env::var("IMBRIDGE_ST_HANDLE") {
            if !value.trim().is_empty() {
                config.handle = value.trim().to_string();
            }
        }
        if let Ok(value) = std::env::var("IMBRIDGE_ST_HOST_HEADER") {
            if !value.trim().is_empty() {
                config.host_header = Some(value.trim().to_string());
            }
        }
        config.timeout_ms = parse_env_number(
            "IMBRIDGE_ST_TIMEOUT_MS",
            config.timeout_ms,
            ST_TIMEOUT_RANGE,
        )?;
        config.generate_hard_timeout_ms = parse_env_number(
            "IMBRIDGE_ST_GENERATE_HARD_TIMEOUT_MS",
            config.generate_hard_timeout_ms,
            ST_HARD_TIMEOUT_RANGE,
        )?;
        config.generate_idle_timeout_ms = parse_env_number(
            "IMBRIDGE_ST_GENERATE_IDLE_TIMEOUT_MS",
            config.generate_idle_timeout_ms,
            ST_IDLE_TIMEOUT_RANGE,
        )?;
        if let Ok(value) = std::env::var("IMBRIDGE_ST_MODE") {
            config.mode = value;
        }
        if let Ok(value) = std::env::var("IMBRIDGE_ST_CONNECTOR_HMAC_KEY") {
            if !value.trim().is_empty() {
                config.connector_hmac_key = Some(value);
            }
        }
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> AppResult<()> {
        if crate::domain::st::StWriteMode::parse(&self.mode).is_none() {
            return Err(AppError::bad_request(
                "CONFIG_INVALID",
                "IMBRIDGE_ST_MODE must be disabled, read_only, test_write, or production_write",
            ));
        }
        if self.handle.trim().is_empty() {
            return Err(AppError::bad_request(
                "CONFIG_INVALID",
                "IMBRIDGE_ST_HANDLE must not be empty",
            ));
        }
        if let Some(base_url) = self.base_url.as_deref() {
            validate_service_url("IMBRIDGE_ST_BASE_URL", base_url, true)?;
        }
        if self.write_mode().allows_write() {
            let key = self
                .connector_hmac_key
                .as_deref()
                .unwrap_or_default()
                .as_bytes();
            if key.len() < 32 {
                return Err(AppError::bad_request(
                    "CONFIG_INVALID",
                    "IMBRIDGE_ST_CONNECTOR_HMAC_KEY must contain at least 32 bytes in write mode",
                ));
            }
        }
        // All construction paths, including JSON and direct Rust callers, use
        // the same inclusive limits as environment parsing.
        for (name, value, range) in [
            ("IMBRIDGE_ST_TIMEOUT_MS", self.timeout_ms, ST_TIMEOUT_RANGE),
            (
                "IMBRIDGE_ST_GENERATE_HARD_TIMEOUT_MS",
                self.generate_hard_timeout_ms,
                ST_HARD_TIMEOUT_RANGE,
            ),
            (
                "IMBRIDGE_ST_GENERATE_IDLE_TIMEOUT_MS",
                self.generate_idle_timeout_ms,
                ST_IDLE_TIMEOUT_RANGE,
            ),
        ] {
            bounded_number(name, Some(value), range)?;
        }
        Ok(())
    }

    pub fn write_mode(&self) -> crate::domain::st::StWriteMode {
        crate::domain::st::StWriteMode::parse(&self.mode)
            .unwrap_or(crate::domain::st::StWriteMode::Disabled)
    }
}

fn parse_env_bool(name: &str, default: bool) -> AppResult<bool> {
    match std::env::var(name) {
        Ok(value) if value == "1" || value.eq_ignore_ascii_case("true") => Ok(true),
        Ok(value) if value == "0" || value.eq_ignore_ascii_case("false") => Ok(false),
        Ok(_) => Err(AppError::bad_request(
            "CONFIG_INVALID",
            format!("{name} must be true, false, 1, or 0"),
        )),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(std::env::VarError::NotUnicode(_)) => Err(AppError::bad_request(
            "CONFIG_INVALID",
            format!("{name} is not valid UTF-8"),
        )),
    }
}

fn bounded_number<T: PartialOrd + std::fmt::Display>(
    name: &str,
    value: Option<T>,
    range: (T, T),
) -> AppResult<T> {
    value
        .filter(|value| *value >= range.0 && *value <= range.1)
        .ok_or_else(|| {
            AppError::bad_request(
                "CONFIG_INVALID",
                format!("{name} must be between {} and {}", range.0, range.1),
            )
        })
}

fn parse_env_number<T>(name: &str, default: T, range: (T, T)) -> AppResult<T>
where
    T: std::str::FromStr + PartialOrd + std::fmt::Display,
{
    match std::env::var(name) {
        Ok(value) => bounded_number(name, value.parse().ok(), range),
        Err(std::env::VarError::NotPresent) => bounded_number(name, Some(default), range),
        Err(std::env::VarError::NotUnicode(_)) => Err(AppError::bad_request(
            "CONFIG_INVALID",
            format!("{name} is not valid UTF-8"),
        )),
    }
}

pub fn validate_service_url(name: &str, value: &str, allow_loopback_http: bool) -> AppResult<()> {
    let url = reqwest::Url::parse(value).map_err(|_| {
        AppError::bad_request(
            "CONFIG_INVALID",
            format!("{name} must be an absolute HTTP(S) URL"),
        )
    })?;
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(AppError::bad_request(
            "CONFIG_INVALID",
            format!("{name} must not contain user info, query, or fragment"),
        ));
    }
    let host = url.host_str().unwrap_or_default();
    let loopback = host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback());
    if url.scheme() != "https" && !(allow_loopback_http && url.scheme() == "http" && loopback) {
        return Err(AppError::bad_request(
            "CONFIG_INVALID",
            format!("{name} must use HTTPS; HTTP is allowed only for loopback addresses"),
        ));
    }
    Ok(())
}

impl AppConfig {
    pub fn validate(&self) -> AppResult<()> {
        if !self.listen.ip().is_loopback() && !self.cookie_secure {
            return Err(AppError::bad_request(
                "CONFIG_INVALID",
                "cookie_secure must be true when IMBRIDGE_LISTEN is not a loopback address",
            ));
        }
        bounded_number("session_ttl_hours", Some(self.session_ttl_hours), SESSION_TTL_RANGE)?;
        self.st.validate()
    }

    pub fn from_env_or_defaults(config_path: Option<&Path>) -> AppResult<Self> {
        if let Some(path) = config_path {
            let raw = std::fs::read_to_string(path)?;
            let config: Self = serde_json::from_str(&raw).map_err(|err| {
                AppError::bad_request("CONFIG_INVALID", format!("invalid config: {err}"))
            })?;
            config.validate()?;
            return Ok(config);
        }
        let data_dir = PathBuf::from(
            std::env::var("IMBRIDGE_DATA_DIR").unwrap_or_else(|_| "./data".to_string()),
        );
        let listen = match std::env::var("IMBRIDGE_LISTEN") {
            Ok(value) => value.parse().map_err(|_| {
                AppError::bad_request(
                    "CONFIG_INVALID",
                    "IMBRIDGE_LISTEN is not a valid socket address",
                )
            })?,
            Err(std::env::VarError::NotPresent) => {
                "127.0.0.1:8787".parse().expect("static listen address")
            }
            Err(std::env::VarError::NotUnicode(_)) => {
                return Err(AppError::bad_request(
                    "CONFIG_INVALID",
                    "IMBRIDGE_LISTEN is not valid UTF-8",
                ));
            }
        };
        let config = Self {
            listen,
            database_path: data_dir.join("app.db"),
            master_key_path: PathBuf::from(
                std::env::var("IMBRIDGE_MASTER_KEY_PATH")
                    .unwrap_or_else(|_| "./master.key".to_string()),
            ),
            session_ttl_hours: parse_env_number("IMBRIDGE_SESSION_TTL_HOURS", 12, SESSION_TTL_RANGE)?,
            cookie_secure: parse_env_bool("IMBRIDGE_COOKIE_SECURE", true)?,
            data_dir,
            st: StClientConfig::from_env()?,
        };
        config.validate()?;
        Ok(config)
    }

    pub fn assets_dir(&self) -> PathBuf {
        self.data_dir.join("assets")
    }

    pub fn ensure_dirs(&self) -> AppResult<()> {
        std::fs::create_dir_all(&self.data_dir)?;
        std::fs::create_dir_all(self.assets_dir())?;
        if let Some(parent) = self.database_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        Ok(())
    }
}
