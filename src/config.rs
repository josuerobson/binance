use crate::error::{AppError, AppResult};
use serde::Deserialize;
use std::env;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
pub struct RiskConfig {
    pub max_positions: usize,
    pub position_size_pct: f64,
    pub stop_loss_pct: f64,
    pub take_profit_1_pct: f64,
    pub take_profit_2_pct: f64,
    pub min_24h_volume_usdt: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ScannerConfig {
    pub momentum_window_secs: u64,
    pub momentum_trigger_pct: f64,
    pub volume_surge_multiplier: f64,
    pub max_spread_pct: f64,
    #[serde(default = "default_buffer_capacity")]
    pub buffer_capacity: usize,
    #[serde(default = "default_min_ticks")]
    pub min_ticks: usize,
}

fn default_buffer_capacity() -> usize {
    1000
}

fn default_min_ticks() -> usize {
    10
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExchangeConfig {
    pub recv_window_ms: u64,
    pub exchange_info_refresh_secs: u64,
    pub listen_key_refresh_secs: u64,
    pub rest_timeout_secs: u64,
    #[serde(default = "default_weight_limit")]
    pub request_weight_limit_per_minute: u32,
}

fn default_weight_limit() -> u32 {
    6000
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    pub health_port: u16,
}

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub api_key: String,
    pub secret_key: String,
    pub dashboard_api_key: String,
    pub use_testnet: bool,
    pub dry_run: bool,
    pub allow_live_trading: bool,
    pub rest_base_url: String,
    pub ws_market_base_url: String,
    pub ws_user_base_url: String,
    pub risk: RiskConfig,
    pub scanner: ScannerConfig,
    pub exchange: ExchangeConfig,
    pub server: ServerConfig,
}

#[derive(Debug, Clone, Deserialize)]
struct FileConfig {
    risk: RiskConfig,
    scanner: ScannerConfig,
    exchange: ExchangeConfig,
    server: ServerConfig,
}

impl AppConfig {
    pub fn load() -> AppResult<Self> {
        dotenvy::dotenv().ok();

        let api_key = required_env("BINANCE_API_KEY")?;
        let secret_key = required_env("BINANCE_SECRET_KEY")?;
        let dashboard_api_key = env::var("DASHBOARD_API_KEY").unwrap_or_default();
        let use_testnet = parse_bool_env("USE_TESTNET", true)?;
        let dry_run = parse_bool_env("DRY_RUN", true)?;
        let allow_live_trading = parse_bool_env("ALLOW_LIVE_TRADING", false)?;

        let configured_path = env::var("BINANCE_CONFIG_PATH").ok();
        let path = configured_path
            .as_deref()
            .filter(|value| Path::new(value).exists())
            .map(ToOwned::to_owned)
            .or_else(|| {
                ["/config/default.toml", "config/default.toml"]
                    .into_iter()
                    .find(|candidate| Path::new(candidate).exists())
                    .map(ToOwned::to_owned)
            })
            .ok_or_else(|| AppError::InvalidConfig("config/default.toml not found".to_owned()))?;

        let file_config: FileConfig = ::config::Config::builder()
            .add_source(::config::File::with_name(path.trim_end_matches(".toml")))
            .build()
            .map_err(|error| AppError::InvalidConfig(error.to_string()))?
            .try_deserialize()
            .map_err(|error| AppError::InvalidConfig(error.to_string()))?;

        let (rest_base_url, ws_market_base_url, ws_user_base_url) = if use_testnet {
            (
                "https://testnet.binance.vision".to_owned(),
                "wss://stream.testnet.binance.vision/ws".to_owned(),
                "wss://stream.testnet.binance.vision/ws".to_owned(),
            )
        } else {
            (
                "https://api.binance.com".to_owned(),
                "wss://stream.binance.com:9443/ws".to_owned(),
                "wss://stream.binance.com:9443/ws".to_owned(),
            )
        };

        let config = Self {
            api_key,
            secret_key,
            dashboard_api_key,
            use_testnet,
            dry_run,
            allow_live_trading,
            rest_base_url,
            ws_market_base_url,
            ws_user_base_url,
            risk: file_config.risk,
            scanner: file_config.scanner,
            exchange: file_config.exchange,
            server: file_config.server,
        };
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> AppResult<()> {
        if self.api_key.trim().is_empty() || self.secret_key.trim().is_empty() {
            return Err(AppError::InvalidConfig(
                "BINANCE_API_KEY and BINANCE_SECRET_KEY cannot be empty".to_owned(),
            ));
        }
        if self.exchange.recv_window_ms == 0 || self.exchange.recv_window_ms > 60_000 {
            return Err(AppError::InvalidConfig(
                "recv_window_ms must be between 1 and 60000".to_owned(),
            ));
        }
        if self.risk.max_positions == 0 {
            return Err(AppError::InvalidConfig(
                "risk.max_positions must be greater than zero".to_owned(),
            ));
        }
        validate_pct(
            "risk.position_size_pct",
            self.risk.position_size_pct,
            0.0,
            100.0,
        )?;
        validate_pct("risk.stop_loss_pct", self.risk.stop_loss_pct, 0.0, 100.0)?;
        validate_pct(
            "risk.take_profit_1_pct",
            self.risk.take_profit_1_pct,
            0.0,
            1000.0,
        )?;
        validate_pct(
            "risk.take_profit_2_pct",
            self.risk.take_profit_2_pct,
            0.0,
            1000.0,
        )?;
        if self.risk.min_24h_volume_usdt < 0.0 || !self.risk.min_24h_volume_usdt.is_finite() {
            return Err(AppError::InvalidConfig(
                "risk.min_24h_volume_usdt must be finite and non-negative".to_owned(),
            ));
        }
        if self.scanner.momentum_window_secs == 0 || self.scanner.min_ticks == 0 {
            return Err(AppError::InvalidConfig(
                "scanner window and min_ticks must be greater than zero".to_owned(),
            ));
        }
        if self.scanner.buffer_capacity < self.scanner.min_ticks {
            return Err(AppError::InvalidConfig(
                "scanner.buffer_capacity must be at least scanner.min_ticks".to_owned(),
            ));
        }
        validate_pct(
            "scanner.momentum_trigger_pct",
            self.scanner.momentum_trigger_pct,
            0.0,
            1000.0,
        )?;
        validate_pct(
            "scanner.max_spread_pct",
            self.scanner.max_spread_pct,
            0.0,
            100.0,
        )?;
        if !self.scanner.volume_surge_multiplier.is_finite()
            || self.scanner.volume_surge_multiplier < 0.0
        {
            return Err(AppError::InvalidConfig(
                "scanner.volume_surge_multiplier must be finite and non-negative".to_owned(),
            ));
        }
        if self.exchange.exchange_info_refresh_secs == 0
            || self.exchange.listen_key_refresh_secs == 0
            || self.exchange.rest_timeout_secs == 0
            || self.exchange.request_weight_limit_per_minute == 0
        {
            return Err(AppError::InvalidConfig(
                "exchange refresh intervals, timeout and request weight limit must be greater than zero".to_owned(),
            ));
        }
        if self.server.health_port == 0 {
            return Err(AppError::InvalidConfig(
                "server.health_port must be greater than zero".to_owned(),
            ));
        }
        if !self.use_testnet && !self.dry_run && !self.allow_live_trading {
            return Err(AppError::InvalidConfig(
                "live trading requires ALLOW_LIVE_TRADING=true as an explicit safety acknowledgement".to_owned(),
            ));
        }
        Ok(())
    }
}

fn required_env(name: &str) -> AppResult<String> {
    let value =
        env::var(name).map_err(|_| AppError::InvalidConfig(format!("{name} is required")))?;
    if value.trim().is_empty() {
        return Err(AppError::InvalidConfig(format!("{name} cannot be empty")));
    }
    Ok(value)
}

fn parse_bool_env(name: &str, default: bool) -> AppResult<bool> {
    match env::var(name) {
        Ok(value) => value
            .parse::<bool>()
            .map_err(|_| AppError::InvalidConfig(format!("{name} must be true or false"))),
        Err(_) => Ok(default),
    }
}

fn validate_pct(name: &str, value: f64, min: f64, max: f64) -> AppResult<()> {
    if !value.is_finite() || value < min || value > max {
        return Err(AppError::InvalidConfig(format!(
            "{name} must be finite and between {min} and {max}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> AppConfig {
        AppConfig {
            api_key: "key".into(),
            secret_key: "secret".into(),
            use_testnet: true,
            dry_run: true,
            allow_live_trading: false,
            rest_base_url: "https://testnet.binance.vision".into(),
            ws_market_base_url: "wss://stream.testnet.binance.vision/ws".into(),
            ws_user_base_url: "wss://stream.testnet.binance.vision/ws".into(),
            risk: RiskConfig {
                max_positions: 2,
                position_size_pct: 10.0,
                stop_loss_pct: 1.5,
                take_profit_1_pct: 3.0,
                take_profit_2_pct: 6.0,
                min_24h_volume_usdt: 5_000_000.0,
            },
            scanner: ScannerConfig {
                momentum_window_secs: 60,
                momentum_trigger_pct: 2.5,
                volume_surge_multiplier: 2.0,
                max_spread_pct: 0.3,
                buffer_capacity: 1000,
                min_ticks: 10,
            },
            exchange: ExchangeConfig {
                recv_window_ms: 5000,
                exchange_info_refresh_secs: 3600,
                listen_key_refresh_secs: 1800,
                rest_timeout_secs: 5,
                request_weight_limit_per_minute: 6000,
            },
            server: ServerConfig { health_port: 8080 },
            dashboard_api_key: "test-key".into(),
        }
    }

    #[test]
    fn rejects_live_trading_without_explicit_acknowledgement() {
        let mut config = sample();
        config.use_testnet = false;
        config.dry_run = false;
        assert!(config.validate().is_err());
    }

    #[test]
    fn accepts_valid_testnet_config() {
        assert!(sample().validate().is_ok());
    }
}
