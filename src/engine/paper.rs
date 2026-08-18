use crate::config::AppConfig;
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[derive(Debug, Clone, Serialize)]
pub struct PaperPosition {
    pub symbol: String,
    pub entry_price: f64,
    pub quantity: f64,
    pub virtual_usdt: f64,
    pub stop_price: f64,
    pub take_profit_price: f64,
    pub opened_at: i64,
    pub stop_loss_pct: f64,
    pub take_profit_pct: f64,
    pub momentum_trigger_pct: f64,
    pub momentum_window_secs: u64,
    pub volume_surge_multiplier: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct PaperTrade {
    pub symbol: String,
    pub entry_price: f64,
    pub exit_price: f64,
    pub quantity: f64,
    pub virtual_usdt: f64,
    pub pnl_usdt: f64,
    pub pnl_pct: f64,
    pub exit_reason: String,
    pub opened_at: i64,
    pub closed_at: i64,
    pub duration_ms: i64,
    pub stop_loss_pct: f64,
    pub take_profit_pct: f64,
    pub momentum_trigger_pct: f64,
    pub momentum_window_secs: u64,
    pub volume_surge_multiplier: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeConfig {
    pub momentum_window_secs: u64,
    pub momentum_trigger_pct: f64,
    pub volume_surge_multiplier: f64,
    pub max_spread_pct: f64,
    pub min_24h_volume_usdt: f64,
    pub stop_loss_pct: f64,
    pub take_profit_pct: f64,
    pub position_size_pct: f64,
    pub max_positions: usize,
    pub paper_balance: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScannerCandidate {
    pub symbol: String,
    pub price: f64,
    pub momentum_pct: f64,
    pub momentum_target_pct: f64,
    pub volume_surge: f64,
    pub volume_surge_target: f64,
    pub volume_24h: f64,
    pub spread_pct: f64,
    pub tick_count: usize,
    pub proximity_score: f64,
    pub updated_at: i64,
}

impl RuntimeConfig {
    pub fn from_app_config(config: &AppConfig) -> Self {
        Self {
            momentum_window_secs: config.scanner.momentum_window_secs,
            momentum_trigger_pct: config.scanner.momentum_trigger_pct,
            volume_surge_multiplier: config.scanner.volume_surge_multiplier,
            max_spread_pct: config.scanner.max_spread_pct,
            min_24h_volume_usdt: config.risk.min_24h_volume_usdt,
            stop_loss_pct: config.risk.stop_loss_pct,
            take_profit_pct: config.risk.take_profit_1_pct,
            position_size_pct: config.risk.position_size_pct,
            max_positions: config.risk.max_positions,
            paper_balance: 10_000.0,
        }
    }

    pub fn validate(&self) -> bool {
        self.momentum_window_secs > 0
            && self.momentum_trigger_pct > 0.0
            && self.volume_surge_multiplier >= 0.0
            && self.max_spread_pct > 0.0
            && self.min_24h_volume_usdt >= 0.0
            && self.stop_loss_pct > 0.0
            && self.take_profit_pct > 0.0
            && self.position_size_pct > 0.0
            && self.position_size_pct <= 100.0
            && self.max_positions > 0
            && self.paper_balance >= 0.0
    }
}
