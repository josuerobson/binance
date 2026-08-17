use crate::binance::models::{BookTickerEvent, MiniTickerEvent};
use crate::config::ScannerConfig;
use crate::engine::state::GlobalState;
use crate::error::{AppError, AppResult};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, RwLock};

// TODO FASE 2: backtesting com dados históricos usando a mesma lógica de detecção

const STABLECOIN_BLACKLIST: [&str; 6] = [
    "USDCUSDT",
    "BUSDUSDT",
    "TUSDUSDT",
    "FDUSDUSDT",
    "USDTUSDT",
    "DAIUSDT",
];

#[derive(Debug, Clone, Copy)]
pub struct PriceTick {
    pub price: f64,
    pub volume: f64,
    pub volume_24h: f64,
    pub bid: f64,
    pub ask: f64,
    pub timestamp_ms: i64,
}

#[derive(Debug, Clone)]
pub struct MomentumSignal {
    pub symbol: String,
    pub current_price: f64,
    pub pct_change: f64,
    pub volume_surge: f64,
    pub bid: f64,
    pub ask: f64,
    pub detected_at: i64,
}

#[derive(Debug, Default)]
pub struct Scanner {
    buffers: HashMap<String, VecDeque<PriceTick>>,
    last_quote_volume: HashMap<String, f64>,
    book_tickers: HashMap<String, (f64, f64)>,
}

impl Scanner {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn update_book_tickers(&mut self, events: &[BookTickerEvent]) {
        for event in events {
            let bid = event.bid_price.parse::<f64>();
            let ask = event.ask_price.parse::<f64>();
            if let (Ok(bid), Ok(ask)) = (bid, ask) {
                if bid.is_finite() && ask.is_finite() && bid > 0.0 && ask >= bid {
                    self.book_tickers.insert(event.symbol.clone(), (bid, ask));
                }
            }
        }
    }

    pub fn process_mini_ticker(
        &mut self,
        event: &MiniTickerEvent,
        config: &ScannerConfig,
        min_24h_volume_usdt: f64,
        state: &GlobalState,
    ) -> Option<MomentumSignal> {
        let price = event.close_price.parse::<f64>().ok()?;
        let volume_24h = event.quote_volume.parse::<f64>().ok()?;
        if !price.is_finite() || price <= 0.0 || !volume_24h.is_finite() || volume_24h < 0.0 {
            return None;
        }
        let previous_volume = self
            .last_quote_volume
            .insert(event.symbol.clone(), volume_24h);
        let volume = match previous_volume {
            Some(previous) if volume_24h >= previous => volume_24h - previous,
            Some(_) => 0.0,
            None => 0.0,
        };
        let (bid, ask) = self.book_tickers.get(&event.symbol).copied()?;
        let tick = PriceTick {
            price,
            volume,
            volume_24h,
            bid,
            ask,
            timestamp_ms: event.event_time_ms,
        };
        self.process_tick(&event.symbol, tick, config, min_24h_volume_usdt, state)
    }

    pub fn process_tick(
        &mut self,
        symbol: &str,
        tick: PriceTick,
        config: &ScannerConfig,
        min_24h_volume_usdt: f64,
        state: &GlobalState,
    ) -> Option<MomentumSignal> {
        if !is_eligible_symbol(symbol) || state.has_symbol(symbol) {
            return None;
        }
        if !valid_tick(&tick) {
            return None;
        }

        let buffer = self
            .buffers
            .entry(symbol.to_owned())
            .or_insert_with(|| VecDeque::with_capacity(config.buffer_capacity));
        buffer.push_back(tick);
        while buffer.len() > config.buffer_capacity {
            buffer.pop_front();
        }
        let cutoff = tick
            .timestamp_ms
            .saturating_sub((config.momentum_window_secs.saturating_mul(1000)) as i64);
        while buffer
            .front()
            .map(|oldest| oldest.timestamp_ms < cutoff)
            .unwrap_or(false)
        {
            buffer.pop_front();
        }
        if buffer.len() < config.min_ticks {
            return None;
        }

        let first = buffer.front()?;
        let pct_change = (tick.price - first.price) / first.price * 100.0;
        let volume_sum = buffer.iter().map(|entry| entry.volume).sum::<f64>();
        let average_volume = volume_sum / buffer.len() as f64;
        if !average_volume.is_finite() || average_volume <= 0.0 {
            return None;
        }
        let volume_surge = tick.volume / average_volume;
        let spread_pct = (tick.ask - tick.bid) / tick.bid * 100.0;

        if pct_change + f64::EPSILON < config.momentum_trigger_pct
            || volume_surge + f64::EPSILON < config.volume_surge_multiplier
            || tick.volume_24h + f64::EPSILON < min_24h_volume_usdt
            || spread_pct > config.max_spread_pct + f64::EPSILON
        {
            return None;
        }

        Some(MomentumSignal {
            symbol: symbol.to_owned(),
            current_price: tick.price,
            pct_change,
            volume_surge,
            bid: tick.bid,
            ask: tick.ask,
            detected_at: tick.timestamp_ms,
        })
    }

    pub fn process_market_batch(
        &mut self,
        events: &[MiniTickerEvent],
        config: &ScannerConfig,
        min_24h_volume_usdt: f64,
        state: &GlobalState,
    ) -> Vec<MomentumSignal> {
        events
            .iter()
            .filter_map(|event| self.process_mini_ticker(event, config, min_24h_volume_usdt, state))
            .collect()
    }
}

pub async fn run_scanner(
    mut market_rx: broadcast::Receiver<Vec<MiniTickerEvent>>,
    mut book_rx: broadcast::Receiver<Vec<BookTickerEvent>>,
    signal_tx: mpsc::Sender<MomentumSignal>,
    config: ScannerConfig,
    min_24h_volume_usdt: f64,
    state: Arc<RwLock<GlobalState>>,
) -> AppResult<()> {
    let mut scanner = Scanner::new();
    loop {
        tokio::select! {
            market = market_rx.recv() => {
                match market {
                    Ok(events) => {
                        let signals = {
                            let state_guard = state.read().await;
                            scanner.process_market_batch(&events, &config, min_24h_volume_usdt, &state_guard)
                        };
                        for signal in signals {
                            tracing::info!(symbol = %signal.symbol, price = %signal.current_price, momentum_pct = %signal.pct_change, volume_surge = %signal.volume_surge, "Momentum signal detected");
                            {
                                let mut st = state.write().await;
                                st.push_signal(crate::engine::state::SignalRecord {
                                    symbol: signal.symbol.clone(),
                                    current_price: signal.current_price,
                                    pct_change: signal.pct_change,
                                    volume_surge: signal.volume_surge,
                                    bid: signal.bid,
                                    ask: signal.ask,
                                    detected_at: signal.detected_at,
                                });
                            }
                            signal_tx.send(signal).await.map_err(|_| AppError::ChannelClosed("signal channel".to_owned()))?;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => tracing::warn!(skipped, "Scanner lagged market messages; continuing with latest data"),
                    Err(broadcast::error::RecvError::Closed) => return Err(AppError::ChannelClosed("market stream".to_owned())),
                }
            }
            book = book_rx.recv() => {
                match book {
                    Ok(events) => scanner.update_book_tickers(&events),
                    Err(broadcast::error::RecvError::Lagged(skipped)) => tracing::warn!(skipped, "Scanner lagged book ticker messages"),
                    Err(broadcast::error::RecvError::Closed) => return Err(AppError::ChannelClosed("book ticker stream".to_owned())),
                }
            }
        }
    }
}

fn is_eligible_symbol(symbol: &str) -> bool {
    symbol.ends_with("USDT") && !STABLECOIN_BLACKLIST.contains(&symbol)
}

fn valid_tick(tick: &PriceTick) -> bool {
    tick.price.is_finite()
        && tick.price > 0.0
        && tick.volume.is_finite()
        && tick.volume >= 0.0
        && tick.volume_24h.is_finite()
        && tick.bid.is_finite()
        && tick.ask.is_finite()
        && tick.bid > 0.0
        && tick.ask >= tick.bid
        && tick.timestamp_ms >= 0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> ScannerConfig {
        ScannerConfig {
            momentum_window_secs: 60,
            momentum_trigger_pct: 2.5,
            volume_surge_multiplier: 2.0,
            max_spread_pct: 0.3,
            buffer_capacity: 1000,
            min_ticks: 10,
        }
    }

    #[test]
    fn emits_signal_after_momentum_and_volume_surge() {
        let mut scanner = Scanner::new();
        let state = GlobalState::with_balance(1000.0);
        let config = config();
        for index in 0..14 {
            let tick = PriceTick {
                price: if index == 14 {
                    103.0
                } else {
                    100.0 + index as f64 * 0.15
                },
                volume: if index == 14 { 2.5 } else { 1.0 },
                volume_24h: 6_000_000.0,
                bid: 102.9,
                ask: 103.0,
                timestamp_ms: index * 4_000,
            };
            assert!(scanner
                .process_tick("ABCUSDT", tick, &config, 5_000_000.0, &state)
                .is_none());
        }
        let signal = scanner.process_tick(
            "ABCUSDT",
            PriceTick {
                price: 103.0,
                volume: 2.5,
                volume_24h: 6_000_000.0,
                bid: 102.9,
                ask: 103.0,
                timestamp_ms: 56_000,
            },
            &config,
            5_000_000.0,
            &state,
        );
        assert!(signal.is_some());
    }

    #[test]
    fn rejects_insufficient_momentum() {
        let mut scanner = Scanner::new();
        let state = GlobalState::with_balance(1000.0);
        let config = config();
        for index in 0..15 {
            let signal = scanner.process_tick(
                "ABCUSDT",
                PriceTick {
                    price: 100.0 + index as f64 * 0.05,
                    volume: 1.0,
                    volume_24h: 6_000_000.0,
                    bid: 100.0,
                    ask: 100.1,
                    timestamp_ms: index * 4_000,
                },
                &config,
                5_000_000.0,
                &state,
            );
            assert!(signal.is_none());
        }
    }

    #[test]
    fn rejects_stablecoin_and_open_position() {
        let mut scanner = Scanner::new();
        let config = config();
        let state = GlobalState::with_balance(1000.0);
        for index in 0..12 {
            let tick = PriceTick {
                price: 100.0 + index as f64,
                volume: 1.0,
                volume_24h: 6_000_000.0,
                bid: 100.0,
                ask: 100.01,
                timestamp_ms: index * 5_000,
            };
            assert!(scanner
                .process_tick("USDCUSDT", tick, &config, 5_000_000.0, &state)
                .is_none());
        }
        let mut state_with_pending = GlobalState::with_balance(1000.0);
        assert!(state_with_pending.reserve_symbol("ABCUSDT"));
        let tick = PriceTick {
            price: 103.0,
            volume: 2.5,
            volume_24h: 6_000_000.0,
            bid: 102.9,
            ask: 103.0,
            timestamp_ms: 60_000,
        };
        assert!(scanner
            .process_tick("ABCUSDT", tick, &config, 5_000_000.0, &state_with_pending)
            .is_none());
    }
}
