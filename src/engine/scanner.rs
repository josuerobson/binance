use crate::binance::models::{BookTickerEvent, MiniTickerEvent};
use crate::config::ScannerConfig;
use crate::engine::arb::{ArbOpportunity, ArbScanner};
use crate::engine::paper::{now_ms, RuntimeConfig, ScannerCandidate};
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
    arb_scanner: ArbScanner,
    arb_pending: Vec<ArbOpportunity>,
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
        // Detect triangular arb opportunities on every book ticker batch.
        let new_opps = self.arb_scanner.detect(&self.book_tickers);
        if !new_opps.is_empty() {
            self.arb_pending.extend(new_opps);
            // Keep pending buffer from growing unboundedly between 3-second flushes.
            if self.arb_pending.len() > 100 {
                self.arb_pending.sort_by(|a, b| b.net_pct.partial_cmp(&a.net_pct).unwrap_or(std::cmp::Ordering::Equal));
                self.arb_pending.truncate(50);
            }
        }
    }

    pub fn drain_arb_opportunities(&mut self) -> Vec<ArbOpportunity> {
        std::mem::take(&mut self.arb_pending)
    }

    pub fn arb_triangles_monitored(&self) -> usize {
        self.arb_scanner.triangles_monitored
    }

    pub fn process_mini_ticker(
        &mut self,
        event: &MiniTickerEvent,
        static_cfg: &ScannerConfig,
        runtime: &RuntimeConfig,
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
        self.process_tick(&event.symbol, tick, static_cfg, runtime, state)
    }

    pub fn process_tick(
        &mut self,
        symbol: &str,
        tick: PriceTick,
        static_cfg: &ScannerConfig,
        runtime: &RuntimeConfig,
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
            .or_insert_with(|| VecDeque::with_capacity(static_cfg.buffer_capacity));
        buffer.push_back(tick);
        while buffer.len() > static_cfg.buffer_capacity {
            buffer.pop_front();
        }
        let cutoff = tick
            .timestamp_ms
            .saturating_sub((runtime.momentum_window_secs.saturating_mul(1000)) as i64);
        while buffer
            .front()
            .map(|oldest| oldest.timestamp_ms < cutoff)
            .unwrap_or(false)
        {
            buffer.pop_front();
        }
        if buffer.len() < static_cfg.min_ticks {
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

        if pct_change + f64::EPSILON < runtime.momentum_trigger_pct
            || volume_surge + f64::EPSILON < runtime.volume_surge_multiplier
            || tick.volume_24h + f64::EPSILON < runtime.min_24h_volume_usdt
            || spread_pct > runtime.max_spread_pct + f64::EPSILON
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

    /// Returns the BTC/USDT momentum over the given window, or None if insufficient data.
    pub fn btc_momentum(&self, window_secs: u64) -> Option<f64> {
        let buffer = self.buffers.get("BTCUSDT")?;
        if buffer.len() < 2 {
            return None;
        }
        let now = now_ms();
        let cutoff = now.saturating_sub((window_secs.saturating_mul(1000)) as i64);
        let window: Vec<_> = buffer.iter().filter(|t| t.timestamp_ms >= cutoff).collect();
        if window.len() < 2 {
            return None;
        }
        let first = window.first()?;
        let last = window.last()?;
        let mom = (last.price - first.price) / first.price * 100.0;
        if mom.is_finite() { Some(mom) } else { None }
    }

    /// Returns true if BTC trend is acceptable for new entries (or filter is disabled).
    pub fn btc_trend_ok(&self, runtime: &RuntimeConfig) -> bool {
        if !runtime.btc_filter_enabled {
            return true;
        }
        match self.btc_momentum(runtime.btc_filter_window_secs) {
            Some(mom) => mom >= runtime.btc_min_momentum_pct,
            None => true, // no BTC data yet — allow
        }
    }

    pub fn collect_candidates(&self, runtime: &RuntimeConfig, state: &GlobalState) -> Vec<ScannerCandidate> {
        let now = now_ms();
        let cutoff = now.saturating_sub((runtime.momentum_window_secs.saturating_mul(1000)) as i64);
        let mut candidates: Vec<ScannerCandidate> = self
            .buffers
            .iter()
            .filter(|(sym, _)| is_eligible_symbol(sym) && !state.has_symbol(sym))
            .filter_map(|(symbol, buffer)| {
                if buffer.len() < 5 {
                    return None;
                }
                let last = buffer.back()?;
                let window: Vec<_> = buffer.iter().filter(|t| t.timestamp_ms >= cutoff).collect();
                if window.len() < 3 {
                    return None;
                }
                let first_in_window = window.first()?;
                let pct_change = (last.price - first_in_window.price) / first_in_window.price * 100.0;
                if !pct_change.is_finite() || pct_change <= 0.0 {
                    return None;
                }
                let volume_sum: f64 = window.iter().map(|t| t.volume).sum();
                let avg_volume = volume_sum / window.len() as f64;
                let volume_surge = if avg_volume > f64::EPSILON { last.volume / avg_volume } else { 0.0 };
                let spread_pct = if last.bid > 0.0 { (last.ask - last.bid) / last.bid * 100.0 } else { f64::MAX };
                let mom_ratio = (pct_change / runtime.momentum_trigger_pct).max(0.0).min(1.5);
                let vol_ratio = (volume_surge / runtime.volume_surge_multiplier).max(0.0).min(1.5);
                let proximity_score = (mom_ratio + vol_ratio) / 2.0;
                Some(ScannerCandidate {
                    symbol: symbol.clone(),
                    price: last.price,
                    momentum_pct: pct_change,
                    momentum_target_pct: runtime.momentum_trigger_pct,
                    volume_surge,
                    volume_surge_target: runtime.volume_surge_multiplier,
                    volume_24h: last.volume_24h,
                    spread_pct,
                    tick_count: buffer.len(),
                    proximity_score,
                    updated_at: last.timestamp_ms,
                })
            })
            .collect();
        candidates.sort_by(|a, b| b.proximity_score.partial_cmp(&a.proximity_score).unwrap_or(std::cmp::Ordering::Equal));
        candidates.truncate(30);
        candidates
    }

    pub fn process_market_batch(
        &mut self,
        events: &[MiniTickerEvent],
        static_cfg: &ScannerConfig,
        runtime: &RuntimeConfig,
        state: &GlobalState,
    ) -> Vec<MomentumSignal> {
        // Always update all buffers (BTC buffer must stay current for the filter).
        let signals: Vec<MomentumSignal> = events
            .iter()
            .filter_map(|event| self.process_mini_ticker(event, static_cfg, runtime, state))
            .collect();

        // Suppress signals when BTC trend is negative.
        if !self.btc_trend_ok(runtime) {
            return vec![];
        }
        signals
    }
}

/// Trailing update: (symbol, new_stop_price, new_highest_price_seen)
type TrailingUpdate = (String, f64, f64);

/// Returns paper position closes and pending trailing stop updates.
fn check_paper_positions(
    events: &[MiniTickerEvent],
    state: &GlobalState,
    trailing_enabled: bool,
) -> (Vec<(String, f64, String, i64)>, Vec<TrailingUpdate>) {
    if state.paper_positions.is_empty() {
        return (vec![], vec![]);
    }
    let ts = events
        .first()
        .map(|e| e.event_time_ms)
        .unwrap_or_else(now_ms);
    let mut closes: Vec<(String, f64, String, i64)> = Vec::new();
    let mut trailing_updates: Vec<TrailingUpdate> = Vec::new();

    for event in events {
        if let Some(pos) = state.paper_positions.get(&event.symbol) {
            if let Ok(price) = event.close_price.parse::<f64>() {
                if !price.is_finite() || price <= 0.0 {
                    continue;
                }
                // Compute effective stop (trailing or fixed).
                let (effective_stop, new_highest) = if trailing_enabled && pos.trailing_stop_distance_pct > 0.0 {
                    let new_highest = price.max(pos.highest_price_seen);
                    let trailing = new_highest * (1.0 - pos.trailing_stop_distance_pct / 100.0);
                    // Stop only moves up — never down.
                    let new_stop = pos.stop_price.max(trailing);
                    (new_stop, new_highest)
                } else {
                    (pos.stop_price, pos.highest_price_seen)
                };

                // Enqueue trailing state update if anything changed.
                if effective_stop > pos.stop_price || new_highest > pos.highest_price_seen {
                    trailing_updates.push((event.symbol.clone(), effective_stop, new_highest));
                }

                if price <= effective_stop {
                    closes.push((event.symbol.clone(), effective_stop, "SL".to_owned(), ts));
                } else if price >= pos.take_profit_price {
                    closes.push((event.symbol.clone(), pos.take_profit_price, "TP".to_owned(), ts));
                }
            }
        }
    }
    (closes, trailing_updates)
}

pub async fn run_scanner(
    mut market_rx: broadcast::Receiver<Vec<MiniTickerEvent>>,
    mut book_rx: broadcast::Receiver<Vec<BookTickerEvent>>,
    signal_tx: mpsc::Sender<MomentumSignal>,
    static_cfg: ScannerConfig,
    runtime: Arc<RwLock<RuntimeConfig>>,
    state: Arc<RwLock<GlobalState>>,
) -> AppResult<()> {
    let mut scanner = Scanner::new();
    let mut last_candidates_update: i64 = 0;
    loop {
        tokio::select! {
            market = market_rx.recv() => {
                match market {
                    Ok(events) => {
                        let rt = runtime.read().await.clone();
                        let signals = {
                            let state_guard = state.read().await;
                            scanner.process_market_batch(&events, &static_cfg, &rt, &state_guard)
                        };

                        // Check paper positions: trailing stop updates + SL/TP closes.
                        let (paper_closes, trailing_updates) = {
                            let state_guard = state.read().await;
                            check_paper_positions(&events, &state_guard, rt.trailing_stop_enabled)
                        };

                        // Apply trailing updates first (moves stops up before checking closes).
                        if !trailing_updates.is_empty() {
                            let mut st = state.write().await;
                            for (sym, new_stop, new_highest) in trailing_updates {
                                st.update_paper_position_trailing(&sym, new_stop, new_highest);
                            }
                        }

                        for (symbol, exit_price, reason, closed_at) in paper_closes {
                            let mut st = state.write().await;
                            if let Some(trade) = st.close_paper_position(&symbol, exit_price, &reason, closed_at) {
                                tracing::info!(
                                    symbol = %trade.symbol,
                                    pnl_pct = %trade.pnl_pct,
                                    pnl_usdt = %trade.pnl_usdt,
                                    reason = %trade.exit_reason,
                                    "Paper position closed"
                                );
                            }
                        }

                        // Update scanner candidates, BTC status, and arb opportunities every 3 seconds.
                        let now = now_ms();
                        if now - last_candidates_update >= 3_000 {
                            last_candidates_update = now;
                            let btc_mom = scanner.btc_momentum(rt.btc_filter_window_secs);
                            let btc_ok = scanner.btc_trend_ok(&rt);
                            let candidates = {
                                let state_guard = state.read().await;
                                scanner.collect_candidates(&rt, &state_guard)
                            };
                            let arb_opps = scanner.drain_arb_opportunities();
                            let arb_triangles = scanner.arb_triangles_monitored();
                            let mut st = state.write().await;
                            st.set_scanner_candidates(candidates);
                            st.set_btc_status(btc_mom, btc_ok);
                            st.set_arb_triangles(arb_triangles);
                            for opp in arb_opps {
                                st.push_arb_opportunity(opp);
                            }
                        }

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

    fn static_cfg() -> ScannerConfig {
        ScannerConfig {
            momentum_window_secs: 60,
            momentum_trigger_pct: 2.5,
            volume_surge_multiplier: 2.0,
            max_spread_pct: 0.3,
            buffer_capacity: 1000,
            min_ticks: 10,
        }
    }

    fn runtime() -> RuntimeConfig {
        RuntimeConfig {
            momentum_window_secs: 60,
            momentum_trigger_pct: 2.5,
            volume_surge_multiplier: 2.0,
            max_spread_pct: 0.3,
            min_24h_volume_usdt: 5_000_000.0,
            stop_loss_pct: 1.5,
            take_profit_pct: 3.0,
            position_size_pct: 10.0,
            max_positions: 2,
            paper_balance: 10_000.0,
            btc_filter_enabled: false, // disabled in tests so BTC buffer absence doesn't block signals
            btc_filter_window_secs: 300,
            btc_min_momentum_pct: 0.0,
            trailing_stop_enabled: true,
            trailing_stop_distance_pct: 1.5,
        }
    }

    #[test]
    fn emits_signal_after_momentum_and_volume_surge() {
        let mut scanner = Scanner::new();
        let state = GlobalState::with_balance(1000.0);
        let scfg = static_cfg();
        let rt = runtime();
        for index in 0..14 {
            let tick = PriceTick {
                price: if index == 14 { 103.0 } else { 100.0 + index as f64 * 0.15 },
                volume: if index == 14 { 2.5 } else { 1.0 },
                volume_24h: 6_000_000.0,
                bid: 102.9,
                ask: 103.0,
                timestamp_ms: index * 4_000,
            };
            assert!(scanner.process_tick("ABCUSDT", tick, &scfg, &rt, &state).is_none());
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
            &scfg,
            &rt,
            &state,
        );
        assert!(signal.is_some());
    }

    #[test]
    fn rejects_insufficient_momentum() {
        let mut scanner = Scanner::new();
        let state = GlobalState::with_balance(1000.0);
        let scfg = static_cfg();
        let rt = runtime();
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
                &scfg,
                &rt,
                &state,
            );
            assert!(signal.is_none());
        }
    }

    #[test]
    fn rejects_stablecoin_and_open_position() {
        let mut scanner = Scanner::new();
        let scfg = static_cfg();
        let rt = runtime();
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
            assert!(scanner.process_tick("USDCUSDT", tick, &scfg, &rt, &state).is_none());
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
        assert!(scanner.process_tick("ABCUSDT", tick, &scfg, &rt, &state_with_pending).is_none());
    }

    #[test]
    fn btc_filter_blocks_signals_when_btc_falling() {
        let mut scanner = Scanner::new();
        let state = GlobalState::with_balance(1000.0);
        let scfg = static_cfg();
        let mut rt = runtime();
        rt.btc_filter_enabled = true;
        rt.btc_min_momentum_pct = 0.0;

        // Feed falling BTC ticks to populate the BTC buffer.
        for index in 0..15 {
            let btc_tick = PriceTick {
                price: 50_000.0 - index as f64 * 100.0, // BTC falling
                volume: 1.0,
                volume_24h: 1_000_000_000.0,
                bid: 49_900.0,
                ask: 49_901.0,
                timestamp_ms: index * 4_000,
            };
            scanner.process_tick("BTCUSDT", btc_tick, &scfg, &rt, &state);
        }

        // Feed a valid signal for another coin — should be suppressed.
        for index in 0..14 {
            scanner.process_tick("ABCUSDT", PriceTick {
                price: 100.0 + index as f64 * 0.15,
                volume: 1.0,
                volume_24h: 6_000_000.0,
                bid: 102.9,
                ask: 103.0,
                timestamp_ms: index * 4_000,
            }, &scfg, &rt, &state);
        }
        let result = scanner.process_tick("ABCUSDT", PriceTick {
            price: 103.0,
            volume: 2.5,
            volume_24h: 6_000_000.0,
            bid: 102.9,
            ask: 103.0,
            timestamp_ms: 56_000,
        }, &scfg, &rt, &state);
        // process_tick itself doesn't check BTC filter; process_market_batch does.
        // Here we verify btc_trend_ok returns false.
        assert!(!scanner.btc_trend_ok(&rt));
        drop(result); // may or may not be Some — process_tick ignores BTC filter
    }
}
