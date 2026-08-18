use crate::binance::exchange_info::ExchangeInfoCache;
use crate::binance::models::{AccountInfo, AssetBalance};
use crate::engine::arb::ArbOpportunity;
use crate::engine::paper::{now_ms, PaperPosition, PaperTrade, RuntimeConfig, ScannerCandidate};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

const SIGNAL_BUFFER_CAPACITY: usize = 200;
const PAPER_HISTORY_CAPACITY: usize = 500;
const ARB_HISTORY_CAPACITY: usize = 500;
const EXPERIMENT_HISTORY_CAPACITY: usize = 200;

#[derive(Debug, Clone, serde::Serialize)]
pub struct ExperimentSlot {
    pub id: u8,
    pub label: String,
    pub ai_provider: String,
    pub config: RuntimeConfig,
    pub paper_balance: f64,
    pub paper_positions: HashMap<String, PaperPosition>,
    pub paper_history: VecDeque<PaperTrade>,
    pub initial_balance: f64,
    pub started_at: i64,
}

impl ExperimentSlot {
    pub fn new(id: u8, label: String, ai_provider: String, config: RuntimeConfig, balance: f64) -> Self {
        Self {
            id,
            label,
            ai_provider,
            config,
            paper_balance: balance,
            paper_positions: HashMap::new(),
            paper_history: VecDeque::with_capacity(EXPERIMENT_HISTORY_CAPACITY),
            initial_balance: balance,
            started_at: now_ms(),
        }
    }

    pub fn trade_count(&self) -> usize { self.paper_history.len() }

    pub fn win_rate(&self) -> f64 {
        if self.paper_history.is_empty() { return 0.0; }
        let wins = self.paper_history.iter().filter(|t| t.pnl_usdt > 0.0).count();
        wins as f64 / self.paper_history.len() as f64
    }

    pub fn avg_pnl_pct(&self) -> f64 {
        if self.paper_history.is_empty() { return 0.0; }
        self.paper_history.iter().map(|t| t.pnl_pct).sum::<f64>() / self.paper_history.len() as f64
    }

    pub fn max_loss_pct(&self) -> f64 {
        self.paper_history.iter()
            .map(|t| if t.pnl_pct < 0.0 { t.pnl_pct.abs() } else { 0.0 })
            .fold(0.0_f64, f64::max)
    }

    pub fn fitness_score(&self) -> f64 {
        let ml = self.max_loss_pct();
        if ml <= 0.0 || self.paper_history.len() < 3 { return 0.0; }
        self.win_rate() * self.avg_pnl_pct().max(0.0) / ml
    }

    pub fn total_pnl_pct(&self) -> f64 {
        if self.initial_balance <= 0.0 { return 0.0; }
        (self.paper_balance - self.initial_balance) / self.initial_balance * 100.0
    }

    pub fn open_position(&mut self, pos: PaperPosition) {
        self.paper_balance = (self.paper_balance - pos.virtual_usdt).max(0.0);
        self.paper_positions.insert(pos.symbol.clone(), pos);
    }

    pub fn close_position(&mut self, symbol: &str, exit_price: f64, exit_reason: &str, closed_at: i64) -> Option<PaperTrade> {
        let pos = self.paper_positions.remove(symbol)?;
        let pnl_usdt = (exit_price - pos.entry_price) * pos.quantity;
        let pnl_pct = (exit_price - pos.entry_price) / pos.entry_price * 100.0;
        self.paper_balance += pos.virtual_usdt + pnl_usdt;
        let trade = PaperTrade {
            symbol: symbol.to_owned(),
            entry_price: pos.entry_price,
            exit_price,
            quantity: pos.quantity,
            virtual_usdt: pos.virtual_usdt,
            pnl_usdt,
            pnl_pct,
            exit_reason: exit_reason.to_owned(),
            opened_at: pos.opened_at,
            closed_at,
            duration_ms: closed_at - pos.opened_at,
            stop_loss_pct: pos.stop_loss_pct,
            take_profit_pct: pos.take_profit_pct,
            momentum_trigger_pct: pos.momentum_trigger_pct,
            momentum_window_secs: pos.momentum_window_secs,
            volume_surge_multiplier: pos.volume_surge_multiplier,
        };
        if self.paper_history.len() >= EXPERIMENT_HISTORY_CAPACITY {
            self.paper_history.pop_front();
        }
        self.paper_history.push_back(trade.clone());
        Some(trade)
    }

    pub fn update_trailing(&mut self, symbol: &str, new_stop: f64, new_highest: f64) {
        if let Some(pos) = self.paper_positions.get_mut(symbol) {
            if new_stop > pos.stop_price { pos.stop_price = new_stop; }
            if new_highest > pos.highest_price_seen { pos.highest_price_seen = new_highest; }
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct SignalRecord {
    pub symbol: String,
    pub current_price: f64,
    pub pct_change: f64,
    pub volume_surge: f64,
    pub bid: f64,
    pub ask: f64,
    pub detected_at: i64,
}

#[derive(Debug, Clone)]
pub struct Position {
    pub symbol: String,
    pub entry_price: f64,
    pub quantity: f64,
    pub entry_order_id: u64,
    pub order_list_id: Option<i64>,
    pub stop_order_id: Option<u64>,
    pub take_profit_order_id: Option<u64>,
    pub opened_at: i64,
}

#[derive(Debug)]
pub struct GlobalState {
    pub open_positions: HashMap<String, Position>,
    pub pending_symbols: HashSet<String>,
    pub blocked_symbols: HashSet<String>,
    pub reconciliation_required: bool,
    pub balances: HashMap<String, AssetBalance>,
    pub usdt_balance: f64,
    pub exchange_info: Arc<ExchangeInfoCache>,
    pub recent_signals: VecDeque<SignalRecord>,
    pub paper_positions: HashMap<String, PaperPosition>,
    pub paper_history: VecDeque<PaperTrade>,
    pub paper_balance: f64,
    pub scanner_candidates: Vec<ScannerCandidate>,
    pub btc_momentum_pct: Option<f64>,
    pub btc_filter_ok: bool,
    pub arb_opportunities: VecDeque<ArbOpportunity>,
    pub arb_triangles_monitored: usize,
    pub experiment_slots: Vec<ExperimentSlot>,
    pub experiment_active: bool,
    pub experiment_cycle_id: u64,
}

impl GlobalState {
    pub fn new(account: &AccountInfo) -> Self {
        let balances = account
            .balances
            .iter()
            .cloned()
            .map(|balance| (balance.asset.clone(), balance))
            .collect::<HashMap<_, _>>();
        let usdt_balance = balances
            .get("USDT")
            .and_then(|balance| balance.free.parse::<f64>().ok())
            .filter(|value| value.is_finite() && *value >= 0.0)
            .unwrap_or(0.0);
        Self {
            open_positions: HashMap::new(),
            pending_symbols: HashSet::new(),
            blocked_symbols: HashSet::new(),
            reconciliation_required: false,
            balances,
            usdt_balance,
            exchange_info: Arc::new(ExchangeInfoCache::new()),
            recent_signals: VecDeque::with_capacity(SIGNAL_BUFFER_CAPACITY),
            paper_positions: HashMap::new(),
            paper_history: VecDeque::with_capacity(PAPER_HISTORY_CAPACITY),
            paper_balance: 10_000.0,
            scanner_candidates: Vec::new(),
            btc_momentum_pct: None,
            btc_filter_ok: true,
            arb_opportunities: VecDeque::with_capacity(ARB_HISTORY_CAPACITY),
            arb_triangles_monitored: 0,
            experiment_slots: Vec::new(),
            experiment_active: false,
            experiment_cycle_id: 0,
        }
    }

    pub fn with_balance(usdt_balance: f64) -> Self {
        Self {
            open_positions: HashMap::new(),
            pending_symbols: HashSet::new(),
            blocked_symbols: HashSet::new(),
            reconciliation_required: false,
            balances: HashMap::new(),
            usdt_balance,
            exchange_info: Arc::new(ExchangeInfoCache::new()),
            recent_signals: VecDeque::with_capacity(SIGNAL_BUFFER_CAPACITY),
            paper_positions: HashMap::new(),
            paper_history: VecDeque::with_capacity(PAPER_HISTORY_CAPACITY),
            paper_balance: 10_000.0,
            scanner_candidates: Vec::new(),
            btc_momentum_pct: None,
            btc_filter_ok: true,
            arb_opportunities: VecDeque::with_capacity(ARB_HISTORY_CAPACITY),
            arb_triangles_monitored: 0,
            experiment_slots: Vec::new(),
            experiment_active: false,
            experiment_cycle_id: 0,
        }
    }

    pub fn start_experiment(&mut self, slots: Vec<ExperimentSlot>) {
        self.experiment_slots = slots;
        self.experiment_active = true;
        self.experiment_cycle_id += 1;
        tracing::info!(cycle_id = self.experiment_cycle_id, slots = self.experiment_slots.len(), "Experiment started");
    }

    pub fn stop_experiment(&mut self) {
        self.experiment_active = false;
        tracing::info!(cycle_id = self.experiment_cycle_id, "Experiment stopped");
    }

    pub fn reset_experiment_slots(&mut self, balance: f64) {
        for slot in &mut self.experiment_slots {
            slot.paper_balance = balance;
            slot.paper_positions.clear();
            slot.paper_history.clear();
            slot.initial_balance = balance;
            slot.started_at = now_ms();
        }
        self.experiment_active = false;
        tracing::info!("Experiment slots reset");
    }

    pub fn push_signal(&mut self, signal: SignalRecord) {
        if self.recent_signals.len() >= SIGNAL_BUFFER_CAPACITY {
            self.recent_signals.pop_front();
        }
        self.recent_signals.push_back(signal);
    }

    pub fn open_positions_count(&self) -> usize {
        self.open_positions.len()
    }

    pub fn active_positions_count(&self) -> usize {
        self.open_positions.len() + self.pending_symbols.len()
    }

    pub fn has_symbol(&self, symbol: &str) -> bool {
        self.open_positions.contains_key(symbol)
            || self.pending_symbols.contains(symbol)
            || self.blocked_symbols.contains(symbol)
    }

    pub fn reserve_symbol(&mut self, symbol: &str) -> bool {
        if self.has_symbol(symbol) {
            return false;
        }
        self.pending_symbols.insert(symbol.to_owned())
    }

    pub fn release_symbol(&mut self, symbol: &str) {
        self.pending_symbols.remove(symbol);
    }

    pub fn block_symbol(&mut self, symbol: &str) {
        self.pending_symbols.remove(symbol);
        self.blocked_symbols.insert(symbol.to_owned());
    }

    pub fn mark_reconciliation_required(&mut self) {
        self.reconciliation_required = true;
    }

    pub fn mark_reconciled(&mut self) {
        self.reconciliation_required = false;
    }

    pub fn add_position(&mut self, position: Position) -> bool {
        self.pending_symbols.remove(&position.symbol);
        self.open_positions
            .insert(position.symbol.clone(), position)
            .is_none()
    }

    pub fn remove_position(&mut self, symbol: &str) -> Option<Position> {
        self.pending_symbols.remove(symbol);
        self.open_positions.remove(symbol)
    }

    pub fn update_account_balances(&mut self, balances: &[AssetBalance]) {
        for balance in balances {
            self.balances.insert(balance.asset.clone(), balance.clone());
        }
        if let Some(usdt) = self.balances.get("USDT") {
            if let Ok(value) = usdt.free.parse::<f64>() {
                if value.is_finite() && value >= 0.0 {
                    self.usdt_balance = value;
                }
            }
        }
    }

    pub fn update_usdt_balance(&mut self, value: f64) {
        if value.is_finite() && value >= 0.0 {
            self.usdt_balance = value;
        }
    }

    pub fn open_paper_position(&mut self, pos: PaperPosition) {
        self.paper_balance = (self.paper_balance - pos.virtual_usdt).max(0.0);
        self.paper_positions.insert(pos.symbol.clone(), pos);
    }

    pub fn close_paper_position(
        &mut self,
        symbol: &str,
        exit_price: f64,
        exit_reason: &str,
        closed_at: i64,
    ) -> Option<PaperTrade> {
        let pos = self.paper_positions.remove(symbol)?;
        let pnl_usdt = (exit_price - pos.entry_price) * pos.quantity;
        let pnl_pct = (exit_price - pos.entry_price) / pos.entry_price * 100.0;
        self.paper_balance += pos.virtual_usdt + pnl_usdt;
        let trade = PaperTrade {
            symbol: symbol.to_owned(),
            entry_price: pos.entry_price,
            exit_price,
            quantity: pos.quantity,
            virtual_usdt: pos.virtual_usdt,
            pnl_usdt,
            pnl_pct,
            exit_reason: exit_reason.to_owned(),
            opened_at: pos.opened_at,
            closed_at,
            duration_ms: closed_at - pos.opened_at,
            stop_loss_pct: pos.stop_loss_pct,
            take_profit_pct: pos.take_profit_pct,
            momentum_trigger_pct: pos.momentum_trigger_pct,
            momentum_window_secs: pos.momentum_window_secs,
            volume_surge_multiplier: pos.volume_surge_multiplier,
        };
        if self.paper_history.len() >= PAPER_HISTORY_CAPACITY {
            self.paper_history.pop_front();
        }
        self.paper_history.push_back(trade.clone());
        Some(trade)
    }

    pub fn has_paper_symbol(&self, symbol: &str) -> bool {
        self.paper_positions.contains_key(symbol)
    }

    pub fn paper_active_count(&self) -> usize {
        self.paper_positions.len()
    }

    pub fn set_paper_balance(&mut self, balance: f64) {
        if balance.is_finite() && balance >= 0.0 {
            self.paper_balance = balance;
        }
    }

    pub fn set_scanner_candidates(&mut self, candidates: Vec<ScannerCandidate>) {
        self.scanner_candidates = candidates;
    }

    pub fn update_paper_position_trailing(&mut self, symbol: &str, new_stop: f64, new_highest: f64) {
        if let Some(pos) = self.paper_positions.get_mut(symbol) {
            if new_stop > pos.stop_price {
                pos.stop_price = new_stop;
            }
            if new_highest > pos.highest_price_seen {
                pos.highest_price_seen = new_highest;
            }
        }
    }

    pub fn set_btc_status(&mut self, momentum_pct: Option<f64>, filter_ok: bool) {
        self.btc_momentum_pct = momentum_pct;
        self.btc_filter_ok = filter_ok;
    }

    pub fn push_arb_opportunity(&mut self, opp: ArbOpportunity) {
        if self.arb_opportunities.len() >= ARB_HISTORY_CAPACITY {
            self.arb_opportunities.pop_front();
        }
        self.arb_opportunities.push_back(opp);
    }

    pub fn set_arb_triangles(&mut self, count: usize) {
        self.arb_triangles_monitored = count;
    }
}

// TODO FASE 2: persistência de posições em banco de dados para recovery após restart
pub async fn reconcile_state(
    client: &crate::binance::client::BinanceClient,
    state: &std::sync::Arc<tokio::sync::RwLock<GlobalState>>,
) -> crate::error::AppResult<()> {
    {
        let mut state_guard = state.write().await;
        state_guard.mark_reconciliation_required();
    }

    let account = client.get_account_info().await?;
    let open_orders = client.get_all_open_orders().await?;
    let open_order_symbols = open_orders
        .iter()
        .map(|order| order.symbol.clone())
        .collect::<std::collections::HashSet<_>>();

    let mut state_guard = state.write().await;
    state_guard.update_account_balances(&account.balances);
    let known_positions = state_guard
        .open_positions
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    for symbol in known_positions {
        if !open_order_symbols.contains(&symbol) {
            state_guard.remove_position(&symbol);
            tracing::info!(symbol = %symbol, "Position removed during REST reconciliation because no protection order remains");
        }
    }
    for symbol in open_order_symbols {
        if !state_guard.open_positions.contains_key(&symbol) {
            state_guard.block_symbol(&symbol);
            tracing::warn!(symbol = %symbol, "Symbol blocked during reconciliation because an open order has no matching local position");
        }
    }
    state_guard.mark_reconciled();
    tracing::info!(
        open_positions = state_guard.open_positions.len(),
        "REST reconciliation completed"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserves_and_releases_symbol_without_duplicate_entries() {
        let mut state = GlobalState::with_balance(1000.0);
        assert!(state.reserve_symbol("BTCUSDT"));
        assert!(!state.reserve_symbol("BTCUSDT"));
        state.release_symbol("BTCUSDT");
        assert!(state.reserve_symbol("BTCUSDT"));
    }

    #[test]
    fn pauses_entries_until_reconciled() {
        let mut state = GlobalState::with_balance(1000.0);
        assert!(!state.reconciliation_required);
        state.mark_reconciliation_required();
        assert!(state.reconciliation_required);
        state.mark_reconciled();
        assert!(!state.reconciliation_required);
    }

    #[test]
    fn derives_free_usdt_from_account() {
        let account = AccountInfo {
            can_trade: true,
            can_withdraw: false,
            can_deposit: true,
            update_time: 0,
            account_type: "SPOT".into(),
            balances: vec![AssetBalance {
                asset: "USDT".into(),
                free: "123.45".into(),
                locked: "0".into(),
            }],
            maker_commission: 0,
            taker_commission: 0,
            buyer_commission: 0,
            seller_commission: 0,
            permissions: vec![],
            brokered: false,
            require_self_trade_prevention: false,
            uid: None,
        };
        let state = GlobalState::new(&account);
        assert_eq!(state.usdt_balance, 123.45);
    }
}
