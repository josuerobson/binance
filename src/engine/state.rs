use crate::binance::exchange_info::ExchangeInfoCache;
use crate::binance::models::{AccountInfo, AssetBalance};
use crate::engine::paper::{PaperPosition, PaperTrade};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

const SIGNAL_BUFFER_CAPACITY: usize = 200;
const PAPER_HISTORY_CAPACITY: usize = 500;

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
        }
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
