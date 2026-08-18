use crate::binance::client::BinanceClient;
use crate::binance::models::{OrderListParams, OrderResponse, Side, UserDataEvent};
use crate::config::AppConfig;
use crate::engine::paper::{now_ms, PaperPosition, RuntimeConfig};
use crate::engine::risk::{
    apply_tick_size, calculate_quantity, check_balance, check_position_limit,
    validate_order_filters,
};
use crate::engine::scanner::MomentumSignal;
use crate::engine::state::{GlobalState, Position};
use crate::error::{AppError, AppResult};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, RwLock};

pub async fn run_executor(
    mut signal_rx: mpsc::Receiver<MomentumSignal>,
    mut user_rx: broadcast::Receiver<UserDataEvent>,
    client: Arc<BinanceClient>,
    state: Arc<RwLock<GlobalState>>,
    config: AppConfig,
    runtime: Arc<RwLock<RuntimeConfig>>,
) -> AppResult<()> {
    loop {
        tokio::select! {
            signal = signal_rx.recv() => {
                let signal = signal.ok_or_else(|| AppError::ChannelClosed("signal channel".to_owned()))?;
                if let Err(error) = process_signal(signal, Arc::clone(&client), Arc::clone(&state), &config, Arc::clone(&runtime)).await {
                    tracing::error!(error = %error, "Signal processing failed");
                }
            }
            event = user_rx.recv() => {
                match event {
                    Ok(event) => handle_user_event(event, &state).await?,
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        let mut state_guard = state.write().await;
                        state_guard.mark_reconciliation_required();
                        tracing::error!(skipped, "Executor lagged user events; live entries are paused until REST reconciliation");
                    }
                    Err(broadcast::error::RecvError::Closed) => return Err(AppError::ChannelClosed("user data stream".to_owned())),
                }
            }
        }
    }
}

async fn process_signal(
    signal: MomentumSignal,
    client: Arc<BinanceClient>,
    state: Arc<RwLock<GlobalState>>,
    config: &AppConfig,
    runtime: Arc<RwLock<RuntimeConfig>>,
) -> AppResult<()> {
    if config.dry_run {
        return open_paper_position(signal, state, runtime).await;
    }

    let (usdt_to_use, quantity, filters) = {
        let mut state_guard = state.write().await;
        if state_guard.reconciliation_required {
            return Err(AppError::InvalidConfig(
                "live entries paused until user data reconciliation completes".to_owned(),
            ));
        }
        check_position_limit(&state_guard, config.risk.max_positions).inspect_err(|_error| {
            tracing::warn!(
                symbol = %signal.symbol,
                reason = "position_limit_reached",
                "Signal ignored"
            );
        })?;
        if !state_guard.reserve_symbol(&signal.symbol) {
            tracing::warn!(symbol = %signal.symbol, reason = "symbol_already_open_or_pending", "Signal ignored");
            return Ok(());
        }
        let usdt_to_use = state_guard.usdt_balance * config.risk.position_size_pct / 100.0;
        if let Err(error) = check_balance(&state_guard, usdt_to_use) {
            state_guard.release_symbol(&signal.symbol);
            tracing::warn!(symbol = %signal.symbol, reason = "insufficient_balance", required = %usdt_to_use, available = %state_guard.usdt_balance, "Signal ignored");
            return Err(error);
        }
        let filters = match state_guard.exchange_info.get(&signal.symbol) {
            Some(filters) => filters,
            None => {
                state_guard.release_symbol(&signal.symbol);
                return Err(AppError::FilterViolation(format!(
                    "no exchange filters cached for {}",
                    signal.symbol
                )));
            }
        };
        let quantity = match calculate_quantity(usdt_to_use, signal.current_price, &filters) {
            Ok(quantity) => quantity,
            Err(error) => {
                state_guard.release_symbol(&signal.symbol);
                return Err(error);
            }
        };
        (usdt_to_use, quantity, filters)
    };

    tracing::info!(symbol = %signal.symbol, usdt_to_use = %usdt_to_use, quantity = %quantity, "Submitting market order");
    let market_order = match client
        .place_market_order(&signal.symbol, Side::Buy, quantity)
        .await
    {
        Ok(order) => order,
        Err(error) => {
            let mut state_guard = state.write().await;
            if matches!(error, AppError::ExecutionStatusUnknown(_)) {
                state_guard.block_symbol(&signal.symbol);
                tracing::error!(symbol = %signal.symbol, "CRITICAL: market order execution status unknown; symbol blocked");
            } else {
                state_guard.release_symbol(&signal.symbol);
            }
            return Err(error);
        }
    };

    let status = market_order.status.as_str();
    if status != "FILLED" && status != "PARTIALLY_FILLED" {
        let mut state_guard = state.write().await;
        state_guard.release_symbol(&signal.symbol);
        tracing::warn!(symbol = %signal.symbol, status, "Market order did not fill; position not opened");
        return Ok(());
    }
    if status == "PARTIALLY_FILLED" {
        tracing::warn!(symbol = %signal.symbol, status, "Market order partially filled; protecting executed quantity");
    }

    let executed_qty = match parse_positive(&market_order.executed_qty, "executedQty") {
        Ok(value) => value,
        Err(error) => {
            let _ = emergency_sell(&client, &signal.symbol, quantity).await;
            let mut state_guard = state.write().await;
            state_guard.block_symbol(&signal.symbol);
            return Err(error);
        }
    };
    let fill_price = match effective_fill_price(&market_order) {
        Ok(value) => value,
        Err(error) => {
            let _ = emergency_sell(&client, &signal.symbol, executed_qty).await;
            let mut state_guard = state.write().await;
            state_guard.block_symbol(&signal.symbol);
            return Err(error);
        }
    };
    if let Err(error) = validate_order_filters(executed_qty, fill_price, &filters, false) {
        let _ = emergency_sell(&client, &signal.symbol, executed_qty).await;
        let mut state_guard = state.write().await;
        state_guard.block_symbol(&signal.symbol);
        return Err(error);
    }
    let protection =
        match protection_parameters(&signal.symbol, fill_price, executed_qty, &filters, config) {
            Ok(value) => value,
            Err(error) => {
                let _ = emergency_sell(&client, &signal.symbol, executed_qty).await;
                let mut state_guard = state.write().await;
                state_guard.block_symbol(&signal.symbol);
                return Err(error);
            }
        };

    let order_list = match client.place_order_list(protection.clone()).await {
        Ok(order_list) => order_list,
        Err(error) => {
            let status_unknown = matches!(&error, AppError::ExecutionStatusUnknown(_));
            if status_unknown {
                let mut state_guard = state.write().await;
                state_guard.block_symbol(&signal.symbol);
                tracing::error!(symbol = %signal.symbol, order_id = market_order.order_id, "CRITICAL: position protection status unknown; symbol blocked");
                return Err(error);
            }

            let emergency_result = emergency_sell(&client, &signal.symbol, executed_qty).await;
            let mut state_guard = state.write().await;
            if emergency_result.is_ok() {
                state_guard.release_symbol(&signal.symbol);
            } else {
                state_guard.block_symbol(&signal.symbol);
                tracing::error!(symbol = %signal.symbol, order_id = market_order.order_id, emergency_error = ?emergency_result.as_ref().err(), "CRITICAL: emergency sell failed; symbol blocked");
            }
            if let Err(emergency_error) = emergency_result {
                return Err(AppError::Other(anyhow::anyhow!(
                    "OCO placement failed: {error}; emergency sell failed: {emergency_error}"
                )));
            }
            return Err(error);
        }
    };

    let (stop_order_id, take_profit_order_id) = order_ids(&order_list);
    let position = Position {
        symbol: signal.symbol.clone(),
        entry_price: fill_price,
        quantity: executed_qty,
        entry_order_id: market_order.order_id,
        order_list_id: Some(order_list.order_list_id),
        stop_order_id,
        take_profit_order_id,
        opened_at: market_order.transact_time,
    };
    let mut state_guard = state.write().await;
    state_guard.add_position(position);
    let spent = market_order
        .cummulative_quote_qty
        .parse::<f64>()
        .unwrap_or(usdt_to_use);
    let remaining_balance = (state_guard.usdt_balance - spent).max(0.0);
    state_guard.update_usdt_balance(remaining_balance);
    tracing::info!(symbol = %signal.symbol, order_id = market_order.order_id, fill_price = %fill_price, quantity = %executed_qty, "Market order filled");
    tracing::info!(symbol = %signal.symbol, stop_price = %protection.below_stop_price.clone().unwrap_or_default(), take_profit_price = %protection.above_price.clone().unwrap_or_default(), "Order list placed");
    Ok(())
}

async fn open_paper_position(
    signal: MomentumSignal,
    state: Arc<RwLock<GlobalState>>,
    runtime: Arc<RwLock<RuntimeConfig>>,
) -> AppResult<()> {
    let rt = runtime.read().await.clone();
    let mut st = state.write().await;

    if st.paper_active_count() >= rt.max_positions {
        tracing::debug!(symbol = %signal.symbol, "Paper position limit reached; signal ignored");
        return Ok(());
    }
    if st.has_paper_symbol(&signal.symbol) {
        tracing::debug!(symbol = %signal.symbol, "Paper position already open; signal ignored");
        return Ok(());
    }
    let virtual_usdt = st.paper_balance * rt.position_size_pct / 100.0;
    if virtual_usdt > st.paper_balance || virtual_usdt <= 0.0 {
        tracing::debug!(symbol = %signal.symbol, "Insufficient paper balance; signal ignored");
        return Ok(());
    }
    let stop_price = signal.current_price * (1.0 - rt.stop_loss_pct / 100.0);
    let take_profit_price = signal.current_price * (1.0 + rt.take_profit_pct / 100.0);
    let quantity = virtual_usdt / signal.current_price;
    let pos = PaperPosition {
        symbol: signal.symbol.clone(),
        entry_price: signal.current_price,
        quantity,
        virtual_usdt,
        stop_price,
        take_profit_price,
        opened_at: now_ms(),
        stop_loss_pct: rt.stop_loss_pct,
        take_profit_pct: rt.take_profit_pct,
        momentum_trigger_pct: rt.momentum_trigger_pct,
        momentum_window_secs: rt.momentum_window_secs,
        volume_surge_multiplier: rt.volume_surge_multiplier,
    };
    tracing::info!(
        symbol = %signal.symbol,
        entry = %signal.current_price,
        stop = %stop_price,
        tp = %take_profit_price,
        virtual_usdt = %virtual_usdt,
        "Paper position opened"
    );
    st.open_paper_position(pos);
    Ok(())
}

async fn emergency_sell(client: &BinanceClient, symbol: &str, quantity: f64) -> AppResult<()> {
    tracing::error!(
        symbol,
        quantity,
        "CRITICAL: position opened without protection; attempting emergency market sell"
    );
    match client
        .place_market_order(symbol, Side::Sell, quantity)
        .await
    {
        Ok(order) if order.status == "FILLED" || order.status == "PARTIALLY_FILLED" => {
            tracing::error!(symbol, status = %order.status, "Emergency sell submitted");
            Ok(())
        }
        Ok(order) => Err(AppError::Other(anyhow::anyhow!(
            "emergency sell did not fill: {}",
            order.status
        ))),
        Err(error) => Err(error),
    }
}

fn protection_parameters(
    symbol: &str,
    fill_price: f64,
    quantity: f64,
    filters: &crate::binance::exchange_info::SymbolFilters,
    config: &AppConfig,
) -> AppResult<OrderListParams> {
    let tick_size = filters.price_filter.tick_size;
    // TODO FASE 2: trailing stop loss dinâmico
    let stop_price = apply_tick_size(
        fill_price * (1.0 - config.risk.stop_loss_pct / 100.0),
        tick_size,
    );
    // TODO FASE 2: take-profit parcial em múltiplas ordens usando take_profit_2_pct
    let take_profit_price = apply_tick_size(
        fill_price * (1.0 + config.risk.take_profit_1_pct / 100.0),
        tick_size,
    );
    let stop_limit_price = apply_tick_size(stop_price * 0.999, tick_size);
    validate_order_filters(quantity, stop_price, filters, false)?;
    validate_order_filters(quantity, take_profit_price, filters, false)?;
    validate_order_filters(quantity, stop_limit_price, filters, false)?;
    if !(stop_limit_price < stop_price && stop_price < fill_price && fill_price < take_profit_price)
    {
        return Err(AppError::FilterViolation(
            "OCO prices do not preserve stop < entry < take-profit ordering".to_owned(),
        ));
    }
    Ok(OrderListParams {
        symbol: symbol.to_owned(),
        side: Side::Sell,
        quantity: format_decimal(
            quantity,
            quantity_precision(filters.quantity_filter_for_market().step_size),
        ),
        above_type: "TAKE_PROFIT_LIMIT".to_owned(),
        below_type: "STOP_LOSS_LIMIT".to_owned(),
        above_price: Some(format_decimal(
            take_profit_price,
            price_precision(tick_size),
        )),
        above_stop_price: Some(format_decimal(
            take_profit_price,
            price_precision(tick_size),
        )),
        above_time_in_force: Some("GTC".to_owned()),
        below_price: Some(format_decimal(stop_limit_price, price_precision(tick_size))),
        below_stop_price: Some(format_decimal(stop_price, price_precision(tick_size))),
        below_time_in_force: Some("GTC".to_owned()),
        list_client_order_id: None,
        above_client_order_id: None,
        below_client_order_id: None,
    })
}

fn effective_fill_price(order: &OrderResponse) -> AppResult<f64> {
    let executed_qty = parse_positive(&order.executed_qty, "executedQty")?;
    let quote_qty = order
        .cummulative_quote_qty
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite() && *value > 0.0);
    if let Some(quote_qty) = quote_qty {
        return Ok(quote_qty / executed_qty);
    }
    let weighted = order
        .fills
        .iter()
        .filter_map(|fill| {
            let price = fill.price.parse::<f64>().ok()?;
            let qty = fill.qty.parse::<f64>().ok()?;
            Some((price, qty))
        })
        .fold((0.0, 0.0), |(quote, quantity), (price, qty)| {
            (quote + price * qty, quantity + qty)
        });
    if weighted.1 > 0.0 {
        return Ok(weighted.0 / weighted.1);
    }
    parse_positive(&order.price, "price")
}

fn parse_positive(value: &str, field: &str) -> AppResult<f64> {
    let parsed = value.parse::<f64>().map_err(|_| AppError::InvalidNumber {
        field: field.to_owned(),
        value: value.to_owned(),
    })?;
    if !parsed.is_finite() || parsed <= 0.0 {
        return Err(AppError::InvalidNumber {
            field: field.to_owned(),
            value: value.to_owned(),
        });
    }
    Ok(parsed)
}

fn order_ids(order_list: &crate::binance::models::OrderListResponse) -> (Option<u64>, Option<u64>) {
    let mut stop_order_id = None;
    let mut take_profit_order_id = None;
    for order in &order_list.order_reports {
        if order.order_type.starts_with("STOP_LOSS") {
            stop_order_id = Some(order.order_id);
        } else if order.order_type.starts_with("TAKE_PROFIT") {
            take_profit_order_id = Some(order.order_id);
        }
    }
    (stop_order_id, take_profit_order_id)
}

async fn handle_user_event(
    event: UserDataEvent,
    state: &Arc<RwLock<GlobalState>>,
) -> AppResult<()> {
    match event {
        UserDataEvent::ExecutionReport(report) => {
            if report.order_status == "FILLED" {
                let mut state_guard = state.write().await;
                let position = state_guard.open_positions.get(&report.symbol).cloned();
                if let Some(position) = position {
                    if report.order_id == position.entry_order_id {
                        return Ok(());
                    }
                    let exit_qty = report
                        .cumulative_filled_quantity
                        .parse::<f64>()
                        .ok()
                        .filter(|value| *value > 0.0)
                        .unwrap_or(position.quantity);
                    let exit_price = report
                        .last_executed_price
                        .parse::<f64>()
                        .ok()
                        .filter(|value| *value > 0.0)
                        .or_else(|| {
                            report
                                .cumulative_quote_quantity
                                .parse::<f64>()
                                .ok()
                                .filter(|value| *value > 0.0)
                                .map(|quote| quote / exit_qty)
                        })
                        .unwrap_or(position.entry_price);
                    let pnl_pct =
                        (exit_price - position.entry_price) / position.entry_price * 100.0;
                    state_guard.remove_position(&report.symbol);
                    tracing::info!(symbol = %report.symbol, entry = %position.entry_price, exit = %exit_price, pnl_pct = %pnl_pct, "Position closed");
                    // TODO FASE 2: notificações via Telegram ou email ao fechar posição
                }
            }
        }
        UserDataEvent::OutboundAccountPosition(update) => {
            let mut state_guard = state.write().await;
            state_guard.update_account_balances(&update.balances);
        }
        UserDataEvent::BalanceUpdate(_) | UserDataEvent::Unknown => {}
    }
    Ok(())
}

fn quantity_precision(step_size: f64) -> usize {
    precision(step_size)
}

fn price_precision(tick_size: f64) -> usize {
    precision(tick_size)
}

fn precision(value: f64) -> usize {
    format!("{value:.16}")
        .split('.')
        .nth(1)
        .map(|fraction| fraction.trim_end_matches('0').len())
        .unwrap_or(0)
}

fn format_decimal(value: f64, precision: usize) -> String {
    format!("{value:.precision$}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binance::exchange_info::{LotSizeFilter, PriceFilter, SymbolFilters};
    use crate::config::{ExchangeConfig, RiskConfig, ScannerConfig, ServerConfig};

    fn config() -> AppConfig {
        AppConfig {
            api_key: "key".into(),
            secret_key: "secret".into(),
            use_testnet: true,
            dry_run: false,
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
            dashboard_api_key: "test".into(),
        }
    }

    fn filters() -> SymbolFilters {
        SymbolFilters {
            lot_size: LotSizeFilter {
                min_qty: 0.001,
                max_qty: 1000.0,
                step_size: 0.001,
            },
            market_lot_size: None,
            price_filter: PriceFilter {
                min_price: 0.01,
                max_price: 1_000_000.0,
                tick_size: 0.01,
            },
            min_notional: Some(10.0),
            min_notional_apply_to_market: true,
            notional: None,
            percent_price: None,
            percent_price_by_side: None,
        }
    }

    #[test]
    fn builds_sell_oco_with_safe_price_ordering() {
        let params = protection_parameters("ABCUSDT", 100.0, 0.2, &filters(), &config())
            .expect("valid protection parameters");
        assert_eq!(params.symbol, "ABCUSDT");
        assert_eq!(params.side.as_str(), "SELL");
        assert_eq!(params.above_type, "TAKE_PROFIT_LIMIT");
        assert_eq!(params.below_type, "STOP_LOSS_LIMIT");
        assert_eq!(params.above_price.as_deref(), Some("103.00"));
        assert_eq!(params.below_stop_price.as_deref(), Some("98.50"));
        assert_eq!(params.below_price.as_deref(), Some("98.40"));
    }

    #[test]
    fn computes_effective_price_from_quote_quantity() {
        let order = OrderResponse {
            executed_qty: "2".into(),
            cummulative_quote_qty: "204".into(),
            price: "0".into(),
            ..OrderResponse::default()
        };
        assert_eq!(effective_fill_price(&order).unwrap(), 102.0);
    }
}
