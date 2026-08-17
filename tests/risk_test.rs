use binance_momentum::binance::exchange_info::{LotSizeFilter, PriceFilter, SymbolFilters};
use binance_momentum::engine::risk::{apply_step_size, calculate_quantity};

fn filters() -> SymbolFilters {
    SymbolFilters {
        lot_size: LotSizeFilter {
            min_qty: 0.00001,
            max_qty: 1000.0,
            step_size: 0.00001,
        },
        market_lot_size: Some(LotSizeFilter {
            min_qty: 0.00001,
            max_qty: 1000.0,
            step_size: 0.00001,
        }),
        price_filter: PriceFilter {
            min_price: 0.000001,
            max_price: 1_000_000.0,
            tick_size: 0.000001,
        },
        min_notional: Some(10.0),
        min_notional_apply_to_market: true,
        notional: None,
        percent_price: None,
        percent_price_by_side: None,
    }
}

#[test]
fn handles_critical_floating_point_cases() {
    assert_eq!(apply_step_size(0.123456, 0.001), 0.123);
    assert_eq!(apply_step_size(10.9999999, 1.0), 10.0);
    assert_eq!(apply_step_size(0.00012345, 0.00001), 0.00012);
}

#[test]
fn calculates_quantity_conservatively_and_enforces_notional() {
    let symbol_filters = filters();
    assert_eq!(
        calculate_quantity(100.0, 20.0, &symbol_filters).unwrap(),
        5.0
    );
    assert!(calculate_quantity(1.0, 20.0, &symbol_filters).is_err());
}
