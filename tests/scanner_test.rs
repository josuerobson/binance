use binance_momentum::config::ScannerConfig;
use binance_momentum::engine::scanner::{PriceTick, Scanner};
use binance_momentum::engine::state::GlobalState;

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

fn tick(price: f64, volume: f64, timestamp_ms: i64) -> PriceTick {
    PriceTick {
        price,
        volume,
        volume_24h: 6_000_000.0,
        bid: price * 0.9995,
        ask: price * 1.0005,
        timestamp_ms,
    }
}

#[test]
fn emits_signal_for_three_percent_momentum_and_volume_surge() {
    let mut scanner = Scanner::new();
    let state = GlobalState::with_balance(1000.0);
    for index in 0..14 {
        assert!(scanner
            .process_tick(
                "ABCUSDT",
                tick(100.0, 1.0, index * 4000),
                &config(),
                5_000_000.0,
                &state
            )
            .is_none());
    }
    let signal = scanner.process_tick(
        "ABCUSDT",
        tick(103.0, 2.5, 56_000),
        &config(),
        5_000_000.0,
        &state,
    );
    assert!(signal.is_some());
}

#[test]
fn rejects_one_percent_momentum_and_stablecoin() {
    let mut scanner = Scanner::new();
    let state = GlobalState::with_balance(1000.0);
    for index in 0..15 {
        assert!(scanner
            .process_tick(
                "ABCUSDT",
                tick(100.0 + index as f64 * 0.07, 1.0, index * 4000),
                &config(),
                5_000_000.0,
                &state,
            )
            .is_none());
    }
    for index in 0..15 {
        assert!(scanner
            .process_tick(
                "USDCUSDT",
                tick(100.0 + index as f64 * 0.4, 2.5, index * 4000),
                &config(),
                5_000_000.0,
                &state,
            )
            .is_none());
    }
}
