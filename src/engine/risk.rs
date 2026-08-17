use crate::binance::exchange_info::SymbolFilters;
use crate::engine::state::GlobalState;
use crate::error::{AppError, AppResult};

pub fn check_position_limit(state: &GlobalState, max: usize) -> AppResult<()> {
    if state.active_positions_count() >= max {
        return Err(AppError::PositionLimitReached { max });
    }
    Ok(())
}

pub fn check_balance(state: &GlobalState, required_usdt: f64) -> AppResult<()> {
    ensure_finite_positive("required_usdt", required_usdt)?;
    if state.usdt_balance + f64::EPSILON < required_usdt {
        return Err(AppError::InsufficientBalance {
            required: required_usdt,
            available: state.usdt_balance,
        });
    }
    Ok(())
}

pub fn calculate_quantity(usdt_amount: f64, price: f64, filters: &SymbolFilters) -> AppResult<f64> {
    ensure_finite_positive("usdt_amount", usdt_amount)?;
    ensure_finite_positive("price", price)?;
    let quantity_filter = filters.quantity_filter_for_market();
    let raw_quantity = usdt_amount / price;
    let quantity = apply_step_size(raw_quantity, quantity_filter.step_size);
    validate_quantity(quantity, quantity_filter.min_qty, quantity_filter.max_qty)?;
    validate_min_notional(quantity, price, filters.minimum_notional(true))?;
    validate_max_notional(quantity, price, filters.maximum_notional(true))?;
    Ok(quantity)
}

pub fn apply_step_size(quantity: f64, step_size: f64) -> f64 {
    if !quantity.is_finite() || !step_size.is_finite() || quantity <= 0.0 || step_size <= 0.0 {
        return 0.0;
    }
    let precision = decimal_places(step_size);
    let factor = 10_f64.powi(precision as i32);
    let scaled_step = (step_size * factor).round();
    let scaled_quantity = (quantity * factor).floor();
    if scaled_step <= 0.0 || !scaled_step.is_finite() {
        return 0.0;
    }
    let units = (scaled_quantity / scaled_step).floor();
    round_decimal((units * scaled_step) / factor, precision)
}

pub fn apply_tick_size(price: f64, tick_size: f64) -> f64 {
    apply_step_size(price, tick_size)
}

pub fn validate_min_notional(
    quantity: f64,
    price: f64,
    min_notional: Option<f64>,
) -> AppResult<()> {
    if let Some(minimum) = min_notional {
        ensure_finite_non_negative("min_notional", minimum)?;
        let notional = quantity * price;
        if !notional.is_finite() || notional + f64::EPSILON < minimum {
            return Err(AppError::FilterViolation(format!(
                "notional {notional} is below minimum {minimum}"
            )));
        }
    }
    Ok(())
}

pub fn validate_max_notional(
    quantity: f64,
    price: f64,
    max_notional: Option<f64>,
) -> AppResult<()> {
    if let Some(maximum) = max_notional {
        ensure_finite_non_negative("max_notional", maximum)?;
        let notional = quantity * price;
        if !notional.is_finite() || notional > maximum + f64::EPSILON {
            return Err(AppError::FilterViolation(format!(
                "notional {notional} exceeds maximum {maximum}"
            )));
        }
    }
    Ok(())
}

pub fn validate_quantity(quantity: f64, min_qty: f64, max_qty: f64) -> AppResult<()> {
    ensure_finite_positive("quantity", quantity)?;
    ensure_finite_non_negative("min_qty", min_qty)?;
    ensure_finite_positive("max_qty", max_qty)?;
    if quantity + f64::EPSILON < min_qty {
        return Err(AppError::FilterViolation(format!(
            "quantity {quantity} is below minQty {min_qty}"
        )));
    }
    if quantity > max_qty + f64::EPSILON {
        return Err(AppError::FilterViolation(format!(
            "quantity {quantity} exceeds maxQty {max_qty}"
        )));
    }
    Ok(())
}

pub fn validate_price(price: f64, filters: &SymbolFilters) -> AppResult<()> {
    ensure_finite_positive("price", price)?;
    let filter = &filters.price_filter;
    if filter.min_price > 0.0 && price + f64::EPSILON < filter.min_price {
        return Err(AppError::FilterViolation(format!(
            "price {price} is below minPrice {}",
            filter.min_price
        )));
    }
    if filter.max_price > 0.0 && price > filter.max_price + f64::EPSILON {
        return Err(AppError::FilterViolation(format!(
            "price {price} exceeds maxPrice {}",
            filter.max_price
        )));
    }
    let normalized = apply_tick_size(price, filter.tick_size);
    if (normalized - price).abs() > tick_tolerance(filter.tick_size) {
        return Err(AppError::FilterViolation(format!(
            "price {price} is not aligned to tickSize {}",
            filter.tick_size
        )));
    }
    Ok(())
}

pub fn validate_order_filters(
    quantity: f64,
    price: f64,
    filters: &SymbolFilters,
    market_order: bool,
) -> AppResult<()> {
    let quantity_filter = if market_order {
        filters.quantity_filter_for_market()
    } else {
        &filters.lot_size
    };
    validate_quantity(quantity, quantity_filter.min_qty, quantity_filter.max_qty)?;
    validate_price(price, filters)?;
    validate_min_notional(quantity, price, filters.minimum_notional(market_order))?;
    validate_max_notional(quantity, price, filters.maximum_notional(market_order))?;
    Ok(())
}

fn decimal_places(value: f64) -> u32 {
    let text = format!("{value:.16}");
    text.split('.')
        .nth(1)
        .map(|fraction| fraction.trim_end_matches('0').len() as u32)
        .unwrap_or(0)
}

fn round_decimal(value: f64, precision: u32) -> f64 {
    let factor = 10_f64.powi(precision as i32);
    (value * factor).round() / factor
}

fn tick_tolerance(tick_size: f64) -> f64 {
    (tick_size.abs() * 1e-6).max(1e-12)
}

fn ensure_finite_positive(field: &str, value: f64) -> AppResult<()> {
    if !value.is_finite() || value <= 0.0 {
        return Err(AppError::InvalidNumber {
            field: field.to_owned(),
            value: value.to_string(),
        });
    }
    Ok(())
}

fn ensure_finite_non_negative(field: &str, value: f64) -> AppResult<()> {
    if !value.is_finite() || value < 0.0 {
        return Err(AppError::InvalidNumber {
            field: field.to_owned(),
            value: value.to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binance::exchange_info::{LotSizeFilter, PriceFilter};

    fn filters() -> SymbolFilters {
        SymbolFilters {
            lot_size: LotSizeFilter {
                min_qty: 0.001,
                max_qty: 100.0,
                step_size: 0.001,
            },
            market_lot_size: Some(LotSizeFilter {
                min_qty: 0.001,
                max_qty: 100.0,
                step_size: 0.001,
            }),
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
    fn applies_step_size_without_rounding_up() {
        assert_eq!(apply_step_size(0.123456, 0.001), 0.123);
        assert_eq!(apply_step_size(10.9999999, 1.0), 10.0);
        assert_eq!(apply_step_size(0.00012345, 0.00001), 0.00012);
    }

    #[test]
    fn calculates_quantity_and_rejects_min_notional() {
        let filters = filters();
        assert_eq!(calculate_quantity(100.0, 20.0, &filters).unwrap(), 5.0);
        assert!(calculate_quantity(1.0, 20.0, &filters).is_err());
    }

    #[test]
    fn validates_aligned_price() {
        let filters = filters();
        assert!(validate_price(100.01, &filters).is_ok());
        assert!(validate_price(100.015, &filters).is_err());
    }
}
