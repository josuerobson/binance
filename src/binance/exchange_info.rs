use crate::binance::client::BinanceClient;
use crate::binance::models::{ExchangeInfoResponse, SymbolFilter};
use crate::error::{AppError, AppResult};
use dashmap::DashMap;
use std::sync::atomic::{AtomicI64, Ordering};

#[derive(Debug, Clone, PartialEq)]
pub struct LotSizeFilter {
    pub min_qty: f64,
    pub max_qty: f64,
    pub step_size: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PriceFilter {
    pub min_price: f64,
    pub max_price: f64,
    pub tick_size: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NotionalFilter {
    pub min_notional: f64,
    pub max_notional: Option<f64>,
    pub apply_min_to_market: bool,
    pub apply_max_to_market: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PercentPriceFilter {
    pub multiplier_up: f64,
    pub multiplier_down: f64,
    pub avg_price_mins: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PercentPriceBySideFilter {
    pub bid_multiplier_up: f64,
    pub bid_multiplier_down: f64,
    pub ask_multiplier_up: f64,
    pub ask_multiplier_down: f64,
    pub avg_price_mins: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SymbolFilters {
    pub lot_size: LotSizeFilter,
    pub market_lot_size: Option<LotSizeFilter>,
    pub price_filter: PriceFilter,
    pub min_notional: Option<f64>,
    pub min_notional_apply_to_market: bool,
    pub notional: Option<NotionalFilter>,
    pub percent_price: Option<PercentPriceFilter>,
    pub percent_price_by_side: Option<PercentPriceBySideFilter>,
}

impl SymbolFilters {
    pub fn quantity_filter_for_market(&self) -> &LotSizeFilter {
        self.market_lot_size.as_ref().unwrap_or(&self.lot_size)
    }

    pub fn minimum_notional(&self, market_order: bool) -> Option<f64> {
        let min_notional = self
            .notional
            .as_ref()
            .filter(|filter| !market_order || filter.apply_min_to_market)
            .map(|filter| filter.min_notional);
        min_notional.or_else(|| {
            self.min_notional
                .filter(|_| !market_order || self.min_notional_apply_to_market)
        })
    }

    pub fn maximum_notional(&self, market_order: bool) -> Option<f64> {
        self.notional
            .as_ref()
            .filter(|filter| !market_order || filter.apply_max_to_market)
            .and_then(|filter| filter.max_notional)
    }
}

#[derive(Debug)]
pub struct ExchangeInfoCache {
    filters: DashMap<String, SymbolFilters>,
    last_refresh_ms: AtomicI64,
}

impl Default for ExchangeInfoCache {
    fn default() -> Self {
        Self::new()
    }
}

impl ExchangeInfoCache {
    pub fn new() -> Self {
        Self {
            filters: DashMap::new(),
            last_refresh_ms: AtomicI64::new(0),
        }
    }

    pub fn get(&self, symbol: &str) -> Option<SymbolFilters> {
        self.filters.get(symbol).map(|entry| entry.value().clone())
    }

    pub fn len(&self) -> usize {
        self.filters.len()
    }

    pub fn symbols(&self) -> Vec<String> {
        self.filters
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    pub fn is_empty(&self) -> bool {
        self.filters.is_empty()
    }

    pub fn last_refresh_ms(&self) -> i64 {
        self.last_refresh_ms.load(Ordering::Relaxed)
    }

    pub async fn refresh(&self, client: &BinanceClient) -> AppResult<()> {
        let response = client.get_exchange_info(&[]).await?;
        self.replace_from_response(response)
    }

    pub fn replace_from_response(&self, response: ExchangeInfoResponse) -> AppResult<()> {
        let mut parsed = Vec::new();
        for symbol in response.symbols {
            if symbol.status != "TRADING" || symbol.quote_asset != "USDT" {
                continue;
            }
            if let Some(filters) = parse_symbol_filters(&symbol.filters, &symbol.symbol)? {
                parsed.push((symbol.symbol, filters));
            }
        }
        self.filters.clear();
        for (symbol, filters) in parsed {
            self.filters.insert(symbol, filters);
        }
        self.last_refresh_ms
            .store(response.server_time, Ordering::Relaxed);
        tracing::info!(
            symbols = self.filters.len(),
            server_time = response.server_time,
            "Exchange filters refreshed"
        );
        Ok(())
    }
}

fn parse_symbol_filters(
    filters: &[SymbolFilter],
    symbol: &str,
) -> AppResult<Option<SymbolFilters>> {
    let mut lot_size = None;
    let mut market_lot_size = None;
    let mut price_filter = None;
    let mut min_notional = None;
    let mut min_notional_apply_to_market = false;
    let mut notional = None;
    let mut percent_price = None;
    let mut percent_price_by_side = None;

    for filter in filters {
        match filter {
            SymbolFilter::LotSize {
                min_qty,
                max_qty,
                step_size,
            } => {
                lot_size = Some(LotSizeFilter {
                    min_qty: decimal(min_qty, symbol, "minQty")?,
                    max_qty: decimal(max_qty, symbol, "maxQty")?,
                    step_size: decimal(step_size, symbol, "stepSize")?,
                });
            }
            SymbolFilter::MarketLotSize {
                min_qty,
                max_qty,
                step_size,
            } => {
                market_lot_size = Some(LotSizeFilter {
                    min_qty: decimal(min_qty, symbol, "marketMinQty")?,
                    max_qty: decimal(max_qty, symbol, "marketMaxQty")?,
                    step_size: decimal(step_size, symbol, "marketStepSize")?,
                });
            }
            SymbolFilter::PriceFilter {
                min_price,
                max_price,
                tick_size,
            } => {
                price_filter = Some(PriceFilter {
                    min_price: decimal(min_price, symbol, "minPrice")?,
                    max_price: decimal(max_price, symbol, "maxPrice")?,
                    tick_size: decimal(tick_size, symbol, "tickSize")?,
                });
            }
            SymbolFilter::MinNotional {
                min_notional: value,
                apply_to_market,
                ..
            } => {
                min_notional = Some(decimal(value, symbol, "minNotional")?);
                min_notional_apply_to_market = *apply_to_market;
            }
            SymbolFilter::Notional {
                min_notional: min,
                apply_min_to_market,
                max_notional: max,
                apply_max_to_market,
                ..
            } => {
                notional = Some(NotionalFilter {
                    min_notional: decimal(min, symbol, "notionalMin")?,
                    max_notional: Some(decimal(max, symbol, "notionalMax")?),
                    apply_min_to_market: *apply_min_to_market,
                    apply_max_to_market: *apply_max_to_market,
                });
            }
            SymbolFilter::PercentPrice {
                multiplier_up,
                multiplier_down,
                avg_price_mins,
            } => {
                percent_price = Some(PercentPriceFilter {
                    multiplier_up: decimal(multiplier_up, symbol, "multiplierUp")?,
                    multiplier_down: decimal(multiplier_down, symbol, "multiplierDown")?,
                    avg_price_mins: *avg_price_mins,
                });
            }
            SymbolFilter::PercentPriceBySide {
                bid_multiplier_up,
                bid_multiplier_down,
                ask_multiplier_up,
                ask_multiplier_down,
                avg_price_mins,
            } => {
                percent_price_by_side = Some(PercentPriceBySideFilter {
                    bid_multiplier_up: decimal(bid_multiplier_up, symbol, "bidMultiplierUp")?,
                    bid_multiplier_down: decimal(bid_multiplier_down, symbol, "bidMultiplierDown")?,
                    ask_multiplier_up: decimal(ask_multiplier_up, symbol, "askMultiplierUp")?,
                    ask_multiplier_down: decimal(ask_multiplier_down, symbol, "askMultiplierDown")?,
                    avg_price_mins: *avg_price_mins,
                });
            }
            SymbolFilter::Unknown => {}
        }
    }

    match (lot_size, price_filter) {
        (Some(lot_size), Some(price_filter)) => Ok(Some(SymbolFilters {
            lot_size,
            market_lot_size,
            price_filter,
            min_notional,
            min_notional_apply_to_market,
            notional,
            percent_price,
            percent_price_by_side,
        })),
        _ => {
            tracing::warn!(
                symbol,
                "Skipping symbol without required LOT_SIZE or PRICE_FILTER"
            );
            Ok(None)
        }
    }
}

fn decimal(value: &str, symbol: &str, field: &str) -> AppResult<f64> {
    let parsed = value.parse::<f64>().map_err(|_| {
        AppError::FilterViolation(format!("{symbol}: invalid {field} value {value}"))
    })?;
    if !parsed.is_finite() || parsed < 0.0 {
        return Err(AppError::FilterViolation(format!(
            "{symbol}: invalid non-finite or negative {field} value {value}"
        )));
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_caches_required_symbol_filters() {
        let response: ExchangeInfoResponse = serde_json::from_str(
            r#"{
                "timezone":"UTC","serverTime":123,
                "rateLimits":[],"exchangeFilters":[],
                "symbols":[{
                    "symbol":"BTCUSDT","status":"TRADING","baseAsset":"BTC",
                    "baseAssetPrecision":8,"quoteAsset":"USDT","quotePrecision":8,
                    "quoteAssetPrecision":8,"isSpotTradingAllowed":true,"permissions":["SPOT"],
                    "filters":[
                        {"filterType":"LOT_SIZE","minQty":"0.00001000","maxQty":"100.00000000","stepSize":"0.00001000"},
                        {"filterType":"MARKET_LOT_SIZE","minQty":"0.00001000","maxQty":"50.00000000","stepSize":"0.00001000"},
                        {"filterType":"PRICE_FILTER","minPrice":"0.01000000","maxPrice":"1000000.00000000","tickSize":"0.01000000"},
                        {"filterType":"NOTIONAL","minNotional":"10.00000000","applyMinToMarket":true,"maxNotional":"100000.00000000","applyMaxToMarket":false,"avgPriceMins":5}
                    ]
                }]
            }"#,
        )
        .expect("exchange info must parse");
        let cache = ExchangeInfoCache::new();
        cache
            .replace_from_response(response)
            .expect("filters must cache");
        let filters = cache.get("BTCUSDT").expect("BTCUSDT must be cached");
        assert_eq!(filters.quantity_filter_for_market().max_qty, 50.0);
        assert_eq!(filters.minimum_notional(true), Some(10.0));
        assert_eq!(filters.price_filter.tick_size, 0.01);
        assert_eq!(cache.last_refresh_ms(), 123);
    }
}
