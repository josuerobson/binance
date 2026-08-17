use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MiniTickerEvent {
    #[serde(rename = "e")]
    pub event_type: String,
    #[serde(rename = "E")]
    pub event_time_ms: i64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "c")]
    pub close_price: String,
    #[serde(rename = "o")]
    pub open_price: String,
    #[serde(rename = "h")]
    pub high_price: String,
    #[serde(rename = "l")]
    pub low_price: String,
    #[serde(rename = "v")]
    pub volume: String,
    #[serde(rename = "q")]
    pub quote_volume: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookTickerEvent {
    #[serde(rename = "u")]
    pub update_id: i64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "b")]
    pub bid_price: String,
    #[serde(rename = "B")]
    pub bid_quantity: String,
    #[serde(rename = "a")]
    pub ask_price: String,
    #[serde(rename = "A")]
    pub ask_quantity: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionReport {
    #[serde(rename = "e")]
    pub event_type: String,
    #[serde(rename = "E")]
    pub event_time_ms: i64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "c")]
    pub client_order_id: String,
    #[serde(rename = "S")]
    pub side: String,
    #[serde(rename = "o")]
    pub order_type: String,
    #[serde(rename = "f")]
    pub time_in_force: String,
    #[serde(rename = "q")]
    pub original_quantity: String,
    #[serde(rename = "p")]
    pub price: String,
    #[serde(rename = "P")]
    pub stop_price: String,
    #[serde(rename = "F")]
    pub iceberg_quantity: String,
    #[serde(rename = "g")]
    pub order_list_id: i64,
    #[serde(rename = "C")]
    pub original_client_order_id: String,
    #[serde(rename = "x")]
    pub execution_type: String,
    #[serde(rename = "X")]
    pub order_status: String,
    #[serde(rename = "r")]
    pub reject_reason: String,
    #[serde(rename = "i")]
    pub order_id: u64,
    #[serde(rename = "l")]
    pub last_executed_quantity: String,
    #[serde(rename = "z")]
    pub cumulative_filled_quantity: String,
    #[serde(rename = "L")]
    pub last_executed_price: String,
    #[serde(rename = "n")]
    pub commission: String,
    #[serde(rename = "N")]
    pub commission_asset: Option<String>,
    #[serde(rename = "T")]
    pub transaction_time_ms: i64,
    #[serde(rename = "t")]
    pub trade_id: i64,
    #[serde(rename = "w")]
    pub is_working: bool,
    #[serde(rename = "m")]
    pub is_maker: bool,
    #[serde(rename = "O")]
    pub order_creation_time_ms: i64,
    #[serde(rename = "Z")]
    pub cumulative_quote_quantity: String,
    #[serde(rename = "Y")]
    pub last_quote_quantity: String,
    #[serde(rename = "Q")]
    pub quote_order_quantity: String,
    #[serde(rename = "W")]
    pub working_time_ms: Option<i64>,
    #[serde(rename = "V")]
    pub self_trade_prevention_mode: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutboundAccountPosition {
    #[serde(rename = "e")]
    pub event_type: String,
    #[serde(rename = "E")]
    pub event_time_ms: i64,
    #[serde(rename = "u")]
    pub last_update_time_ms: i64,
    #[serde(rename = "B")]
    pub balances: Vec<AssetBalance>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BalanceUpdate {
    #[serde(rename = "a")]
    pub asset: String,
    #[serde(rename = "d")]
    pub balance_delta: String,
    #[serde(rename = "T")]
    pub clear_time_ms: i64,
}

#[derive(Debug, Clone)]
pub enum UserDataEvent {
    ExecutionReport(Box<ExecutionReport>),
    OutboundAccountPosition(OutboundAccountPosition),
    BalanceUpdate(BalanceUpdate),
    Unknown,
}

impl<'de> Deserialize<'de> for UserDataEvent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        let event_type = value.get("e").and_then(serde_json::Value::as_str);
        match event_type {
            Some("executionReport") => serde_json::from_value(value)
                .map(|event| Self::ExecutionReport(Box::new(event)))
                .map_err(serde::de::Error::custom),
            Some("outboundAccountPosition") => serde_json::from_value(value)
                .map(Self::OutboundAccountPosition)
                .map_err(serde::de::Error::custom),
            Some("balanceUpdate") => serde_json::from_value(value)
                .map(Self::BalanceUpdate)
                .map_err(serde::de::Error::custom),
            _ => Ok(Self::Unknown),
        }
    }
}

impl Serialize for UserDataEvent {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::ExecutionReport(event) => event.serialize(serializer),
            Self::OutboundAccountPosition(event) => event.serialize(serializer),
            Self::BalanceUpdate(event) => event.serialize(serializer),
            Self::Unknown => serializer.serialize_unit(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetBalance {
    #[serde(rename = "a", alias = "asset")]
    pub asset: String,
    #[serde(rename = "f", alias = "free")]
    pub free: String,
    #[serde(rename = "l", alias = "locked")]
    pub locked: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountInfo {
    #[serde(rename = "makerCommission", default)]
    pub maker_commission: u32,
    #[serde(rename = "takerCommission", default)]
    pub taker_commission: u32,
    #[serde(rename = "buyerCommission", default)]
    pub buyer_commission: u32,
    #[serde(rename = "sellerCommission", default)]
    pub seller_commission: u32,
    #[serde(rename = "canTrade")]
    pub can_trade: bool,
    #[serde(rename = "canWithdraw")]
    pub can_withdraw: bool,
    #[serde(rename = "canDeposit")]
    pub can_deposit: bool,
    #[serde(rename = "updateTime")]
    pub update_time: i64,
    #[serde(rename = "accountType")]
    pub account_type: String,
    pub balances: Vec<AssetBalance>,
    #[serde(default)]
    pub permissions: Vec<String>,
    #[serde(default)]
    pub brokered: bool,
    #[serde(rename = "requireSelfTradePrevention", default)]
    pub require_self_trade_prevention: bool,
    #[serde(default)]
    pub uid: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Fill {
    pub price: String,
    pub qty: String,
    pub commission: String,
    #[serde(rename = "commissionAsset")]
    pub commission_asset: String,
    #[serde(default)]
    pub trade_id: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct OrderResponse {
    pub symbol: String,
    #[serde(rename = "orderId")]
    pub order_id: u64,
    #[serde(rename = "orderListId")]
    pub order_list_id: i64,
    #[serde(rename = "clientOrderId")]
    pub client_order_id: String,
    #[serde(rename = "transactTime")]
    pub transact_time: i64,
    pub price: String,
    #[serde(rename = "origQty")]
    pub orig_qty: String,
    #[serde(rename = "executedQty")]
    pub executed_qty: String,
    #[serde(rename = "origQuoteOrderQty")]
    pub orig_quote_order_qty: String,
    #[serde(rename = "cummulativeQuoteQty")]
    pub cummulative_quote_qty: String,
    pub status: String,
    #[serde(rename = "timeInForce")]
    pub time_in_force: String,
    #[serde(rename = "type")]
    pub order_type: String,
    pub side: String,
    #[serde(rename = "workingTime", default)]
    pub working_time: Option<i64>,
    #[serde(rename = "selfTradePreventionMode", default)]
    pub self_trade_prevention_mode: Option<String>,
    #[serde(rename = "stopPrice", default)]
    pub stop_price: Option<String>,
    #[serde(rename = "icebergQty", default)]
    pub iceberg_qty: Option<String>,
    #[serde(default)]
    pub fills: Vec<Fill>,
}

pub type Order = OrderResponse;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct OrderListItem {
    pub symbol: String,
    #[serde(rename = "orderId")]
    pub order_id: u64,
    #[serde(rename = "clientOrderId")]
    pub client_order_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct OrderListResponse {
    #[serde(rename = "orderListId")]
    pub order_list_id: i64,
    #[serde(rename = "contingencyType")]
    pub contingency_type: String,
    #[serde(rename = "listStatusType")]
    pub list_status_type: String,
    #[serde(rename = "listOrderStatus")]
    pub list_order_status: String,
    #[serde(rename = "listClientOrderId")]
    pub list_client_order_id: String,
    #[serde(rename = "transactionTime")]
    pub transaction_time: i64,
    pub symbol: String,
    pub orders: Vec<OrderListItem>,
    #[serde(rename = "orderReports", default)]
    pub order_reports: Vec<OrderResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeInfoResponse {
    pub timezone: String,
    #[serde(rename = "serverTime")]
    pub server_time: i64,
    #[serde(rename = "rateLimits", default)]
    pub rate_limits: Vec<RateLimit>,
    #[serde(rename = "exchangeFilters", default)]
    pub exchange_filters: Vec<serde_json::Value>,
    pub symbols: Vec<ExchangeSymbolInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimit {
    #[serde(rename = "rateLimitType")]
    pub rate_limit_type: String,
    pub interval: String,
    #[serde(rename = "intervalNum")]
    pub interval_num: u32,
    pub limit: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeSymbolInfo {
    pub symbol: String,
    pub status: String,
    #[serde(rename = "baseAsset")]
    pub base_asset: String,
    #[serde(rename = "baseAssetPrecision")]
    pub base_asset_precision: u32,
    #[serde(rename = "quoteAsset")]
    pub quote_asset: String,
    #[serde(rename = "quotePrecision")]
    pub quote_precision: u32,
    #[serde(rename = "quoteAssetPrecision", default)]
    pub quote_asset_precision: Option<u32>,
    #[serde(rename = "isSpotTradingAllowed", default)]
    pub is_spot_trading_allowed: Option<bool>,
    #[serde(default)]
    pub permissions: Vec<String>,
    pub filters: Vec<SymbolFilter>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "filterType")]
pub enum SymbolFilter {
    #[serde(rename = "LOT_SIZE")]
    LotSize {
        #[serde(rename = "minQty")]
        min_qty: String,
        #[serde(rename = "maxQty")]
        max_qty: String,
        #[serde(rename = "stepSize")]
        step_size: String,
    },
    #[serde(rename = "MARKET_LOT_SIZE")]
    MarketLotSize {
        #[serde(rename = "minQty")]
        min_qty: String,
        #[serde(rename = "maxQty")]
        max_qty: String,
        #[serde(rename = "stepSize")]
        step_size: String,
    },
    #[serde(rename = "PRICE_FILTER")]
    PriceFilter {
        #[serde(rename = "minPrice")]
        min_price: String,
        #[serde(rename = "maxPrice")]
        max_price: String,
        #[serde(rename = "tickSize")]
        tick_size: String,
    },
    #[serde(rename = "MIN_NOTIONAL")]
    MinNotional {
        #[serde(rename = "minNotional")]
        min_notional: String,
        #[serde(rename = "applyToMarket")]
        apply_to_market: bool,
        #[serde(rename = "avgPriceMins")]
        avg_price_mins: u32,
    },
    #[serde(rename = "NOTIONAL")]
    Notional {
        #[serde(rename = "minNotional")]
        min_notional: String,
        #[serde(rename = "applyMinToMarket")]
        apply_min_to_market: bool,
        #[serde(rename = "maxNotional")]
        max_notional: String,
        #[serde(rename = "applyMaxToMarket")]
        apply_max_to_market: bool,
        #[serde(rename = "avgPriceMins")]
        avg_price_mins: u32,
    },
    #[serde(rename = "PERCENT_PRICE")]
    PercentPrice {
        #[serde(rename = "multiplierUp")]
        multiplier_up: String,
        #[serde(rename = "multiplierDown")]
        multiplier_down: String,
        #[serde(rename = "avgPriceMins")]
        avg_price_mins: u32,
    },
    #[serde(rename = "PERCENT_PRICE_BY_SIDE")]
    PercentPriceBySide {
        #[serde(rename = "bidMultiplierUp")]
        bid_multiplier_up: String,
        #[serde(rename = "bidMultiplierDown")]
        bid_multiplier_down: String,
        #[serde(rename = "askMultiplierUp")]
        ask_multiplier_up: String,
        #[serde(rename = "askMultiplierDown")]
        ask_multiplier_down: String,
        #[serde(rename = "avgPriceMins")]
        avg_price_mins: u32,
    },
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinanceError {
    pub code: i32,
    pub msg: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Side {
    #[serde(rename = "BUY")]
    Buy,
    #[serde(rename = "SELL")]
    Sell,
}

impl Side {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Buy => "BUY",
            Self::Sell => "SELL",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderListParams {
    pub symbol: String,
    pub side: Side,
    pub quantity: String,
    pub above_type: String,
    pub below_type: String,
    pub above_price: Option<String>,
    pub above_stop_price: Option<String>,
    pub above_time_in_force: Option<String>,
    pub below_price: Option<String>,
    pub below_stop_price: Option<String>,
    pub below_time_in_force: Option<String>,
    pub list_client_order_id: Option<String>,
    pub above_client_order_id: Option<String>,
    pub below_client_order_id: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_mini_ticker_strings() {
        let event: MiniTickerEvent = serde_json::from_str(
            r#"{"e":"24hrMiniTicker","E":1672515782136,"s":"BNBUSDT","c":"250.1","o":"245.0","h":"252.0","l":"240.0","v":"1000","q":"250000"}"#,
        )
        .expect("mini ticker must parse");
        assert_eq!(event.symbol, "BNBUSDT");
        assert_eq!(event.quote_volume, "250000");
    }

    #[test]
    fn parses_execution_report_by_event_type() {
        let event: UserDataEvent = serde_json::from_str(
            r#"{
                "e":"executionReport","E":1499405658658,"s":"ETHBTC","c":"mF9ehD8z","S":"BUY","o":"LIMIT","f":"GTC","q":"1.00000000","p":"0.10264410","P":"0.00000000","F":"0.00000000","g":-1,"C":"null","x":"NEW","X":"NEW","r":"NONE","i":4293153,"l":"0.00000000","z":"0.00000000","L":"0.00000000","n":"0","N":null,"T":1499405658657,"t":-1,"w":true,"m":false,"O":1499405658657,"Z":"0.00000000","Y":"0.00000000","Q":"0.00000000","V":"NONE"
            }"#,
        )
        .expect("execution report must parse");
        match event {
            UserDataEvent::ExecutionReport(report) => {
                assert_eq!(report.symbol, "ETHBTC");
                assert_eq!(report.order_status, "NEW");
            }
            _ => panic!("expected execution report"),
        }
    }
}
