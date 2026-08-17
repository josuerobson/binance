use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("Binance API error {code}: {msg}")]
    BinanceApi { code: i32, msg: String },

    #[error("Binance returned HTTP {status}: {body}")]
    BinanceHttp { status: u16, body: String },

    #[error("request execution status is unknown after HTTP 5xx: {0}")]
    ExecutionStatusUnknown(String),

    #[error("insufficient balance: need {required} USDT, have {available}")]
    InsufficientBalance { required: f64, available: f64 },

    #[error("position limit reached: max {max} positions")]
    PositionLimitReached { max: usize },

    #[error("filter violation: {0}")]
    FilterViolation(String),

    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("invalid numeric value for {field}: {value}")]
    InvalidNumber { field: String, value: String },

    #[error("WebSocket disconnected: {0}")]
    WsDisconnected(String),

    #[error("rate limit exceeded; retry after {retry_after_secs:?} seconds")]
    RateLimitExceeded { retry_after_secs: Option<u64> },

    #[error("rate limit capacity is exhausted before request")]
    RateLimitCapacityExhausted,

    #[error("channel closed: {0}")]
    ChannelClosed(String),

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub type AppResult<T> = Result<T, AppError>;
