use crate::binance::auth::{self, timestamp_ms_with_delta};
use crate::binance::models::{
    AccountInfo, BinanceError, ExchangeInfoResponse, Order, OrderListParams, OrderListResponse,
    OrderResponse, Side,
};
use crate::config::AppConfig;
use crate::error::{AppError, AppResult};
use reqwest::{header::HeaderMap, Client as HttpClient, RequestBuilder, Response, StatusCode};
use serde::de::DeserializeOwned;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

const MAX_NETWORK_RETRIES: usize = 3;

#[derive(Debug)]
struct RateLimitTracker {
    used_weight: u32,
    limit: u32,
    window_started: Instant,
    warned: bool,
}

impl RateLimitTracker {
    fn new(limit: u32) -> Self {
        Self {
            used_weight: 0,
            limit,
            window_started: Instant::now(),
            warned: false,
        }
    }

    fn reset_if_needed(&mut self) {
        if self.window_started.elapsed() >= Duration::from_secs(60) {
            self.used_weight = 0;
            self.warned = false;
            self.window_started = Instant::now();
        }
    }

    fn reserve(&mut self, weight: u32) -> AppResult<()> {
        self.reset_if_needed();
        if self.used_weight.saturating_add(weight) >= self.limit {
            tracing::error!(
                used_weight = self.used_weight,
                request_weight = weight,
                limit = self.limit,
                "REST request weight capacity exhausted"
            );
            return Err(AppError::RateLimitCapacityExhausted);
        }
        self.used_weight = self.used_weight.saturating_add(weight);
        self.warn_if_needed();
        Ok(())
    }

    fn update_from_headers(&mut self, headers: &HeaderMap) {
        for (name, value) in headers.iter() {
            let name = name.as_str().to_ascii_lowercase();
            if name.starts_with("x-mbx-used-weight-") {
                if let Ok(value) = value.to_str().unwrap_or_default().parse::<u32>() {
                    self.used_weight = value;
                    self.warn_if_needed();
                }
            }
        }
    }

    fn warn_if_needed(&mut self) {
        let threshold = (self.limit as f64 * 0.8).floor() as u32;
        if self.used_weight >= threshold && !self.warned {
            self.warned = true;
            tracing::warn!(
                used_weight = self.used_weight,
                limit = self.limit,
                "REST request weight reached 80 percent of configured limit"
            );
        }
    }
}

#[derive(Clone)]
pub struct BinanceClient {
    http: HttpClient,
    base_url: String,
    api_key: String,
    secret_key: String,
    recv_window_ms: u64,
    clock_delta_ms: Arc<std::sync::atomic::AtomicI64>,
    rate_limit: Arc<Mutex<RateLimitTracker>>,
}

impl std::fmt::Debug for BinanceClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BinanceClient")
            .field("base_url", &self.base_url)
            .field("api_key", &"<redacted>")
            .field("secret_key", &"<redacted>")
            .field("recv_window_ms", &self.recv_window_ms)
            .finish_non_exhaustive()
    }
}

impl BinanceClient {
    pub fn new(config: &AppConfig) -> AppResult<Self> {
        let http = HttpClient::builder()
            .timeout(Duration::from_secs(config.exchange.rest_timeout_secs))
            .user_agent("binance-momentum/0.1.0")
            .build()?;
        Ok(Self {
            http,
            base_url: config.rest_base_url.trim_end_matches('/').to_owned(),
            api_key: config.api_key.clone(),
            secret_key: config.secret_key.clone(),
            recv_window_ms: config.exchange.recv_window_ms,
            clock_delta_ms: Arc::new(std::sync::atomic::AtomicI64::new(0)),
            rate_limit: Arc::new(Mutex::new(RateLimitTracker::new(
                config.exchange.request_weight_limit_per_minute,
            ))),
        })
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub fn set_clock_delta_ms(&self, delta_ms: i64) {
        self.clock_delta_ms
            .store(delta_ms, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn clock_delta_ms(&self) -> i64 {
        self.clock_delta_ms
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub async fn ping(&self) -> AppResult<()> {
        let response = self
            .send_with_retry(|| self.http.get(self.endpoint("/api/v3/ping")), 1, true)
            .await?;
        self.decode_empty(response, false).await
    }

    pub async fn get_server_time(&self) -> AppResult<i64> {
        let response = self
            .send_with_retry(|| self.http.get(self.endpoint("/api/v3/time")), 1, true)
            .await?;
        let payload: ServerTimeResponse = self.decode_json(response, false).await?;
        Ok(payload.server_time)
    }

    pub async fn get_account_info(&self) -> AppResult<AccountInfo> {
        let query = self.signed_query(&[]);
        let response = self
            .send_with_retry(
                || {
                    self.authenticated(
                        self.http
                            .get(format!("{}?{}", self.endpoint("/api/v3/account"), query)),
                        "",
                    )
                },
                20,
                true,
            )
            .await?;
        self.decode_json(response, false).await
    }

    pub async fn get_exchange_info(&self, symbols: &[&str]) -> AppResult<ExchangeInfoResponse> {
        let query = if symbols.is_empty() {
            String::new()
        } else {
            let json = serde_json::to_string(&symbols)?;
            let encoded = url::form_urlencoded::Serializer::new(String::new())
                .append_pair("symbols", &json)
                .finish();
            format!("?{encoded}")
        };
        let response = self
            .send_with_retry(
                || {
                    self.http.get(format!(
                        "{}{}",
                        self.endpoint("/api/v3/exchangeInfo"),
                        query
                    ))
                },
                20,
                true,
            )
            .await?;
        self.decode_json(response, false).await
    }

    pub async fn get_listen_key(&self) -> AppResult<String> {
        let response = self
            .send_with_retry(
                || self.authenticated(self.http.post(self.endpoint("/api/v3/userDataStream")), ""),
                1,
                true,
            )
            .await?;
        let payload: ListenKeyResponse = self.decode_json(response, false).await?;
        Ok(payload.listen_key)
    }

    pub async fn keepalive_listen_key(&self, key: &str) -> AppResult<()> {
        let body = form_encoded(&[("listenKey", key)]);
        let response = self
            .send_with_retry(
                || {
                    self.authenticated(
                        self.http
                            .put(self.endpoint("/api/v3/userDataStream"))
                            .header(
                                reqwest::header::CONTENT_TYPE,
                                "application/x-www-form-urlencoded",
                            )
                            .body(body.clone()),
                        "",
                    )
                },
                1,
                true,
            )
            .await?;
        self.decode_empty(response, false).await
    }

    pub async fn place_market_order(
        &self,
        symbol: &str,
        side: Side,
        quantity: f64,
    ) -> AppResult<OrderResponse> {
        ensure_finite_positive("quantity", quantity)?;
        let params = [
            ("symbol", symbol.to_owned()),
            ("side", side.as_str().to_owned()),
            ("type", "MARKET".to_owned()),
            ("quantity", quantity.to_string()),
            ("newOrderRespType", "FULL".to_owned()),
        ];
        let query = self.signed_query(
            &params
                .iter()
                .map(|(key, value)| (*key, value.as_str()))
                .collect::<Vec<_>>(),
        );
        let response = self
            .send_with_retry(
                || {
                    self.authenticated(
                        self.http
                            .post(self.endpoint("/api/v3/order"))
                            .header(
                                reqwest::header::CONTENT_TYPE,
                                "application/x-www-form-urlencoded",
                            )
                            .body(query.clone()),
                        "",
                    )
                },
                1,
                false,
            )
            .await?;
        self.decode_json(response, true).await
    }

    pub async fn place_order_list(&self, params: OrderListParams) -> AppResult<OrderListResponse> {
        let mut values = vec![
            ("symbol", params.symbol),
            ("side", params.side.as_str().to_owned()),
            ("quantity", params.quantity),
            ("aboveType", params.above_type),
            ("belowType", params.below_type),
        ];
        push_optional(&mut values, "abovePrice", params.above_price);
        push_optional(&mut values, "aboveStopPrice", params.above_stop_price);
        push_optional(&mut values, "aboveTimeInForce", params.above_time_in_force);
        push_optional(&mut values, "belowPrice", params.below_price);
        push_optional(&mut values, "belowStopPrice", params.below_stop_price);
        push_optional(&mut values, "belowTimeInForce", params.below_time_in_force);
        push_optional(
            &mut values,
            "listClientOrderId",
            params.list_client_order_id,
        );
        push_optional(
            &mut values,
            "aboveClientOrderId",
            params.above_client_order_id,
        );
        push_optional(
            &mut values,
            "belowClientOrderId",
            params.below_client_order_id,
        );

        let refs = values
            .iter()
            .map(|(key, value)| (*key, value.as_str()))
            .collect::<Vec<_>>();
        let query = self.signed_query(&refs);
        let response = self
            .send_with_retry(
                || {
                    self.authenticated(
                        self.http
                            .post(self.endpoint("/api/v3/orderList/oco"))
                            .header(
                                reqwest::header::CONTENT_TYPE,
                                "application/x-www-form-urlencoded",
                            )
                            .body(query.clone()),
                        "",
                    )
                },
                1,
                false,
            )
            .await?;
        self.decode_json(response, true).await
    }

    pub async fn cancel_order(&self, symbol: &str, order_id: u64) -> AppResult<()> {
        let params = [
            ("symbol", symbol.to_owned()),
            ("orderId", order_id.to_string()),
        ];
        let query = self.signed_query(
            &params
                .iter()
                .map(|(key, value)| (*key, value.as_str()))
                .collect::<Vec<_>>(),
        );
        let response = self
            .send_with_retry(
                || {
                    self.authenticated(
                        self.http.delete(format!(
                            "{}?{}",
                            self.endpoint("/api/v3/order"),
                            query.clone()
                        )),
                        "",
                    )
                },
                1,
                false,
            )
            .await?;
        self.decode_empty(response, true).await
    }

    pub async fn get_all_open_orders(&self) -> AppResult<Vec<Order>> {
        let query = self.signed_query(&[]);
        let response = self
            .send_with_retry(
                || {
                    self.authenticated(
                        self.http
                            .get(format!("{}?{}", self.endpoint("/api/v3/openOrders"), query)),
                        "",
                    )
                },
                40,
                true,
            )
            .await?;
        self.decode_json(response, false).await
    }

    pub async fn get_open_orders(&self, symbol: &str) -> AppResult<Vec<Order>> {
        let params = [("symbol", symbol.to_owned())];
        let query = self.signed_query(
            &params
                .iter()
                .map(|(key, value)| (*key, value.as_str()))
                .collect::<Vec<_>>(),
        );
        let response = self
            .send_with_retry(
                || {
                    self.authenticated(
                        self.http
                            .get(format!("{}?{}", self.endpoint("/api/v3/openOrders"), query)),
                        "",
                    )
                },
                6,
                true,
            )
            .await?;
        self.decode_json(response, false).await
    }

    fn endpoint(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    fn signed_query(&self, params: &[(&str, &str)]) -> String {
        let mut owned = params
            .iter()
            .map(|(key, value)| (*key, *value))
            .collect::<Vec<_>>();
        let timestamp = timestamp_ms_with_delta(self.clock_delta_ms()).to_string();
        let recv_window = self.recv_window_ms.to_string();
        owned.push(("recvWindow", recv_window.as_str()));
        owned.push(("timestamp", timestamp.as_str()));
        auth::build_signed_query(&owned, &self.secret_key)
    }

    fn authenticated(
        &self,
        request: RequestBuilder,
        encoded_query_or_body: &str,
    ) -> RequestBuilder {
        let request = request.header("X-MBX-APIKEY", &self.api_key);
        if encoded_query_or_body.is_empty() {
            request
        } else {
            request.body(encoded_query_or_body.to_owned())
        }
    }

    async fn send_with_retry<F>(
        &self,
        build: F,
        weight: u32,
        retry_network_errors: bool,
    ) -> AppResult<Response>
    where
        F: Fn() -> RequestBuilder,
    {
        {
            let mut tracker = self.rate_limit.lock().await;
            tracker.reserve(weight)?;
        }

        let mut attempt = 0;
        loop {
            match build().send().await {
                Ok(response) => {
                    let mut tracker = self.rate_limit.lock().await;
                    tracker.update_from_headers(response.headers());
                    return Ok(response);
                }
                Err(error) if retry_network_errors && attempt < MAX_NETWORK_RETRIES => {
                    attempt += 1;
                    let delay_ms = 100_u64.saturating_mul(2_u64.saturating_pow(attempt as u32 - 1));
                    tracing::warn!(attempt, delay_ms, error = %error, "REST network error, retrying");
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                }
                Err(error) => return Err(AppError::Http(error)),
            }
        }
    }

    async fn decode_json<T: DeserializeOwned>(
        &self,
        response: Response,
        order_operation: bool,
    ) -> AppResult<T> {
        let status = response.status();
        let retry_after = parse_retry_after(response.headers());
        let body = response.text().await?;
        if status == StatusCode::TOO_MANY_REQUESTS || status == StatusCode::IM_A_TEAPOT {
            return Err(AppError::RateLimitExceeded {
                retry_after_secs: retry_after,
            });
        }
        if status.is_server_error() && order_operation {
            return Err(AppError::ExecutionStatusUnknown(body));
        }
        if !status.is_success() {
            if let Ok(error) = serde_json::from_str::<BinanceError>(&body) {
                return Err(AppError::BinanceApi {
                    code: error.code,
                    msg: error.msg,
                });
            }
            return Err(AppError::BinanceHttp {
                status: status.as_u16(),
                body,
            });
        }
        serde_json::from_str(&body).map_err(AppError::Json)
    }

    async fn decode_empty(&self, response: Response, order_operation: bool) -> AppResult<()> {
        let status = response.status();
        let retry_after = parse_retry_after(response.headers());
        let body = response.text().await?;
        if status == StatusCode::TOO_MANY_REQUESTS || status == StatusCode::IM_A_TEAPOT {
            return Err(AppError::RateLimitExceeded {
                retry_after_secs: retry_after,
            });
        }
        if status.is_server_error() && order_operation {
            return Err(AppError::ExecutionStatusUnknown(body));
        }
        if !status.is_success() {
            if let Ok(error) = serde_json::from_str::<BinanceError>(&body) {
                return Err(AppError::BinanceApi {
                    code: error.code,
                    msg: error.msg,
                });
            }
            return Err(AppError::BinanceHttp {
                status: status.as_u16(),
                body,
            });
        }
        Ok(())
    }
}

#[derive(Debug, serde::Deserialize)]
struct ServerTimeResponse {
    #[serde(rename = "serverTime")]
    server_time: i64,
}

#[derive(Debug, serde::Deserialize)]
struct ListenKeyResponse {
    #[serde(rename = "listenKey")]
    listen_key: String,
}

fn push_optional(
    values: &mut Vec<(&'static str, String)>,
    key: &'static str,
    value: Option<String>,
) {
    if let Some(value) = value {
        values.push((key, value));
    }
}

fn form_encoded(values: &[(&str, &str)]) -> String {
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    for (key, value) in values {
        serializer.append_pair(key, value);
    }
    serializer.finish()
}

fn parse_retry_after(headers: &HeaderMap) -> Option<u64> {
    headers
        .get("retry-after")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
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
