use crate::error::{AppError, AppResult};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::future::Future;
use std::time::{SystemTime, UNIX_EPOCH};
use url::form_urlencoded::Serializer;

pub type HmacSha256 = Hmac<Sha256>;

pub fn sign(secret: &str, query_string: &str) -> String {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .expect("HMAC-SHA256 accepts keys of every length");
    mac.update(query_string.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

pub fn timestamp_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after UNIX epoch")
        .as_millis() as i64
}

pub fn timestamp_ms_with_delta(clock_delta_ms: i64) -> i64 {
    timestamp_ms().saturating_add(clock_delta_ms)
}

pub fn encode_params(params: &[(&str, &str)]) -> String {
    let mut serializer = Serializer::new(String::new());
    for (key, value) in params {
        serializer.append_pair(key, value);
    }
    serializer.finish()
}

pub fn build_signed_query(params: &[(&str, &str)], secret: &str) -> String {
    let query = encode_params(params);
    let signature = sign(secret, &query);
    format!("{query}&signature={signature}")
}

pub async fn sync_server_time<F, Fut>(get_server_time: F) -> AppResult<i64>
where
    F: Fn() -> Fut,
    Fut: Future<Output = AppResult<i64>>,
{
    let local_before = timestamp_ms();
    let server_time = get_server_time().await?;
    let local_after = timestamp_ms();
    let midpoint = local_before.saturating_add((local_after - local_before) / 2);
    Ok(server_time.saturating_sub(midpoint))
}

pub fn parse_timestamp(value: &str) -> AppResult<i64> {
    value.parse::<i64>().map_err(|_| AppError::InvalidNumber {
        field: "timestamp".to_owned(),
        value: value.to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signs_official_binance_vector() {
        // The prompt contained an inconsistent secret; this key is the one paired with the
        // expected signature in Binance's current official example.
        let secret = concat!(
            "NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1e3",
            "UZjInClVN65XAbvqqM6A7H5fATj0j"
        );
        let payload = "symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1&recvWindow=5000&timestamp=1499827319559";
        let expected = "c8db56825ae71d6d79447849e617115f4a920fa2acdcab2b053c4b2838bd6b71";
        assert_eq!(sign(secret, payload), expected);
    }

    #[test]
    fn preserves_parameter_order_and_adds_signature() {
        let query = build_signed_query(&[("symbol", "BTCUSDT"), ("side", "BUY")], "secret");
        assert!(query.starts_with("symbol=BTCUSDT&side=BUY&signature="));
    }
}
