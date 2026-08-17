use crate::binance::models::{BookTickerEvent, MiniTickerEvent, UserDataEvent};
use crate::error::{AppError, AppResult};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio_retry::strategy::ExponentialBackoff;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use url::Url;

const MAX_RECONNECT_ATTEMPTS: usize = 10;

pub async fn run_market_stream(
    base_ws_url: &str,
    tx: broadcast::Sender<Vec<MiniTickerEvent>>,
) -> AppResult<()> {
    let url = stream_url(base_ws_url, "!miniTicker@arr")?;
    let mut delays = ExponentialBackoff::from_millis(100)
        .max_delay(Duration::from_secs(30))
        .take(MAX_RECONNECT_ATTEMPTS);
    let mut attempt = 0usize;

    loop {
        match connect_async(url.as_str()).await {
            Ok((mut socket, _response)) => {
                attempt = 0;
                tracing::info!(stream = "market_mini_ticker", url = %url, "WebSocket connected");
                while let Some(message) = socket.next().await {
                    match message {
                        Ok(Message::Text(text)) => {
                            match serde_json::from_str::<Vec<MiniTickerEvent>>(&text) {
                                Ok(events) => {
                                    let _ = tx.send(events);
                                }
                                Err(error) => {
                                    tracing::warn!(stream = "market_mini_ticker", error = %error, "Ignoring malformed market message")
                                }
                            }
                        }
                        Ok(Message::Ping(payload)) => {
                            socket
                                .send(Message::Pong(payload))
                                .await
                                .map_err(ws_error)?;
                        }
                        Ok(Message::Close(frame)) => {
                            tracing::warn!(stream = "market_mini_ticker", reason = ?frame, "WebSocket closed by peer");
                            break;
                        }
                        Ok(Message::Binary(bytes)) => {
                            if let Ok(text) = String::from_utf8(bytes.to_vec()) {
                                if let Ok(events) =
                                    serde_json::from_str::<Vec<MiniTickerEvent>>(&text)
                                {
                                    let _ = tx.send(events);
                                }
                            }
                        }
                        Ok(Message::Pong(_)) => {}
                        Ok(Message::Frame(_)) => {}
                        Err(error) => {
                            tracing::warn!(stream = "market_mini_ticker", error = %error, "WebSocket read error");
                            break;
                        }
                    }
                }
            }
            Err(error) => {
                attempt = attempt.saturating_add(1);
                tracing::warn!(stream = "market_mini_ticker", attempt, error = %error, "WebSocket connection failed");
            }
        }

        attempt = attempt.saturating_add(1);
        let delay = delays.next().ok_or_else(|| {
            AppError::WsDisconnected("market mini ticker exhausted reconnect attempts".to_owned())
        })?;
        tracing::warn!(
            stream = "market_mini_ticker",
            attempt,
            delay_ms = delay.as_millis(),
            "WebSocket disconnected, reconnecting"
        );
        tokio::time::sleep(delay).await;
    }
}

pub async fn run_book_ticker_stream(
    base_ws_url: &str,
    symbols: Vec<String>,
    tx: broadcast::Sender<Vec<BookTickerEvent>>,
) -> AppResult<()> {
    if symbols.is_empty() {
        return Err(AppError::InvalidConfig(
            "book ticker stream requires symbols".to_owned(),
        ));
    }
    let url = stream_base_url(base_ws_url)?;
    let params = symbols
        .iter()
        .map(|symbol| format!("{}@bookTicker", symbol.to_ascii_lowercase()))
        .collect::<Vec<_>>();
    let subscription = serde_json::json!({
        "method": "SUBSCRIBE",
        "params": params,
        "id": 1
    });

    let mut delays = ExponentialBackoff::from_millis(100)
        .max_delay(Duration::from_secs(30))
        .take(MAX_RECONNECT_ATTEMPTS);
    let mut attempt = 0usize;
    loop {
        match connect_async(url.as_str()).await {
            Ok((mut socket, _response)) => {
                attempt = 0;
                socket
                    .send(Message::Text(subscription.to_string()))
                    .await
                    .map_err(ws_error)?;
                tracing::info!(
                    stream = "book_ticker",
                    symbols = symbols.len(),
                    "Book ticker WebSocket connected"
                );
                while let Some(message) = socket.next().await {
                    match message {
                        Ok(Message::Text(text)) => {
                            if let Ok(envelope) =
                                serde_json::from_str::<CombinedMessage<BookTickerEvent>>(&text)
                            {
                                let _ = tx.send(vec![envelope.data]);
                            } else if let Ok(event) = serde_json::from_str::<BookTickerEvent>(&text)
                            {
                                let _ = tx.send(vec![event]);
                            }
                        }
                        Ok(Message::Ping(payload)) => {
                            socket
                                .send(Message::Pong(payload))
                                .await
                                .map_err(ws_error)?;
                        }
                        Ok(Message::Close(frame)) => {
                            tracing::warn!(stream = "book_ticker", reason = ?frame, "WebSocket closed by peer");
                            break;
                        }
                        Ok(Message::Pong(_)) | Ok(Message::Binary(_)) | Ok(Message::Frame(_)) => {}
                        Err(error) => {
                            tracing::warn!(stream = "book_ticker", error = %error, "WebSocket read error");
                            break;
                        }
                    }
                }
            }
            Err(error) => {
                attempt = attempt.saturating_add(1);
                tracing::warn!(stream = "book_ticker", attempt, error = %error, "WebSocket connection failed");
            }
        }
        attempt = attempt.saturating_add(1);
        let delay = delays.next().ok_or_else(|| {
            AppError::WsDisconnected("book ticker exhausted reconnect attempts".to_owned())
        })?;
        tracing::warn!(
            stream = "book_ticker",
            attempt,
            delay_ms = delay.as_millis(),
            "WebSocket disconnected, reconnecting"
        );
        tokio::time::sleep(delay).await;
    }
}

pub async fn run_user_data_stream(
    base_ws_url: &str,
    listen_key: &str,
    tx: broadcast::Sender<UserDataEvent>,
) -> AppResult<()> {
    let url = stream_url(base_ws_url, listen_key)?;
    let mut delays = ExponentialBackoff::from_millis(100)
        .max_delay(Duration::from_secs(30))
        .take(MAX_RECONNECT_ATTEMPTS);
    let mut attempt = 0usize;

    loop {
        match connect_async(url.as_str()).await {
            Ok((mut socket, _response)) => {
                attempt = 0;
                tracing::info!(stream = "user_data", "User Data WebSocket connected");
                while let Some(message) = socket.next().await {
                    match message {
                        Ok(Message::Text(text)) => {
                            match serde_json::from_str::<UserDataEvent>(&text) {
                                Ok(event) => {
                                    let _ = tx.send(event);
                                }
                                Err(error) => {
                                    tracing::warn!(stream = "user_data", error = %error, "Ignoring malformed user event")
                                }
                            }
                        }
                        Ok(Message::Ping(payload)) => {
                            socket
                                .send(Message::Pong(payload))
                                .await
                                .map_err(ws_error)?;
                        }
                        Ok(Message::Close(frame)) => {
                            tracing::warn!(stream = "user_data", reason = ?frame, "WebSocket closed by peer");
                            break;
                        }
                        Ok(Message::Binary(bytes)) => {
                            if let Ok(text) = String::from_utf8(bytes.to_vec()) {
                                if let Ok(event) = serde_json::from_str::<UserDataEvent>(&text) {
                                    let _ = tx.send(event);
                                }
                            }
                        }
                        Ok(Message::Pong(_)) | Ok(Message::Frame(_)) => {}
                        Err(error) => {
                            tracing::warn!(stream = "user_data", error = %error, "WebSocket read error");
                            break;
                        }
                    }
                }
            }
            Err(error) => {
                attempt = attempt.saturating_add(1);
                tracing::warn!(stream = "user_data", attempt, error = %error, "WebSocket connection failed");
            }
        }
        attempt = attempt.saturating_add(1);
        let delay = delays.next().ok_or_else(|| {
            AppError::WsDisconnected("user data exhausted reconnect attempts".to_owned())
        })?;
        tracing::warn!(
            stream = "user_data",
            attempt,
            delay_ms = delay.as_millis(),
            "WebSocket disconnected, reconnecting"
        );
        tokio::time::sleep(delay).await;
    }
}

#[derive(Debug, Deserialize)]
struct CombinedMessage<T> {
    data: T,
}

fn stream_base_url(base_ws_url: &str) -> AppResult<Url> {
    let base = base_ws_url.trim_end_matches('/');
    let root = base.strip_suffix("/ws").unwrap_or(base);
    let stream_url = format!("{root}/stream");
    Url::parse(&stream_url).map_err(|error| AppError::WsDisconnected(error.to_string()))
}

fn stream_url(base_ws_url: &str, suffix: &str) -> AppResult<Url> {
    let base = base_ws_url.trim_end_matches('/');
    let url = format!("{}/{}", base, suffix);
    Url::parse(&url).map_err(|error| AppError::WsDisconnected(error.to_string()))
}

fn ws_error(error: tokio_tungstenite::tungstenite::Error) -> AppError {
    AppError::WsDisconnected(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_combined_stream_url_from_ws_base() {
        let url = stream_base_url("wss://stream.testnet.binance.vision/ws").expect("valid URL");
        assert_eq!(url.as_str(), "wss://stream.testnet.binance.vision/stream");
    }

    #[test]
    fn builds_raw_mini_ticker_url() {
        let url =
            stream_url("wss://stream.binance.com:9443/ws", "!miniTicker@arr").expect("valid URL");
        assert_eq!(
            url.as_str(),
            "wss://stream.binance.com:9443/ws/!miniTicker@arr"
        );
    }
}
