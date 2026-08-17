use crate::binance::client::BinanceClient;
use crate::engine::state::{reconcile_state, GlobalState};
use crate::error::{AppError, AppResult};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::RwLock;

// TODO FASE 2: dashboard web de monitoramento completo (métricas, histórico de trades)
pub async fn run_health_server(state: Arc<RwLock<GlobalState>>, port: u16) -> AppResult<()> {
    let listener = TcpListener::bind(("0.0.0.0", port))
        .await
        .map_err(|error| AppError::Other(anyhow::anyhow!("health server bind failed: {error}")))?;
    tracing::info!(port, "Health server listening");
    loop {
        let (mut socket, peer) = listener.accept().await.map_err(|error| {
            AppError::Other(anyhow::anyhow!("health server accept failed: {error}"))
        })?;
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            let mut request = [0_u8; 1024];
            let read_result = socket.read(&mut request).await;
            if let Ok(bytes_read) = read_result {
                let request_line = String::from_utf8_lossy(&request[..bytes_read]);
                let (status, body) = if request_line.starts_with("GET /health ") {
                    let state_guard = state.read().await;
                    let healthy = !state_guard.reconciliation_required;
                    let status = if healthy {
                        "200 OK"
                    } else {
                        "503 Service Unavailable"
                    };
                    let body = serde_json::json!({
                        "status": if healthy { "ok" } else { "reconciling" },
                        "reconciliation_required": state_guard.reconciliation_required,
                        "open_positions": state_guard.open_positions.len(),
                        "blocked_symbols": state_guard.blocked_symbols.len(),
                        "cached_symbols": state_guard.exchange_info.len(),
                    })
                    .to_string();
                    (status, body)
                } else {
                    ("404 Not Found", "not found".to_owned())
                };
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                if let Err(error) = socket.write_all(response.as_bytes()).await {
                    tracing::warn!(?peer, error = %error, "Health response write failed");
                }
            }
        });
    }
}

pub async fn run_exchange_info_refresh(
    client: Arc<BinanceClient>,
    state: Arc<RwLock<GlobalState>>,
    refresh_secs: u64,
) -> AppResult<()> {
    let refresh_secs = refresh_secs.max(30);
    loop {
        tokio::time::sleep(Duration::from_secs(refresh_secs)).await;
        let cache = { state.read().await.exchange_info.clone() };
        if let Err(error) = cache.refresh(&client).await {
            tracing::error!(error = %error, "Periodic exchangeInfo refresh failed; retaining previous cache");
        }
    }
}

pub async fn run_reconciliation_loop(
    client: Arc<BinanceClient>,
    state: Arc<RwLock<GlobalState>>,
) -> AppResult<()> {
    loop {
        tokio::time::sleep(Duration::from_secs(5)).await;
        let required = { state.read().await.reconciliation_required };
        if required {
            match reconcile_state(&client, &state).await {
                Ok(()) => tracing::info!("Post-gap reconciliation succeeded"),
                Err(error) => {
                    tracing::error!(error = %error, "Post-gap reconciliation failed; entries remain paused")
                }
            }
        }
    }
}

pub async fn run_listen_key_keepalive(
    client: Arc<BinanceClient>,
    listen_key: String,
    refresh_secs: u64,
) -> AppResult<()> {
    let refresh_secs = refresh_secs.max(60);
    loop {
        tokio::time::sleep(Duration::from_secs(refresh_secs)).await;
        if let Err(error) = client.keepalive_listen_key(&listen_key).await {
            tracing::error!(error = %error, "User Data listenKey keepalive failed");
        } else {
            tracing::debug!("User Data listenKey keepalive succeeded");
        }
    }
}
