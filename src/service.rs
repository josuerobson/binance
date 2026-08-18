use crate::binance::client::BinanceClient;
use crate::config::AppConfig;
use crate::engine::paper::{now_ms, RuntimeConfig};
use crate::engine::state::{reconcile_state, GlobalState};
use crate::error::{AppError, AppResult};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::RwLock;

fn extract_path(request: &str) -> &str {
    request
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or("/")
}

fn extract_method(request: &str) -> &str {
    request
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().next())
        .unwrap_or("GET")
}

fn extract_body(request: &str) -> &str {
    request
        .find("\r\n\r\n")
        .map(|pos| request[pos + 4..].trim_end_matches('\0'))
        .unwrap_or("")
}

fn extract_api_key(request: &str) -> Option<&str> {
    for line in request.lines() {
        if line.len() >= 10 && line[..10].eq_ignore_ascii_case("x-api-key:") {
            return Some(line[10..].trim());
        }
    }
    None
}

fn auth_ok(request: &str, expected: &str) -> bool {
    if expected.is_empty() {
        return true;
    }
    extract_api_key(request).map_or(false, |k| k == expected)
}

fn json_200(body: String) -> String {
    format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {len}\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n{body}",
        len = body.len()
    )
}

fn json_err(status: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {len}\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n{body}",
        len = body.len()
    )
}

pub async fn run_health_server(
    state: Arc<RwLock<GlobalState>>,
    config: Arc<AppConfig>,
    client: Arc<BinanceClient>,
    runtime: Arc<RwLock<RuntimeConfig>>,
) -> AppResult<()> {
    let port = config.server.health_port;
    let listener = TcpListener::bind(("0.0.0.0", port))
        .await
        .map_err(|e| AppError::Other(anyhow::anyhow!("health server bind failed: {e}")))?;
    tracing::info!(port, "Health server listening");

    loop {
        let (mut socket, peer) = listener.accept().await.map_err(|e| {
            AppError::Other(anyhow::anyhow!("health server accept failed: {e}"))
        })?;
        let state = Arc::clone(&state);
        let config = Arc::clone(&config);
        let client = Arc::clone(&client);
        let runtime = Arc::clone(&runtime);
        tokio::spawn(async move {
            let mut buf = [0_u8; 8192];
            let Ok(n) = socket.read(&mut buf).await else {
                return;
            };
            let req = String::from_utf8_lossy(&buf[..n]);
            let path = extract_path(&req).to_owned();
            let method = extract_method(&req).to_owned();

            let needs_auth = matches!(
                path.as_str(),
                "/api/snapshot"
                    | "/api/signals"
                    | "/api/config"
                    | "/api/latency"
                    | "/api/paper/positions"
                    | "/api/paper/history"
                    | "/api/runtime/config"
                    | "/api/scanner/candidates"
            );
            if needs_auth && !auth_ok(&req, &config.dashboard_api_key) {
                let resp = json_err("401 Unauthorized", r#"{"error":"Unauthorized"}"#);
                if let Err(e) = socket.write_all(resp.as_bytes()).await {
                    tracing::warn!(?peer, error=%e, "Response write failed");
                }
                return;
            }

            let response = match path.as_str() {
                "/health" => {
                    let g = state.read().await;
                    let ok = !g.reconciliation_required;
                    let body = serde_json::json!({
                        "status": if ok { "ok" } else { "reconciling" },
                        "reconciliation_required": g.reconciliation_required,
                        "open_positions": g.open_positions.len(),
                        "blocked_symbols": g.blocked_symbols.len(),
                        "cached_symbols": g.exchange_info.len(),
                    })
                    .to_string();
                    let st = if ok { "200 OK" } else { "503 Service Unavailable" };
                    format!(
                        "HTTP/1.1 {st}\r\nContent-Type: application/json\r\nContent-Length: {len}\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n{body}",
                        len = body.len()
                    )
                }

                "/api/snapshot" => {
                    let g = state.read().await;
                    let ok = !g.reconciliation_required;
                    let usdt_locked = g
                        .balances
                        .get("USDT")
                        .and_then(|b| b.locked.parse::<f64>().ok())
                        .filter(|v| v.is_finite() && *v >= 0.0)
                        .unwrap_or(0.0);
                    let positions: Vec<_> = g
                        .open_positions
                        .values()
                        .map(|p| {
                            serde_json::json!({
                                "symbol": p.symbol,
                                "entry_price": p.entry_price,
                                "quantity": p.quantity,
                                "current_price": serde_json::Value::Null,
                                "pnl_pct": serde_json::Value::Null,
                                "pnl_usdt": serde_json::Value::Null,
                                "entry_order_id": p.entry_order_id,
                                "order_list_id": p.order_list_id,
                                "opened_at": p.opened_at,
                            })
                        })
                        .collect();
                    let pending: Vec<&String> = g.pending_symbols.iter().collect();
                    let blocked: Vec<&String> = g.blocked_symbols.iter().collect();
                    let body = serde_json::json!({
                        "timestamp_ms": now_ms(),
                        "balance": {
                            "usdt_free": g.usdt_balance,
                            "usdt_locked": usdt_locked,
                        },
                        "positions": positions,
                        "symbols": {
                            "pending": pending,
                            "blocked": blocked,
                            "cached_count": g.exchange_info.len(),
                        },
                        "health": {
                            "status": if ok { "ok" } else { "reconciling" },
                            "reconciliation_required": g.reconciliation_required,
                            "open_positions": g.open_positions.len(),
                            "blocked_symbols": g.blocked_symbols.len(),
                            "cached_symbols": g.exchange_info.len(),
                        },
                        "request_weight": {
                            "used_weight": 0,
                            "limit": config.exchange.request_weight_limit_per_minute,
                            "window_seconds_remaining": 60,
                        },
                        "mode": {
                            "use_testnet": config.use_testnet,
                            "dry_run": config.dry_run,
                        },
                    })
                    .to_string();
                    json_200(body)
                }

                "/api/signals" => {
                    let g = state.read().await;
                    let signals: Vec<_> = g.recent_signals.iter().rev().cloned().collect();
                    let count = signals.len();
                    let body = serde_json::json!({ "count": count, "signals": signals }).to_string();
                    json_200(body)
                }

                "/api/config" => {
                    let body = serde_json::json!({
                        "mode": {
                            "use_testnet": config.use_testnet,
                            "dry_run": config.dry_run,
                            "allow_live_trading": config.allow_live_trading,
                        },
                        "scanner": {
                            "momentum_trigger_pct": config.scanner.momentum_trigger_pct,
                            "momentum_window_secs": config.scanner.momentum_window_secs,
                            "volume_surge_multiplier": config.scanner.volume_surge_multiplier,
                            "max_spread_pct": config.scanner.max_spread_pct,
                        },
                        "risk": {
                            "stop_loss_pct": config.risk.stop_loss_pct,
                            "take_profit_1_pct": config.risk.take_profit_1_pct,
                            "take_profit_2_pct": config.risk.take_profit_2_pct,
                            "max_positions": config.risk.max_positions,
                            "position_size_pct": config.risk.position_size_pct,
                            "min_24h_volume_usdt": config.risk.min_24h_volume_usdt,
                        },
                    })
                    .to_string();
                    json_200(body)
                }

                "/api/latency" => {
                    let t_before = now_ms();
                    match client.get_server_time().await {
                        Ok(binance_time) => {
                            let t_after = now_ms();
                            let rtt_ms = t_after - t_before;
                            let clock_diff_ms = binance_time - (t_before + rtt_ms / 2);
                            let body = serde_json::json!({
                                "rtt_ms": rtt_ms,
                                "binance_server_time_ms": binance_time,
                                "local_time_ms": t_after,
                                "clock_diff_ms": clock_diff_ms,
                            })
                            .to_string();
                            json_200(body)
                        }
                        Err(e) => {
                            let msg = e.to_string();
                            json_err("502 Bad Gateway", &format!(r#"{{"error":{}}}"#, serde_json::json!(msg)))
                        }
                    }
                }

                "/api/paper/positions" => {
                    let g = state.read().await;
                    let positions: Vec<_> = g.paper_positions.values().collect();
                    let body = serde_json::json!({
                        "count": positions.len(),
                        "paper_balance": g.paper_balance,
                        "positions": positions,
                    })
                    .to_string();
                    json_200(body)
                }

                "/api/paper/history" => {
                    let g = state.read().await;
                    let history: Vec<_> = g.paper_history.iter().rev().cloned().collect();
                    let wins = history.iter().filter(|t| t.pnl_usdt > 0.0).count();
                    let total = history.len();
                    let total_pnl: f64 = history.iter().map(|t| t.pnl_usdt).sum();
                    let body = serde_json::json!({
                        "count": total,
                        "wins": wins,
                        "losses": total - wins,
                        "win_rate_pct": if total > 0 { wins as f64 / total as f64 * 100.0 } else { 0.0 },
                        "total_pnl_usdt": total_pnl,
                        "paper_balance": g.paper_balance,
                        "history": history,
                    })
                    .to_string();
                    json_200(body)
                }

                "/api/runtime/config" => {
                    match method.as_str() {
                        "GET" => {
                            let rt = runtime.read().await.clone();
                            json_200(serde_json::to_string(&rt).unwrap_or_default())
                        }
                        "POST" => {
                            let body = extract_body(&req);
                            match serde_json::from_str::<RuntimeConfig>(body) {
                                Ok(new_cfg) if new_cfg.validate() => {
                                    let mut rt = runtime.write().await;
                                    // Update paper_balance in state if it changed
                                    if (new_cfg.paper_balance - rt.paper_balance).abs() > f64::EPSILON {
                                        let mut st = state.write().await;
                                        st.set_paper_balance(new_cfg.paper_balance);
                                    }
                                    *rt = new_cfg;
                                    tracing::info!("Runtime config updated via API");
                                    json_200(serde_json::to_string(&*rt).unwrap_or_default())
                                }
                                Ok(_) => json_err(
                                    "400 Bad Request",
                                    r#"{"error":"invalid config values"}"#,
                                ),
                                Err(_) => json_err(
                                    "400 Bad Request",
                                    r#"{"error":"invalid JSON body"}"#,
                                ),
                            }
                        }
                        _ => json_err("405 Method Not Allowed", r#"{"error":"method not allowed"}"#),
                    }
                }

                "/api/scanner/candidates" => {
                    let g = state.read().await;
                    let rt = runtime.read().await;
                    let body = serde_json::json!({
                        "count": g.scanner_candidates.len(),
                        "candidates": g.scanner_candidates,
                        "btc_momentum_pct": g.btc_momentum_pct,
                        "btc_filter_ok": g.btc_filter_ok,
                        "btc_filter_enabled": rt.btc_filter_enabled,
                        "btc_filter_window_secs": rt.btc_filter_window_secs,
                        "btc_min_momentum_pct": rt.btc_min_momentum_pct,
                    })
                    .to_string();
                    json_200(body)
                }

                _ => json_err("404 Not Found", r#"{"error":"not found"}"#),
            };

            if let Err(e) = socket.write_all(response.as_bytes()).await {
                tracing::warn!(?peer, error=%e, "Response write failed");
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
