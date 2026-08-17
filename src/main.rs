use binance_momentum::binance::auth::sync_server_time;
use binance_momentum::binance::client::BinanceClient;
use binance_momentum::binance::ws::{
    run_book_ticker_stream, run_market_stream, run_user_data_stream,
};
use binance_momentum::config::AppConfig;
use binance_momentum::engine::executor::run_executor;
use binance_momentum::engine::scanner::run_scanner;
use binance_momentum::engine::state::{reconcile_state, GlobalState};
use binance_momentum::error::{AppError, AppResult};
use binance_momentum::service::{
    run_exchange_info_refresh, run_health_server, run_listen_key_keepalive, run_reconciliation_loop,
};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, RwLock};
use tokio::task::JoinSet;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    init_tracing();
    if let Err(error) = run().await {
        tracing::error!(error = %error, "Application stopped with error");
        std::process::exit(1);
    }
}

async fn run() -> AppResult<()> {
    let config = AppConfig::load()?;
    tracing::info!(
        use_testnet = config.use_testnet,
        dry_run = config.dry_run,
        "Starting binance-momentum"
    );

    let client = Arc::new(BinanceClient::new(&config)?);
    client.ping().await?;
    let clock_delta_ms = sync_server_time(|| client.get_server_time()).await?;
    client.set_clock_delta_ms(clock_delta_ms);
    tracing::info!(clock_delta_ms, "Binance server clock synchronized");

    let account = client.get_account_info().await?;
    if !account.can_trade && !config.dry_run {
        return Err(AppError::InvalidConfig(
            "Binance account cannot trade while live execution is enabled".to_owned(),
        ));
    }
    let state = Arc::new(RwLock::new(GlobalState::new(&account)));

    let exchange_cache = { state.read().await.exchange_info.clone() };
    exchange_cache.refresh(&client).await?;
    if exchange_cache.is_empty() {
        return Err(AppError::InvalidConfig(
            "exchangeInfo returned no eligible USDT symbols".to_owned(),
        ));
    }
    reconcile_state(&client, &state).await?;

    // POST /api/v3/userDataStream foi removido do Testnet (HTTP 410).
    // Quando ausente, o bot continua sem User Data Stream e usa apenas a
    // reconciliação REST para rastrear posições.
    let listen_key: Option<String> = match client.get_listen_key().await {
        Ok(key) => Some(key),
        Err(AppError::BinanceHttp { status: 410, .. }) => {
            tracing::warn!("POST /api/v3/userDataStream returned 410 Gone — User Data Stream unavailable; position tracking via REST reconciliation only");
            None
        }
        Err(e) => return Err(e),
    };
    let symbols = exchange_cache.symbols();
    tracing::info!(symbols = symbols.len(), user_data_stream = listen_key.is_some(), "Preflight completed");

    let (market_tx, _) = broadcast::channel(2048);
    let (book_tx, _) = broadcast::channel(4096);
    let (user_tx, _) = broadcast::channel(2048);
    let (signal_tx, signal_rx) = mpsc::channel(256);
    let market_rx = market_tx.subscribe();
    let book_rx = book_tx.subscribe();
    let user_rx = user_tx.subscribe();
    let mut tasks = JoinSet::<AppResult<()>>::new();

    {
        let url = config.ws_market_base_url.clone();
        let tx = market_tx.clone();
        tasks.spawn(async move { run_market_stream(&url, tx).await });
    }
    {
        let url = config.ws_market_base_url.clone();
        let tx = book_tx.clone();
        tasks.spawn(async move { run_book_ticker_stream(&url, symbols, tx).await });
    }
    if let Some(ref key) = listen_key {
        let url = config.ws_user_base_url.clone();
        let key = key.clone();
        let tx = user_tx.clone();
        tasks.spawn(async move { run_user_data_stream(&url, &key, tx).await });
    }
    {
        let state_for_scanner = Arc::clone(&state);
        let scanner_config = config.scanner.clone();
        let min_volume = config.risk.min_24h_volume_usdt;
        tasks.spawn(async move {
            run_scanner(
                market_rx,
                book_rx,
                signal_tx,
                scanner_config,
                min_volume,
                state_for_scanner,
            )
            .await
        });
    }
    {
        let client_for_executor = Arc::clone(&client);
        let state_for_executor = Arc::clone(&state);
        let executor_config = config.clone();
        tasks.spawn(async move {
            run_executor(
                signal_rx,
                user_rx,
                client_for_executor,
                state_for_executor,
                executor_config,
            )
            .await
        });
    }
    {
        let state_for_health = Arc::clone(&state);
        let config_for_health = Arc::new(config.clone());
        let client_for_health = Arc::clone(&client);
        tasks.spawn(async move {
            run_health_server(state_for_health, config_for_health, client_for_health).await
        });
    }
    {
        let client_for_refresh = Arc::clone(&client);
        let state_for_refresh = Arc::clone(&state);
        tasks.spawn(async move {
            run_exchange_info_refresh(
                client_for_refresh,
                state_for_refresh,
                config.exchange.exchange_info_refresh_secs,
            )
            .await
        });
    }
    {
        let client_for_reconciliation = Arc::clone(&client);
        let state_for_reconciliation = Arc::clone(&state);
        tasks.spawn(async move {
            run_reconciliation_loop(client_for_reconciliation, state_for_reconciliation).await
        });
    }
    if let Some(key) = listen_key {
        let client_for_keepalive = Arc::clone(&client);
        tasks.spawn(async move {
            run_listen_key_keepalive(
                client_for_keepalive,
                key,
                config.exchange.listen_key_refresh_secs,
            )
            .await
        });
    }

    let outcome = tokio::select! {
        signal = tokio::signal::ctrl_c() => match signal {
            Ok(()) => {
                tracing::info!("Shutdown signal received");
                Ok(())
            }
            Err(error) => Err(AppError::Other(anyhow::anyhow!("failed to listen for shutdown signal: {error}"))),
        },
        result = tasks.join_next() => match result {
            Some(Ok(Ok(()))) => Err(AppError::Other(anyhow::anyhow!("a service exited unexpectedly"))),
            Some(Ok(Err(error))) => Err(error),
            Some(Err(error)) => Err(AppError::Other(anyhow::anyhow!("service task panicked: {error}"))),
            None => Err(AppError::Other(anyhow::anyhow!("all service tasks exited unexpectedly"))),
        },
    };

    tasks.abort_all();
    while let Some(result) = tasks.join_next().await {
        if let Err(error) = result {
            tracing::debug!(error = %error, "Service task stopped during shutdown");
        }
    }
    tracing::info!("Shutdown complete");
    outcome
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .json()
        .with_target(false)
        .with_current_span(false)
        .init();
}
