use binance_momentum::engine::state::GlobalState;
use binance_momentum::service::run_health_server;
use std::sync::Arc;
use tokio::sync::RwLock;

#[tokio::test]
async fn health_reports_operational_and_reconciling_states() {
    let state = Arc::new(RwLock::new(GlobalState::with_balance(1000.0)));
    let task = tokio::spawn(run_health_server(Arc::clone(&state), 18081));
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let client = reqwest::Client::new();
    let healthy = client
        .get("http://127.0.0.1:18081/health")
        .send()
        .await
        .expect("health server must accept requests");
    assert_eq!(healthy.status(), reqwest::StatusCode::OK);
    assert!(healthy.text().await.expect("health body").contains("ok"));

    state.write().await.mark_reconciliation_required();
    let reconciling = client
        .get("http://127.0.0.1:18081/health")
        .send()
        .await
        .expect("health server must remain available");
    assert_eq!(
        reconciling.status(),
        reqwest::StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(reconciling
        .text()
        .await
        .expect("health body")
        .contains("reconciling"));

    task.abort();
    let _ = task.await;
}
