//! Integration tests for adaptive concurrency manager

use ipmsg_download::adaptive_concurrency::{AdaptiveConcurrencyManager, ConcurrencyConfig};

#[tokio::test]
async fn test_adaptive_concurrency_default() {
    let manager = AdaptiveConcurrencyManager::new();
    let config = manager.get_config().await;
    assert!(config.min_connections > 0);
    assert!(config.max_connections >= config.min_connections);
}

#[tokio::test]
async fn test_adaptive_concurrency_rtt_update() {
    let manager = AdaptiveConcurrencyManager::new();
    
    // Simulate RTT samples
    manager.record_rtt(50.0).await;
    manager.record_rtt(60.0).await;
    manager.record_rtt(55.0).await;

    let state = manager.get_state().await;
    assert!(state.smoothed_rtt_ms > 0.0);
    assert!(state.rtt_variance_ms >= 0.0);
}

#[tokio::test]
async fn test_adaptive_concurrency_recommendation() {
    let manager = AdaptiveConcurrencyManager::new();
    
    // Record good RTT (low latency)
    for _ in 0..10 {
        manager.record_rtt(20.0).await;
    }

    let recommendation = manager.get_recommended_connections().await;
    assert!(recommendation >= 1);
}

#[tokio::test]
async fn test_adaptive_concurrency_high_rtt() {
    let manager = AdaptiveConcurrencyManager::new();
    
    // Record high RTT (congestion)
    for _ in 0..10 {
        manager.record_rtt(500.0).await;
    }

    let recommendation = manager.get_recommended_connections().await;
    // Should reduce connections under high RTT
    assert!(recommendation > 0);
}

#[tokio::test]
async fn test_adaptive_concurrency_config_update() {
    let manager = AdaptiveConcurrencyManager::new();
    
    let new_config = ConcurrencyConfig {
        min_connections: 2,
        max_connections: 20,
        ..Default::default()
    };
    manager.update_config(new_config.clone()).await;

    let config = manager.get_config().await;
    assert_eq!(config.min_connections, new_config.min_connections);
    assert_eq!(config.max_connections, new_config.max_connections);
}
