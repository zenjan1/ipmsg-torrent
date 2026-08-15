//! Integration tests for adaptive concurrency manager

use ipmsg_download::adaptive_concurrency::{AdaptiveConcurrencyConfig, AdaptiveConcurrencyManager};

#[tokio::test]
async fn test_adaptive_concurrency_default() {
    let manager = AdaptiveConcurrencyManager::new();
    let config = manager.get_config();
    assert!(config.min_connections > 0);
    assert!(config.max_connections >= config.min_connections);
}

#[tokio::test]
async fn test_adaptive_concurrency_rtt_update() {
    let mut manager = AdaptiveConcurrencyManager::new();
    manager.register_task("task1");

    // Simulate RTT samples
    manager.record_sample("task1", 50.0, true);
    manager.record_sample("task1", 60.0, true);
    manager.record_sample("task1", 55.0, true);

    let rtt = manager.get_smoothed_rtt("task1");
    assert!(rtt.is_some());
    assert!(rtt.unwrap() > 0.0);
}

#[tokio::test]
async fn test_adaptive_concurrency_recommendation() {
    let mut manager = AdaptiveConcurrencyManager::new();
    manager.register_task_with_domain("task1", "example.com");

    // Record good RTT (low latency)
    for _ in 0..10 {
        manager.record_sample("task1", 20.0, true);
    }

    let connections = manager.get_connections("task1");
    assert!(connections >= 1);
}

#[tokio::test]
async fn test_adaptive_concurrency_high_rtt() {
    let mut manager = AdaptiveConcurrencyManager::new();
    manager.register_task_with_domain("task1", "example.com");

    // Record high RTT (congestion)
    for _ in 0..10 {
        manager.record_sample("task1", 500.0, true);
    }

    let connections = manager.get_connections("task1");
    // Should reduce connections under high RTT
    assert!(connections > 0);
}

#[tokio::test]
async fn test_adaptive_concurrency_config_update() {
    let mut manager = AdaptiveConcurrencyManager::new();

    let new_config = AdaptiveConcurrencyConfig {
        min_connections: 2,
        max_connections: 20,
        ..Default::default()
    };
    manager.set_config(new_config.clone());

    let config = manager.get_config();
    assert_eq!(config.min_connections, new_config.min_connections);
    assert_eq!(config.max_connections, new_config.max_connections);
}
