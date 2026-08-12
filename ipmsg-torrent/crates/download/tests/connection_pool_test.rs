//! Integration tests for connection pool functionality

use ipmsg_download::connection_pool::{ConnectionPool, PoolConfig};
use std::net::SocketAddr;
use tokio::net::TcpListener;

#[tokio::test]
async fn test_connection_pool_basic() {
    let pool = ConnectionPool::new();
    let stats = pool.stats().await;
    assert_eq!(stats.total_addresses, 0);
    assert_eq!(stats.total_connections, 0);
}

#[tokio::test]
async fn test_connection_pool_connect_and_return() {
    // Start a test server
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let pool = ConnectionPool::new();

    // Connect to server
    let stream = pool.get_or_connect(addr).await.unwrap();

    // Return to pool
    pool.return_connection(stream, addr).await;

    let stats = pool.stats().await;
    assert_eq!(stats.total_addresses, 1);
    assert_eq!(stats.total_connections, 1);
}

#[tokio::test]
async fn test_connection_pool_reuse() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let pool = ConnectionPool::new();

    // First connection
    let stream1 = pool.get_or_connect(addr).await.unwrap();
    pool.return_connection(stream1, addr).await;

    // Second connection should reuse
    let stream2 = pool.get_or_connect(addr).await.unwrap();
    pool.return_connection(stream2, addr).await;

    let stats = pool.stats().await;
    assert_eq!(stats.total_addresses, 1);
    assert_eq!(stats.total_connections, 1); // Still 1, reused
}

#[tokio::test]
async fn test_connection_pool_cleanup() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let config = PoolConfig {
        max_idle_secs: 0, // Immediate expiry
        ..Default::default()
    };
    let pool = ConnectionPool::with_config(config);

    let stream = pool.get_or_connect(addr).await.unwrap();
    pool.return_connection(stream, addr).await;

    // Cleanup should remove expired connections
    pool.cleanup().await;

    let stats = pool.stats().await;
    assert_eq!(stats.total_connections, 0);
}

#[tokio::test]
async fn test_connection_pool_dns_cache() {
    let pool = ConnectionPool::new();

    // Cache a DNS result
    pool.cache_dns_result("example.com".to_string(), "127.0.0.1".parse().unwrap());

    // Resolve from cache
    let cached = pool.resolve_dns("example.com".to_string()).await;
    assert!(cached.is_some());
    assert_eq!(cached.unwrap(), "127.0.0.1".parse().unwrap());

    // Non-existent domain
    let missing = pool.resolve_dns("nonexistent.com".to_string()).await;
    assert!(missing.is_none());
}

#[tokio::test]
async fn test_connection_pool_config_update() {
    let pool = ConnectionPool::new();

    let config = pool.get_config_async().await;
    assert_eq!(config.max_connections_per_addr, 4);

    let new_config = PoolConfig {
        max_connections_per_addr: 8,
        ..Default::default()
    };
    pool.update_config(new_config).await;

    let updated = pool.get_config_async().await;
    assert_eq!(updated.max_connections_per_addr, 8);
}

#[tokio::test]
async fn test_connection_pool_clear() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let pool = ConnectionPool::new();

    let stream = pool.get_or_connect(addr).await.unwrap();
    pool.return_connection(stream, addr).await;

    pool.clear().await;

    let stats = pool.stats().await;
    assert_eq!(stats.total_addresses, 0);
    assert_eq!(stats.total_connections, 0);
}
