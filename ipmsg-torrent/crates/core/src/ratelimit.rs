use std::collections::HashMap;
use std::time::{Duration, Instant};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    pub max_messages: u32,
    pub window_secs: u64,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            max_messages: 10,
            window_secs: 5,
        }
    }
}

pub struct RateLimiter {
    config: RateLimitConfig,
    buckets: Arc<RwLock<HashMap<String, Vec<Instant>>>>,
}

impl RateLimiter {
    pub fn new(config: RateLimitConfig) -> Self {
        Self {
            config,
            buckets: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn check(&self, peer_id: &str) -> bool {
        let now = Instant::now();
        let mut buckets = self.buckets.write().await;
        let window = Duration::from_secs(self.config.window_secs);
        
        let timestamps = buckets.entry(peer_id.to_string()).or_insert_with(Vec::new);
        
        // Remove old timestamps outside the window
        timestamps.retain(|&t| now.duration_since(t) < window);
        
        if timestamps.len() >= self.config.max_messages as usize {
            return false;
        }
        
        timestamps.push(now);
        true
    }
    
    pub async fn reset(&self, peer_id: &str) {
        let mut buckets = self.buckets.write().await;
        buckets.remove(peer_id);
    }
}

pub struct CoverTraffic {
    enabled: Arc<RwLock<bool>>,
    interval_secs: u64,
}

impl CoverTraffic {
    pub fn new(enabled: bool, interval_secs: u64) -> Self {
        Self {
            enabled: Arc::new(RwLock::new(enabled)),
            interval_secs,
        }
    }
    
    pub async fn is_enabled(&self) -> bool {
        *self.enabled.read().await
    }
    
    pub async fn set_enabled(&self, enabled: bool) {
        *self.enabled.write().await = enabled;
    }
    
    pub fn interval(&self) -> Duration {
        Duration::from_secs(self.interval_secs)
    }
}
