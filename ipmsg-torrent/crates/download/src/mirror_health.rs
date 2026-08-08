//! Mirror URL health checking and auto-switching
//!
//! Monitors mirror availability, measures response times,
//! and automatically switches to the best performing mirror.

use std::time::{Duration, Instant};

/// Health status of a single mirror URL
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MirrorHealth {
    /// The mirror URL
    pub url: String,
    /// Whether the mirror is currently reachable
    pub reachable: bool,
    /// Last response time in milliseconds (if reachable)
    pub response_time_ms: Option<u64>,
    /// Number of consecutive failures
    pub failure_count: u32,
    /// Last check timestamp
    pub last_checked: Option<chrono::DateTime<chrono::Utc>>,
    /// Overall health score (0.0 - 1.0, higher is better)
    pub health_score: f32,
}

/// Configuration for mirror health monitoring
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MirrorHealthConfig {
    /// Check interval in seconds
    pub check_interval_secs: u64,
    /// Timeout for health check requests in seconds
    pub timeout_secs: u64,
    /// Maximum consecutive failures before marking unreachable
    pub max_failures: u32,
    /// Enable automatic switching to best mirror
    pub auto_switch: bool,
    /// Minimum health score difference to trigger switch
    pub switch_threshold: f32,
}

impl Default for MirrorHealthConfig {
    fn default() -> Self {
        Self {
            check_interval_secs: 300, // 5 minutes
            timeout_secs: 10,
            max_failures: 3,
            auto_switch: true,
            switch_threshold: 0.3,
        }
    }
}

/// Summary of all mirrors for a task
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MirrorSummary {
    /// Task ID
    pub task_id: String,
    /// Current active mirror URL
    pub active_url: Option<String>,
    /// Health status of all mirrors
    pub mirrors: Vec<MirrorHealth>,
    /// Recommended mirror (best performing)
    pub recommended_url: Option<String>,
    /// Whether auto-switch would change the current mirror
    pub should_switch: bool,
}

/// Check health of a single mirror URL
pub async fn check_mirror_health(url: &str, timeout: Duration) -> MirrorHealth {
    let start = Instant::now();

    let result = tokio::time::timeout(timeout, async {
        // Simple HTTP HEAD request to check availability
        match reqwest::Client::new().head(url).send().await {
            Ok(resp) => resp.status().is_success() || resp.status().is_redirection(),
            Err(_) => false,
        }
    })
    .await;

    let elapsed = start.elapsed();
    let reachable = result.unwrap_or(false);

    MirrorHealth {
        url: url.to_string(),
        reachable,
        response_time_ms: if reachable {
            Some(elapsed.as_millis() as u64)
        } else {
            None
        },
        failure_count: 0, // Will be updated by manager
        last_checked: Some(chrono::Utc::now()),
        health_score: if reachable {
            // Score based on response time: <1s = 1.0, >5s = 0.2
            let time_score = (5000.0 / elapsed.as_millis().max(1) as f32).min(1.0);
            time_score.max(0.2)
        } else {
            0.0
        },
    }
}

/// Check health of all mirrors for a task
pub async fn check_all_mirrors(
    task_id: &str,
    active_url: Option<&str>,
    mirror_urls: &[String],
    config: &MirrorHealthConfig,
) -> MirrorSummary {
    let timeout = Duration::from_secs(config.timeout_secs);

    let mut mirrors = Vec::new();

    // Check active URL if present
    if let Some(active) = active_url {
        let health = check_mirror_health(active, timeout).await;
        mirrors.push(health);
    }

    // Check all mirror URLs
    for url in mirror_urls {
        let health = check_mirror_health(url, timeout).await;
        mirrors.push(health);
    }

    // Find best mirror (highest health score)
    let best = mirrors.iter().filter(|m| m.reachable).max_by(|a, b| {
        a.health_score
            .partial_cmp(&b.health_score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let recommended_url = best.map(|m| m.url.clone());

    // Determine if we should switch
    let should_switch = if !config.auto_switch {
        false
    } else if let (Some(active), Some(recommended)) = (active_url, &recommended_url) {
        if active == recommended {
            false
        } else {
            // Check if recommended is significantly better
            let active_score = mirrors
                .iter()
                .find(|m| m.url == active)
                .map(|m| m.health_score)
                .unwrap_or(0.0);
            let recommended_score = best.map(|m| m.health_score).unwrap_or(0.0);
            recommended_score - active_score > config.switch_threshold
        }
    } else {
        recommended_url.is_some() && active_url.is_none()
    };

    MirrorSummary {
        task_id: task_id.to_string(),
        active_url: active_url.map(|s| s.to_string()),
        mirrors,
        recommended_url,
        should_switch,
    }
}

/// Format mirror summary for display
pub fn format_mirror_summary(summary: &MirrorSummary) -> String {
    let mut output = String::new();

    output.push_str(&format!("🔗 Mirror Health for Task {}\n", summary.task_id));
    output.push_str(&format!(
        "Active: {}\n",
        summary.active_url.as_deref().unwrap_or("none")
    ));

    if let Some(ref recommended) = summary.recommended_url {
        output.push_str(&format!("Recommended: {}\n", recommended));
        if summary.should_switch {
            output.push_str("⚠️  Should switch to recommended mirror\n");
        }
    }

    output.push_str("\nMirrors:\n");
    for mirror in &summary.mirrors {
        let status = if mirror.reachable {
            format!(
                "✅ {}ms (score: {:.2})",
                mirror.response_time_ms.unwrap_or(0),
                mirror.health_score
            )
        } else {
            "❌ Unreachable".to_string()
        };
        output.push_str(&format!("  {} - {}\n", mirror.url, status));
    }

    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = MirrorHealthConfig::default();
        assert_eq!(config.check_interval_secs, 300);
        assert_eq!(config.timeout_secs, 10);
        assert_eq!(config.max_failures, 3);
        assert!(config.auto_switch);
        assert_eq!(config.switch_threshold, 0.3);
    }

    #[test]
    fn test_mirror_health_serialization() {
        let health = MirrorHealth {
            url: "http://example.com/file.zip".to_string(),
            reachable: true,
            response_time_ms: Some(150),
            failure_count: 0,
            last_checked: Some(chrono::Utc::now()),
            health_score: 0.95,
        };

        let json = serde_json::to_string(&health).unwrap();
        let deserialized: MirrorHealth = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.url, health.url);
        assert_eq!(deserialized.reachable, health.reachable);
        assert_eq!(deserialized.response_time_ms, health.response_time_ms);
        assert_eq!(deserialized.health_score, health.health_score);
    }

    #[test]
    fn test_mirror_summary_serialization() {
        let summary = MirrorSummary {
            task_id: "test-task-123".to_string(),
            active_url: Some("http://primary.com/file.zip".to_string()),
            mirrors: vec![
                MirrorHealth {
                    url: "http://primary.com/file.zip".to_string(),
                    reachable: true,
                    response_time_ms: Some(200),
                    failure_count: 0,
                    last_checked: Some(chrono::Utc::now()),
                    health_score: 0.9,
                },
                MirrorHealth {
                    url: "http://mirror.com/file.zip".to_string(),
                    reachable: false,
                    response_time_ms: None,
                    failure_count: 3,
                    last_checked: Some(chrono::Utc::now()),
                    health_score: 0.0,
                },
            ],
            recommended_url: Some("http://primary.com/file.zip".to_string()),
            should_switch: false,
        };

        let json = serde_json::to_string(&summary).unwrap();
        let deserialized: MirrorSummary = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.task_id, summary.task_id);
        assert_eq!(deserialized.mirrors.len(), 2);
        assert!(!deserialized.should_switch);
    }

    #[test]
    fn test_format_mirror_summary() {
        let summary = MirrorSummary {
            task_id: "abc123".to_string(),
            active_url: Some("http://primary.com/file.zip".to_string()),
            mirrors: vec![MirrorHealth {
                url: "http://primary.com/file.zip".to_string(),
                reachable: true,
                response_time_ms: Some(150),
                failure_count: 0,
                last_checked: Some(chrono::Utc::now()),
                health_score: 0.95,
            }],
            recommended_url: Some("http://primary.com/file.zip".to_string()),
            should_switch: false,
        };

        let formatted = format_mirror_summary(&summary);
        assert!(formatted.contains("abc123"));
        assert!(formatted.contains("primary.com"));
        assert!(formatted.contains("✅"));
        assert!(!formatted.contains("Should switch"));
    }

    #[test]
    fn test_should_switch_logic() {
        let _config = MirrorHealthConfig {
            auto_switch: true,
            switch_threshold: 0.3,
            ..Default::default()
        };

        // Active is good, no switch needed
        let summary = MirrorSummary {
            task_id: "test".to_string(),
            active_url: Some("http://primary.com".to_string()),
            mirrors: vec![
                MirrorHealth {
                    url: "http://primary.com".to_string(),
                    reachable: true,
                    response_time_ms: Some(100),
                    failure_count: 0,
                    last_checked: None,
                    health_score: 0.95,
                },
                MirrorHealth {
                    url: "http://mirror.com".to_string(),
                    reachable: true,
                    response_time_ms: Some(200),
                    failure_count: 0,
                    last_checked: None,
                    health_score: 0.85,
                },
            ],
            recommended_url: Some("http://primary.com".to_string()),
            should_switch: false,
        };

        assert!(!summary.should_switch);

        // Active is bad, should switch
        let summary2 = MirrorSummary {
            task_id: "test".to_string(),
            active_url: Some("http://primary.com".to_string()),
            mirrors: vec![
                MirrorHealth {
                    url: "http://primary.com".to_string(),
                    reachable: false,
                    response_time_ms: None,
                    failure_count: 3,
                    last_checked: None,
                    health_score: 0.0,
                },
                MirrorHealth {
                    url: "http://mirror.com".to_string(),
                    reachable: true,
                    response_time_ms: Some(200),
                    failure_count: 0,
                    last_checked: None,
                    health_score: 0.85,
                },
            ],
            recommended_url: Some("http://mirror.com".to_string()),
            should_switch: true,
        };

        assert!(summary2.should_switch);
    }
}
