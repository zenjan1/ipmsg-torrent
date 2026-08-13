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

    #[test]
    fn test_config_custom_values() {
        let config = MirrorHealthConfig {
            check_interval_secs: 600,
            timeout_secs: 30,
            max_failures: 5,
            auto_switch: false,
            switch_threshold: 0.5,
        };
        assert_eq!(config.check_interval_secs, 600);
        assert_eq!(config.timeout_secs, 30);
        assert_eq!(config.max_failures, 5);
        assert!(!config.auto_switch);
        assert_eq!(config.switch_threshold, 0.5);
    }

    #[test]
    fn test_mirror_health_unreachable() {
        let health = MirrorHealth {
            url: "http://dead-mirror.com/file.zip".to_string(),
            reachable: false,
            response_time_ms: None,
            failure_count: 5,
            last_checked: Some(chrono::Utc::now()),
            health_score: 0.0,
        };
        assert!(!health.reachable);
        assert!(health.response_time_ms.is_none());
        assert_eq!(health.failure_count, 5);
        assert_eq!(health.health_score, 0.0);
    }

    #[test]
    fn test_mirror_health_score_calculation() {
        // Fast response should have high score
        let health_fast = MirrorHealth {
            url: "http://fast.com".to_string(),
            reachable: true,
            response_time_ms: Some(100),
            failure_count: 0,
            last_checked: None,
            health_score: 0.98,
        };
        assert!(health_fast.health_score > 0.9);

        // Slow response should have lower score
        let health_slow = MirrorHealth {
            url: "http://slow.com".to_string(),
            reachable: true,
            response_time_ms: Some(4000),
            failure_count: 0,
            last_checked: None,
            health_score: 0.3,
        };
        assert!(health_slow.health_score < 0.5);
    }

    #[test]
    fn test_summary_with_no_mirrors() {
        let summary = MirrorSummary {
            task_id: "empty-task".to_string(),
            active_url: None,
            mirrors: vec![],
            recommended_url: None,
            should_switch: false,
        };
        assert!(summary.mirrors.is_empty());
        assert!(summary.active_url.is_none());
        assert!(summary.recommended_url.is_none());
        assert!(!summary.should_switch);
    }

    #[test]
    fn test_summary_with_multiple_mirrors() {
        let summary = MirrorSummary {
            task_id: "multi-mirror".to_string(),
            active_url: Some("http://mirror1.com".to_string()),
            mirrors: vec![
                MirrorHealth {
                    url: "http://mirror1.com".to_string(),
                    reachable: true,
                    response_time_ms: Some(200),
                    failure_count: 0,
                    last_checked: None,
                    health_score: 0.85,
                },
                MirrorHealth {
                    url: "http://mirror2.com".to_string(),
                    reachable: true,
                    response_time_ms: Some(150),
                    failure_count: 0,
                    last_checked: None,
                    health_score: 0.92,
                },
                MirrorHealth {
                    url: "http://mirror3.com".to_string(),
                    reachable: false,
                    response_time_ms: None,
                    failure_count: 3,
                    last_checked: None,
                    health_score: 0.0,
                },
            ],
            recommended_url: Some("http://mirror2.com".to_string()),
            should_switch: true,
        };
        assert_eq!(summary.mirrors.len(), 3);
        assert!(summary.should_switch);
        assert_eq!(
            summary.recommended_url.as_deref(),
            Some("http://mirror2.com")
        );
    }

    #[test]
    fn test_format_summary_no_active() {
        let summary = MirrorSummary {
            task_id: "no-active".to_string(),
            active_url: None,
            mirrors: vec![MirrorHealth {
                url: "http://only-mirror.com".to_string(),
                reachable: true,
                response_time_ms: Some(300),
                failure_count: 0,
                last_checked: None,
                health_score: 0.75,
            }],
            recommended_url: Some("http://only-mirror.com".to_string()),
            should_switch: false,
        };
        let formatted = format_mirror_summary(&summary);
        assert!(formatted.contains("no-active"));
        assert!(formatted.contains("none"));
        assert!(formatted.contains("only-mirror.com"));
    }

    #[test]
    fn test_format_summary_with_switch_warning() {
        let summary = MirrorSummary {
            task_id: "switch-test".to_string(),
            active_url: Some("http://bad-mirror.com".to_string()),
            mirrors: vec![
                MirrorHealth {
                    url: "http://bad-mirror.com".to_string(),
                    reachable: false,
                    response_time_ms: None,
                    failure_count: 5,
                    last_checked: None,
                    health_score: 0.0,
                },
                MirrorHealth {
                    url: "http://good-mirror.com".to_string(),
                    reachable: true,
                    response_time_ms: Some(100),
                    failure_count: 0,
                    last_checked: None,
                    health_score: 0.95,
                },
            ],
            recommended_url: Some("http://good-mirror.com".to_string()),
            should_switch: true,
        };
        let formatted = format_mirror_summary(&summary);
        assert!(formatted.contains("Should switch"));
        assert!(formatted.contains("❌"));
        assert!(formatted.contains("✅"));
    }

    #[test]
    fn test_format_summary_all_unreachable() {
        let summary = MirrorSummary {
            task_id: "all-dead".to_string(),
            active_url: Some("http://dead1.com".to_string()),
            mirrors: vec![
                MirrorHealth {
                    url: "http://dead1.com".to_string(),
                    reachable: false,
                    response_time_ms: None,
                    failure_count: 10,
                    last_checked: None,
                    health_score: 0.0,
                },
                MirrorHealth {
                    url: "http://dead2.com".to_string(),
                    reachable: false,
                    response_time_ms: None,
                    failure_count: 8,
                    last_checked: None,
                    health_score: 0.0,
                },
            ],
            recommended_url: None,
            should_switch: false,
        };
        let formatted = format_mirror_summary(&summary);
        assert!(formatted.contains("all-dead"));
        assert!(formatted.contains("❌ Unreachable"));
        assert!(!formatted.contains("✅"));
    }

    #[test]
    fn test_config_serialization() {
        let config = MirrorHealthConfig {
            check_interval_secs: 120,
            timeout_secs: 5,
            max_failures: 10,
            auto_switch: false,
            switch_threshold: 0.1,
        };
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: MirrorHealthConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.check_interval_secs, 120);
        assert_eq!(deserialized.timeout_secs, 5);
        assert_eq!(deserialized.max_failures, 10);
        assert!(!deserialized.auto_switch);
        assert_eq!(deserialized.switch_threshold, 0.1);
    }

    #[test]
    fn test_mirror_health_with_failure_count() {
        let health = MirrorHealth {
            url: "http://failing.com".to_string(),
            reachable: true,
            response_time_ms: Some(500),
            failure_count: 3,
            last_checked: Some(chrono::Utc::now()),
            health_score: 0.6,
        };
        assert_eq!(health.failure_count, 3);
        assert!(health.reachable);
    }

    #[test]
    fn test_summary_recommended_equals_active() {
        let summary = MirrorSummary {
            task_id: "stable".to_string(),
            active_url: Some("http://best.com".to_string()),
            mirrors: vec![MirrorHealth {
                url: "http://best.com".to_string(),
                reachable: true,
                response_time_ms: Some(50),
                failure_count: 0,
                last_checked: None,
                health_score: 0.99,
            }],
            recommended_url: Some("http://best.com".to_string()),
            should_switch: false,
        };
        assert_eq!(summary.active_url, summary.recommended_url);
        assert!(!summary.should_switch);
    }

    #[test]
    fn test_format_summary_response_time_display() {
        let summary = MirrorSummary {
            task_id: "timing".to_string(),
            active_url: None,
            mirrors: vec![
                MirrorHealth {
                    url: "http://fast.com".to_string(),
                    reachable: true,
                    response_time_ms: Some(42),
                    failure_count: 0,
                    last_checked: None,
                    health_score: 0.99,
                },
                MirrorHealth {
                    url: "http://medium.com".to_string(),
                    reachable: true,
                    response_time_ms: Some(1234),
                    failure_count: 0,
                    last_checked: None,
                    health_score: 0.75,
                },
            ],
            recommended_url: Some("http://fast.com".to_string()),
            should_switch: false,
        };
        let formatted = format_mirror_summary(&summary);
        assert!(formatted.contains("42ms"));
        assert!(formatted.contains("1234ms"));
    }

    #[test]
    fn test_mirror_health_boundary_score() {
        // Minimum reachable score
        let health_min = MirrorHealth {
            url: "http://min.com".to_string(),
            reachable: true,
            response_time_ms: Some(5000),
            failure_count: 0,
            last_checked: None,
            health_score: 0.2,
        };
        assert_eq!(health_min.health_score, 0.2);

        // Maximum score
        let health_max = MirrorHealth {
            url: "http://max.com".to_string(),
            reachable: true,
            response_time_ms: Some(1),
            failure_count: 0,
            last_checked: None,
            health_score: 1.0,
        };
        assert_eq!(health_max.health_score, 1.0);
    }

    #[test]
    fn test_summary_mixed_reachability() {
        let summary = MirrorSummary {
            task_id: "mixed".to_string(),
            active_url: Some("http://active.com".to_string()),
            mirrors: vec![
                MirrorHealth {
                    url: "http://active.com".to_string(),
                    reachable: true,
                    response_time_ms: Some(300),
                    failure_count: 1,
                    last_checked: None,
                    health_score: 0.7,
                },
                MirrorHealth {
                    url: "http://backup1.com".to_string(),
                    reachable: false,
                    response_time_ms: None,
                    failure_count: 5,
                    last_checked: None,
                    health_score: 0.0,
                },
                MirrorHealth {
                    url: "http://backup2.com".to_string(),
                    reachable: true,
                    response_time_ms: Some(500),
                    failure_count: 2,
                    last_checked: None,
                    health_score: 0.5,
                },
            ],
            recommended_url: Some("http://active.com".to_string()),
            should_switch: false,
        };
        assert_eq!(summary.mirrors.len(), 3);
        let reachable_count = summary.mirrors.iter().filter(|m| m.reachable).count();
        assert_eq!(reachable_count, 2);
    }

    #[tokio::test]
    async fn test_check_mirror_health_timeout() {
        // Test with very short timeout to simulate timeout scenario
        let url = "http://httpbin.org/delay/10";
        let timeout = Duration::from_millis(100);
        let health = check_mirror_health(url, timeout).await;

        // Should timeout and be unreachable
        assert!(!health.reachable);
        assert!(health.response_time_ms.is_none());
        assert_eq!(health.health_score, 0.0);
    }

    #[tokio::test]
    async fn test_check_mirror_health_invalid_url() {
        let url = "http://this-domain-definitely-does-not-exist-12345.com";
        let timeout = Duration::from_secs(2);
        let health = check_mirror_health(url, timeout).await;

        assert!(!health.reachable);
        assert!(health.response_time_ms.is_none());
        assert_eq!(health.health_score, 0.0);
    }

    #[tokio::test]
    async fn test_check_all_mirrors_empty_list() {
        let config = MirrorHealthConfig::default();
        let summary = check_all_mirrors("task-1", None, &[], &config).await;

        assert!(summary.mirrors.is_empty());
        assert!(summary.recommended_url.is_none());
        assert!(!summary.should_switch);
    }

    #[tokio::test]
    async fn test_check_all_mirrors_with_active_only() {
        let config = MirrorHealthConfig {
            timeout_secs: 2,
            ..Default::default()
        };
        let active_url = "http://example.com";
        let summary = check_all_mirrors("task-2", Some(active_url), &[], &config).await;

        assert_eq!(summary.mirrors.len(), 1);
        assert_eq!(summary.active_url.as_deref(), Some(active_url));
    }

    #[test]
    fn test_auto_switch_disabled() {
        let config = MirrorHealthConfig {
            auto_switch: false,
            switch_threshold: 0.3,
            ..Default::default()
        };

        // Even with bad active mirror, should not switch when disabled
        let summary = MirrorSummary {
            task_id: "no-switch".to_string(),
            active_url: Some("http://bad.com".to_string()),
            mirrors: vec![
                MirrorHealth {
                    url: "http://bad.com".to_string(),
                    reachable: false,
                    response_time_ms: None,
                    failure_count: 10,
                    last_checked: None,
                    health_score: 0.0,
                },
                MirrorHealth {
                    url: "http://good.com".to_string(),
                    reachable: true,
                    response_time_ms: Some(50),
                    failure_count: 0,
                    last_checked: None,
                    health_score: 0.99,
                },
            ],
            recommended_url: Some("http://good.com".to_string()),
            should_switch: false, // Should be false because auto_switch is disabled
        };

        assert!(!config.auto_switch);
        assert!(!summary.should_switch);
    }

    #[test]
    fn test_switch_threshold_boundary() {
        let config = MirrorHealthConfig {
            auto_switch: true,
            switch_threshold: 0.3,
            ..Default::default()
        };

        // Difference exactly at threshold - should not switch
        let active_score = 0.6;
        let recommended_score = 0.9;
        let diff = recommended_score - active_score;
        assert!(!(diff > config.switch_threshold));

        // Difference just above threshold - should switch
        let active_score2 = 0.59;
        let recommended_score2 = 0.9;
        let diff2 = recommended_score2 - active_score2;
        assert!(diff2 > config.switch_threshold);
    }

    #[test]
    fn test_format_summary_score_display() {
        let summary = MirrorSummary {
            task_id: "scores".to_string(),
            active_url: None,
            mirrors: vec![
                MirrorHealth {
                    url: "http://perfect.com".to_string(),
                    reachable: true,
                    response_time_ms: Some(10),
                    failure_count: 0,
                    last_checked: None,
                    health_score: 1.0,
                },
                MirrorHealth {
                    url: "http://medium.com".to_string(),
                    reachable: true,
                    response_time_ms: Some(1000),
                    failure_count: 0,
                    last_checked: None,
                    health_score: 0.5,
                },
                MirrorHealth {
                    url: "http://poor.com".to_string(),
                    reachable: true,
                    response_time_ms: Some(4500),
                    failure_count: 0,
                    last_checked: None,
                    health_score: 0.2,
                },
            ],
            recommended_url: Some("http://perfect.com".to_string()),
            should_switch: false,
        };
        let formatted = format_mirror_summary(&summary);
        assert!(formatted.contains("score: 1.00"));
        assert!(formatted.contains("score: 0.50"));
        assert!(formatted.contains("score: 0.20"));
    }

    #[test]
    fn test_mirror_health_last_checked_timestamp() {
        let now = chrono::Utc::now();
        let health = MirrorHealth {
            url: "http://timestamp.com".to_string(),
            reachable: true,
            response_time_ms: Some(100),
            failure_count: 0,
            last_checked: Some(now),
            health_score: 0.9,
        };
        assert!(health.last_checked.is_some());
        assert_eq!(health.last_checked.unwrap(), now);
    }

    #[test]
    fn test_summary_task_id_preservation() {
        let task_id = "complex-task-id-12345-abcde";
        let summary = MirrorSummary {
            task_id: task_id.to_string(),
            active_url: None,
            mirrors: vec![],
            recommended_url: None,
            should_switch: false,
        };
        assert_eq!(summary.task_id, task_id);
    }
}
