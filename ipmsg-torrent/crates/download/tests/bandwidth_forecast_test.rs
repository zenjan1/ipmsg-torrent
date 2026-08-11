//! Tests for bandwidth forecast CLI and REST API integration (Phase 136)

use ipmsg_download::{DownloadManager, bandwidth_forecast::ForecastConfig};
use tempfile::TempDir;

#[tokio::test]
async fn test_bandwidth_forecast_config_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().to_path_buf();

    let dm = DownloadManager::new(data_dir.clone());

    // Set custom config
    let config = ForecastConfig {
        enabled: true,
        min_samples: 10,
        max_samples: 500,
        trend_window_secs: 600,
        high_confidence_threshold: 0.8,
        medium_confidence_threshold: 0.5,
    };

    dm.set_bandwidth_forecast_config(config.clone()).await;

    // Verify config is set
    let retrieved = dm.get_bandwidth_forecast_config().await;
    assert_eq!(retrieved.enabled, config.enabled);
    assert_eq!(retrieved.min_samples, config.min_samples);
    assert_eq!(retrieved.max_samples, config.max_samples);
    assert_eq!(retrieved.trend_window_secs, config.trend_window_secs);
}

#[tokio::test]
async fn test_bandwidth_forecast_summary_empty() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().to_path_buf();

    let dm = DownloadManager::new(data_dir.clone());

    let summary = dm.get_bandwidth_forecast_summary().await;
    assert_eq!(summary.total_domains, 0);
    assert_eq!(summary.high_confidence_count, 0);
    assert_eq!(summary.avg_predicted_speed_bps, 0.0);
    assert!(summary.forecasts.is_empty());
}

#[tokio::test]
async fn test_bandwidth_forecast_predict_no_data() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().to_path_buf();

    let dm = DownloadManager::new(data_dir.clone());

    let forecast = dm.forecast_bandwidth("example.com").await;
    assert_eq!(forecast.key, "example.com");
    assert_eq!(forecast.predicted_speed_bps, 0.0);
    assert_eq!(forecast.sample_count, 0);
}

#[tokio::test]
async fn test_bandwidth_forecast_clear_domain() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().to_path_buf();

    let dm = DownloadManager::new(data_dir.clone());

    // This should not panic even if domain doesn't exist
    dm.clear_bandwidth_forecast_domain("nonexistent.com").await;
}

#[tokio::test]
async fn test_bandwidth_forecast_clear_all() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().to_path_buf();

    let dm = DownloadManager::new(data_dir.clone());

    // This should not panic even if no data exists
    dm.clear_bandwidth_forecast().await;
}

#[tokio::test]
async fn test_bandwidth_forecast_eta_no_data() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().to_path_buf();

    let dm = DownloadManager::new(data_dir.clone());

    let eta = dm.estimate_download_eta("example.com", 1_000_000).await;
    assert!(eta.is_none());
}

#[tokio::test]
async fn test_bandwidth_forecast_config_disabled() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().to_path_buf();

    let dm = DownloadManager::new(data_dir.clone());

    let config = ForecastConfig {
        enabled: false,
        min_samples: 5,
        max_samples: 200,
        trend_window_secs: 300,
        high_confidence_threshold: 0.7,
        medium_confidence_threshold: 0.4,
    };

    dm.set_bandwidth_forecast_config(config).await;

    let retrieved = dm.get_bandwidth_forecast_config().await;
    assert!(!retrieved.enabled);
}

#[tokio::test]
async fn test_bandwidth_forecast_multiple_domains() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().to_path_buf();

    let dm = DownloadManager::new(data_dir.clone());

    // Query multiple domains - should all return empty forecasts
    let forecast1 = dm.forecast_bandwidth("domain1.com").await;
    let forecast2 = dm.forecast_bandwidth("domain2.com").await;
    let forecast3 = dm.forecast_bandwidth("domain3.com").await;

    assert_eq!(forecast1.key, "domain1.com");
    assert_eq!(forecast2.key, "domain2.com");
    assert_eq!(forecast3.key, "domain3.com");

    // Summary should still be empty
    let summary = dm.get_bandwidth_forecast_summary().await;
    assert_eq!(summary.total_domains, 0);
}
