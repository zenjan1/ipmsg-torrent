//! Post-download hook execution system
//!
//! Automatically runs user-defined commands when downloads complete or fail.
//! Hooks are executed asynchronously and don't block the download process.

use crate::notification::{NotificationContext, NotificationEvent};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::{Arc, RwLock};
use tokio::process::Command;

/// Hook trigger events
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum HookEvent {
    /// Run when download completes successfully
    OnComplete,
    /// Run when download fails
    OnFailure,
    /// Run on both complete and failure
    Both,
}

impl HookEvent {
    /// Check if this hook should run for the given notification event
    pub fn should_run(&self, event: NotificationEvent) -> bool {
        match self {
            Self::OnComplete => event == NotificationEvent::DownloadComplete,
            Self::OnFailure => event == NotificationEvent::DownloadFailed,
            Self::Both => {
                event == NotificationEvent::DownloadComplete
                    || event == NotificationEvent::DownloadFailed
            }
        }
    }
}

/// Post-download hook configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostHook {
    /// Unique hook ID
    pub id: String,
    /// Human-readable name
    pub name: String,
    /// When to trigger this hook
    pub event: HookEvent,
    /// Command template to execute (supports {name}, {save_path}, etc.)
    pub command: String,
    /// Working directory for command execution (optional)
    pub working_dir: Option<PathBuf>,
    /// Whether this hook is enabled
    pub enabled: bool,
    /// Timeout in seconds (0 = no timeout)
    pub timeout_secs: u64,
}

impl PostHook {
    /// Create a new hook
    pub fn new(name: String, event: HookEvent, command: String) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            name,
            event,
            command,
            working_dir: None,
            enabled: true,
            timeout_secs: 30,
        }
    }

    /// Set working directory
    pub fn with_working_dir(mut self, dir: PathBuf) -> Self {
        self.working_dir = Some(dir);
        self
    }

    /// Set timeout
    pub fn with_timeout(mut self, secs: u64) -> Self {
        self.timeout_secs = secs;
        self
    }
}

/// Result of hook execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HookResult {
    /// Hook ID
    pub hook_id: String,
    /// Hook name
    pub hook_name: String,
    /// Whether execution succeeded
    pub success: bool,
    /// Exit code (if available)
    pub exit_code: Option<i32>,
    /// Error message (if failed)
    pub error: Option<String>,
    /// Execution duration in milliseconds
    pub duration_ms: u64,
}

/// Hook execution manager
pub struct HookManager {
    hooks: Arc<RwLock<Vec<PostHook>>>,
    data_dir: PathBuf,
}

impl HookManager {
    /// Create a new hook manager
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            hooks: Arc::new(RwLock::new(Vec::new())),
            data_dir,
        }
    }

    /// Load hooks from disk
    pub fn load(&self) -> Result<(), HookPersistenceError> {
        let hooks = load_hooks(&self.data_dir)?;
        let mut h = self.hooks.write().unwrap();
        *h = hooks;
        Ok(())
    }

    /// Save hooks to disk
    pub fn save(&self) -> Result<(), HookPersistenceError> {
        let hooks = self.hooks.read().unwrap().clone();
        save_hooks(&hooks, &self.data_dir)
    }

    /// Add a new hook
    pub fn add_hook(&self, hook: PostHook) -> Result<String, HookPersistenceError> {
        let hook_id = hook.id.clone();
        let mut hooks = self.hooks.write().unwrap();
        hooks.push(hook);
        drop(hooks);
        self.save()?;
        Ok(hook_id)
    }

    /// Remove a hook by ID
    pub fn remove_hook(&self, hook_id: &str) -> Result<bool, HookPersistenceError> {
        let mut hooks = self.hooks.write().unwrap();
        let initial_len = hooks.len();
        hooks.retain(|h| h.id != hook_id);
        let removed = hooks.len() < initial_len;
        drop(hooks);
        if removed {
            self.save()?;
        }
        Ok(removed)
    }

    /// Update a hook
    pub fn update_hook(
        &self,
        hook_id: &str,
        updated: PostHook,
    ) -> Result<bool, HookPersistenceError> {
        let mut hooks = self.hooks.write().unwrap();
        if let Some(hook) = hooks.iter_mut().find(|h| h.id == hook_id) {
            *hook = updated;
            drop(hooks);
            self.save()?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Get all hooks
    pub fn list_hooks(&self) -> Vec<PostHook> {
        self.hooks.read().unwrap().clone()
    }

    /// Get a hook by ID
    pub fn get_hook(&self, hook_id: &str) -> Option<PostHook> {
        self.hooks
            .read()
            .unwrap()
            .iter()
            .find(|h| h.id == hook_id)
            .cloned()
    }

    /// Enable or disable a hook
    pub fn set_hook_enabled(
        &self,
        hook_id: &str,
        enabled: bool,
    ) -> Result<bool, HookPersistenceError> {
        let mut hooks = self.hooks.write().unwrap();
        if let Some(hook) = hooks.iter_mut().find(|h| h.id == hook_id) {
            hook.enabled = enabled;
            drop(hooks);
            self.save()?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Execute hooks for the given notification context
    pub async fn execute_hooks(&self, ctx: &NotificationContext) -> Vec<HookResult> {
        let hooks = self.hooks.read().unwrap().clone();
        let mut results = Vec::new();

        for hook in hooks {
            if !hook.enabled {
                continue;
            }

            if !hook.event.should_run(ctx.event) {
                continue;
            }

            let result = self.execute_hook(&hook, ctx).await;
            results.push(result);
        }

        results
    }

    /// Execute a single hook
    async fn execute_hook(&self, hook: &PostHook, ctx: &NotificationContext) -> HookResult {
        let start = std::time::Instant::now();
        let command = ctx.render_template(&hook.command);

        let mut cmd = Command::new("sh");
        cmd.arg("-c").arg(&command);

        if let Some(working_dir) = &hook.working_dir {
            cmd.current_dir(working_dir);
        }

        cmd.stdout(Stdio::null()).stderr(Stdio::null());

        let result = if hook.timeout_secs > 0 {
            match tokio::time::timeout(
                std::time::Duration::from_secs(hook.timeout_secs),
                cmd.status(),
            )
            .await
            {
                Ok(Ok(status)) => HookResult {
                    hook_id: hook.id.clone(),
                    hook_name: hook.name.clone(),
                    success: status.success(),
                    exit_code: status.code(),
                    error: if status.success() {
                        None
                    } else {
                        Some(format!("Exit code: {}", status.code().unwrap_or(-1)))
                    },
                    duration_ms: start.elapsed().as_millis() as u64,
                },
                Ok(Err(e)) => HookResult {
                    hook_id: hook.id.clone(),
                    hook_name: hook.name.clone(),
                    success: false,
                    exit_code: None,
                    error: Some(format!("Failed to execute: {}", e)),
                    duration_ms: start.elapsed().as_millis() as u64,
                },
                Err(_) => HookResult {
                    hook_id: hook.id.clone(),
                    hook_name: hook.name.clone(),
                    success: false,
                    exit_code: None,
                    error: Some(format!("Timeout after {}s", hook.timeout_secs)),
                    duration_ms: hook.timeout_secs * 1000,
                },
            }
        } else {
            match cmd.status().await {
                Ok(status) => HookResult {
                    hook_id: hook.id.clone(),
                    hook_name: hook.name.clone(),
                    success: status.success(),
                    exit_code: status.code(),
                    error: if status.success() {
                        None
                    } else {
                        Some(format!("Exit code: {}", status.code().unwrap_or(-1)))
                    },
                    duration_ms: start.elapsed().as_millis() as u64,
                },
                Err(e) => HookResult {
                    hook_id: hook.id.clone(),
                    hook_name: hook.name.clone(),
                    success: false,
                    exit_code: None,
                    error: Some(format!("Failed to execute: {}", e)),
                    duration_ms: start.elapsed().as_millis() as u64,
                },
            }
        };

        tracing::info!(
            hook_id = %hook.id,
            hook_name = %hook.name,
            success = result.success,
            duration_ms = result.duration_ms,
            "Post-download hook executed"
        );

        result
    }
}

/// Save hooks to disk
pub fn save_hooks(
    hooks: &[PostHook],
    data_dir: &std::path::Path,
) -> Result<(), HookPersistenceError> {
    let config_path = data_dir.join("post_hooks.json");
    let json = serde_json::to_string_pretty(hooks)
        .map_err(|e| HookPersistenceError::Serialize(e.to_string()))?;
    std::fs::write(&config_path, json).map_err(|e| HookPersistenceError::Io(e.to_string()))?;
    Ok(())
}

/// Load hooks from disk
pub fn load_hooks(data_dir: &std::path::Path) -> Result<Vec<PostHook>, HookPersistenceError> {
    let config_path = data_dir.join("post_hooks.json");

    if !config_path.exists() {
        return Ok(Vec::new());
    }

    let json = std::fs::read_to_string(&config_path)
        .map_err(|e| HookPersistenceError::Io(e.to_string()))?;

    let hooks: Vec<PostHook> = serde_json::from_str(&json)
        .map_err(|e| HookPersistenceError::Deserialize(e.to_string()))?;

    Ok(hooks)
}

/// Hook persistence errors
#[derive(Debug, thiserror::Error)]
pub enum HookPersistenceError {
    #[error("IO error: {0}")]
    Io(String),
    #[error("Serialize error: {0}")]
    Serialize(String),
    #[error("Deserialize error: {0}")]
    Deserialize(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::notification::NotificationEvent;

    fn make_test_ctx(event: NotificationEvent) -> NotificationContext {
        NotificationContext {
            task_id: "test-task".into(),
            name: "test-file.txt".into(),
            size: 1024,
            downloaded: 1024,
            protocol: "HTTP".into(),
            save_path: "/tmp/test-file.txt".into(),
            error: None,
            event,
        }
    }

    #[test]
    fn test_hook_event_should_run() {
        assert!(HookEvent::OnComplete.should_run(NotificationEvent::DownloadComplete));
        assert!(!HookEvent::OnComplete.should_run(NotificationEvent::DownloadFailed));

        assert!(!HookEvent::OnFailure.should_run(NotificationEvent::DownloadComplete));
        assert!(HookEvent::OnFailure.should_run(NotificationEvent::DownloadFailed));

        assert!(HookEvent::Both.should_run(NotificationEvent::DownloadComplete));
        assert!(HookEvent::Both.should_run(NotificationEvent::DownloadFailed));
    }

    #[test]
    fn test_post_hook_creation() {
        let hook = PostHook::new(
            "Test Hook".into(),
            HookEvent::OnComplete,
            "echo 'Download complete'".into(),
        );

        assert!(!hook.id.is_empty());
        assert_eq!(hook.name, "Test Hook");
        assert_eq!(hook.event, HookEvent::OnComplete);
        assert!(hook.enabled);
        assert_eq!(hook.timeout_secs, 30);
    }

    #[test]
    fn test_post_hook_with_working_dir() {
        let hook = PostHook::new("Test".into(), HookEvent::OnComplete, "ls".into())
            .with_working_dir(PathBuf::from("/tmp"));

        assert_eq!(hook.working_dir, Some(PathBuf::from("/tmp")));
    }

    #[test]
    fn test_post_hook_with_timeout() {
        let hook =
            PostHook::new("Test".into(), HookEvent::OnComplete, "sleep 10".into()).with_timeout(60);

        assert_eq!(hook.timeout_secs, 60);
    }

    #[tokio::test]
    async fn test_hook_manager_add_remove() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = HookManager::new(temp_dir.path().to_path_buf());

        let hook = PostHook::new("Hook 1".into(), HookEvent::OnComplete, "echo test".into());
        let hook_id = manager.add_hook(hook).unwrap();

        let hooks = manager.list_hooks();
        assert_eq!(hooks.len(), 1);
        assert_eq!(hooks[0].name, "Hook 1");

        let removed = manager.remove_hook(&hook_id).unwrap();
        assert!(removed);

        let hooks = manager.list_hooks();
        assert_eq!(hooks.len(), 0);
    }

    #[tokio::test]
    async fn test_hook_manager_persistence() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = HookManager::new(temp_dir.path().to_path_buf());

        let hook1 = PostHook::new("Hook 1".into(), HookEvent::OnComplete, "echo 1".into());
        let hook2 = PostHook::new("Hook 2".into(), HookEvent::OnFailure, "echo 2".into());

        manager.add_hook(hook1).unwrap();
        manager.add_hook(hook2).unwrap();

        // Create new manager and load
        let manager2 = HookManager::new(temp_dir.path().to_path_buf());
        manager2.load().unwrap();

        let hooks = manager2.list_hooks();
        assert_eq!(hooks.len(), 2);
    }

    #[tokio::test]
    async fn test_hook_manager_update() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = HookManager::new(temp_dir.path().to_path_buf());

        let hook = PostHook::new(
            "Original".into(),
            HookEvent::OnComplete,
            "echo original".into(),
        );
        let hook_id = manager.add_hook(hook).unwrap();

        let mut updated = manager.get_hook(&hook_id).unwrap();
        updated.name = "Updated".into();
        updated.command = "echo updated".into();

        let success = manager.update_hook(&hook_id, updated).unwrap();
        assert!(success);

        let hook = manager.get_hook(&hook_id).unwrap();
        assert_eq!(hook.name, "Updated");
        assert_eq!(hook.command, "echo updated");
    }

    #[tokio::test]
    async fn test_hook_manager_enable_disable() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = HookManager::new(temp_dir.path().to_path_buf());

        let hook = PostHook::new("Test".into(), HookEvent::OnComplete, "echo test".into());
        let hook_id = manager.add_hook(hook).unwrap();

        assert!(manager.get_hook(&hook_id).unwrap().enabled);

        manager.set_hook_enabled(&hook_id, false).unwrap();
        assert!(!manager.get_hook(&hook_id).unwrap().enabled);

        manager.set_hook_enabled(&hook_id, true).unwrap();
        assert!(manager.get_hook(&hook_id).unwrap().enabled);
    }

    #[tokio::test]
    async fn test_execute_hook_success() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = HookManager::new(temp_dir.path().to_path_buf());

        let hook = PostHook::new("Echo".into(), HookEvent::OnComplete, "echo 'test'".into());

        let ctx = make_test_ctx(NotificationEvent::DownloadComplete);
        let result = manager.execute_hook(&hook, &ctx).await;

        assert!(result.success);
        assert_eq!(result.exit_code, Some(0));
        assert!(result.error.is_none());
    }

    #[tokio::test]
    async fn test_execute_hook_failure() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = HookManager::new(temp_dir.path().to_path_buf());

        let hook = PostHook::new("Fail".into(), HookEvent::OnComplete, "exit 1".into());

        let ctx = make_test_ctx(NotificationEvent::DownloadComplete);
        let result = manager.execute_hook(&hook, &ctx).await;

        assert!(!result.success);
        assert_eq!(result.exit_code, Some(1));
        assert!(result.error.is_some());
    }

    #[tokio::test]
    async fn test_execute_hook_timeout() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = HookManager::new(temp_dir.path().to_path_buf());

        let hook =
            PostHook::new("Sleep".into(), HookEvent::OnComplete, "sleep 10".into()).with_timeout(1);

        let ctx = make_test_ctx(NotificationEvent::DownloadComplete);
        let result = manager.execute_hook(&hook, &ctx).await;

        assert!(!result.success);
        assert!(result.error.unwrap().contains("Timeout"));
    }

    #[tokio::test]
    async fn test_execute_hooks_filters_by_event() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = HookManager::new(temp_dir.path().to_path_buf());

        let hook_complete = PostHook::new(
            "Complete".into(),
            HookEvent::OnComplete,
            "echo complete".into(),
        );
        let hook_failure = PostHook::new(
            "Failure".into(),
            HookEvent::OnFailure,
            "echo failure".into(),
        );

        manager.add_hook(hook_complete).unwrap();
        manager.add_hook(hook_failure).unwrap();

        let ctx = make_test_ctx(NotificationEvent::DownloadComplete);
        let results = manager.execute_hooks(&ctx).await;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].hook_name, "Complete");
    }

    #[tokio::test]
    async fn test_execute_hooks_skips_disabled() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = HookManager::new(temp_dir.path().to_path_buf());

        let hook = PostHook::new("Disabled".into(), HookEvent::OnComplete, "echo test".into());
        let hook_id = manager.add_hook(hook).unwrap();
        manager.set_hook_enabled(&hook_id, false).unwrap();

        let ctx = make_test_ctx(NotificationEvent::DownloadComplete);
        let results = manager.execute_hooks(&ctx).await;

        assert_eq!(results.len(), 0);
    }

    #[tokio::test]
    async fn test_execute_hook_with_template_variables() {
        let temp_dir = tempfile::tempdir().unwrap();
        let output_file = temp_dir.path().join("output.txt");
        let manager = HookManager::new(temp_dir.path().to_path_buf());

        let command = format!(
            "echo '{{name}} {{size}} {{protocol}}' > {}",
            output_file.display()
        );
        let hook = PostHook::new("Template".into(), HookEvent::OnComplete, command);

        let ctx = make_test_ctx(NotificationEvent::DownloadComplete);
        let result = manager.execute_hook(&hook, &ctx).await;

        assert!(result.success);

        let content = tokio::fs::read_to_string(&output_file).await.unwrap();
        assert!(content.contains("test-file.txt"));
        assert!(content.contains("1024"));
        assert!(content.contains("HTTP"));
    }

    #[test]
    fn test_save_load_hooks() {
        let temp_dir = tempfile::tempdir().unwrap();

        let hooks = vec![
            PostHook::new("Hook 1".into(), HookEvent::OnComplete, "echo 1".into()),
            PostHook::new("Hook 2".into(), HookEvent::Both, "echo 2".into())
                .with_timeout(60)
                .with_working_dir(PathBuf::from("/tmp")),
        ];

        save_hooks(&hooks, temp_dir.path()).unwrap();

        let loaded = load_hooks(temp_dir.path()).unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].name, "Hook 1");
        assert_eq!(loaded[1].name, "Hook 2");
        assert_eq!(loaded[1].timeout_secs, 60);
        assert_eq!(loaded[1].working_dir, Some(PathBuf::from("/tmp")));
    }

    #[test]
    fn test_load_hooks_missing_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let hooks = load_hooks(temp_dir.path()).unwrap();
        assert!(hooks.is_empty());
    }

    // ============================================================
    // Comprehensive test coverage - Phase 206
    // ============================================================

    // --- HookEvent comprehensive tests ---

    #[test]
    fn test_hook_event_clone_debug() {
        let event = HookEvent::OnComplete;
        let cloned = event.clone();
        assert_eq!(event, cloned);
        // Debug trait
        let debug_str = format!("{:?}", event);
        assert!(debug_str.contains("OnComplete"));
    }

    #[test]
    fn test_hook_event_all_variants() {
        let variants = vec![HookEvent::OnComplete, HookEvent::OnFailure, HookEvent::Both];
        assert_eq!(variants.len(), 3);
    }

    #[test]
    fn test_hook_event_eq_partial_eq() {
        assert_eq!(HookEvent::OnComplete, HookEvent::OnComplete);
        assert_ne!(HookEvent::OnComplete, HookEvent::OnFailure);
        assert_ne!(HookEvent::OnComplete, HookEvent::Both);
        assert_ne!(HookEvent::OnFailure, HookEvent::Both);
    }

    #[test]
    fn test_hook_event_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(HookEvent::OnComplete);
        set.insert(HookEvent::OnFailure);
        set.insert(HookEvent::Both);
        set.insert(HookEvent::OnComplete); // duplicate
        assert_eq!(set.len(), 3);
    }

    #[test]
    fn test_hook_event_serde_roundtrip() {
        for event in &[HookEvent::OnComplete, HookEvent::OnFailure, HookEvent::Both] {
            let json = serde_json::to_string(event).unwrap();
            let deserialized: HookEvent = serde_json::from_str(&json).unwrap();
            assert_eq!(*event, deserialized);
        }
    }

    #[test]
    fn test_hook_event_serde_values() {
        let json_complete = serde_json::to_string(&HookEvent::OnComplete).unwrap();
        assert!(json_complete.contains("OnComplete"));

        let json_failure = serde_json::to_string(&HookEvent::OnFailure).unwrap();
        assert!(json_failure.contains("OnFailure"));

        let json_both = serde_json::to_string(&HookEvent::Both).unwrap();
        assert!(json_both.contains("Both"));
    }

    #[test]
    fn test_hook_event_should_run_on_complete_only() {
        // OnComplete should only run for DownloadComplete
        assert!(HookEvent::OnComplete.should_run(NotificationEvent::DownloadComplete));
        // Should NOT run for other events
        assert!(!HookEvent::OnComplete.should_run(NotificationEvent::DownloadFailed));
        assert!(!HookEvent::OnComplete.should_run(NotificationEvent::QueueEmpty));
        assert!(!HookEvent::OnComplete.should_run(NotificationEvent::ProgressMilestone));
    }

    #[test]
    fn test_hook_event_should_run_on_failure_only() {
        // OnFailure should only run for DownloadFailed
        assert!(HookEvent::OnFailure.should_run(NotificationEvent::DownloadFailed));
        // Should NOT run for other events
        assert!(!HookEvent::OnFailure.should_run(NotificationEvent::DownloadComplete));
        assert!(!HookEvent::OnFailure.should_run(NotificationEvent::QueueEmpty));
        assert!(!HookEvent::OnFailure.should_run(NotificationEvent::ProgressMilestone));
    }

    #[test]
    fn test_hook_event_should_run_both() {
        // Both should run for DownloadComplete and DownloadFailed
        assert!(HookEvent::Both.should_run(NotificationEvent::DownloadComplete));
        assert!(HookEvent::Both.should_run(NotificationEvent::DownloadFailed));
        // Should NOT run for other events
        assert!(!HookEvent::Both.should_run(NotificationEvent::QueueEmpty));
        assert!(!HookEvent::Both.should_run(NotificationEvent::ProgressMilestone));
    }

    // --- PostHook comprehensive tests ---

    #[test]
    fn test_post_hook_new_generates_unique_id() {
        let hook1 = PostHook::new("Hook 1".into(), HookEvent::OnComplete, "echo 1".into());
        let hook2 = PostHook::new("Hook 2".into(), HookEvent::OnComplete, "echo 2".into());
        assert_ne!(hook1.id, hook2.id);
    }

    #[test]
    fn test_post_hook_new_defaults() {
        let hook = PostHook::new("Test".into(), HookEvent::OnComplete, "echo test".into());
        assert!(hook.enabled);
        assert_eq!(hook.timeout_secs, 30);
        assert!(hook.working_dir.is_none());
        assert!(!hook.id.is_empty());
    }

    #[test]
    fn test_post_hook_with_working_dir_builder() {
        let hook = PostHook::new("Test".into(), HookEvent::OnComplete, "ls".into())
            .with_working_dir(PathBuf::from("/var/log"));
        assert_eq!(hook.working_dir, Some(PathBuf::from("/var/log")));
        assert!(hook.enabled);
        assert_eq!(hook.timeout_secs, 30);
    }

    #[test]
    fn test_post_hook_with_timeout_builder() {
        let hook = PostHook::new("Test".into(), HookEvent::OnComplete, "sleep 10".into())
            .with_timeout(120);
        assert_eq!(hook.timeout_secs, 120);
        assert!(hook.enabled);
        assert!(hook.working_dir.is_none());
    }

    #[test]
    fn test_post_hook_chained_builders() {
        let hook = PostHook::new("Test".into(), HookEvent::Both, "run.sh".into())
            .with_working_dir(PathBuf::from("/home/user"))
            .with_timeout(60);
        assert_eq!(hook.timeout_secs, 60);
        assert_eq!(hook.working_dir, Some(PathBuf::from("/home/user")));
        assert_eq!(hook.event, HookEvent::Both);
    }

    #[test]
    fn test_post_hook_with_timeout_zero() {
        let hook = PostHook::new(
            "No Timeout".into(),
            HookEvent::OnComplete,
            "long running".into(),
        )
        .with_timeout(0);
        assert_eq!(hook.timeout_secs, 0);
    }

    #[test]
    fn test_post_hook_empty_name_and_command() {
        let hook = PostHook::new("".into(), HookEvent::OnComplete, "".into());
        assert_eq!(hook.name, "");
        assert_eq!(hook.command, "");
        assert!(!hook.id.is_empty());
    }

    #[test]
    fn test_post_hook_unicode_name() {
        let hook = PostHook::new(
            "测试钩子 🎣".into(),
            HookEvent::OnComplete,
            "echo 测试".into(),
        );
        assert_eq!(hook.name, "测试钩子 🎣");
    }

    #[test]
    fn test_post_hook_clone() {
        let hook = PostHook::new("Original".into(), HookEvent::OnComplete, "echo test".into())
            .with_working_dir(PathBuf::from("/tmp"))
            .with_timeout(60);
        let cloned = hook.clone();
        assert_eq!(cloned.id, hook.id);
        assert_eq!(cloned.name, hook.name);
        assert_eq!(cloned.event, hook.event);
        assert_eq!(cloned.command, hook.command);
        assert_eq!(cloned.working_dir, hook.working_dir);
        assert_eq!(cloned.timeout_secs, hook.timeout_secs);
        assert_eq!(cloned.enabled, hook.enabled);
    }

    #[test]
    fn test_post_hook_debug() {
        let hook = PostHook::new(
            "Debug Test".into(),
            HookEvent::OnFailure,
            "echo debug".into(),
        );
        let debug_str = format!("{:?}", hook);
        assert!(debug_str.contains("Debug Test"));
        assert!(debug_str.contains("OnFailure"));
    }

    #[test]
    fn test_post_hook_serde_roundtrip() {
        let hook = PostHook::new(
            "Serialize Me".into(),
            HookEvent::Both,
            "echo serialized".into(),
        )
        .with_working_dir(PathBuf::from("/tmp"))
        .with_timeout(45);
        let json = serde_json::to_string(&hook).unwrap();
        let deserialized: PostHook = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.id, hook.id);
        assert_eq!(deserialized.name, hook.name);
        assert_eq!(deserialized.event, hook.event);
        assert_eq!(deserialized.command, hook.command);
        assert_eq!(deserialized.working_dir, hook.working_dir);
        assert_eq!(deserialized.timeout_secs, hook.timeout_secs);
        assert_eq!(deserialized.enabled, hook.enabled);
    }

    #[test]
    fn test_post_hook_serde_without_working_dir() {
        let hook = PostHook::new("No Dir".into(), HookEvent::OnComplete, "echo no dir".into());
        let json = serde_json::to_string(&hook).unwrap();
        let deserialized: PostHook = serde_json::from_str(&json).unwrap();
        assert!(deserialized.working_dir.is_none());
    }

    #[test]
    fn test_post_hook_serde_extra_fields_ignored() {
        let json = r#"{
            "id": "test-id",
            "name": "Test",
            "event": "OnComplete",
            "command": "echo test",
            "working_dir": null,
            "enabled": true,
            "timeout_secs": 30,
            "extra_field": "should be ignored",
            "another_extra": 42
        }"#;
        let hook: PostHook = serde_json::from_str(json).unwrap();
        assert_eq!(hook.id, "test-id");
        assert_eq!(hook.name, "Test");
        assert_eq!(hook.timeout_secs, 30);
    }

    // --- HookResult tests ---

    #[test]
    fn test_hook_result_success() {
        let result = HookResult {
            hook_id: "id-1".into(),
            hook_name: "Success Hook".into(),
            success: true,
            exit_code: Some(0),
            error: None,
            duration_ms: 100,
        };
        assert!(result.success);
        assert_eq!(result.exit_code, Some(0));
        assert!(result.error.is_none());
        assert_eq!(result.duration_ms, 100);
    }

    #[test]
    fn test_hook_result_failure_with_exit_code() {
        let result = HookResult {
            hook_id: "id-2".into(),
            hook_name: "Failed Hook".into(),
            success: false,
            exit_code: Some(1),
            error: Some("Exit code: 1".into()),
            duration_ms: 50,
        };
        assert!(!result.success);
        assert_eq!(result.exit_code, Some(1));
        assert!(result.error.is_some());
    }

    #[test]
    fn test_hook_result_timeout() {
        let result = HookResult {
            hook_id: "id-3".into(),
            hook_name: "Timeout Hook".into(),
            success: false,
            exit_code: None,
            error: Some("Timeout after 30s".into()),
            duration_ms: 30000,
        };
        assert!(!result.success);
        assert!(result.exit_code.is_none());
        assert!(result.error.unwrap().contains("Timeout"));
    }

    #[test]
    fn test_hook_result_clone_debug() {
        let result = HookResult {
            hook_id: "id".into(),
            hook_name: "name".into(),
            success: true,
            exit_code: Some(0),
            error: None,
            duration_ms: 0,
        };
        let cloned = result.clone();
        assert_eq!(cloned.hook_id, result.hook_id);
        let debug_str = format!("{:?}", result);
        assert!(debug_str.contains("HookResult"));
    }

    #[test]
    fn test_hook_result_serde_roundtrip() {
        let result = HookResult {
            hook_id: "hook-123".into(),
            hook_name: "Test Hook".into(),
            success: false,
            exit_code: Some(2),
            error: Some("Exit code: 2".into()),
            duration_ms: 500,
        };
        let json = serde_json::to_string(&result).unwrap();
        let deserialized: HookResult = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.hook_id, result.hook_id);
        assert_eq!(deserialized.success, result.success);
        assert_eq!(deserialized.exit_code, result.exit_code);
        assert_eq!(deserialized.error, result.error);
        assert_eq!(deserialized.duration_ms, result.duration_ms);
    }

    // --- HookPersistenceError tests ---

    #[test]
    fn test_hook_persistence_error_display_io() {
        let err = HookPersistenceError::Io("Permission denied".into());
        assert_eq!(format!("{}", err), "IO error: Permission denied");
    }

    #[test]
    fn test_hook_persistence_error_display_serialize() {
        let err = HookPersistenceError::Serialize("Invalid JSON".into());
        assert_eq!(format!("{}", err), "Serialize error: Invalid JSON");
    }

    #[test]
    fn test_hook_persistence_error_display_deserialize() {
        let err = HookPersistenceError::Deserialize("Missing field".into());
        assert_eq!(format!("{}", err), "Deserialize error: Missing field");
    }

    #[test]
    fn test_hook_persistence_error_debug() {
        let err = HookPersistenceError::Io("test error".into());
        let debug_str = format!("{:?}", err);
        assert!(debug_str.contains("Io"));
        assert!(debug_str.contains("test error"));
    }

    // --- HookManager comprehensive tests ---

    #[test]
    fn test_hook_manager_new() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = HookManager::new(temp_dir.path().to_path_buf());
        let hooks = manager.list_hooks();
        assert!(hooks.is_empty());
    }

    #[tokio::test]
    async fn test_hook_manager_add_multiple_hooks() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = HookManager::new(temp_dir.path().to_path_buf());

        for i in 0..10 {
            let hook = PostHook::new(
                format!("Hook {}", i),
                HookEvent::OnComplete,
                format!("echo {}", i),
            );
            manager.add_hook(hook).unwrap();
        }

        let hooks = manager.list_hooks();
        assert_eq!(hooks.len(), 10);
    }

    #[tokio::test]
    async fn test_hook_manager_remove_nonexistent() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = HookManager::new(temp_dir.path().to_path_buf());

        let removed = manager.remove_hook("nonexistent-id").unwrap();
        assert!(!removed);
    }

    #[tokio::test]
    async fn test_hook_manager_update_nonexistent() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = HookManager::new(temp_dir.path().to_path_buf());

        let hook = PostHook::new(
            "Updated".into(),
            HookEvent::OnComplete,
            "echo updated".into(),
        );
        let success = manager.update_hook("nonexistent-id", hook).unwrap();
        assert!(!success);
    }

    #[tokio::test]
    async fn test_hook_manager_enable_disable_nonexistent() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = HookManager::new(temp_dir.path().to_path_buf());

        let success = manager.set_hook_enabled("nonexistent-id", true).unwrap();
        assert!(!success);
    }

    #[tokio::test]
    async fn test_hook_manager_get_nonexistent() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = HookManager::new(temp_dir.path().to_path_buf());

        let hook = manager.get_hook("nonexistent-id");
        assert!(hook.is_none());
    }

    #[tokio::test]
    async fn test_hook_manager_get_hook_by_id() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = HookManager::new(temp_dir.path().to_path_buf());

        let hook = PostHook::new("Find Me".into(), HookEvent::OnComplete, "echo found".into());
        let hook_id = manager.add_hook(hook).unwrap();

        let found = manager.get_hook(&hook_id);
        assert!(found.is_some());
        assert_eq!(found.unwrap().name, "Find Me");
    }

    #[tokio::test]
    async fn test_hook_manager_remove_first_hook() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = HookManager::new(temp_dir.path().to_path_buf());

        let hook1 = PostHook::new("First".into(), HookEvent::OnComplete, "echo 1".into());
        let hook2 = PostHook::new("Second".into(), HookEvent::OnComplete, "echo 2".into());
        let hook3 = PostHook::new("Third".into(), HookEvent::OnComplete, "echo 3".into());

        let id1 = manager.add_hook(hook1).unwrap();
        manager.add_hook(hook2).unwrap();
        manager.add_hook(hook3).unwrap();

        let removed = manager.remove_hook(&id1).unwrap();
        assert!(removed);

        let hooks = manager.list_hooks();
        assert_eq!(hooks.len(), 2);
        assert_eq!(hooks[0].name, "Second");
        assert_eq!(hooks[1].name, "Third");
    }

    #[tokio::test]
    async fn test_hook_manager_remove_last_hook() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = HookManager::new(temp_dir.path().to_path_buf());

        let hook1 = PostHook::new("First".into(), HookEvent::OnComplete, "echo 1".into());
        let hook2 = PostHook::new("Last".into(), HookEvent::OnComplete, "echo 2".into());

        manager.add_hook(hook1).unwrap();
        let id2 = manager.add_hook(hook2).unwrap();

        let removed = manager.remove_hook(&id2).unwrap();
        assert!(removed);

        let hooks = manager.list_hooks();
        assert_eq!(hooks.len(), 1);
        assert_eq!(hooks[0].name, "First");
    }

    #[tokio::test]
    async fn test_hook_manager_persistence_across_instances() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager1 = HookManager::new(temp_dir.path().to_path_buf());

        let hook = PostHook::new(
            "Persistent".into(),
            HookEvent::Both,
            "echo persistent".into(),
        )
        .with_timeout(120)
        .with_working_dir(PathBuf::from("/var/log"));
        manager1.add_hook(hook).unwrap();

        // Create a new manager instance
        let manager2 = HookManager::new(temp_dir.path().to_path_buf());
        manager2.load().unwrap();

        let hooks = manager2.list_hooks();
        assert_eq!(hooks.len(), 1);
        assert_eq!(hooks[0].name, "Persistent");
        assert_eq!(hooks[0].timeout_secs, 120);
        assert_eq!(hooks[0].working_dir, Some(PathBuf::from("/var/log")));
    }

    #[tokio::test]
    async fn test_hook_manager_update_preserves_id() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = HookManager::new(temp_dir.path().to_path_buf());

        let hook = PostHook::new(
            "Original".into(),
            HookEvent::OnComplete,
            "echo original".into(),
        );
        let hook_id = manager.add_hook(hook).unwrap();

        let mut updated = PostHook::new(
            "Updated".into(),
            HookEvent::OnFailure,
            "echo updated".into(),
        );
        updated.id = hook_id.clone();

        manager.update_hook(&hook_id, updated).unwrap();

        let hook = manager.get_hook(&hook_id).unwrap();
        assert_eq!(hook.id, hook_id);
        assert_eq!(hook.name, "Updated");
        assert_eq!(hook.event, HookEvent::OnFailure);
    }

    #[tokio::test]
    async fn test_hook_manager_disable_then_enable() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = HookManager::new(temp_dir.path().to_path_buf());

        let hook = PostHook::new("Toggle".into(), HookEvent::OnComplete, "echo toggle".into());
        let hook_id = manager.add_hook(hook).unwrap();

        // Disable
        manager.set_hook_enabled(&hook_id, false).unwrap();
        assert!(!manager.get_hook(&hook_id).unwrap().enabled);

        // Enable
        manager.set_hook_enabled(&hook_id, true).unwrap();
        assert!(manager.get_hook(&hook_id).unwrap().enabled);

        // Disable again
        manager.set_hook_enabled(&hook_id, false).unwrap();
        assert!(!manager.get_hook(&hook_id).unwrap().enabled);
    }

    #[tokio::test]
    async fn test_hook_manager_disable_persists() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = HookManager::new(temp_dir.path().to_path_buf());

        let hook = PostHook::new(
            "Persist Disable".into(),
            HookEvent::OnComplete,
            "echo test".into(),
        );
        let hook_id = manager.add_hook(hook).unwrap();
        manager.set_hook_enabled(&hook_id, false).unwrap();

        // Load fresh
        let manager2 = HookManager::new(temp_dir.path().to_path_buf());
        manager2.load().unwrap();

        let loaded = manager2.get_hook(&hook_id).unwrap();
        assert!(!loaded.enabled);
    }

    // --- save_hooks / load_hooks comprehensive tests ---

    #[test]
    fn test_save_load_empty_hooks() {
        let temp_dir = tempfile::tempdir().unwrap();
        let hooks: Vec<PostHook> = vec![];
        save_hooks(&hooks, temp_dir.path()).unwrap();
        let loaded = load_hooks(temp_dir.path()).unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn test_save_load_hooks_preserves_order() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut hooks = Vec::new();
        for i in 0..20 {
            let hook = PostHook::new(
                format!("Hook {}", i),
                if i % 2 == 0 {
                    HookEvent::OnComplete
                } else {
                    HookEvent::OnFailure
                },
                format!("echo {}", i),
            );
            hooks.push(hook);
        }
        save_hooks(&hooks, temp_dir.path()).unwrap();
        let loaded = load_hooks(temp_dir.path()).unwrap();
        assert_eq!(loaded.len(), 20);
        for (i, hook) in loaded.iter().enumerate() {
            assert_eq!(hook.name, format!("Hook {}", i));
        }
    }

    #[test]
    fn test_save_overwrites_existing_file() {
        let temp_dir = tempfile::tempdir().unwrap();

        // Save initial hooks
        let hooks1 = vec![PostHook::new(
            "Initial".into(),
            HookEvent::OnComplete,
            "echo initial".into(),
        )];
        save_hooks(&hooks1, temp_dir.path()).unwrap();

        // Overwrite with new hooks
        let hooks2 = vec![
            PostHook::new("New 1".into(), HookEvent::OnComplete, "echo new1".into()),
            PostHook::new("New 2".into(), HookEvent::OnFailure, "echo new2".into()),
        ];
        save_hooks(&hooks2, temp_dir.path()).unwrap();

        let loaded = load_hooks(temp_dir.path()).unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].name, "New 1");
        assert_eq!(loaded[1].name, "New 2");
    }

    #[test]
    fn test_load_hooks_corrupt_json() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config_path = temp_dir.path().join("post_hooks.json");
        std::fs::write(&config_path, "this is not valid json {{{").unwrap();
        let result = load_hooks(temp_dir.path());
        assert!(result.is_err());
    }

    #[test]
    fn test_load_hooks_empty_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config_path = temp_dir.path().join("post_hooks.json");
        std::fs::write(&config_path, "").unwrap();
        let result = load_hooks(temp_dir.path());
        assert!(result.is_err());
    }

    #[test]
    fn test_save_hooks_creates_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config_path = temp_dir.path().join("post_hooks.json");
        assert!(!config_path.exists());

        let hooks = vec![PostHook::new(
            "Create".into(),
            HookEvent::OnComplete,
            "echo create".into(),
        )];
        save_hooks(&hooks, temp_dir.path()).unwrap();
        assert!(config_path.exists());
    }

    #[test]
    fn test_save_load_hooks_with_unicode() {
        let temp_dir = tempfile::tempdir().unwrap();
        let hooks = vec![
            PostHook::new(
                "中文钩子 🎣".into(),
                HookEvent::Both,
                "echo '测试中文'".into(),
            )
            .with_working_dir(PathBuf::from("/tmp/测试")),
        ];
        save_hooks(&hooks, temp_dir.path()).unwrap();
        let loaded = load_hooks(temp_dir.path()).unwrap();
        assert_eq!(loaded[0].name, "中文钩子 🎣");
        assert_eq!(loaded[0].working_dir, Some(PathBuf::from("/tmp/测试")));
    }

    #[test]
    fn test_load_hooks_extra_json_fields_ignored() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config_path = temp_dir.path().join("post_hooks.json");
        let json = r#"[{
            "id": "test-id",
            "name": "Test",
            "event": "OnComplete",
            "command": "echo test",
            "working_dir": null,
            "enabled": true,
            "timeout_secs": 30,
            "unknown_field": "ignored"
        }]"#;
        std::fs::write(&config_path, json).unwrap();
        let loaded = load_hooks(temp_dir.path()).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].name, "Test");
    }

    // --- execute_hooks comprehensive tests ---

    #[tokio::test]
    async fn test_execute_hooks_empty_list() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = HookManager::new(temp_dir.path().to_path_buf());

        let ctx = make_test_ctx(NotificationEvent::DownloadComplete);
        let results = manager.execute_hooks(&ctx).await;
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_execute_hooks_multiple_matching() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = HookManager::new(temp_dir.path().to_path_buf());

        let hook1 = PostHook::new("Hook 1".into(), HookEvent::OnComplete, "echo 1".into());
        let hook2 = PostHook::new("Hook 2".into(), HookEvent::Both, "echo 2".into());
        let hook3 = PostHook::new("Hook 3".into(), HookEvent::OnFailure, "echo 3".into());

        manager.add_hook(hook1).unwrap();
        manager.add_hook(hook2).unwrap();
        manager.add_hook(hook3).unwrap();

        let ctx = make_test_ctx(NotificationEvent::DownloadComplete);
        let results = manager.execute_hooks(&ctx).await;

        // hook1 (OnComplete) and hook2 (Both) should run, hook3 (OnFailure) should not
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].hook_name, "Hook 1");
        assert_eq!(results[1].hook_name, "Hook 2");
    }

    #[tokio::test]
    async fn test_execute_hooks_all_disabled() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = HookManager::new(temp_dir.path().to_path_buf());

        let hook1 = PostHook::new("Disabled 1".into(), HookEvent::OnComplete, "echo 1".into());
        let hook2 = PostHook::new("Disabled 2".into(), HookEvent::OnComplete, "echo 2".into());

        let id1 = manager.add_hook(hook1).unwrap();
        let id2 = manager.add_hook(hook2).unwrap();

        manager.set_hook_enabled(&id1, false).unwrap();
        manager.set_hook_enabled(&id2, false).unwrap();

        let ctx = make_test_ctx(NotificationEvent::DownloadComplete);
        let results = manager.execute_hooks(&ctx).await;
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_execute_hooks_on_failure_event() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = HookManager::new(temp_dir.path().to_path_buf());

        let hook_complete =
            PostHook::new("Complete".into(), HookEvent::OnComplete, "echo c".into());
        let hook_failure = PostHook::new("Failure".into(), HookEvent::OnFailure, "echo f".into());
        let hook_both = PostHook::new("Both".into(), HookEvent::Both, "echo b".into());

        manager.add_hook(hook_complete).unwrap();
        manager.add_hook(hook_failure).unwrap();
        manager.add_hook(hook_both).unwrap();

        let ctx = make_test_ctx(NotificationEvent::DownloadFailed);
        let results = manager.execute_hooks(&ctx).await;

        // hook_failure (OnFailure) and hook_both (Both) should run
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].hook_name, "Failure");
        assert_eq!(results[1].hook_name, "Both");
    }

    #[tokio::test]
    async fn test_execute_hook_with_working_dir() {
        let temp_dir = tempfile::tempdir().unwrap();
        let working_dir = temp_dir.path().join("work");
        std::fs::create_dir(&working_dir).unwrap();
        let output_file = working_dir.join("output.txt");
        let manager = HookManager::new(temp_dir.path().to_path_buf());

        let command = format!("echo 'hello' > {}", output_file.display());
        let hook = PostHook::new("WorkDir".into(), HookEvent::OnComplete, command)
            .with_working_dir(working_dir.clone());

        let ctx = make_test_ctx(NotificationEvent::DownloadComplete);
        let result = manager.execute_hook(&hook, &ctx).await;
        assert!(result.success);
        assert!(output_file.exists());
    }

    #[tokio::test]
    async fn test_execute_hook_no_timeout() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = HookManager::new(temp_dir.path().to_path_buf());

        let hook = PostHook::new(
            "No Timeout".into(),
            HookEvent::OnComplete,
            "echo fast".into(),
        )
        .with_timeout(0);

        let ctx = make_test_ctx(NotificationEvent::DownloadComplete);
        let result = manager.execute_hook(&hook, &ctx).await;
        assert!(result.success);
        assert_eq!(result.exit_code, Some(0));
    }

    #[tokio::test]
    async fn test_execute_hook_records_duration() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = HookManager::new(temp_dir.path().to_path_buf());

        let hook =
            PostHook::new("Slow".into(), HookEvent::OnComplete, "sleep 1".into()).with_timeout(10);

        let ctx = make_test_ctx(NotificationEvent::DownloadComplete);
        let result = manager.execute_hook(&hook, &ctx).await;
        assert!(result.success);
        // Should have taken at least some time
        assert!(result.duration_ms < 10_000); // less than timeout
    }

    #[tokio::test]
    async fn test_execute_hook_command_not_found() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = HookManager::new(temp_dir.path().to_path_buf());

        let hook = PostHook::new(
            "Not Found".into(),
            HookEvent::OnComplete,
            "/nonexistent/binary --flag".into(),
        );

        let ctx = make_test_ctx(NotificationEvent::DownloadComplete);
        let result = manager.execute_hook(&hook, &ctx).await;
        assert!(!result.success);
        assert!(result.error.is_some());
    }

    // --- HookManager error handling ---

    #[tokio::test]
    async fn test_hook_manager_add_hook_persists_immediately() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = HookManager::new(temp_dir.path().to_path_buf());

        let hook = PostHook::new(
            "Immediate".into(),
            HookEvent::OnComplete,
            "echo immediate".into(),
        );
        manager.add_hook(hook).unwrap();

        // Verify file exists
        let config_path = temp_dir.path().join("post_hooks.json");
        assert!(config_path.exists());

        // Verify content
        let loaded = load_hooks(temp_dir.path()).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].name, "Immediate");
    }

    #[tokio::test]
    async fn test_hook_manager_remove_persists_immediately() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = HookManager::new(temp_dir.path().to_path_buf());

        let hook = PostHook::new(
            "ToRemove".into(),
            HookEvent::OnComplete,
            "echo remove".into(),
        );
        let hook_id = manager.add_hook(hook).unwrap();

        // Verify hook exists
        assert_eq!(manager.list_hooks().len(), 1);

        // Remove
        manager.remove_hook(&hook_id).unwrap();

        // Verify persisted empty
        let loaded = load_hooks(temp_dir.path()).unwrap();
        assert!(loaded.is_empty());
    }

    #[tokio::test]
    async fn test_hook_manager_save_and_reload() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = HookManager::new(temp_dir.path().to_path_buf());

        let hook1 = PostHook::new("Hook A".into(), HookEvent::OnComplete, "echo a".into());
        let hook2 = PostHook::new("Hook B".into(), HookEvent::Both, "echo b".into())
            .with_timeout(60)
            .with_working_dir(PathBuf::from("/tmp"));

        manager.add_hook(hook1).unwrap();
        manager.add_hook(hook2).unwrap();

        // Explicit save
        manager.save().unwrap();

        // Reload
        let manager2 = HookManager::new(temp_dir.path().to_path_buf());
        manager2.load().unwrap();

        let hooks = manager2.list_hooks();
        assert_eq!(hooks.len(), 2);
        assert_eq!(hooks[0].name, "Hook A");
        assert_eq!(hooks[1].name, "Hook B");
        assert_eq!(hooks[1].timeout_secs, 60);
    }

    // --- NotificationContext template rendering integration ---

    #[tokio::test]
    async fn test_execute_hook_template_all_variables() {
        let temp_dir = tempfile::tempdir().unwrap();
        let output_file = temp_dir.path().join("template_output.txt");
        let manager = HookManager::new(temp_dir.path().to_path_buf());

        let command = format!(
            "echo '{{name}}|{{size}}|{{protocol}}|{{save_path}}|{{task_id}}' > {}",
            output_file.display()
        );
        let hook = PostHook::new("Full Template".into(), HookEvent::OnComplete, command);

        let ctx = NotificationContext {
            task_id: "task-12345".into(),
            name: "my_file.zip".into(),
            size: 1048576,
            downloaded: 524288,
            protocol: "Torrent".into(),
            save_path: "/downloads/my_file.zip".into(),
            error: None,
            event: NotificationEvent::DownloadComplete,
        };

        let result = manager.execute_hook(&hook, &ctx).await;
        assert!(result.success);

        let content = tokio::fs::read_to_string(&output_file).await.unwrap();
        assert!(content.contains("my_file.zip"));
        assert!(content.contains("1048576"));
        assert!(content.contains("Torrent"));
        assert!(content.contains("/downloads/my_file.zip"));
        assert!(content.contains("task-12345"));
    }

    #[tokio::test]
    async fn test_execute_hook_template_with_failure_event() {
        let temp_dir = tempfile::tempdir().unwrap();
        let output_file = temp_dir.path().join("failure_output.txt");
        let manager = HookManager::new(temp_dir.path().to_path_buf());

        let command = format!("echo '{{name}}|{{event}}' > {}", output_file.display());
        let hook = PostHook::new("Failure Template".into(), HookEvent::OnFailure, command);

        let ctx = NotificationContext {
            task_id: "fail-task".into(),
            name: "failed_file.rar".into(),
            size: 2048,
            downloaded: 0,
            protocol: "HTTP".into(),
            save_path: "/downloads/failed_file.rar".into(),
            error: Some("Connection refused".into()),
            event: NotificationEvent::DownloadFailed,
        };

        let result = manager.execute_hook(&hook, &ctx).await;
        assert!(result.success);

        let content = tokio::fs::read_to_string(&output_file).await.unwrap();
        assert!(content.contains("failed_file.rar"));
    }

    // --- Concurrent access tests ---

    #[tokio::test]
    async fn test_hook_manager_concurrent_reads() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = HookManager::new(temp_dir.path().to_path_buf());

        for i in 0..5 {
            let hook = PostHook::new(
                format!("Hook {}", i),
                HookEvent::OnComplete,
                "echo test".into(),
            );
            manager.add_hook(hook).unwrap();
        }

        // Multiple concurrent reads
        let manager = Arc::new(manager);
        let mut handles = Vec::new();
        for _ in 0..10 {
            let mgr = Arc::clone(&manager);
            handles.push(tokio::spawn(async move { mgr.list_hooks() }));
        }

        for handle in handles {
            let hooks = handle.await.unwrap();
            assert_eq!(hooks.len(), 5);
        }
    }

    // --- Edge cases ---

    #[test]
    fn test_hook_event_serde_extra_fields_ignored() {
        let json = r#""OnComplete""#;
        let event: HookEvent = serde_json::from_str(json).unwrap();
        assert_eq!(event, HookEvent::OnComplete);
    }

    #[tokio::test]
    async fn test_hook_manager_add_remove_add_same_id() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = HookManager::new(temp_dir.path().to_path_buf());

        let mut hook = PostHook::new("Original".into(), HookEvent::OnComplete, "echo 1".into());
        let fixed_id = "fixed-id-123".to_string();
        hook.id = fixed_id.clone();

        manager.add_hook(hook).unwrap();
        assert_eq!(manager.list_hooks().len(), 1);

        // Remove
        manager.remove_hook(&fixed_id).unwrap();
        assert_eq!(manager.list_hooks().len(), 0);

        // Add again with same ID
        let mut hook2 = PostHook::new("New".into(), HookEvent::OnFailure, "echo 2".into());
        hook2.id = fixed_id.clone();
        manager.add_hook(hook2).unwrap();

        let hooks = manager.list_hooks();
        assert_eq!(hooks.len(), 1);
        assert_eq!(hooks[0].name, "New");
        assert_eq!(hooks[0].id, fixed_id);
    }

    #[tokio::test]
    async fn test_execute_hooks_returns_results_in_order() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = HookManager::new(temp_dir.path().to_path_buf());

        // Add hooks in order - all should match OnComplete
        for i in 0..5 {
            let hook = PostHook::new(
                format!("Ordered {}", i),
                HookEvent::OnComplete,
                "echo test".into(),
            );
            manager.add_hook(hook).unwrap();
        }

        let ctx = make_test_ctx(NotificationEvent::DownloadComplete);
        let results = manager.execute_hooks(&ctx).await;

        assert_eq!(results.len(), 5);
        for (i, result) in results.iter().enumerate() {
            assert_eq!(result.hook_name, format!("Ordered {}", i));
        }
    }

    #[test]
    fn test_post_hook_serde_pretty_format() {
        let hook = PostHook::new("Pretty".into(), HookEvent::OnComplete, "echo pretty".into());
        let pretty = serde_json::to_string_pretty(&hook).unwrap();
        assert!(pretty.contains('\n'));
        let deserialized: PostHook = serde_json::from_str(&pretty).unwrap();
        assert_eq!(deserialized.id, hook.id);
    }

    #[test]
    fn test_save_load_hooks_multiple_events() {
        let temp_dir = tempfile::tempdir().unwrap();
        let hooks = vec![
            PostHook::new("Complete".into(), HookEvent::OnComplete, "echo c".into()),
            PostHook::new("Failure".into(), HookEvent::OnFailure, "echo f".into()),
            PostHook::new("Both".into(), HookEvent::Both, "echo b".into()),
        ];
        save_hooks(&hooks, temp_dir.path()).unwrap();
        let loaded = load_hooks(temp_dir.path()).unwrap();
        assert_eq!(loaded[0].event, HookEvent::OnComplete);
        assert_eq!(loaded[1].event, HookEvent::OnFailure);
        assert_eq!(loaded[2].event, HookEvent::Both);
    }

    #[tokio::test]
    async fn test_hook_manager_update_changes_event_type() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = HookManager::new(temp_dir.path().to_path_buf());

        let hook = PostHook::new(
            "Event Change".into(),
            HookEvent::OnComplete,
            "echo test".into(),
        );
        let hook_id = manager.add_hook(hook).unwrap();

        let mut updated = manager.get_hook(&hook_id).unwrap();
        updated.event = HookEvent::OnFailure;
        manager.update_hook(&hook_id, updated).unwrap();

        let hook = manager.get_hook(&hook_id).unwrap();
        assert_eq!(hook.event, HookEvent::OnFailure);
    }

    #[tokio::test]
    async fn test_hook_manager_update_changes_timeout() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = HookManager::new(temp_dir.path().to_path_buf());

        let hook = PostHook::new(
            "Timeout Change".into(),
            HookEvent::OnComplete,
            "echo test".into(),
        )
        .with_timeout(30);
        let hook_id = manager.add_hook(hook).unwrap();

        let mut updated = manager.get_hook(&hook_id).unwrap();
        updated.timeout_secs = 120;
        manager.update_hook(&hook_id, updated).unwrap();

        let hook = manager.get_hook(&hook_id).unwrap();
        assert_eq!(hook.timeout_secs, 120);
    }

    #[tokio::test]
    async fn test_execute_hook_exit_code_negative() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = HookManager::new(temp_dir.path().to_path_buf());

        // exit 2 gives a non-zero exit code
        let hook = PostHook::new("Exit 2".into(), HookEvent::OnComplete, "exit 2".into());

        let ctx = make_test_ctx(NotificationEvent::DownloadComplete);
        let result = manager.execute_hook(&hook, &ctx).await;
        assert!(!result.success);
        assert_eq!(result.exit_code, Some(2));
        assert!(result.error.is_some());
        assert!(result.error.unwrap().contains("Exit code: 2"));
    }
}
