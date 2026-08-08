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
}
