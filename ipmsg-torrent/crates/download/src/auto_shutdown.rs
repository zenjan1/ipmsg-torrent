//! Auto-shutdown on queue empty
//!
//! When all download tasks reach a terminal state (Complete, Error, or no tasks remain),
//! optionally exit the application or execute a shell command.

use serde::{Deserialize, Serialize};
use std::path::Path;
use std::process::Stdio;
use tokio::process::Command;

/// What to do when all downloads finish
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum AutoShutdownAction {
    /// Do nothing (default)
    #[default]
    Disabled,
    /// Exit the application with code 0
    Exit,
    /// Execute a shell command
    Shell {
        /// Command to execute, e.g., "notify-send 'All downloads done'"
        command: String,
    },
}

impl AutoShutdownAction {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Exit => "exit",
            Self::Shell { .. } => "shell",
        }
    }

    /// Parse from user input string
    pub fn from_str_opt(s: &str) -> Option<Self> {
        let trimmed = s.trim();
        let lower = trimmed.to_lowercase();
        match lower.as_str() {
            "disabled" | "none" | "off" => Some(Self::Disabled),
            "exit" | "quit" => Some(Self::Exit),
            _ => {
                if let Some(_cmd) = lower.strip_prefix("shell:") {
                    // Use original (case-preserved) text after "shell:"
                    let cmd = trimmed[6..].trim();
                    if cmd.is_empty() {
                        None
                    } else {
                        Some(Self::Shell {
                            command: cmd.to_string(),
                        })
                    }
                } else {
                    None
                }
            }
        }
    }

    /// Format for display
    pub fn display(&self) -> String {
        match self {
            Self::Disabled => "disabled".to_string(),
            Self::Exit => "exit (shutdown app)".to_string(),
            Self::Shell { command } => format!("shell: {}", command),
        }
    }
}

/// Auto-shutdown configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoShutdownConfig {
    /// What action to take when queue is empty
    pub action: AutoShutdownAction,
    /// Only trigger when there are truly no remaining tasks (including completed/error)
    /// If false (default), triggers when no running/queued tasks remain
    pub require_empty_queue: bool,
}

impl Default for AutoShutdownConfig {
    fn default() -> Self {
        Self {
            action: AutoShutdownAction::Disabled,
            require_empty_queue: false,
        }
    }
}

/// Check if all tasks are in a terminal state
pub fn all_tasks_terminal(
    running_count: usize,
    queued_count: usize,
    paused_count: usize,
    downloading_count: usize,
) -> bool {
    running_count == 0 && queued_count == 0 && paused_count == 0 && downloading_count == 0
}

/// Check if queue is "empty enough" (no active work)
pub fn queue_is_idle(running_count: usize, queued_count: usize, downloading_count: usize) -> bool {
    running_count == 0 && queued_count == 0 && downloading_count == 0
}

/// Save auto-shutdown configuration to disk.
pub fn save_auto_shutdown_config(
    config: &AutoShutdownConfig,
    data_dir: &Path,
) -> Result<(), AutoShutdownPersistenceError> {
    let config_path = data_dir.join("auto_shutdown_config.json");
    let json = serde_json::to_string_pretty(config)
        .map_err(|e| AutoShutdownPersistenceError::Serialize(e.to_string()))?;
    std::fs::write(&config_path, json)
        .map_err(|e| AutoShutdownPersistenceError::Io(e.to_string()))?;
    Ok(())
}

/// Load auto-shutdown configuration from disk.
/// Returns `Ok(None)` if no config file exists.
pub fn load_auto_shutdown_config(
    data_dir: &Path,
) -> Result<Option<AutoShutdownConfig>, AutoShutdownPersistenceError> {
    let config_path = data_dir.join("auto_shutdown_config.json");
    if !config_path.exists() {
        return Ok(None);
    }
    let json = std::fs::read_to_string(&config_path)
        .map_err(|e| AutoShutdownPersistenceError::Io(e.to_string()))?;
    let config: AutoShutdownConfig = serde_json::from_str(&json)
        .map_err(|e| AutoShutdownPersistenceError::Deserialize(e.to_string()))?;
    Ok(Some(config))
}

/// Errors when persisting auto-shutdown configuration.
#[derive(Debug, thiserror::Error)]
pub enum AutoShutdownPersistenceError {
    #[error("IO error: {0}")]
    Io(String),
    #[error("serialize error: {0}")]
    Serialize(String),
    #[error("deserialize error: {0}")]
    Deserialize(String),
}

/// Execute the auto-shutdown action.
/// Returns `true` if the application should exit.
pub async fn execute_shutdown_action(config: &AutoShutdownConfig) -> bool {
    match &config.action {
        AutoShutdownAction::Disabled => false,
        AutoShutdownAction::Exit => true,
        AutoShutdownAction::Shell { command } => {
            let result = Command::new("sh")
                .arg("-c")
                .arg(command)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .await;

            match result {
                Ok(status) => {
                    tracing::info!(
                        command = %command,
                        exit_code = status.code(),
                        "Auto-shutdown shell command executed"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        command = %command,
                        error = %e,
                        "Failed to execute auto-shutdown shell command"
                    );
                }
            }
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_action_is_disabled() {
        assert_eq!(AutoShutdownAction::default(), AutoShutdownAction::Disabled);
    }

    #[test]
    fn test_from_str_opt_disabled() {
        assert_eq!(
            AutoShutdownAction::from_str_opt("disabled"),
            Some(AutoShutdownAction::Disabled)
        );
        assert_eq!(
            AutoShutdownAction::from_str_opt("none"),
            Some(AutoShutdownAction::Disabled)
        );
        assert_eq!(
            AutoShutdownAction::from_str_opt("off"),
            Some(AutoShutdownAction::Disabled)
        );
    }

    #[test]
    fn test_from_str_opt_exit() {
        assert_eq!(
            AutoShutdownAction::from_str_opt("exit"),
            Some(AutoShutdownAction::Exit)
        );
        assert_eq!(
            AutoShutdownAction::from_str_opt("quit"),
            Some(AutoShutdownAction::Exit)
        );
    }

    #[test]
    fn test_from_str_opt_shell() {
        assert_eq!(
            AutoShutdownAction::from_str_opt("shell:echo done"),
            Some(AutoShutdownAction::Shell {
                command: "echo done".to_string()
            })
        );
        assert_eq!(
            AutoShutdownAction::from_str_opt("shell: notify-send 'All done'"),
            Some(AutoShutdownAction::Shell {
                command: "notify-send 'All done'".to_string()
            })
        );
    }

    #[test]
    fn test_from_str_opt_invalid() {
        assert_eq!(AutoShutdownAction::from_str_opt("shell:"), None);
        assert_eq!(AutoShutdownAction::from_str_opt("shell:  "), None);
        assert_eq!(AutoShutdownAction::from_str_opt("unknown"), None);
    }

    #[test]
    fn test_all_tasks_terminal() {
        assert!(all_tasks_terminal(0, 0, 0, 0));
        assert!(!all_tasks_terminal(1, 0, 0, 0));
        assert!(!all_tasks_terminal(0, 1, 0, 0));
        assert!(!all_tasks_terminal(0, 0, 1, 0));
        assert!(!all_tasks_terminal(0, 0, 0, 1));
        // All complete (only terminal states counted externally)
        assert!(all_tasks_terminal(0, 0, 0, 0));
    }

    #[test]
    fn test_queue_is_idle() {
        assert!(queue_is_idle(0, 0, 0));
        assert!(!queue_is_idle(1, 0, 0));
        assert!(!queue_is_idle(0, 1, 0));
        assert!(!queue_is_idle(0, 0, 1));
        assert!(queue_is_idle(0, 0, 0));
    }

    #[test]
    fn test_display() {
        assert_eq!(AutoShutdownAction::Disabled.display(), "disabled");
        assert_eq!(AutoShutdownAction::Exit.display(), "exit (shutdown app)");
        assert_eq!(
            AutoShutdownAction::Shell {
                command: "echo done".to_string()
            }
            .display(),
            "shell: echo done"
        );
    }

    #[test]
    fn test_default_config() {
        let config = AutoShutdownConfig::default();
        assert_eq!(config.action, AutoShutdownAction::Disabled);
        assert!(!config.require_empty_queue);
    }

    #[test]
    fn test_config_serialization() {
        let config = AutoShutdownConfig {
            action: AutoShutdownAction::Shell {
                command: "echo done".to_string(),
            },
            require_empty_queue: true,
        };
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: AutoShutdownConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.action, config.action);
        assert_eq!(deserialized.require_empty_queue, config.require_empty_queue);
    }

    #[tokio::test]
    async fn test_execute_disabled() {
        let config = AutoShutdownConfig::default();
        assert!(!execute_shutdown_action(&config).await);
    }

    #[tokio::test]
    async fn test_execute_exit() {
        let config = AutoShutdownConfig {
            action: AutoShutdownAction::Exit,
            require_empty_queue: false,
        };
        assert!(execute_shutdown_action(&config).await);
    }

    #[tokio::test]
    async fn test_execute_shell_command() {
        let config = AutoShutdownConfig {
            action: AutoShutdownAction::Shell {
                command: "true".to_string(),
            },
            require_empty_queue: false,
        };
        // Shell commands don't trigger exit
        assert!(!execute_shutdown_action(&config).await);
    }

    #[tokio::test]
    async fn test_execute_shell_command_failure() {
        // Should not panic even if command fails
        let config = AutoShutdownConfig {
            action: AutoShutdownAction::Shell {
                command: "false".to_string(),
            },
            require_empty_queue: false,
        };
        assert!(!execute_shutdown_action(&config).await);
    }

    #[test]
    fn test_save_load_auto_shutdown_config() {
        let tmp = tempfile::tempdir().unwrap();
        let config = AutoShutdownConfig {
            action: AutoShutdownAction::Shell {
                command: "echo done".to_string(),
            },
            require_empty_queue: true,
        };
        save_auto_shutdown_config(&config, tmp.path()).unwrap();
        let loaded = load_auto_shutdown_config(tmp.path()).unwrap().unwrap();
        assert_eq!(loaded.action, config.action);
        assert_eq!(loaded.require_empty_queue, config.require_empty_queue);
    }

    #[test]
    fn test_load_auto_shutdown_config_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let loaded = load_auto_shutdown_config(tmp.path()).unwrap();
        assert!(loaded.is_none());
    }

    #[test]
    fn test_save_auto_shutdown_config_overwrite() {
        let tmp = tempfile::tempdir().unwrap();
        let config1 = AutoShutdownConfig {
            action: AutoShutdownAction::Exit,
            require_empty_queue: false,
        };
        save_auto_shutdown_config(&config1, tmp.path()).unwrap();
        let config2 = AutoShutdownConfig {
            action: AutoShutdownAction::Disabled,
            require_empty_queue: true,
        };
        save_auto_shutdown_config(&config2, tmp.path()).unwrap();
        let loaded = load_auto_shutdown_config(tmp.path()).unwrap().unwrap();
        assert_eq!(loaded.action, AutoShutdownAction::Disabled);
        assert!(loaded.require_empty_queue);
    }

    // ========== Phase 223: Comprehensive Test Coverage ==========

    // --- AutoShutdownAction serde 往返 ---

    #[test]
    fn test_action_serde_roundtrip_disabled() {
        let action = AutoShutdownAction::Disabled;
        let json = serde_json::to_string(&action).unwrap();
        let deserialized: AutoShutdownAction = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, AutoShutdownAction::Disabled);
    }

    #[test]
    fn test_action_serde_roundtrip_exit() {
        let action = AutoShutdownAction::Exit;
        let json = serde_json::to_string(&action).unwrap();
        let deserialized: AutoShutdownAction = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, AutoShutdownAction::Exit);
    }

    #[test]
    fn test_action_serde_roundtrip_shell() {
        let action = AutoShutdownAction::Shell {
            command: "echo hello".to_string(),
        };
        let json = serde_json::to_string(&action).unwrap();
        let deserialized: AutoShutdownAction = serde_json::from_str(&json).unwrap();
        assert_eq!(
            deserialized,
            AutoShutdownAction::Shell {
                command: "echo hello".to_string()
            }
        );
    }

    #[test]
    fn test_action_serde_roundtrip_shell_unicode() {
        let action = AutoShutdownAction::Shell {
            command: "notify-send '下载完成 🎉'".to_string(),
        };
        let json = serde_json::to_string(&action).unwrap();
        let deserialized: AutoShutdownAction = serde_json::from_str(&json).unwrap();
        assert_eq!(
            deserialized,
            AutoShutdownAction::Shell {
                command: "notify-send '下载完成 🎉'".to_string()
            }
        );
    }

    // --- AutoShutdownAction label ---

    #[test]
    fn test_action_label_disabled() {
        assert_eq!(AutoShutdownAction::Disabled.label(), "disabled");
    }

    #[test]
    fn test_action_label_exit() {
        assert_eq!(AutoShutdownAction::Exit.label(), "exit");
    }

    #[test]
    fn test_action_label_shell() {
        assert_eq!(
            AutoShutdownAction::Shell {
                command: "test".to_string()
            }
            .label(),
            "shell"
        );
    }

    // --- AutoShutdownAction from_str_opt 边界 ---

    #[test]
    fn test_from_str_opt_case_insensitive() {
        assert_eq!(
            AutoShutdownAction::from_str_opt("DISABLED"),
            Some(AutoShutdownAction::Disabled)
        );
        assert_eq!(
            AutoShutdownAction::from_str_opt("Exit"),
            Some(AutoShutdownAction::Exit)
        );
        assert_eq!(
            AutoShutdownAction::from_str_opt("QUIT"),
            Some(AutoShutdownAction::Exit)
        );
        assert_eq!(
            AutoShutdownAction::from_str_opt("NONE"),
            Some(AutoShutdownAction::Disabled)
        );
        assert_eq!(
            AutoShutdownAction::from_str_opt("OFF"),
            Some(AutoShutdownAction::Disabled)
        );
    }

    #[test]
    fn test_from_str_opt_with_whitespace() {
        assert_eq!(
            AutoShutdownAction::from_str_opt("  disabled  "),
            Some(AutoShutdownAction::Disabled)
        );
        assert_eq!(
            AutoShutdownAction::from_str_opt("  exit  "),
            Some(AutoShutdownAction::Exit)
        );
    }

    #[test]
    fn test_from_str_opt_empty_string() {
        assert_eq!(AutoShutdownAction::from_str_opt(""), None);
    }

    #[test]
    fn test_from_str_opt_whitespace_only() {
        assert_eq!(AutoShutdownAction::from_str_opt("   "), None);
    }

    #[test]
    fn test_from_str_opt_shell_unicode_command() {
        assert_eq!(
            AutoShutdownAction::from_str_opt("shell:echo 你好世界"),
            Some(AutoShutdownAction::Shell {
                command: "echo 你好世界".to_string()
            })
        );
    }

    #[test]
    fn test_from_str_opt_shell_complex_command() {
        let cmd = "notify-send -i /tmp/icon.png 'Done' && systemctl suspend";
        let input = format!("shell:{}", cmd);
        let result = AutoShutdownAction::from_str_opt(&input);
        assert_eq!(
            result,
            Some(AutoShutdownAction::Shell {
                command: cmd.to_string()
            })
        );
    }

    #[test]
    fn test_from_str_opt_shell_case_insensitive_prefix() {
        assert_eq!(
            AutoShutdownAction::from_str_opt("SHELL:echo done"),
            Some(AutoShutdownAction::Shell {
                command: "echo done".to_string()
            })
        );
        assert_eq!(
            AutoShutdownAction::from_str_opt("Shell:echo done"),
            Some(AutoShutdownAction::Shell {
                command: "echo done".to_string()
            })
        );
    }

    // --- AutoShutdownAction traits ---

    #[test]
    fn test_action_clone() {
        let action = AutoShutdownAction::Shell {
            command: "test".to_string(),
        };
        let cloned = action.clone();
        assert_eq!(action, cloned);
    }

    #[test]
    fn test_action_debug() {
        let action = AutoShutdownAction::Shell {
            command: "test".to_string(),
        };
        let debug = format!("{:?}", action);
        assert!(debug.contains("Shell"));
        assert!(debug.contains("test"));
    }

    #[test]
    fn test_action_eq_variants() {
        assert_eq!(AutoShutdownAction::Disabled, AutoShutdownAction::Disabled);
        assert_eq!(AutoShutdownAction::Exit, AutoShutdownAction::Exit);
        assert_ne!(AutoShutdownAction::Disabled, AutoShutdownAction::Exit);
        assert_ne!(
            AutoShutdownAction::Shell {
                command: "a".to_string()
            },
            AutoShutdownAction::Shell {
                command: "b".to_string()
            }
        );
        assert_eq!(
            AutoShutdownAction::Shell {
                command: "x".to_string()
            },
            AutoShutdownAction::Shell {
                command: "x".to_string()
            }
        );
    }

    // --- AutoShutdownConfig serde ---

    #[test]
    fn test_config_serde_roundtrip_disabled() {
        let config = AutoShutdownConfig {
            action: AutoShutdownAction::Disabled,
            require_empty_queue: false,
        };
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: AutoShutdownConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.action, config.action);
        assert_eq!(deserialized.require_empty_queue, config.require_empty_queue);
    }

    #[test]
    fn test_config_serde_roundtrip_exit() {
        let config = AutoShutdownConfig {
            action: AutoShutdownAction::Exit,
            require_empty_queue: true,
        };
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: AutoShutdownConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.action, config.action);
        assert_eq!(deserialized.require_empty_queue, config.require_empty_queue);
    }

    #[test]
    fn test_config_pretty_serde() {
        let config = AutoShutdownConfig {
            action: AutoShutdownAction::Shell {
                command: "echo done".to_string(),
            },
            require_empty_queue: true,
        };
        let json = serde_json::to_string_pretty(&config).unwrap();
        assert!(json.contains('\n'));
        let deserialized: AutoShutdownConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.action, config.action);
    }

    #[test]
    fn test_config_extra_fields_ignored() {
        let json = r#"{"action":"Disabled","require_empty_queue":false,"extra_field":"ignored"}"#;
        let config: AutoShutdownConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.action, AutoShutdownAction::Disabled);
        assert!(!config.require_empty_queue);
    }

    #[test]
    fn test_config_clone() {
        let config = AutoShutdownConfig {
            action: AutoShutdownAction::Exit,
            require_empty_queue: true,
        };
        let cloned = config.clone();
        assert_eq!(cloned.action, config.action);
        assert_eq!(cloned.require_empty_queue, config.require_empty_queue);
    }

    #[test]
    fn test_config_debug() {
        let config = AutoShutdownConfig {
            action: AutoShutdownAction::Disabled,
            require_empty_queue: false,
        };
        let debug = format!("{:?}", config);
        assert!(debug.contains("AutoShutdownConfig"));
        assert!(debug.contains("Disabled"));
    }

    // --- all_tasks_terminal 边界 ---

    #[test]
    fn test_all_tasks_terminal_all_nonzero() {
        assert!(!all_tasks_terminal(1, 1, 1, 1));
    }

    #[test]
    fn test_all_tasks_terminal_large_values() {
        assert!(!all_tasks_terminal(100, 200, 300, 400));
    }

    #[test]
    fn test_all_tasks_terminal_only_running() {
        assert!(!all_tasks_terminal(5, 0, 0, 0));
    }

    #[test]
    fn test_all_tasks_terminal_only_queued() {
        assert!(!all_tasks_terminal(0, 5, 0, 0));
    }

    #[test]
    fn test_all_tasks_terminal_only_paused() {
        assert!(!all_tasks_terminal(0, 0, 5, 0));
    }

    #[test]
    fn test_all_tasks_terminal_only_downloading() {
        assert!(!all_tasks_terminal(0, 0, 0, 5));
    }

    // --- queue_is_idle 边界 ---

    #[test]
    fn test_queue_is_idle_large_values() {
        assert!(!queue_is_idle(100, 200, 300));
    }

    #[test]
    fn test_queue_is_idle_only_running() {
        assert!(!queue_is_idle(1, 0, 0));
    }

    #[test]
    fn test_queue_is_idle_only_queued() {
        assert!(!queue_is_idle(0, 1, 0));
    }

    #[test]
    fn test_queue_is_idle_only_downloading() {
        assert!(!queue_is_idle(0, 0, 1));
    }

    #[test]
    fn test_queue_is_idle_all_nonzero() {
        assert!(!queue_is_idle(1, 1, 1));
    }

    // --- display 边界 ---

    #[test]
    fn test_display_shell_unicode() {
        let action = AutoShutdownAction::Shell {
            command: "echo 你好 🌍".to_string(),
        };
        assert_eq!(action.display(), "shell: echo 你好 🌍");
    }

    #[test]
    fn test_display_shell_empty_command() {
        let action = AutoShutdownAction::Shell {
            command: String::new(),
        };
        assert_eq!(action.display(), "shell: ");
    }

    #[test]
    fn test_display_shell_long_command() {
        let cmd = "a".repeat(500);
        let action = AutoShutdownAction::Shell {
            command: cmd.clone(),
        };
        let display = action.display();
        assert!(display.starts_with("shell: "));
        assert!(display.contains(&cmd));
    }

    // --- 持久化 ---

    #[test]
    fn test_save_creates_file() {
        let tmp = tempfile::tempdir().unwrap();
        let config_path = tmp.path().join("auto_shutdown_config.json");
        assert!(!config_path.exists());
        let config = AutoShutdownConfig::default();
        save_auto_shutdown_config(&config, tmp.path()).unwrap();
        assert!(config_path.exists());
    }

    #[test]
    fn test_save_no_tmp_residual() {
        let tmp = tempfile::tempdir().unwrap();
        let config = AutoShutdownConfig::default();
        save_auto_shutdown_config(&config, tmp.path()).unwrap();
        let files: Vec<_> = std::fs::read_dir(tmp.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert_eq!(files.len(), 1);
        let name = files[0].file_name();
        assert_eq!(name, "auto_shutdown_config.json");
    }

    #[test]
    fn test_load_corrupt_json() {
        let tmp = tempfile::tempdir().unwrap();
        let config_path = tmp.path().join("auto_shutdown_config.json");
        std::fs::write(&config_path, "not valid json{{{").unwrap();
        let result = load_auto_shutdown_config(tmp.path());
        assert!(result.is_err());
    }

    #[test]
    fn test_load_empty_file() {
        let tmp = tempfile::tempdir().unwrap();
        let config_path = tmp.path().join("auto_shutdown_config.json");
        std::fs::write(&config_path, "").unwrap();
        let result = load_auto_shutdown_config(tmp.path());
        assert!(result.is_err());
    }

    #[test]
    fn test_save_load_roundtrip_pretty() {
        let tmp = tempfile::tempdir().unwrap();
        let config = AutoShutdownConfig {
            action: AutoShutdownAction::Shell {
                command: "echo pretty".to_string(),
            },
            require_empty_queue: true,
        };
        save_auto_shutdown_config(&config, tmp.path()).unwrap();
        // save uses pretty format, verify it's valid
        let content =
            std::fs::read_to_string(tmp.path().join("auto_shutdown_config.json")).unwrap();
        assert!(content.contains('\n'));
        let loaded = load_auto_shutdown_config(tmp.path()).unwrap().unwrap();
        assert_eq!(loaded.action, config.action);
        assert_eq!(loaded.require_empty_queue, config.require_empty_queue);
    }

    #[test]
    fn test_save_load_unicode_command() {
        let tmp = tempfile::tempdir().unwrap();
        let config = AutoShutdownConfig {
            action: AutoShutdownAction::Shell {
                command: "notify-send '下载完成 🎉 恭喜'".to_string(),
            },
            require_empty_queue: false,
        };
        save_auto_shutdown_config(&config, tmp.path()).unwrap();
        let loaded = load_auto_shutdown_config(tmp.path()).unwrap().unwrap();
        assert_eq!(loaded.action, config.action);
    }

    #[test]
    fn test_save_load_disabled_config() {
        let tmp = tempfile::tempdir().unwrap();
        let config = AutoShutdownConfig::default();
        save_auto_shutdown_config(&config, tmp.path()).unwrap();
        let loaded = load_auto_shutdown_config(tmp.path()).unwrap().unwrap();
        assert_eq!(loaded.action, AutoShutdownAction::Disabled);
        assert!(!loaded.require_empty_queue);
    }

    // --- AutoShutdownPersistenceError ---

    #[test]
    fn test_error_display_io() {
        let err = AutoShutdownPersistenceError::Io("disk full".to_string());
        assert_eq!(format!("{}", err), "IO error: disk full");
    }

    #[test]
    fn test_error_display_serialize() {
        let err = AutoShutdownPersistenceError::Serialize("invalid data".to_string());
        assert_eq!(format!("{}", err), "serialize error: invalid data");
    }

    #[test]
    fn test_error_display_deserialize() {
        let err = AutoShutdownPersistenceError::Deserialize("unexpected token".to_string());
        assert_eq!(format!("{}", err), "deserialize error: unexpected token");
    }

    #[test]
    fn test_error_debug() {
        let err = AutoShutdownPersistenceError::Io("test".to_string());
        let debug = format!("{:?}", err);
        assert!(debug.contains("Io"));
        assert!(debug.contains("test"));
    }

    // --- 异步执行边界 ---

    #[tokio::test]
    async fn test_execute_shell_with_output() {
        let config = AutoShutdownConfig {
            action: AutoShutdownAction::Shell {
                command: "echo hello world".to_string(),
            },
            require_empty_queue: false,
        };
        // Shell commands always return false (don't trigger exit)
        assert!(!execute_shutdown_action(&config).await);
    }

    #[tokio::test]
    async fn test_execute_shell_nonexistent_command() {
        let config = AutoShutdownConfig {
            action: AutoShutdownAction::Shell {
                command: "nonexistent_command_xyz_12345".to_string(),
            },
            require_empty_queue: false,
        };
        // Should not panic even with invalid command
        assert!(!execute_shutdown_action(&config).await);
    }

    #[tokio::test]
    async fn test_execute_shell_unicode_output() {
        let config = AutoShutdownConfig {
            action: AutoShutdownAction::Shell {
                command: "echo 你好世界".to_string(),
            },
            require_empty_queue: false,
        };
        assert!(!execute_shutdown_action(&config).await);
    }

    // --- 完整生命周期 ---

    #[test]
    fn test_complete_lifecycle() {
        let tmp = tempfile::tempdir().unwrap();

        // 1. Start with no config
        assert!(load_auto_shutdown_config(tmp.path()).unwrap().is_none());

        // 2. Create and save config
        let config = AutoShutdownConfig {
            action: AutoShutdownAction::Shell {
                command: "echo done".to_string(),
            },
            require_empty_queue: true,
        };
        save_auto_shutdown_config(&config, tmp.path()).unwrap();

        // 3. Load and verify
        let loaded = load_auto_shutdown_config(tmp.path()).unwrap().unwrap();
        assert_eq!(loaded.action, config.action);
        assert_eq!(loaded.require_empty_queue, config.require_empty_queue);

        // 4. Overwrite with new config
        let config2 = AutoShutdownConfig {
            action: AutoShutdownAction::Exit,
            require_empty_queue: false,
        };
        save_auto_shutdown_config(&config2, tmp.path()).unwrap();

        // 5. Load and verify overwrite
        let loaded2 = load_auto_shutdown_config(tmp.path()).unwrap().unwrap();
        assert_eq!(loaded2.action, AutoShutdownAction::Exit);
        assert!(!loaded2.require_empty_queue);
    }

    #[test]
    fn test_config_default_equals_new() {
        let config = AutoShutdownConfig::default();
        assert_eq!(config.action, AutoShutdownAction::Disabled);
        assert!(!config.require_empty_queue);
    }
}
