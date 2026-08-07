//! Auto-shutdown on queue empty
//!
//! When all download tasks reach a terminal state (Complete, Error, or no tasks remain),
//! optionally exit the application or execute a shell command.

use serde::{Deserialize, Serialize};
use std::process::Stdio;
use tokio::process::Command;

/// What to do when all downloads finish
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AutoShutdownAction {
    /// Do nothing (default)
    Disabled,
    /// Exit the application with code 0
    Exit,
    /// Execute a shell command
    Shell {
        /// Command to execute, e.g., "notify-send 'All downloads done'"
        command: String,
    },
}

impl Default for AutoShutdownAction {
    fn default() -> Self {
        Self::Disabled
    }
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
}
