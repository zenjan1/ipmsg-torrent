use clap::Parser;
use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use ipmsg_core::{P2PEngine, P2PEvent, SendCommand};
use ipmsg_protocol::message::{ChannelId, ChatMessage};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, Paragraph, Tabs};
use std::collections::HashMap;
use std::io::{self, stdout};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "ipmsg")]
#[command(about = "IPMsg-Torrent P2P Chat Client")]
struct Cli {
    #[arg(short, long, default_value = "Anonymous")]
    username: String,
    #[arg(long)]
    bootstrap: Option<String>,
    #[arg(long)]
    data_dir: Option<String>,
    /// Join a channel on startup (e.g., "general", "geo:u4pruy")
    #[arg(long)]
    join: Option<Vec<String>>,
    /// Run in headless mode (no TUI, log to stdout)
    #[arg(long)]
    headless: bool,
    /// TCP/UDP port to listen on (default: 0 = random)
    #[arg(long, default_value = "0")]
    port: u16,
}

/// IRC-style command parser
#[derive(Debug)]
enum Command {
    Help,
    Nick(String),
    Msg {
        target: String,
        content: String,
    },
    Peers,
    Join(String),
    Leave(String),
    GeoJoin(String),
    Who,
    Ping,
    #[allow(dead_code)] // Planned: file transfer support
    File {
        target: String,
        path: String,
    },
    Share {
        path: String,
        tags: Vec<String>,
    },
    Unshare {
        hash: String,
    },
    Search {
        query: String,
        tags: Vec<String>,
    },
    Files,
    Download {
        hash: String,
        peer: String,
    },
    /// Multi-protocol download (torrent/ed2k/url)
    Dl {
        target: String,
    },
    /// List download tasks
    Dls,
    /// Pause a download task
    Dlp {
        task_id: String,
    },
    /// Resume a download task
    Dlr {
        task_id: String,
    },
    /// Set download speed limit (e.g., "100KB/s", "1MB/s", "0" for unlimited)
    DlSpeed {
        limit: String,
    },
    /// Set per-task download speed limit (e.g., "/dltaskspeed <task_id> 100KB/s")
    DlTaskSpeed {
        task_id: String,
        limit: String,
    },
    /// Set download timeout and auto-retry (e.g., "30s", "5m", "0" to disable)
    DlTimeout {
        timeout: String,
        max_retries: u32,
    },
    /// Set maximum concurrent downloads (0 = unlimited)
    DlConcurrent {
        max: usize,
    },
    /// Pause all running downloads
    DlPauseAll,
    /// Resume all paused downloads
    DlResumeAll,
    /// Remove all completed downloads
    DlRmCompleted,
    /// Remove all failed downloads
    DlRmFailed,
    /// Show download statistics
    DlStats,
    /// Show download queue health report
    DlHealth,
    /// Show speed history for a task or all tasks
    DlSpeedHistory {
        task_id: Option<String>,
    },
    /// Configure auto-cleanup of completed/failed downloads
    DlCleanup {
        /// "status", "enable", "disable", "set <completed_retention> [failed_retention]"
        args: Vec<String>,
    },
    /// Add tags to a download task
    DlTag {
        task_id: String,
        tags: Vec<String>,
    },
    /// Remove tags from a download task
    DlUntag {
        task_id: String,
        tags: Vec<String>,
    },
    /// List all tags or filter by tag
    DlTags {
        tag: Option<String>,
    },
    /// Search/filter/sort download tasks
    DlFind {
        /// Search query (substring match in name, case-insensitive)
        query: Option<String>,
        /// Filter by state: running, paused, completed, error, queued
        state_filter: Option<String>,
        /// Filter by protocol: torrent, ed2k, xunlei, magnet, p2p
        protocol: Option<String>,
        /// Sort by: name, size, progress, speed, created (default: created)
        sort: Option<String>,
        /// Sort ascending (default: descending for most fields)
        asc: bool,
    },
    /// Set download task priority (high/normal/low)
    DlPriority {
        task_id: String,
        priority: String,
    },
    /// Configure download notifications (desktop/shell/log/webhook)
    DlNotify {
        /// Action: enable, disable, desktop, shell, log, webhook, status
        action: String,
        /// Configuration value (depends on action)
        value: Option<String>,
    },
    /// Set download schedule time window (e.g., "09:00-17:00" or "none" to disable)
    DlSchedule {
        task_id: String,
        /// Time window "HH:MM-HH:MM" or "none" to remove schedule
        window: String,
    },
    /// Set bandwidth weight for a download task (1-10)
    DlBandwidth {
        task_id: String,
        /// Bandwidth weight (1-10, higher = more bandwidth)
        weight: u8,
    },
    /// Show bandwidth monitoring dashboard
    DlBandwidthMon,
    /// Show speed trend chart (sparkline)
    DlChart {
        /// Time window in seconds (default: 300 = 5 min)
        window: Option<u64>,
    },
    /// Move a task up/down in the queue or to top/bottom
    DlQueueMove {
        task_id: String,
        /// Direction: "up", "down", "top", "bottom"
        direction: String,
    },
    /// Set download task dependencies
    DlDeps {
        /// Task ID to set dependencies for
        task_id: String,
        /// Comma-separated list of task IDs this task depends on, or "none" to clear
        deps: String,
    },
    /// Batch import URLs from a file or inline list
    DlBatch {
        /// File path containing URLs (one per line), or inline URLs separated by spaces
        source: String,
    },
    /// Extract download URLs from arbitrary text content
    DlExtract {
        /// File path containing text with embedded URLs
        path: String,
    },
    /// Export download tasks to a JSON file
    DlExport {
        /// Output file path (e.g., /tmp/tasks.json)
        path: String,
        /// Optional description
        description: Option<String>,
    },
    /// Import download tasks from a JSON file
    DlImport {
        /// Input file path (e.g., /tmp/tasks.json)
        path: String,
    },
    /// Download URL using multi-segment parallel connections (like aria2/IDM)
    DlSegment {
        /// HTTP/HTTPS URL to download
        url: String,
    },
    /// Configure auto-shutdown when all downloads complete
    DlAutoshutdown {
        /// Action: "disabled", "exit", or "shell:<command>"
        action: String,
    },
    /// Set download save path
    DlPath {
        /// Path to save downloads to (e.g., /home/user/downloads)
        path: String,
    },
    /// Enable/disable auto-organize by file type
    DlOrganize {
        /// "on" to enable, "off" to disable
        enabled: String,
    },
    /// Configure download proxy (e.g., "socks5://127.0.0.1:1080" or "none" to disable)
    DlProxy {
        /// Proxy URL (e.g., "socks5://host:port", "http://user:pass@host:port") or "none"
        url: String,
    },
    /// Test proxy connection
    DlProxyTest,
    /// Rename a download task
    DlRename {
        /// Task ID to rename
        task_id: String,
        /// New name for the task
        new_name: String,
    },
    /// Set or clear notes/description for a download task
    DlNotes {
        /// Task ID to set notes for
        task_id: String,
        /// Notes text (empty or "clear" to remove notes)
        notes: Option<String>,
    },
    /// Set or clear group for a download task
    DlGroup {
        /// Task ID to set group for
        task_id: String,
        /// Group name (empty or "clear" to remove from group)
        group: Option<String>,
    },
    /// List all download groups
    DlGroups,
    /// Set or clear mirror/fallback URLs for a download task
    DlMirror {
        /// Task ID to set mirrors for
        task_id: String,
        /// Comma-separated mirror URLs (empty or "clear" to remove mirrors)
        urls: Vec<String>,
    },
    /// List mirror URLs for a download task
    DlMirrorList {
        /// Task ID to list mirrors for
        task_id: String,
    },
    /// Set checksum for a download task
    DlChecksum {
        task_id: String,
        checksum: String,
        algorithm: Option<String>,
    },
    /// Manage post-download hooks
    DlHook {
        subcommand: String,
        args: Vec<String>,
    },
    /// Manage RSS/Atom feed subscriptions
    DlRss {
        subcommand: String,
        args: Vec<String>,
    },
    /// Show ETA estimates for active downloads
    DlEta {
        task_id: Option<String>,
    },
    /// Manage auto-categorization rules
    DlAutoRule {
        subcommand: String,
        args: Vec<String>,
    },
    Block {
        peer: String,
    },
    Unblock {
        peer: String,
    },
    Fingerprint,
    /// Send message to legacy IPMSG peer by IP address
    IpMsg {
        ip: String,
        message: String,
    },
    /// List legacy IPMSG peers
    IpMsgPeers,
    Clear,
    Quit,
    Unknown(String),
}

fn parse_command(input: &str) -> Command {
    let input = input.strip_prefix('/').unwrap_or(input);
    let parts: Vec<&str> = input.splitn(3, ' ').collect();

    match parts[0].to_lowercase().as_str() {
        "help" | "h" => Command::Help,
        "nick" | "n" => {
            if parts.len() > 1 {
                Command::Nick(parts[1].to_string())
            } else {
                Command::Unknown("nick requires a name".to_string())
            }
        }
        "msg" | "m" | "dm" => {
            if parts.len() >= 3 {
                Command::Msg {
                    target: parts[1].to_string(),
                    content: parts[2].to_string(),
                }
            } else {
                Command::Unknown("/msg <peer> <text>".to_string())
            }
        }
        "peers" | "p" | "list" => Command::Peers,
        "join" | "j" => {
            if parts.len() > 1 {
                let name = parts[1].to_string();
                if name.starts_with("geo:") {
                    Command::GeoJoin(name.strip_prefix("geo:").unwrap().to_string())
                } else {
                    Command::Join(name)
                }
            } else {
                Command::Unknown("/join <channel>".to_string())
            }
        }
        "leave" | "part" | "l" => {
            if parts.len() > 1 {
                Command::Leave(parts[1].to_string())
            } else {
                Command::Unknown("/leave <channel>".to_string())
            }
        }
        "who" | "w" => Command::Who,
        "ping" => Command::Ping,
        "file" | "send" => {
            if parts.len() >= 3 {
                Command::File {
                    target: parts[1].to_string(),
                    path: parts[2].to_string(),
                }
            } else {
                Command::Unknown("/file <peer> <path>".to_string())
            }
        }
        "share" => {
            if parts.len() >= 2 {
                let path = parts[1].to_string();
                let tags = if parts.len() >= 3 {
                    parts[2].split(',').map(|s| s.trim().to_string()).collect()
                } else {
                    Vec::new()
                };
                Command::Share { path, tags }
            } else {
                Command::Unknown("/share <path> [tags]".to_string())
            }
        }
        "unshare" => {
            if parts.len() >= 2 {
                Command::Unshare {
                    hash: parts[1].to_string(),
                }
            } else {
                Command::Unknown("/unshare <hash>".to_string())
            }
        }
        "search" => {
            if parts.len() >= 2 {
                let query = parts[1].to_string();
                let tags = if parts.len() >= 3 {
                    parts[2].split(',').map(|s| s.trim().to_string()).collect()
                } else {
                    Vec::new()
                };
                Command::Search { query, tags }
            } else {
                Command::Unknown("/search <query> [tags]".to_string())
            }
        }
        "files" => Command::Files,
        "download" | "dl" => {
            if parts.len() >= 3 {
                Command::Download {
                    hash: parts[1].to_string(),
                    peer: parts[2].to_string(),
                }
            } else if parts.len() == 2 {
                // /dl <torrent|ed2k|url>
                Command::Dl {
                    target: parts[1].to_string(),
                }
            } else {
                Command::Unknown("/download <hash> <peer> or /dl <target>".to_string())
            }
        }
        "dls" => Command::Dls,
        "dlp" => {
            if parts.len() >= 2 {
                Command::Dlp {
                    task_id: parts[1].to_string(),
                }
            } else {
                Command::Unknown("/dlp <task_id>".to_string())
            }
        }
        "dlr" => {
            if parts.len() >= 2 {
                Command::Dlr {
                    task_id: parts[1].to_string(),
                }
            } else {
                Command::Unknown("/dlr <task_id>".to_string())
            }
        }
        "dlspeed" | "dl-speed" => {
            if parts.len() >= 2 {
                Command::DlSpeed {
                    limit: parts[1].to_string(),
                }
            } else {
                Command::Unknown("/dlspeed <limit>".to_string())
            }
        }
        "dltaskspeed" | "dl-task-speed" => {
            if parts.len() >= 3 {
                Command::DlTaskSpeed {
                    task_id: parts[1].to_string(),
                    limit: parts[2].to_string(),
                }
            } else {
                Command::Unknown("/dltaskspeed <task_id> <limit>".to_string())
            }
        }
        "dltimeout" | "dl-timeout" => {
            if parts.len() >= 2 {
                let max_retries = parts.get(2).and_then(|s| s.parse().ok()).unwrap_or(3);
                Command::DlTimeout {
                    timeout: parts[1].to_string(),
                    max_retries,
                }
            } else {
                Command::Unknown("/dltimeout <timeout> [max_retries]".to_string())
            }
        }
        "dlconcurrent" | "dl-concurrent" => {
            if parts.len() >= 2 {
                match parts[1].parse::<usize>() {
                    Ok(max) => Command::DlConcurrent { max },
                    Err(_) => Command::Unknown("/dlconcurrent <number>".to_string()),
                }
            } else {
                Command::Unknown("/dlconcurrent <number>".to_string())
            }
        }
        "dlpauseall" | "dl-pause-all" => Command::DlPauseAll,
        "dlresumeall" | "dl-resume-all" => Command::DlResumeAll,
        "dlrmcompleted" | "dl-rm-completed" => Command::DlRmCompleted,
        "dlrmfailed" | "dl-rm-failed" => Command::DlRmFailed,
        "dlstats" | "dl-stats" => Command::DlStats,
        "dlhealth" | "dl-health" | "dlh" => Command::DlHealth,
        "dlspeedhist" | "dl-speed-hist" | "dlsh" => {
            // /dlsh [task_id]
            let args = &parts[1..];
            let task_id = if args.is_empty() {
                None
            } else {
                Some(args[0].to_string())
            };
            Command::DlSpeedHistory { task_id }
        }
        "dlcleanup" | "dl-cleanup" | "dlcl" => {
            let args: Vec<String> = parts[1..].iter().map(|s| s.to_string()).collect();
            Command::DlCleanup { args }
        }
        "dltag" | "dl-tag" => {
            if parts.len() >= 3 {
                let task_id = parts[1].to_string();
                let tags: Vec<String> = parts[2].split(',').map(|s| s.trim().to_string()).collect();
                Command::DlTag { task_id, tags }
            } else {
                Command::Unknown("/dltag <task_id> <tag1,tag2,...>".to_string())
            }
        }
        "dluntag" | "dl-untag" => {
            if parts.len() >= 3 {
                let task_id = parts[1].to_string();
                let tags: Vec<String> = parts[2].split(',').map(|s| s.trim().to_string()).collect();
                Command::DlUntag { task_id, tags }
            } else {
                Command::Unknown("/dluntag <task_id> <tag1,tag2,...>".to_string())
            }
        }
        "dltags" | "dl-tags" => {
            let tag = parts.get(1).map(|s| s.to_string());
            Command::DlTags { tag }
        }
        "dlfind" | "dl-find" | "dlf" => {
            // /dlfind [query] [--state=running] [--protocol=torrent] [--sort=name] [--asc]
            let args: Vec<&str> = input.split_whitespace().collect();
            let mut query: Option<String> = None;
            let mut state_filter: Option<String> = None;
            let mut protocol: Option<String> = None;
            let mut sort: Option<String> = None;
            let mut asc = false;
            for arg in args.iter().skip(1) {
                if let Some(val) = arg.strip_prefix("--state=") {
                    state_filter = Some(val.to_string());
                } else if let Some(val) = arg.strip_prefix("--protocol=") {
                    protocol = Some(val.to_string());
                } else if let Some(val) = arg.strip_prefix("--sort=") {
                    sort = Some(val.to_string());
                } else if *arg == "--asc" {
                    asc = true;
                } else if query.is_none() && !arg.starts_with("--") {
                    query = Some(arg.to_string());
                } else {
                    // Unknown flag, ignore
                }
            }
            Command::DlFind {
                query,
                state_filter,
                protocol,
                sort,
                asc,
            }
        }
        "dlpriority" | "dl-priority" | "dlpri" => {
            // /dlpriority <task_id> <high|normal|low>
            let args: Vec<&str> = input.split_whitespace().collect();
            if args.len() >= 3 {
                Command::DlPriority {
                    task_id: args[1].to_string(),
                    priority: args[2].to_string(),
                }
            } else {
                Command::Unknown("/dlpriority <task_id> <high|normal|low>".to_string())
            }
        }
        "dlnotify" | "dl-notify" | "dln" => {
            // /dlnotify <action> [value]
            // Actions: enable, disable, desktop, shell <cmd>, log <path>, webhook <url>, status
            let args: Vec<&str> = input.split_whitespace().collect();
            if args.len() >= 2 {
                let action = args[1].to_string();
                let value = if args.len() >= 3 {
                    Some(args[2..].join(" "))
                } else {
                    None
                };
                Command::DlNotify { action, value }
            } else {
                Command::Unknown(
                    "/dlnotify <enable|disable|desktop|shell|log|webhook|status>".to_string(),
                )
            }
        }
        "dlschedule" | "dl-schedule" | "dlsch" => {
            // /dlschedule <task_id> <HH:MM-HH:MM|none>
            let args: Vec<&str> = input.split_whitespace().collect();
            if args.len() >= 3 {
                Command::DlSchedule {
                    task_id: args[1].to_string(),
                    window: args[2].to_string(),
                }
            } else {
                Command::Unknown("/dlschedule <task_id> <HH:MM-HH:MM|none>".to_string())
            }
        }
        "dlbw" | "dl-bandwidth" | "dl-bw" => {
            // /dlbw <task_id> <weight>
            let args: Vec<&str> = input.split_whitespace().collect();
            if args.len() >= 3 {
                if let Ok(w) = args[2].parse::<u8>() {
                    Command::DlBandwidth {
                        task_id: args[1].to_string(),
                        weight: w,
                    }
                } else {
                    Command::Unknown("/dlbw <task_id> <1-10>".to_string())
                }
            } else {
                Command::Unknown("/dlbw <task_id> <1-10>".to_string())
            }
        }
        "dlbwmon" | "dl-bandwidth-mon" | "dlbwm" => Command::DlBandwidthMon,
        "dlchart" | "dl-chart" | "dlc" => {
            let window = if parts.len() > 1 {
                parts[1].parse::<u64>().ok()
            } else {
                None
            };
            Command::DlChart { window }
        }
        "dlqmove" | "dl-queue-move" | "dlqm" => {
            // /dlqmove <task_id> <up|down|top|bottom>
            let args: Vec<&str> = input.split_whitespace().collect();
            if args.len() >= 3 {
                let dir = args[2].to_lowercase();
                if ["up", "down", "top", "bottom"].contains(&dir.as_str()) {
                    Command::DlQueueMove {
                        task_id: args[1].to_string(),
                        direction: dir,
                    }
                } else {
                    Command::Unknown("/dlqmove <task_id> <up|down|top|bottom>".to_string())
                }
            } else {
                Command::Unknown("/dlqmove <task_id> <up|down|top|bottom>".to_string())
            }
        }
        "dlddeps" | "dl-deps" | "dld" => {
            // /dlddeps <task_id> <dep1,dep2,dep3|none>
            let args: Vec<&str> = input.split_whitespace().collect();
            if args.len() >= 3 {
                Command::DlDeps {
                    task_id: args[1].to_string(),
                    deps: args[2].to_string(),
                }
            } else {
                Command::Unknown("/dlddeps <task_id> <dep1,dep2,dep3|none>".to_string())
            }
        }
        "dlbatch" | "dl-import" => {
            // /dlbatch <file_path> - Import URLs from a file (one per line)
            let args: Vec<&str> = input.split_whitespace().collect();
            if args.len() >= 2 {
                Command::DlBatch {
                    source: args[1].to_string(),
                }
            } else {
                Command::Unknown("/dlbatch <file_path>".to_string())
            }
        }
        "dlextract" | "dl-extract" => {
            // /dlextract <file_path> - Extract download URLs from arbitrary text
            let args: Vec<&str> = input.split_whitespace().collect();
            if args.len() >= 2 {
                Command::DlExtract {
                    path: args[1].to_string(),
                }
            } else {
                Command::Unknown("/dlextract <file_path>".to_string())
            }
        }
        "dlexport" | "dl-export" => {
            // /dlexport <output_path> [description]
            let args: Vec<&str> = input.splitn(3, ' ').collect();
            if args.len() >= 2 {
                Command::DlExport {
                    path: args[1].to_string(),
                    description: args.get(2).map(|s| s.to_string()),
                }
            } else {
                Command::Unknown("/dlexport <output_path> [description]".to_string())
            }
        }
        "dlimp" | "dl-imp" => {
            // /dlimp <input_path> - Import tasks from a JSON export file
            let args: Vec<&str> = input.split_whitespace().collect();
            if args.len() >= 2 {
                Command::DlImport {
                    path: args[1].to_string(),
                }
            } else {
                Command::Unknown("/dlimp <input_path>".to_string())
            }
        }
        "dlsegment" | "dl-segment" | "dlseg" => {
            // /dlsegment <url> - Download URL using multi-segment parallel connections
            let args: Vec<&str> = input.splitn(2, ' ').collect();
            if args.len() >= 2 && !args[1].trim().is_empty() {
                Command::DlSegment {
                    url: args[1].trim().to_string(),
                }
            } else {
                Command::Unknown("/dlsegment <url>".to_string())
            }
        }
        "dlautoshutdown" | "dl-auto-shutdown" | "dlas" => {
            // /dlautoshutdown <disabled|exit|shell:<command>>
            let args: Vec<&str> = input.splitn(2, ' ').collect();
            if args.len() >= 2 {
                Command::DlAutoshutdown {
                    action: args[1].to_string(),
                }
            } else {
                Command::Unknown("/dlautoshutdown <disabled|exit|shell:<command>>".to_string())
            }
        }
        "dlpath" | "dl-path" | "dlsp" => {
            // /dlpath <path>
            let args: Vec<&str> = input.splitn(2, ' ').collect();
            if args.len() >= 2 {
                Command::DlPath {
                    path: args[1].to_string(),
                }
            } else {
                Command::Unknown("/dlpath <path>".to_string())
            }
        }
        "dlorganize" | "dl-organize" | "dlorg" => {
            // /dlorganize <on|off>
            let args: Vec<&str> = input.splitn(2, ' ').collect();
            if args.len() >= 2 {
                Command::DlOrganize {
                    enabled: args[1].to_string(),
                }
            } else {
                Command::Unknown("/dlorganize <on|off>".to_string())
            }
        }
        "dlproxy" | "dl-proxy" | "dlpx" => {
            if parts.len() >= 2 {
                if parts[1] == "test" {
                    Command::DlProxyTest
                } else {
                    Command::DlProxy {
                        url: parts[1].to_string(),
                    }
                }
            } else {
                Command::Unknown("/dlproxy <url|test|none>".to_string())
            }
        }
        "dlrename" | "dl-rename" | "dlrn" => {
            // /dlrename <task_id> <new_name>
            let args: Vec<&str> = input.splitn(3, ' ').collect();
            if args.len() >= 3 {
                Command::DlRename {
                    task_id: args[1].to_string(),
                    new_name: args[2].to_string(),
                }
            } else {
                Command::Unknown("/dlrename <task_id> <new_name>".to_string())
            }
        }
        "dlnotes" | "dl-note" | "dlnote" => {
            // /dlnotes <task_id> [notes_text|clear]
            let args: Vec<&str> = input.splitn(3, ' ').collect();
            if args.len() >= 2 {
                let notes = if args.len() >= 3 {
                    let n = args[2].trim();
                    if n.eq_ignore_ascii_case("clear") || n.is_empty() {
                        None
                    } else {
                        Some(n.to_string())
                    }
                } else {
                    None
                };
                Command::DlNotes {
                    task_id: args[1].to_string(),
                    notes,
                }
            } else {
                Command::Unknown("/dlnotes <task_id> [notes|clear]".to_string())
            }
        }
        "dlgroup" | "dl-group" | "dlgrp" => {
            // /dlgroup <task_id> [group_name|clear]
            let args: Vec<&str> = input.splitn(3, ' ').collect();
            if args.len() >= 2 {
                let group = if args.len() >= 3 {
                    let g = args[2].trim();
                    if g.eq_ignore_ascii_case("clear") || g.is_empty() {
                        None
                    } else {
                        Some(g.to_string())
                    }
                } else {
                    None
                };
                Command::DlGroup {
                    task_id: args[1].to_string(),
                    group,
                }
            } else {
                Command::Unknown("/dlgroup <task_id> [group|clear]".to_string())
            }
        }
        "dlgroups" | "dl-groups" | "dlgrps" => Command::DlGroups,
        "dlarule" | "dl-auto-rule" | "dlar" => {
            // /dlarule <add|list|del> [args...]
            let args: Vec<&str> = input.splitn(2, ' ').collect();
            if args.len() >= 2 {
                let rest = args[1].trim();
                let sub_parts: Vec<&str> = rest.splitn(2, ' ').collect();
                let subcommand = sub_parts[0].to_string();
                let sub_args = if sub_parts.len() > 1 {
                    sub_parts[1].to_string()
                } else {
                    String::new()
                };
                Command::DlAutoRule {
                    subcommand,
                    args: vec![sub_args],
                }
            } else {
                Command::Unknown("/dlarule <add|list|del> [args...]".to_string())
            }
        }
        "dlarules" | "dl-auto-rules" | "dlars" => Command::DlAutoRule {
            subcommand: "list".to_string(),
            args: vec![],
        },
        "dlmirror" | "dl-mirror" | "dlmir" => {
            // /dlmirror <task_id> <url1,url2,...|clear>
            let args: Vec<&str> = input.splitn(3, ' ').collect();
            if args.len() >= 2 {
                let urls = if args.len() >= 3 {
                    let raw = args[2].trim();
                    if raw.eq_ignore_ascii_case("clear") || raw.is_empty() {
                        Vec::new()
                    } else {
                        raw.split(',')
                            .map(|s| s.trim().to_string())
                            .filter(|s| !s.is_empty())
                            .collect()
                    }
                } else {
                    Vec::new()
                };
                Command::DlMirror {
                    task_id: args[1].to_string(),
                    urls,
                }
            } else {
                Command::Unknown("/dlmirror <task_id> <url1,url2,...|clear>".to_string())
            }
        }
        "dlmirrors" | "dl-mirror-list" | "dlmirr" => {
            // /dlmirrors <task_id>
            let args: Vec<&str> = input.splitn(2, ' ').collect();
            if args.len() >= 2 {
                Command::DlMirrorList {
                    task_id: args[1].to_string(),
                }
            } else {
                Command::Unknown("/dlmirrors <task_id>".to_string())
            }
        }
        "dlchecksum" | "dl-cs" | "dlcs" => {
            // /dlchecksum <task_id> <checksum> [algorithm]
            let args: Vec<&str> = input.splitn(4, ' ').collect();
            if args.len() >= 3 {
                Command::DlChecksum {
                    task_id: args[1].to_string(),
                    checksum: args[2].to_string(),
                    algorithm: args.get(3).map(|s| s.to_string()),
                }
            } else {
                Command::Unknown("/dlchecksum <task_id> <checksum> [algorithm]".to_string())
            }
        }
        "dlhook" | "dl-hook" | "dlhk" => {
            // /dlhook <subcommand> [args...]
            // Subcommands: list, add, remove, enable, disable
            let args: Vec<&str> = input.split_whitespace().collect();
            if args.len() >= 2 {
                Command::DlHook {
                    subcommand: args[1].to_string(),
                    args: args[2..].iter().map(|s| s.to_string()).collect(),
                }
            } else {
                Command::Unknown("/dlhook <list|add|remove|enable|disable> [args...]".to_string())
            }
        }
        "dlrss" | "dl-rss" | "dlfeed" => {
            // /dlrss <subcommand> [args...]
            // Subcommands: list, add, remove, enable, disable, poll
            let args: Vec<&str> = input.split_whitespace().collect();
            if args.len() >= 2 {
                Command::DlRss {
                    subcommand: args[1].to_string(),
                    args: args[2..].iter().map(|s| s.to_string()).collect(),
                }
            } else {
                Command::Unknown(
                    "/dlrss <list|add|remove|enable|disable|poll> [args...]".to_string(),
                )
            }
        }
        "dleta" | "dl-eta" => Command::DlEta {
            task_id: parts.get(1).map(|s| s.to_string()),
        },
        "block" => {
            if parts.len() >= 2 {
                Command::Block {
                    peer: parts[1].to_string(),
                }
            } else {
                Command::Unknown("/block <peer>".to_string())
            }
        }
        "unblock" => {
            if parts.len() >= 2 {
                Command::Unblock {
                    peer: parts[1].to_string(),
                }
            } else {
                Command::Unknown("/unblock <peer>".to_string())
            }
        }
        "fingerprint" | "fp" => Command::Fingerprint,
        "ipmsg" | "legacy" => {
            if parts.len() >= 3 {
                Command::IpMsg {
                    ip: parts[1].to_string(),
                    message: parts[2].to_string(),
                }
            } else {
                Command::Unknown("/ipmsg <ip> <message>".to_string())
            }
        }
        "ipmsg-peers" | "legacy-peers" => Command::IpMsgPeers,
        "clear" | "cls" => Command::Clear,
        "quit" | "exit" | "q" => Command::Quit,
        _ => Command::Unknown(input.to_string()),
    }
}

fn command_help() -> String {
    vec![
        "/help          - Show this help",
        "/nick <name>   - Change display name",
        "/msg <peer>    - Send DM to peer",
        "/peers         - List connected peers",
        "/join <name>   - Join a channel",
        "/join geo:<h>  - Join location channel (geohash)",
        "/leave <name>  - Leave a channel",
        "/who           - Show online peers",
        "/ping          - Pong!",
        "/share <path> [tags] - Share a file with the network",
        "/unshare <hash> - Stop sharing a file",
        "/search <query> [tags] - Search for files in the network",
        "/files         - List all shared files",
        "/download <hash> <peer> - Download a file from a peer",
        "/dl <target>       - Multi-protocol download (torrent/ed2k/url)",
        "/dls               - List download tasks",
        "/dlp <task_id>     - Pause a download task",
        "/dlr <task_id>     - Resume a download task",
        "/dlspeed <limit>   - Set global download speed limit (e.g., 100KB/s, 1MB/s, 0=unlimited)",
        "/dltaskspeed <id> <limit> - Set per-task speed limit (0=use global default)",
        "/dltimeout <timeout> [max_retries] - Set download timeout (e.g., 30s, 5m, 0=disable)",
        "/dlconcurrent <n> - Set max concurrent downloads (0=unlimited)",
        "/dlpauseall      - Pause all running downloads",
        "/dlresumeall     - Resume all paused downloads",
        "/dlrmcompleted   - Remove all completed downloads",
        "/dlrmfailed      - Remove all failed downloads",
        "/dlstats         - Show download statistics",
        "/dlhealth        - Show download queue health report",
        "/dlspeedhist [id] - Show speed history (all tasks or specific task)",
        "/dlcleanup [cmd]  - Auto-cleanup completed/failed (status|enable|disable|set|run)",
        "/dltag <id> <tags>   - Add tags to a download (comma-separated)",
        "/dluntag <id> <tags> - Remove tags from a download",
        "/dltags [tag]    - List all tags, or filter tasks by tag",
        "/dlfind [query] [--state=X] [--protocol=X] [--sort=X] [--asc] - Search/filter downloads",
        "/dlpriority <id> <high|normal|low> - Set download task priority",
        "/dlbw <id> <1-10>    - Set bandwidth weight (higher = more bandwidth)",
        "/dlbwmon           - Show bandwidth monitoring dashboard",
        "/dlproxy <url|test|none> - Configure download proxy (e.g., socks5://127.0.0.1:1080) or test connection",
        "/dlqmove <id> <up|down|top|bottom> - Move task in queue",
        "/dlddeps <id> <dep1,dep2,...|none> - Set task dependencies",
        "/dlexport <path> [desc] - Export tasks to JSON file",
        "/dlimp <path>      - Import tasks from JSON export file",
        "/dlsegment <url>   - Download URL using multi-segment parallel connections",
        "/dlextract <path>  - Extract download URLs from arbitrary text file",
        "/dlautoshutdown <disabled|exit|shell:<cmd>> - Auto-shutdown when all downloads complete",
        "/dlnotify <action> [value] - Configure notifications (enable/disable/desktop/shell/log/webhook/status)",
        "/dlpath <path>       - Set download save path (absolute path)",
        "/dlorganize <on|off> - Enable/disable auto-organize by file type",
        "/dlrename <id> <name> - Rename a download task",
        "/dlnotes <id> [text|clear] - Set or clear task notes/description",
        "/dlgroup <id> [group|clear] - Set or clear task group",
        "/dlgroups           - List all download groups",
        "/dlmirror <id> <urls> - Set mirror URLs (comma-separated, 'clear' to remove)",
        "/dlmirrors <id>     - List mirrors for a task",
        "/dlchecksum <id> <hash> [algo] - Set checksum for verification (algo: md5/sha1/sha256/ed2k)",
        "/dlhook <list|add|remove|enable|disable> - Manage post-download hooks",
        "/dlrss <list|add|remove|enable|disable|poll> - Manage RSS/Atom feed subscriptions",
        "/dleta [task_id]   - Show ETA estimates for active downloads",
        "/block <peer>  - Block a peer",
        "/unblock <peer> - Unblock a peer",
        "/fingerprint   - Show your fingerprint for verification",
        "/ipmsg <ip> <msg> - Send message to legacy IPMSG peer",
        "/ipmsg-peers     - List legacy IPMSG peers",
        "/clear         - Clear messages",
        "/quit          - Exit",
    ]
    .join("\n")
}

struct TabView {
    name: String,
    messages: Vec<ChatMessage>,
    channel: Option<ChannelId>,
}

struct SharedState {
    tabs: Vec<TabView>,
    active_tab: usize,
    peers: Vec<String>,
    peer_details: HashMap<String, (String, Vec<String>)>, // peer_id -> (username, platforms)
    status: String,
    my_peer_id: String,
    my_fingerprint: String,
    input: String,
    running: bool,
    username: String,
    download_manager: Arc<ipmsg_download::DownloadManager>,
}

impl SharedState {
    fn new(peer_id: String, username: String, data_dir: PathBuf) -> Self {
        let main_tab = TabView {
            name: "main".to_string(),
            messages: Vec::new(),
            channel: None,
        };
        let download_manager = Arc::new(ipmsg_download::DownloadManager::new(data_dir));
        Self {
            tabs: vec![main_tab],
            active_tab: 0,
            peers: Vec::new(),
            peer_details: HashMap::new(),
            status: "Ready".to_string(),
            my_peer_id: peer_id,
            my_fingerprint: String::new(),
            input: String::new(),
            running: true,
            username,
            download_manager,
        }
    }

    fn active_tab(&self) -> &TabView {
        &self.tabs[self.active_tab]
    }

    fn find_or_create_tab(&mut self, name: &str, channel: Option<ChannelId>) -> usize {
        if let Some(idx) = self.tabs.iter().position(|t| t.name == name) {
            idx
        } else {
            self.tabs.push(TabView {
                name: name.to_string(),
                messages: Vec::new(),
                channel,
            });
            self.tabs.len() - 1
        }
    }

    fn find_tab_for_channel(&self, channel: &ChannelId) -> Option<usize> {
        self.tabs
            .iter()
            .position(|t| t.channel.as_ref() == Some(channel))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_new("ipmsg=info,libp2p=warn").unwrap_or_default())
        .with_writer(std::io::stderr)
        .init();

    let data_dir = match &cli.data_dir {
        Some(path) if path.starts_with("~") => {
            let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());
            PathBuf::from(format!("{}{}", home, &path[1..]))
        }
        Some(path) => PathBuf::from(path),
        None => PathBuf::from(format!(
            "{}/.ipmsg",
            std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string())
        )),
    };

    let bootstrap: Vec<String> = cli
        .bootstrap
        .as_ref()
        .map(|s| s.split(',').map(|s| s.trim().to_string()).collect())
        .unwrap_or_default();

    let username = cli.username.clone();
    let mut engine = P2PEngine::new(data_dir.clone())?;
    let peer_id = engine
        .start(cli.username.clone(), bootstrap, cli.port)
        .await?;
    let fingerprint = engine.my_fingerprint();

    // Start legacy IPMSG compatibility server
    if let Err(e) = engine.start_ipmsg_compat().await {
        eprintln!("Warning: Failed to start IPMSG compat server: {}", e);
    }

    let mut event_rx = engine.take_receiver().expect("receiver already taken");
    let cmd_tx = engine
        .take_command_sender()
        .expect("command sender already taken");

    // Spawn swarm loop
    tokio::spawn(async move {
        engine.run_event_loop().await;
    });

    // Headless mode: just log events to stdout
    if cli.headless {
        println!("P2P engine running in headless mode. Press Ctrl+C to exit.");
        println!("Commands: /msg <peer> <text>, /peers, /help");

        // Create state for headless mode
        let mut state_obj = SharedState::new(peer_id.clone(), username.clone(), data_dir.clone());
        state_obj.my_fingerprint = fingerprint.clone();
        let state = Arc::new(Mutex::new(state_obj));

        // Spawn stdin reader thread
        let (stdin_tx, mut stdin_rx) = tokio::sync::mpsc::channel::<String>(16);
        std::thread::spawn(move || {
            use std::io::BufRead;
            let stdin = std::io::stdin();
            for line in stdin.lock().lines() {
                match line {
                    Ok(l) => {
                        if stdin_tx.blocking_send(l).is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });

        loop {
            tokio::select! {
                Ok(()) = tokio::signal::ctrl_c() => {
                    break;
                }
                Some(line) = stdin_rx.recv() => {
                    let line = line.trim().to_string();
                    if !line.is_empty() {
                        let cmd = parse_command(&line);
                        handle_command_headless(&state, &cmd_tx, &cmd, &peer_id).await;
                    }
                }
                result = event_rx.recv() => {
                    match result {
                        Some(evt) => {
                            match evt {
                                P2PEvent::MessageReceived(msg) => {
                                    let content = match &msg.kind {
                                        ipmsg_protocol::message::MessageType::Text { content } => content.clone(),
                                        _ => msg.kind.label().to_string(),
                                    };
                                    println!("[{}] {}: {}", msg.timestamp.format("%H:%M"), msg.from, content);
                                }
                                P2PEvent::MessageSent(msg) => {
                                    println!("[{}] you: {}", msg.timestamp.format("%H:%M"),
                                        match &msg.kind {
                                            ipmsg_protocol::message::MessageType::Text { content } => content.clone(),
                                            _ => msg.kind.label().to_string(),
                                        });
                                }
                                P2PEvent::PeerJoined { peer_id: pid, username: uname, .. } => {
                                    println!("Peer joined: {} ({})", uname, &pid[..8.min(pid.len())]);
                                }
                                P2PEvent::PeerLeft { peer_id: pid } => {
                                    println!("Peer left: {}", &pid[..8.min(pid.len())]);
                                }
                                P2PEvent::Status(st) => { println!("Status: {}", st); }
                                P2PEvent::ExternalAddress(addr) => {
                                    println!("External address: {}", addr);
                                }
                                _ => {}
                            }
                        }
                        None => break,
                    }
                }
            }
        }
        println!("Goodbye!");
        return Ok(());
    }

    let mut terminal = setup_terminal()?;
    let mut state_obj = SharedState::new(peer_id, username.clone(), data_dir.clone());
    state_obj.my_fingerprint = fingerprint;
    let state = Arc::new(Mutex::new(state_obj));

    // Auto-join channels
    if let Some(channels) = &cli.join {
        let mut s = state.lock().await;
        for ch in channels {
            if ch.starts_with("geo:") {
                let hash = ch.strip_prefix("geo:").unwrap();
                let channel = ChannelId::Geohash(hash.to_string());
                let idx = s.find_or_create_tab(&format!("@{}", hash), Some(channel.clone()));
                s.active_tab = idx;
                s.add_system_message("main", format!("Joined geohash channel @{}", hash));
            } else {
                let channel = ChannelId::Group(ch.clone());
                let idx = s.find_or_create_tab(&format!("#{}", ch), Some(channel.clone()));
                s.active_tab = idx;
                s.add_system_message("main", format!("Joined channel #{}", ch));
            }
        }
    }

    // Main TUI loop
    loop {
        // Drain events
        while let Ok(evt) = event_rx.try_recv() {
            let mut s = state.lock().await;
            match evt {
                P2PEvent::MessageReceived(msg) => {
                    let target = if let Some(ref ch) = msg.channel {
                        if let Some(idx) = s.find_tab_for_channel(ch) {
                            let old = s.active_tab;
                            s.active_tab = idx;
                            s.tabs[idx].messages.push(msg);
                            s.active_tab = old;
                            continue;
                        } else {
                            let name = ch.label();
                            s.find_or_create_tab(&name, Some(ch.clone()));
                            s.tabs.last_mut().unwrap().messages.push(msg);
                            continue;
                        }
                    } else if msg.to.as_ref() == Some(&s.my_peer_id) {
                        // DM to us
                        let tab_name = format!("dm:{}", &msg.from[..8.min(msg.from.len())]);
                        let idx = s.find_or_create_tab(&tab_name, None);
                        s.tabs[idx].messages.push(msg);
                        continue;
                    } else {
                        "main".to_string()
                    };
                    s.add_message(&target, msg);
                }
                P2PEvent::MessageSent(msg) => {
                    let tab = s.active_tab;
                    s.tabs[tab].messages.push(msg);
                }
                P2PEvent::PeerJoined {
                    peer_id: pid,
                    username: uname,
                    platforms,
                } => {
                    if !s.peers.contains(&pid) {
                        s.peers.push(pid.clone());
                    }
                    s.peer_details
                        .insert(pid.clone(), (uname.clone(), platforms.clone()));
                    let platforms_str = if platforms.is_empty() {
                        String::new()
                    } else {
                        format!(" [{}]", platforms.join(", "))
                    };
                    s.set_status(format!(
                        "Peer joined: {}{}{}",
                        uname,
                        platforms_str,
                        &pid[..8.min(pid.len())]
                    ));
                }
                P2PEvent::PeerLeft { peer_id: pid } => {
                    s.peers.retain(|p| p != &pid);
                    s.set_status(format!("Peer left: {}", &pid[..8.min(pid.len())]));
                }
                P2PEvent::Typing { from } => {
                    s.set_status(format!("{} is typing...", from));
                }
                P2PEvent::Status(st) => {
                    s.set_status(st);
                }
                P2PEvent::ExternalAddress(addr) => {
                    s.add_system_message("main", format!("External address: {}", addr));
                }
                P2PEvent::PeerAddressesDiscovered { peer_id, addrs } => {
                    tracing::debug!(%peer_id, count = addrs.len(), "Peer addresses saved for bootstrap");
                }
                P2PEvent::LegacyPeerDiscovered { name, host, ip } => {
                    s.add_system_message(
                        "main",
                        format!("Legacy IPMSG peer discovered: {}@{} ({})", name, host, ip),
                    );
                }
                P2PEvent::LegacyPeerLeft { name, ip } => {
                    s.add_system_message(
                        "main",
                        format!("Legacy IPMSG peer left: {} ({})", name, ip),
                    );
                }
                P2PEvent::LegacyMessageReceived {
                    from,
                    ip,
                    content,
                    has_attachment,
                } => {
                    let attach_str = if has_attachment {
                        " [has attachment]"
                    } else {
                        ""
                    };
                    s.add_system_message(
                        "main",
                        format!("[IPMSG] {} ({}): {}{}", from, ip, content, attach_str),
                    );
                }
                _ => {}
            }
        }

        {
            let s = state.lock().await;
            draw(&mut terminal, &s)?;
            if !s.running {
                break;
            }
            drop(s);

            if event::poll(Duration::from_millis(100))?
                && let Event::Key(key) = event::read()?
            {
                if key.kind != KeyEventKind::Press {
                    continue;
                }
                let mut s = state.lock().await;
                match key.code {
                    KeyCode::Enter => {
                        let input = s.input.clone();
                        s.input.clear();
                        drop(s);
                        handle_command(&state, &cmd_tx, &input).await;
                    }
                    KeyCode::Char(c) => {
                        state.lock().await.input.push(c);
                    }
                    KeyCode::Backspace => {
                        state.lock().await.input.pop();
                    }
                    KeyCode::Esc => {
                        state.lock().await.running = false;
                    }
                    KeyCode::Tab => {
                        let len = state.lock().await.tabs.len();
                        if len > 1 {
                            let mut s = state.lock().await;
                            s.active_tab = (s.active_tab + 1) % len;
                        }
                    }
                    KeyCode::Left => {
                        let mut s = state.lock().await;
                        if s.active_tab > 0 {
                            s.active_tab -= 1;
                        }
                    }
                    KeyCode::Right => {
                        let mut s = state.lock().await;
                        if s.active_tab + 1 < s.tabs.len() {
                            s.active_tab += 1;
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    restore_terminal(terminal)?;
    println!("Goodbye!");
    Ok(())
}

impl SharedState {
    fn add_message(&mut self, tab: &str, msg: ChatMessage) {
        if let Some(idx) = self.tabs.iter().position(|t| t.name == tab) {
            self.tabs[idx].messages.push(msg);
        }
    }

    fn add_system_message(&mut self, tab: &str, text: String) {
        let msg = ChatMessage::new_text("system".to_string(), None, text);
        self.add_message(tab, msg);
    }

    fn set_status(&mut self, text: String) {
        self.status = text;
    }
}

async fn handle_command(
    state: &Arc<Mutex<SharedState>>,
    cmd_tx: &tokio::sync::mpsc::UnboundedSender<SendCommand>,
    input: &str,
) {
    if !input.starts_with('/') {
        // Regular message - send to active tab's channel or broadcast
        let s = state.lock().await;
        let content = input.to_string();
        let active = s.active_tab;
        let tab_name = s.tabs[active].name.clone();
        let channel = s.tabs[active].channel.clone();
        drop(s);

        if let Some(ch) = channel {
            let _ = cmd_tx.send(SendCommand::SendToChannel {
                channel: ch,
                content,
            });
        } else if let Some(peer) = tab_name.strip_prefix("dm:") {
            // DM: peer is the short identifier, we need the full peer ID
            let full_peer = {
                let s = state.lock().await;
                s.peers.iter().find(|p| p.starts_with(peer)).cloned()
            };
            if let Some(peer_id) = full_peer {
                let _ = cmd_tx.send(SendCommand::SendText {
                    to: peer_id,
                    content,
                });
            }
        } else {
            // Broadcast to main
            let _ = cmd_tx.send(SendCommand::Broadcast { content });
        }
        return;
    }

    let cmd = parse_command(input);
    match cmd {
        Command::Help => {
            let mut s = state.lock().await;
            s.add_system_message("main", command_help());
        }
        Command::Nick(name) => {
            let mut s = state.lock().await;
            let old = s.username.clone();
            s.username = name.clone();
            s.add_system_message("main", format!("{} is now known as {}", old, name));
        }
        Command::Msg { target, content } => {
            let full_peer = {
                let s = state.lock().await;
                s.peers.iter().find(|p| p.starts_with(&target)).cloned()
            };
            if let Some(peer_id) = full_peer {
                let _ = cmd_tx.send(SendCommand::SendText {
                    to: peer_id,
                    content: content.clone(),
                });
                let mut s = state.lock().await;
                s.add_system_message("main", format!("Sent DM to {}", target));
            } else {
                let mut s = state.lock().await;
                s.add_system_message("main", format!("Peer not found: {}", target));
            }
        }
        Command::Peers => {
            let s = state.lock().await;
            let peer_list: Vec<String> = s
                .peers
                .iter()
                .map(|p| {
                    let detail = s.peer_details.get(p);
                    match detail {
                        Some((uname, platforms)) => format!(
                            "{} - {} [{}]",
                            &p[..8.min(p.len())],
                            uname,
                            platforms.join(", ")
                        ),
                        None => format!("{} - unknown", &p[..8.min(p.len())]),
                    }
                })
                .collect();
            let mut s = state.lock().await;
            if peer_list.is_empty() {
                s.add_system_message("main", "No peers connected".to_string());
            } else {
                s.add_system_message(
                    "main",
                    format!(
                        "Connected peers ({}):\n{}",
                        peer_list.len(),
                        peer_list.join("\n")
                    ),
                );
            }
        }
        Command::Join(name) => {
            let channel = ChannelId::Group(name.clone());
            let tab_name = format!("#{}", name);
            let msg_text = format!("Joined channel #{}", name);
            let _ = cmd_tx.send(SendCommand::AddChannel {
                channel: channel.clone(),
            });
            let mut s = state.lock().await;
            let idx = s.find_or_create_tab(&tab_name, Some(channel));
            s.active_tab = idx;
            s.add_system_message(&tab_name, msg_text);
        }
        Command::GeoJoin(hash) => {
            let channel = ChannelId::Geohash(hash.clone());
            let tab_name = format!("@{}", hash);
            let msg_text = format!("Joined geohash channel @{}", hash);
            let _ = cmd_tx.send(SendCommand::AddChannel {
                channel: channel.clone(),
            });
            let mut s = state.lock().await;
            let idx = s.find_or_create_tab(&tab_name, Some(channel));
            s.active_tab = idx;
            s.add_system_message(&tab_name, msg_text);
        }
        Command::Leave(name) => {
            let mut s = state.lock().await;
            if let Some(idx) = s
                .tabs
                .iter()
                .position(|t| t.name == format!("#{}", name) || t.name == format!("@{}", name))
            {
                let removed_name = s.tabs[idx].name.clone();
                let removed_channel = s.tabs[idx].channel.clone();
                s.tabs.remove(idx);
                if let Some(channel) = removed_channel {
                    let _ = cmd_tx.send(SendCommand::RemoveChannel { channel });
                }
                if s.active_tab >= s.tabs.len() {
                    s.active_tab = s.tabs.len().saturating_sub(1);
                }
                s.add_system_message("main", format!("Left {}", removed_name));
            }
        }
        Command::Who => {
            let s = state.lock().await;
            let mut lines = vec![format!("Online peers ({}):", s.peers.len())];
            for p in &s.peers {
                if let Some((uname, platforms)) = s.peer_details.get(p) {
                    lines.push(format!(
                        "  {} - {} ({})",
                        &p[..8.min(p.len())],
                        uname,
                        platforms.join(", ")
                    ));
                }
            }
            drop(s);
            let mut s = state.lock().await;
            s.add_system_message("main", lines.join("\n"));
        }
        Command::Ping => {
            let mut s = state.lock().await;
            s.add_system_message("main", "Pong! (local)".to_string());
        }
        Command::Clear => {
            let mut s = state.lock().await;
            let idx = s.active_tab;
            s.tabs[idx].messages.clear();
        }
        Command::Quit => {
            state.lock().await.running = false;
        }
        Command::Unknown(why) => {
            let mut s = state.lock().await;
            s.add_system_message("main", format!("Unknown command: {}", why));
        }
        Command::File { target, path } => {
            // Initiate file transfer to peer: share file then notify peer
            let full_peer = {
                let s = state.lock().await;
                s.peers.iter().find(|p| p.starts_with(&target)).cloned()
            };
            if let Some(_peer_id) = full_peer {
                let _ = cmd_tx.send(SendCommand::ShareFile {
                    path: PathBuf::from(&path),
                    tags: vec![],
                    description: Some(format!("Direct transfer to {}", target)),
                });
                let mut s = state.lock().await;
                s.add_system_message(
                    "main",
                    format!("Sharing file {} with peer {}...", path, target),
                );
            } else {
                let mut s = state.lock().await;
                s.add_system_message("main", format!("Peer not found: {}", target));
            }
        }
        Command::Share { path, tags } => {
            let _ = cmd_tx.send(SendCommand::ShareFile {
                path: PathBuf::from(path),
                tags,
                description: None,
            });
            let mut s = state.lock().await;
            s.add_system_message("main", "Sharing file...".to_string());
        }
        Command::Unshare { hash } => {
            let _ = cmd_tx.send(SendCommand::UnshareFile { hash });
            let mut s = state.lock().await;
            s.add_system_message("main", "File unshared".to_string());
        }
        Command::Search { query, tags } => {
            let _ = cmd_tx.send(SendCommand::SearchFiles { query, tags });
            let mut s = state.lock().await;
            s.add_system_message("main", "Searching for files...".to_string());
        }
        Command::Files => {
            let _ = cmd_tx.send(SendCommand::ListFiles);
            let mut s = state.lock().await;
            s.add_system_message("main", "Listing shared files...".to_string());
        }
        Command::Download { hash, peer } => {
            let _ = cmd_tx.send(SendCommand::DownloadFile {
                file_hash: hash.clone(),
                from_peer: peer.clone(),
            });
            let mut s = state.lock().await;
            s.add_system_message(
                "main",
                format!(
                    "Downloading file {} from {}...",
                    &hash[..8.min(hash.len())],
                    &peer[..8.min(peer.len())]
                ),
            );
        }
        Command::Dl { target } => {
            let s = state.lock().await;
            let download_manager = s.download_manager.clone();
            drop(s);

            let mut s = state.lock().await;
            if target.ends_with(".torrent") {
                // Torrent file
                match download_manager
                    .add_torrent(std::path::PathBuf::from(&target))
                    .await
                {
                    Ok(task_id) => {
                        s.add_system_message(
                            "main",
                            format!(
                                "Started torrent download: {}",
                                &task_id[..8.min(task_id.len())]
                            ),
                        );
                    }
                    Err(e) => {
                        s.add_system_message("main", format!("Failed to start torrent: {}", e));
                    }
                }
            } else if target.starts_with("ed2k://") {
                // ed2k link: ed2k://|file|<name>|<size>|<hash>|/
                let parts: Vec<&str> = target.split('|').collect();
                if parts.len() >= 5 {
                    let name = parts[2].to_string();
                    let size: u64 = parts[3].parse().unwrap_or(0);
                    let hash = parts[4].to_string();
                    match ipmsg_download::ed2k::Ed2kFileHash::from_hex(&hash) {
                        Ok(file_hash) => {
                            match download_manager
                                .add_ed2k(file_hash, size, name, vec![])
                                .await
                            {
                                Ok(task_id) => {
                                    s.add_system_message(
                                        "main",
                                        format!(
                                            "Started ed2k download: {}",
                                            &task_id[..8.min(task_id.len())]
                                        ),
                                    );
                                }
                                Err(e) => {
                                    s.add_system_message(
                                        "main",
                                        format!("Failed to start ed2k: {}", e),
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            s.add_system_message("main", format!("Invalid hash: {}", e));
                        }
                    }
                } else {
                    s.add_system_message("main", "Invalid ed2k link format".to_string());
                }
            } else if target.starts_with("http://")
                || target.starts_with("https://")
                || target.starts_with("ftp://")
            {
                // HTTP/FTP URL (auto-detect size via HEAD)
                match download_manager.add_url(&target).await {
                    Ok(task_id) => {
                        s.add_system_message(
                            "main",
                            format!(
                                "Started P2SP download: {}",
                                &task_id[..8.min(task_id.len())]
                            ),
                        );
                    }
                    Err(e) => {
                        s.add_system_message("main", format!("Failed to start P2SP: {}", e));
                    }
                }
            } else {
                s.add_system_message("main", "Unsupported download target. Use .torrent file, ed2k:// link, or http(s):// URL".to_string());
            }
        }
        Command::Dls => {
            let s = state.lock().await;
            let download_manager = s.download_manager.clone();
            drop(s);

            let tasks = download_manager.list_tasks().await;
            let mut s = state.lock().await;
            if tasks.is_empty() {
                s.add_system_message("main", "No download tasks".to_string());
            } else {
                let mut lines = vec![format!("Download tasks ({}):", tasks.len())];
                for task in tasks {
                    let progress = task.progress();
                    let state_str = task.state_label();
                    let speed_str = if task.speed_bps > 0.0 {
                        format!(" {:.1} KB/s", task.speed_bps / 1024.0)
                    } else {
                        String::new()
                    };
                    let error_str = task
                        .error
                        .as_ref()
                        .map(|e| format!(" [{}]", e))
                        .unwrap_or_default();
                    lines.push(format!(
                        "  {} - {} [{:.1}%] ({}){}{}",
                        &task.id[..8.min(task.id.len())],
                        task.name,
                        progress,
                        state_str,
                        speed_str,
                        error_str
                    ));
                }
                s.add_system_message("main", lines.join("\n"));
            }
        }
        Command::Dlp { task_id } => {
            let s = state.lock().await;
            let download_manager = s.download_manager.clone();
            drop(s);

            let mut s = state.lock().await;
            if download_manager.pause_task(&task_id).await {
                s.add_system_message(
                    "main",
                    format!("Paused task {}", &task_id[..8.min(task_id.len())]),
                );
            } else {
                s.add_system_message(
                    "main",
                    format!("Failed to pause task {}", &task_id[..8.min(task_id.len())]),
                );
            }
        }
        Command::Dlr { task_id } => {
            let s = state.lock().await;
            let download_manager = s.download_manager.clone();
            drop(s);

            let mut s = state.lock().await;
            if download_manager.resume_task(&task_id).await {
                s.add_system_message(
                    "main",
                    format!("Resumed task {}", &task_id[..8.min(task_id.len())]),
                );
            } else {
                s.add_system_message(
                    "main",
                    format!("Failed to resume task {}", &task_id[..8.min(task_id.len())]),
                );
            }
        }
        Command::DlSpeed { limit } => {
            let s = state.lock().await;
            let download_manager = s.download_manager.clone();
            drop(s);

            // Parse speed limit string
            let bytes_per_sec = parse_speed_limit(&limit);
            let mut s = state.lock().await;
            match bytes_per_sec {
                Some(bps) => {
                    download_manager.set_global_speed_limit(bps).await;
                    let limit_str = if bps == 0 {
                        "unlimited".to_string()
                    } else {
                        format!("{:.1} KB/s", bps as f64 / 1024.0)
                    };
                    s.add_system_message(
                        "main",
                        format!("Download speed limit set to {}", limit_str),
                    );
                }
                None => {
                    s.add_system_message(
                        "main",
                        "Invalid speed limit format. Use: 100KB/s, 1MB/s, or 0 for unlimited"
                            .to_string(),
                    );
                }
            }
        }
        Command::DlTaskSpeed { task_id, limit } => {
            let s = state.lock().await;
            let download_manager = s.download_manager.clone();
            drop(s);

            let bytes_per_sec = parse_speed_limit(&limit);
            let mut s = state.lock().await;
            match bytes_per_sec {
                Some(bps) => {
                    let limit_opt = if bps == 0 { None } else { Some(bps) };
                    download_manager
                        .set_task_speed_limit_per_task(&task_id, limit_opt)
                        .await;
                    let limit_str = if bps == 0 {
                        "unlimited (global default)".to_string()
                    } else {
                        format!("{:.1} KB/s", bps as f64 / 1024.0)
                    };
                    s.add_system_message(
                        "main",
                        format!("Task {} speed limit set to {}", task_id, limit_str),
                    );
                }
                None => {
                    s.add_system_message(
                        "main",
                        "Invalid speed limit format. Use: 100KB/s, 1MB/s, or 0 for unlimited"
                            .to_string(),
                    );
                }
            }
        }
        Command::DlTimeout {
            timeout,
            max_retries,
        } => {
            let s = state.lock().await;
            let download_manager = s.download_manager.clone();
            drop(s);

            // Parse timeout string (e.g., "30s", "5m", "0" to disable)
            let timeout_secs = parse_timeout(&timeout);
            match timeout_secs {
                Some(secs) => {
                    download_manager.set_timeout_secs(secs);
                    download_manager.set_max_retries(max_retries);
                    let timeout_str = if secs == 0 {
                        "disabled".to_string()
                    } else {
                        format!("{}s", secs)
                    };
                    let mut s = state.lock().await;
                    s.add_system_message(
                        "main",
                        format!(
                            "Download timeout set to {} (max retries: {})",
                            timeout_str, max_retries
                        ),
                    );
                }
                None => {
                    let mut s = state.lock().await;
                    s.add_system_message(
                        "main",
                        "Invalid timeout format. Use: 30s, 5m, or 0 to disable".to_string(),
                    );
                }
            }
        }
        Command::DlConcurrent { max } => {
            let s = state.lock().await;
            let download_manager = s.download_manager.clone();
            drop(s);
            download_manager.set_max_concurrent(max);
            let mut s = state.lock().await;
            let msg = if max == 0 {
                "Maximum concurrent downloads set to unlimited".to_string()
            } else {
                format!("Maximum concurrent downloads set to {}", max)
            };
            s.add_system_message("main", msg);
        }
        Command::DlPauseAll => {
            let s = state.lock().await;
            let download_manager = s.download_manager.clone();
            drop(s);
            let count = download_manager.pause_all().await;
            let mut s = state.lock().await;
            s.add_system_message("main", format!("Paused {} download(s)", count));
        }
        Command::DlResumeAll => {
            let s = state.lock().await;
            let download_manager = s.download_manager.clone();
            drop(s);
            let count = download_manager.resume_all().await;
            let mut s = state.lock().await;
            s.add_system_message("main", format!("Resumed {} download(s)", count));
        }
        Command::DlRmCompleted => {
            let s = state.lock().await;
            let download_manager = s.download_manager.clone();
            drop(s);
            let count = download_manager.remove_completed().await;
            let mut s = state.lock().await;
            s.add_system_message("main", format!("Removed {} completed download(s)", count));
        }
        Command::DlRmFailed => {
            let s = state.lock().await;
            let download_manager = s.download_manager.clone();
            drop(s);
            let count = download_manager.remove_failed().await;
            let mut s = state.lock().await;
            s.add_system_message("main", format!("Removed {} failed download(s)", count));
        }
        Command::DlStats => {
            let s = state.lock().await;
            let download_manager = s.download_manager.clone();
            drop(s);
            let stats = download_manager.get_stats().await;
            let speed_str = format_speed(stats.total_speed_bps);
            let size_str = format_size(stats.total_downloaded);
            let total_size_str = format_size(stats.total_size);
            let msg = format!(
                "Download Statistics:\n\
                 Total: {} | Running: {} | Paused: {} | Completed: {} | Queued: {} | Error: {}\n\
                 Speed: {} | Downloaded: {} / {}\n\
                 Protocols: Torrent={} Ed2k={} Xunlei={} Magnet={} P2P={}",
                stats.total_tasks,
                stats.running,
                stats.paused,
                stats.completed,
                stats.queued,
                stats.errored,
                speed_str,
                size_str,
                total_size_str,
                stats.by_protocol.torrent,
                stats.by_protocol.ed2k,
                stats.by_protocol.xunlei,
                stats.by_protocol.magnet,
                stats.by_protocol.p2p,
            );
            let mut s = state.lock().await;
            s.add_system_message("main", msg);
        }
        Command::DlHealth => {
            let s = state.lock().await;
            let download_manager = s.download_manager.clone();
            drop(s);
            let config = ipmsg_download::queue_health::HealthMonitorConfig::default();
            let report = download_manager.get_queue_health_report(&config).await;
            let msg = report.format_report();
            let mut s = state.lock().await;
            s.add_system_message("main", msg);
        }
        Command::DlSpeedHistory { task_id } => {
            let s = state.lock().await;
            let download_manager = s.download_manager.clone();
            drop(s);

            match task_id {
                Some(id) => match download_manager.get_task_speed_history(&id).await {
                    Some(summary) => {
                        let msg = summary.format_summary();
                        let mut s = state.lock().await;
                        s.add_system_message("main", msg);
                    }
                    None => {
                        let mut s = state.lock().await;
                        s.add_system_message("main", format!("No speed history for task {}", id));
                    }
                },
                None => {
                    let summaries = download_manager.get_all_speed_history_summaries().await;
                    if summaries.is_empty() {
                        let mut s = state.lock().await;
                        s.add_system_message("main", "No speed history for any task".to_string());
                    } else {
                        let mut msg = String::from("Speed History Summary:\n");
                        for summary in summaries {
                            msg.push_str(&format!(
                                "  Task {}: {:.1} KB/s (avg 5m: {:.1} KB/s, peak: {:.1} KB/s, {} samples)\n",
                                summary.task_id,
                                summary.latest_speed / 1024.0,
                                summary.avg_5min / 1024.0,
                                summary.peak_speed / 1024.0,
                                summary.sample_count,
                            ));
                        }
                        let mut s = state.lock().await;
                        s.add_system_message("main", msg);
                    }
                }
            }
        }
        Command::DlCleanup { args } => {
            let s = state.lock().await;
            let download_manager = s.download_manager.clone();
            drop(s);

            if args.is_empty() || args[0] == "status" {
                let config = download_manager.get_auto_cleanup().await;
                let msg = config.display();
                let mut s = state.lock().await;
                s.add_system_message("main", msg);
            } else if args[0] == "enable" {
                let mut config = download_manager.get_auto_cleanup().await;
                config.enabled = true;
                download_manager.set_auto_cleanup(config.clone()).await;
                let mut s = state.lock().await;
                s.add_system_message("main", "✅ Auto-cleanup enabled".to_string());
            } else if args[0] == "disable" {
                let mut config = download_manager.get_auto_cleanup().await;
                config.enabled = false;
                download_manager.set_auto_cleanup(config).await;
                let mut s = state.lock().await;
                s.add_system_message("main", "🚫 Auto-cleanup disabled".to_string());
            } else if args[0] == "set" && args.len() >= 2 {
                use ipmsg_download::auto_cleanup::parse_duration_secs;
                let completed = parse_duration_secs(&args[1]);
                let failed = if args.len() >= 3 {
                    parse_duration_secs(&args[2])
                } else {
                    None
                };
                let config = ipmsg_download::auto_cleanup::AutoCleanupConfig {
                    enabled: true,
                    completed_retention_secs: completed,
                    failed_retention_secs: failed,
                    check_interval_secs: 300,
                };
                download_manager.set_auto_cleanup(config.clone()).await;
                let msg = config.display();
                let mut s = state.lock().await;
                s.add_system_message("main", format!("✅ {}", msg));
            } else if args[0] == "run" {
                let count = download_manager.run_auto_cleanup().await;
                let mut s = state.lock().await;
                s.add_system_message("main", format!("🧹 Auto-cleanup removed {} tasks", count));
            } else {
                let mut s = state.lock().await;
                s.add_system_message(
                    "main",
                    "Usage: /dlcleanup [status|enable|disable|set <retention> [failed_retention]|run]"
                        .to_string(),
                );
            }
        }
        Command::DlTag { task_id, tags } => {
            let s = state.lock().await;
            let download_manager = s.download_manager.clone();
            drop(s);
            let success = download_manager.add_tags(&task_id, tags.clone()).await;
            let mut s = state.lock().await;
            if success {
                s.add_system_message("main", format!("Added tags to task {}", task_id));
            } else {
                s.add_system_message("main", format!("Task {} not found", task_id));
            }
        }
        Command::DlUntag { task_id, tags } => {
            let s = state.lock().await;
            let download_manager = s.download_manager.clone();
            drop(s);
            let success = download_manager.remove_tags(&task_id, tags).await;
            let mut s = state.lock().await;
            if success {
                s.add_system_message("main", format!("Removed tags from task {}", task_id));
            } else {
                s.add_system_message("main", format!("Task {} not found", task_id));
            }
        }
        Command::DlTags { tag } => {
            let s = state.lock().await;
            let download_manager = s.download_manager.clone();
            drop(s);
            if let Some(tag) = tag {
                let tasks = download_manager.list_tasks_by_tag(&tag).await;
                let mut s = state.lock().await;
                if tasks.is_empty() {
                    s.add_system_message("main", format!("No tasks with tag '{}'", tag));
                } else {
                    let msg = format!("Tasks with tag '{}':\n{}", tag, format_task_list(&tasks));
                    s.add_system_message("main", msg);
                }
            } else {
                let tags = download_manager.list_all_tags().await;
                let mut s = state.lock().await;
                if tags.is_empty() {
                    s.add_system_message("main", "No tags found".to_string());
                } else {
                    s.add_system_message("main", format!("All tags: {}", tags.join(", ")));
                }
            }
        }
        Command::DlFind {
            query,
            state_filter,
            protocol,
            sort,
            asc,
        } => {
            use ipmsg_download::{DownloadProtocol, DownloadState, TaskFilter, TaskSortBy};
            let s = state.lock().await;
            let download_manager = s.download_manager.clone();
            drop(s);

            let filter = TaskFilter {
                query,
                state: state_filter
                    .as_deref()
                    .and_then(|s| match s.to_lowercase().as_str() {
                        "running" | "downloading" => Some(DownloadState::Downloading),
                        "paused" => Some(DownloadState::Paused),
                        "completed" | "complete" => Some(DownloadState::Complete),
                        "error" | "failed" => Some(DownloadState::Error),
                        "queued" => Some(DownloadState::Queued),
                        _ => None,
                    }),
                protocol: protocol
                    .as_deref()
                    .and_then(|p| match p.to_lowercase().as_str() {
                        "torrent" => Some(DownloadProtocol::Torrent),
                        "ed2k" => Some(DownloadProtocol::Ed2k),
                        "xunlei" | "http" | "ftp" => Some(DownloadProtocol::Xunlei),
                        "magnet" => Some(DownloadProtocol::Magnet),
                        "p2p" => Some(DownloadProtocol::P2P),
                        _ => None,
                    }),
                tag: None,
            };

            let sort_by = sort.as_deref().map(|s| match s.to_lowercase().as_str() {
                "name" => {
                    if asc {
                        TaskSortBy::NameAsc
                    } else {
                        TaskSortBy::NameDesc
                    }
                }
                "size" => {
                    if asc {
                        TaskSortBy::SizeAsc
                    } else {
                        TaskSortBy::SizeDesc
                    }
                }
                "progress" => {
                    if asc {
                        TaskSortBy::ProgressAsc
                    } else {
                        TaskSortBy::ProgressDesc
                    }
                }
                "speed" => TaskSortBy::SpeedDesc,
                "created" => {
                    if asc {
                        TaskSortBy::CreatedAsc
                    } else {
                        TaskSortBy::CreatedDesc
                    }
                }
                _ => {
                    if asc {
                        TaskSortBy::CreatedAsc
                    } else {
                        TaskSortBy::CreatedDesc
                    }
                }
            });

            let tasks = download_manager.list_tasks_filtered(filter, sort_by).await;
            let mut s = state.lock().await;
            if tasks.is_empty() {
                s.add_system_message("main", "No matching download tasks".to_string());
            } else {
                let msg = format!(
                    "Found {} task(s):\n{}",
                    tasks.len(),
                    format_task_list(&tasks)
                );
                s.add_system_message("main", msg);
            }
        }
        Command::DlPriority { task_id, priority } => {
            use ipmsg_download::DownloadPriority;
            let s = state.lock().await;
            let download_manager = s.download_manager.clone();
            drop(s);

            let pri = match DownloadPriority::from_str_opt(&priority) {
                Some(p) => p,
                None => {
                    let mut s = state.lock().await;
                    s.add_system_message(
                        "main",
                        format!("Invalid priority: {}. Use high/normal/low", priority),
                    );
                    return;
                }
            };

            if download_manager.set_priority(&task_id, pri).await {
                let mut s = state.lock().await;
                s.add_system_message(
                    "main",
                    format!(
                        "Set task {} priority to {}",
                        &task_id[..8.min(task_id.len())],
                        pri.label()
                    ),
                );
            } else {
                let mut s = state.lock().await;
                s.add_system_message(
                    "main",
                    format!("Task {} not found", &task_id[..8.min(task_id.len())]),
                );
            }
        }
        Command::DlNotify { action, value } => {
            use ipmsg_download::{NotificationChannel, NotificationConfig, NotificationEvent};
            let s = state.lock().await;
            let download_manager = s.download_manager.clone();
            drop(s);

            match action.as_str() {
                "status" => {
                    let mut s = state.lock().await;
                    s.add_system_message(
                        "main",
                        "Notification system: configured via /dlnotify".to_string(),
                    );
                    s.add_system_message(
                        "main",
                        "Actions: enable, disable, desktop, shell <cmd>, log <path>, webhook <url>"
                            .to_string(),
                    );
                }
                "enable" => {
                    let config = NotificationConfig {
                        enabled: true,
                        channels: vec![NotificationChannel::Desktop],
                        events: vec![
                            NotificationEvent::DownloadComplete,
                            NotificationEvent::DownloadFailed,
                        ],
                    };
                    download_manager.set_notification_config(config);
                    let mut s = state.lock().await;
                    s.add_system_message(
                        "main",
                        "✅ Download notifications enabled (desktop)".to_string(),
                    );
                }
                "disable" => {
                    download_manager.set_notification_config(NotificationConfig::disabled());
                    let mut s = state.lock().await;
                    s.add_system_message("main", "❌ Download notifications disabled".to_string());
                }
                "desktop" => {
                    let config = NotificationConfig {
                        enabled: true,
                        channels: vec![NotificationChannel::Desktop],
                        events: vec![NotificationEvent::DownloadComplete],
                    };
                    download_manager.set_notification_config(config);
                    let mut s = state.lock().await;
                    s.add_system_message(
                        "main",
                        "🖥️ Desktop notifications enabled for download completion".to_string(),
                    );
                }
                "shell" => {
                    let cmd = match value {
                        Some(c) => c,
                        None => {
                            let mut s = state.lock().await;
                            s.add_system_message("main", "Usage: /dlnotify shell <command>\nTemplate vars: {name}, {size}, {save_path}, {protocol}, {event}".to_string());
                            return;
                        }
                    };
                    let config = NotificationConfig {
                        enabled: true,
                        channels: vec![NotificationChannel::Shell {
                            command: cmd.clone(),
                        }],
                        events: vec![
                            NotificationEvent::DownloadComplete,
                            NotificationEvent::DownloadFailed,
                        ],
                    };
                    download_manager.set_notification_config(config);
                    let mut s = state.lock().await;
                    s.add_system_message("main", format!("🐚 Shell notification enabled: {}", cmd));
                }
                "log" => {
                    let path = match value {
                        Some(p) => p,
                        None => {
                            let mut s = state.lock().await;
                            s.add_system_message("main", "Usage: /dlnotify log <path>".to_string());
                            return;
                        }
                    };
                    let config = NotificationConfig {
                        enabled: true,
                        channels: vec![NotificationChannel::LogFile {
                            path: path.clone().into(),
                        }],
                        events: vec![
                            NotificationEvent::DownloadComplete,
                            NotificationEvent::DownloadFailed,
                        ],
                    };
                    download_manager.set_notification_config(config);
                    let mut s = state.lock().await;
                    s.add_system_message(
                        "main",
                        format!("📝 Log file notification enabled: {}", path),
                    );
                }
                "webhook" => {
                    let url = match value {
                        Some(u) => u,
                        None => {
                            let mut s = state.lock().await;
                            s.add_system_message(
                                "main",
                                "Usage: /dlnotify webhook <url>".to_string(),
                            );
                            return;
                        }
                    };
                    let config = NotificationConfig {
                        enabled: true,
                        channels: vec![NotificationChannel::Webhook {
                            url: url.clone(),
                            secret: None,
                        }],
                        events: vec![
                            NotificationEvent::DownloadComplete,
                            NotificationEvent::DownloadFailed,
                        ],
                    };
                    download_manager.set_notification_config(config);
                    let mut s = state.lock().await;
                    s.add_system_message(
                        "main",
                        format!("🔗 Webhook notification enabled: {}", url),
                    );
                }
                _ => {
                    let mut s = state.lock().await;
                    s.add_system_message("main", format!("Unknown action: {}. Use: enable, disable, desktop, shell, log, webhook, status", action));
                }
            }
        }
        Command::DlSchedule { task_id, window } => {
            use ipmsg_download::TimeWindow;
            let s = state.lock().await;
            let download_manager = s.download_manager.clone();
            drop(s);

            if window.to_lowercase() == "none" || window.to_lowercase() == "off" {
                let ok = download_manager.set_schedule(&task_id, None).await;
                let mut s = state.lock().await;
                if ok {
                    s.add_system_message(
                        "main",
                        format!(
                            "📅 Schedule removed for task {}",
                            &task_id[..8.min(task_id.len())]
                        ),
                    );
                } else {
                    s.add_system_message(
                        "main",
                        format!("Task {} not found", &task_id[..8.min(task_id.len())]),
                    );
                }
            } else if let Some((start, end)) = window.split_once('-') {
                let parse_hhmm = |s: &str| -> Option<(u8, u8)> {
                    let parts: Vec<&str> = s.split(':').collect();
                    if parts.len() != 2 {
                        return None;
                    }
                    let h = parts[0].parse::<u8>().ok()?;
                    let m = parts[1].parse::<u8>().ok()?;
                    if h > 23 || m > 59 {
                        return None;
                    }
                    Some((h, m))
                };
                match (parse_hhmm(start), parse_hhmm(end)) {
                    (Some((sh, sm)), Some((eh, em))) => {
                        let tw = TimeWindow::new(sh, sm, eh, em);
                        let ok = download_manager.set_schedule(&task_id, tw).await;
                        let mut s = state.lock().await;
                        if ok {
                            s.add_system_message(
                                "main",
                                format!(
                                    "📅 Schedule {} set for task {}",
                                    tw.map(|w| w.format()).unwrap_or_default(),
                                    &task_id[..8.min(task_id.len())]
                                ),
                            );
                        } else {
                            s.add_system_message(
                                "main",
                                format!("Task {} not found", &task_id[..8.min(task_id.len())]),
                            );
                        }
                    }
                    _ => {
                        let mut s = state.lock().await;
                        s.add_system_message(
                            "main",
                            "Invalid time format. Use HH:MM-HH:MM (e.g., 09:00-17:00)".to_string(),
                        );
                    }
                }
            } else {
                let mut s = state.lock().await;
                s.add_system_message(
                    "main",
                    "Usage: /dlschedule <task_id> <HH:MM-HH:MM|none>".to_string(),
                );
            }
        }
        Command::DlBandwidth { task_id, weight } => {
            let s = state.lock().await;
            let download_manager = s.download_manager.clone();
            drop(s);

            let weight = weight.clamp(1, 10);
            let ok = download_manager
                .set_bandwidth_weight(&task_id, weight)
                .await;
            let mut s = state.lock().await;
            if ok {
                s.add_system_message(
                    "main",
                    format!(
                        "⚖️ Bandwidth weight set to {} for task {}",
                        weight,
                        &task_id[..8.min(task_id.len())]
                    ),
                );
            } else {
                s.add_system_message(
                    "main",
                    format!("Task {} not found", &task_id[..8.min(task_id.len())]),
                );
            }
        }
        Command::DlBandwidthMon => {
            let s = state.lock().await;
            let download_manager = s.download_manager.clone();
            drop(s);

            // Collect task speed info
            let tasks = download_manager.list_tasks().await;
            let running_tasks: Vec<_> = tasks
                .iter()
                .filter(|t| t.state == ipmsg_download::DownloadState::Downloading)
                .cloned()
                .collect();

            let task_speeds: Vec<_> = running_tasks
                .iter()
                .map(|t| (t.id.clone(), t.name.clone(), t.speed_bps, t.downloaded))
                .collect();

            let dashboard = download_manager
                .bandwidth_monitor()
                .dashboard(task_speeds)
                .await;

            let mut lines = Vec::new();
            lines.push("📊 Bandwidth Monitor".to_string());
            lines.push("═".repeat(50));

            // Current speed
            lines.push(format!(
                "Current: ↓ {}/s  ↑ {}/s",
                format_speed(dashboard.current_download_bps),
                format_speed(dashboard.current_upload_bps)
            ));
            lines.push(String::new());

            // Window stats
            let format_window = |label: &str, stats: &ipmsg_download::BandwidthStats| -> String {
                format!(
                    "{}: avg ↓ {}/s  peak ↓ {}/s  ({} samples, {}s window)",
                    label,
                    format_speed(stats.avg_download_bps),
                    format_speed(stats.peak_download_bps),
                    stats.sample_count,
                    stats.window_secs
                )
            };
            lines.push(format_window("Last 5 min", &dashboard.last_5min));
            lines.push(format_window("Last 15 min", &dashboard.last_15min));
            lines.push(format_window("Last 60 min", &dashboard.last_60min));

            // Per-task breakdown
            if !dashboard.tasks.is_empty() {
                lines.push(String::new());
                lines.push("Per-task:".to_string());
                for task in &dashboard.tasks {
                    lines.push(format!(
                        "  {} : ↓ {}/s  total {}",
                        truncate_name(&task.task_name, 20),
                        format_speed(task.current_bps),
                        format_size(task.total_downloaded)
                    ));
                }
            }

            let mut s = state.lock().await;
            s.add_system_message("main", lines.join("\n"));
        }
        Command::DlChart { window } => {
            let s = state.lock().await;
            let download_manager = s.download_manager.clone();
            drop(s);

            let window_secs = window.unwrap_or(300);
            let history = download_manager.bandwidth_monitor().history().await;

            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let cutoff = now.saturating_sub(window_secs);

            let samples: Vec<_> = history
                .into_iter()
                .filter(|s| s.timestamp >= cutoff)
                .collect();

            let mut lines = Vec::new();
            lines.push(format!("📈 Speed Trend (last {}s)", window_secs));
            lines.push("═".repeat(60));

            if samples.len() < 2 {
                lines.push("No data yet".to_string());
            } else {
                // Calculate stats
                let speeds: Vec<f64> = samples.iter().map(|s| s.download_bps).collect();
                let max_speed = speeds.iter().cloned().fold(0.0f64, f64::max);
                let avg_speed = speeds.iter().sum::<f64>() / speeds.len() as f64;
                let total_bytes = samples
                    .iter()
                    .map(|s| s.download_bps * 10.0 / 8.0)
                    .sum::<f64>() as u64;

                lines.push(format!(
                    "Avg: {}/s  Peak: {}/s  Samples: {}  Est. Total: {}",
                    format_speed(avg_speed),
                    format_speed(max_speed),
                    samples.len(),
                    format_size(total_bytes)
                ));
                lines.push(String::new());

                // Sparkline chart
                let sparkline = generate_sparkline(&speeds, 50);
                lines.push(sparkline);

                // Time range
                let start_time = chrono::DateTime::from_timestamp(samples[0].timestamp as i64, 0)
                    .map(|dt| dt.format("%H:%M:%S").to_string())
                    .unwrap_or_default();
                let end_time = chrono::DateTime::from_timestamp(
                    samples[samples.len() - 1].timestamp as i64,
                    0,
                )
                .map(|dt| dt.format("%H:%M:%S").to_string())
                .unwrap_or_default();
                lines.push(format!("{} → {}", start_time, end_time));
            }

            let mut s = state.lock().await;
            s.add_system_message("main", lines.join("\n"));
        }
        Command::DlQueueMove { task_id, direction } => {
            let s = state.lock().await;
            let download_manager = s.download_manager.clone();
            drop(s);

            let success = match direction.as_str() {
                "up" => download_manager.move_task_up(&task_id).await,
                "down" => download_manager.move_task_down(&task_id).await,
                "top" => download_manager.move_task_to_top(&task_id).await,
                "bottom" => download_manager.move_task_to_bottom(&task_id).await,
                _ => false,
            };

            let mut s = state.lock().await;
            if success {
                s.add_system_message(
                    "main",
                    format!(
                        "📋 Task {} moved {} in queue",
                        &task_id[..8.min(task_id.len())],
                        direction
                    ),
                );
            } else {
                s.add_system_message(
                    "main",
                    format!(
                        "Task {} not found or cannot be moved {}",
                        &task_id[..8.min(task_id.len())],
                        direction
                    ),
                );
            }
        }
        Command::DlDeps { task_id, deps } => {
            let s = state.lock().await;
            let download_manager = s.download_manager.clone();
            drop(s);

            let dep_list: Vec<String> = if deps.to_lowercase() == "none" {
                Vec::new()
            } else {
                deps.split(',').map(|s| s.trim().to_string()).collect()
            };

            let success = download_manager
                .set_dependencies(&task_id, dep_list.clone())
                .await;

            let mut s = state.lock().await;
            if success {
                if dep_list.is_empty() {
                    s.add_system_message(
                        "main",
                        format!(
                            "🔗 Task {} dependencies cleared",
                            &task_id[..8.min(task_id.len())]
                        ),
                    );
                } else {
                    s.add_system_message(
                        "main",
                        format!(
                            "🔗 Task {} now depends on {} task(s)",
                            &task_id[..8.min(task_id.len())],
                            dep_list.len()
                        ),
                    );
                }
            } else {
                s.add_system_message(
                    "main",
                    format!(
                        "❌ Failed to set dependencies for task {} (task not found, dependency not found, or cycle detected)",
                        &task_id[..8.min(task_id.len())]
                    ),
                );
            }
        }
        Command::DlBatch { source } => {
            let s = state.lock().await;
            let download_manager = s.download_manager.clone();
            drop(s);

            // Read URLs from file
            let urls: Vec<String> = match tokio::fs::read_to_string(&source).await {
                Ok(content) => content
                    .lines()
                    .map(|l| l.trim().to_string())
                    .filter(|l| !l.is_empty() && !l.starts_with('#'))
                    .collect(),
                Err(e) => {
                    let mut s = state.lock().await;
                    s.add_system_message("main", format!("❌ Failed to read {}: {}", source, e));
                    return;
                }
            };

            if urls.is_empty() {
                let mut s = state.lock().await;
                s.add_system_message("main", "⚠️ No URLs found in file".to_string());
                return;
            }

            let results = download_manager.import_urls(&urls).await;
            let added = results
                .iter()
                .filter(|r| matches!(r.outcome, ipmsg_download::ImportOutcome::Added(_)))
                .count();
            let skipped = results
                .iter()
                .filter(|r| matches!(r.outcome, ipmsg_download::ImportOutcome::SkippedDuplicate))
                .count();
            let failed = results
                .iter()
                .filter(|r| matches!(r.outcome, ipmsg_download::ImportOutcome::Failed(_)))
                .count();

            let mut msg = format!("📥 Batch import: {} added", added);
            if skipped > 0 {
                msg.push_str(&format!(", {} skipped (duplicate)", skipped));
            }
            if failed > 0 {
                msg.push_str(&format!(", {} failed", failed));
                // Show first few failures
                for r in results
                    .iter()
                    .filter(|r| matches!(r.outcome, ipmsg_download::ImportOutcome::Failed(_)))
                    .take(3)
                {
                    if let ipmsg_download::ImportOutcome::Failed(e) = &r.outcome {
                        msg.push_str(&format!("\n  ❌ {} - {}", r.url, e));
                    }
                }
            }

            let mut s = state.lock().await;
            s.add_system_message("main", msg);
        }
        Command::DlExtract { path } => {
            let s = state.lock().await;
            let download_manager = s.download_manager.clone();
            drop(s);

            // Read text content from file
            let content = match tokio::fs::read_to_string(&path).await {
                Ok(c) => c,
                Err(e) => {
                    let mut s = state.lock().await;
                    s.add_system_message("main", format!("❌ Failed to read {}: {}", path, e));
                    return;
                }
            };

            // Extract URLs from text
            let urls = ipmsg_download::extract_urls_from_text(&content);

            if urls.is_empty() {
                let mut s = state.lock().await;
                s.add_system_message("main", "⚠️ No download URLs found in file".to_string());
                return;
            }

            let results = download_manager.import_urls(&urls).await;
            let added = results
                .iter()
                .filter(|r| matches!(r.outcome, ipmsg_download::ImportOutcome::Added(_)))
                .count();
            let skipped = results
                .iter()
                .filter(|r| matches!(r.outcome, ipmsg_download::ImportOutcome::SkippedDuplicate))
                .count();
            let failed = results
                .iter()
                .filter(|r| matches!(r.outcome, ipmsg_download::ImportOutcome::Failed(_)))
                .count();

            let mut msg = format!(
                "🔍 Extracted {} URLs from {}\n📥 Imported: {} added",
                urls.len(),
                path,
                added
            );
            if skipped > 0 {
                msg.push_str(&format!(", {} skipped (duplicate)", skipped));
            }
            if failed > 0 {
                msg.push_str(&format!(", {} failed", failed));
                // Show first few failures
                for r in results
                    .iter()
                    .filter(|r| matches!(r.outcome, ipmsg_download::ImportOutcome::Failed(_)))
                    .take(3)
                {
                    if let ipmsg_download::ImportOutcome::Failed(e) = &r.outcome {
                        msg.push_str(&format!("\n  ❌ {} - {}", r.url, e));
                    }
                }
            }

            let mut s = state.lock().await;
            s.add_system_message("main", msg);
        }
        Command::DlExport { path, description } => {
            let s = state.lock().await;
            let download_manager = s.download_manager.clone();
            drop(s);

            let tasks = download_manager.list_tasks().await;
            let output_path = std::path::PathBuf::from(&path);

            match ipmsg_download::task_export::export_tasks(&tasks, &output_path, description) {
                Ok(count) => {
                    let mut s = state.lock().await;
                    s.add_system_message(
                        "main",
                        format!(
                            "✅ Exported {} task{} to {}",
                            count,
                            if count == 1 { "" } else { "s" },
                            path
                        ),
                    );
                }
                Err(e) => {
                    let mut s = state.lock().await;
                    s.add_system_message("main", format!("❌ Export failed: {}", e));
                }
            }
        }
        Command::DlImport { path } => {
            let s = state.lock().await;
            let download_manager = s.download_manager.clone();
            drop(s);

            let input_path = std::path::PathBuf::from(&path);
            let exported = match ipmsg_download::task_export::import_tasks(&input_path) {
                Ok(tasks) => tasks,
                Err(e) => {
                    let mut s = state.lock().await;
                    s.add_system_message("main", format!("❌ Import failed: {}", e));
                    return;
                }
            };

            if exported.is_empty() {
                let mut s = state.lock().await;
                s.add_system_message("main", "⚠️ No tasks found in export file".to_string());
                return;
            }

            let prepared = ipmsg_download::task_export::prepare_imported_tasks(exported);
            let mut imported = 0;
            let mut skipped = 0;

            for (exported_task, _new_id, source_url) in prepared {
                // Try to re-add via source URL if available
                if let Some(url) = &source_url {
                    match download_manager.add_url(url).await {
                        Ok(_task_id) => {
                            imported += 1;
                            continue;
                        }
                        Err(_) => {
                            skipped += 1;
                            continue;
                        }
                    }
                }
                // Tasks without source URL can't be re-imported directly
                skipped += 1;
                let _ = exported_task; // consumed
            }

            let mut msg = format!(
                "📥 Import: {} task{} re-added via URL",
                imported,
                if imported == 1 { "" } else { "s" }
            );
            if skipped > 0 {
                msg.push_str(&format!(
                    ", {} task{} added without source (manual start needed)",
                    skipped,
                    if skipped == 1 { "" } else { "s" }
                ));
            }

            let mut s = state.lock().await;
            s.add_system_message("main", msg);
        }
        Command::DlSegment { url } => {
            let s = state.lock().await;
            let download_manager = s.download_manager.clone();
            drop(s);

            match download_manager.add_http_multisegment(&url).await {
                Ok(task_id) => {
                    let mut s = state.lock().await;
                    s.add_system_message(
                        "main",
                        format!("✅ Multi-segment download added (id: {})", &task_id[..8]),
                    );
                }
                Err(e) => {
                    let mut s = state.lock().await;
                    s.add_system_message(
                        "main",
                        format!("❌ Multi-segment download failed: {}", e),
                    );
                }
            }
        }
        Command::DlAutoshutdown { action } => {
            let s = state.lock().await;
            let download_manager = s.download_manager.clone();
            drop(s);

            use ipmsg_download::auto_shutdown::{AutoShutdownAction, AutoShutdownConfig};

            let config = download_manager.get_auto_shutdown().await;

            if action == "status" {
                let mut s = state.lock().await;
                s.add_system_message(
                    "main",
                    format!("🔧 Auto-shutdown: {}", config.action.display()),
                );
                return;
            }

            match AutoShutdownAction::from_str_opt(&action) {
                Some(new_action) => {
                    let new_config = AutoShutdownConfig {
                        action: new_action,
                        require_empty_queue: config.require_empty_queue,
                    };
                    download_manager.set_auto_shutdown(new_config.clone()).await;
                    let mut s = state.lock().await;
                    s.add_system_message(
                        "main",
                        format!("✅ Auto-shutdown set to: {}", new_config.action.display()),
                    );
                }
                None => {
                    let mut s = state.lock().await;
                    s.add_system_message(
                        "main",
                        "❌ Invalid action. Use: disabled, exit, or shell:<command>".to_string(),
                    );
                }
            }
        }
        Command::DlPath { path } => {
            let s = state.lock().await;
            let download_manager = s.download_manager.clone();
            drop(s);

            let path = std::path::PathBuf::from(path);
            if !path.is_absolute() {
                let mut s = state.lock().await;
                s.add_system_message(
                    "main",
                    "❌ Path must be absolute (e.g., /home/user/downloads)".to_string(),
                );
                return;
            }

            download_manager.set_save_path(path.clone()).await;
            let mut s = state.lock().await;
            s.add_system_message(
                "main",
                format!("✅ Download save path set to: {}", path.display()),
            );
        }
        Command::DlOrganize { enabled } => {
            let s = state.lock().await;
            let download_manager = s.download_manager.clone();
            drop(s);

            match enabled.to_lowercase().as_str() {
                "on" | "true" | "1" | "yes" => {
                    download_manager.set_auto_organize(true).await;
                    let mut s = state.lock().await;
                    s.add_system_message(
                        "main",
                        "✅ Auto-organize enabled. Files will be sorted by type.".to_string(),
                    );
                }
                "off" | "false" | "0" | "no" => {
                    download_manager.set_auto_organize(false).await;
                    let mut s = state.lock().await;
                    s.add_system_message("main", "✅ Auto-organize disabled.".to_string());
                }
                _ => {
                    let mut s = state.lock().await;
                    s.add_system_message("main", "❌ Usage: /dlorganize <on|off>".to_string());
                }
            }
        }
        Command::DlProxy { url } => {
            let s = state.lock().await;
            let download_manager = s.download_manager.clone();
            drop(s);

            if url.to_lowercase() == "none"
                || url.to_lowercase() == "off"
                || url.to_lowercase() == "disable"
            {
                download_manager.set_proxy(None).await;
                let mut s = state.lock().await;
                s.add_system_message("main", "✅ Proxy disabled.".to_string());
            } else if let Ok(proxy_cfg) = ipmsg_download::proxy::ProxyConfig::parse(&url) {
                download_manager.set_proxy(Some(proxy_cfg.clone())).await;
                let mut s = state.lock().await;
                s.add_system_message(
                    "main",
                    format!("✅ Proxy configured: {}", proxy_cfg.to_url()),
                );
            } else {
                let mut s = state.lock().await;
                s.add_system_message(
                    "main",
                    "❌ Invalid proxy URL. Examples: socks5://127.0.0.1:1080, http://user:pass@proxy:8080".to_string(),
                );
            }
        }
        Command::DlProxyTest => {
            let s = state.lock().await;
            let download_manager = s.download_manager.clone();
            drop(s);

            let mut s = state.lock().await;
            s.add_system_message("main", "🔍 Testing proxy connection...".to_string());
            drop(s);

            let result = download_manager.test_proxy_connection().await;
            let mut s = state.lock().await;

            match result {
                Some(test_result) => {
                    if test_result.success {
                        s.add_system_message(
                            "main",
                            format!(
                                "✅ Proxy connection successful! Latency: {}ms",
                                test_result.latency_ms.unwrap_or(0)
                            ),
                        );
                    } else {
                        s.add_system_message(
                            "main",
                            format!(
                                "❌ Proxy connection failed: {}",
                                test_result.error.as_deref().unwrap_or("unknown error")
                            ),
                        );
                    }
                }
                None => {
                    s.add_system_message(
                        "main",
                        "⚠️ No proxy configured. Use /dlproxy <url> to set one.".to_string(),
                    );
                }
            }
        }
        Command::DlRename { task_id, new_name } => {
            let s = state.lock().await;
            let download_manager = s.download_manager.clone();
            drop(s);

            if download_manager
                .rename_task(&task_id, new_name.clone())
                .await
            {
                let mut s = state.lock().await;
                s.add_system_message(
                    "main",
                    format!("✅ Task {} renamed to '{}'", task_id, new_name),
                );
            } else {
                let mut s = state.lock().await;
                s.add_system_message(
                    "main",
                    format!(
                        "❌ Failed to rename task {} (not found or empty name)",
                        task_id
                    ),
                );
            }
        }
        Command::DlNotes { task_id, notes } => {
            let s = state.lock().await;
            let download_manager = s.download_manager.clone();
            drop(s);

            if download_manager
                .set_task_notes(&task_id, notes.clone())
                .await
            {
                let mut s = state.lock().await;
                match notes {
                    Some(n) => s.add_system_message(
                        "main",
                        format!("✅ Notes set for task {}: {}", task_id, n),
                    ),
                    None => s.add_system_message(
                        "main",
                        format!("✅ Notes cleared for task {}", task_id),
                    ),
                }
            } else {
                let mut s = state.lock().await;
                s.add_system_message(
                    "main",
                    format!("❌ Failed to set notes for task {} (not found)", task_id),
                );
            }
        }
        Command::DlGroup { task_id, group } => {
            let s = state.lock().await;
            let download_manager = s.download_manager.clone();
            drop(s);

            if download_manager
                .set_task_group(&task_id, group.clone())
                .await
            {
                let mut s = state.lock().await;
                match group {
                    Some(g) => s.add_system_message(
                        "main",
                        format!("✅ Task {} assigned to group '{}'", task_id, g),
                    ),
                    None => s.add_system_message(
                        "main",
                        format!("✅ Task {} removed from group", task_id),
                    ),
                }
            } else {
                let mut s = state.lock().await;
                s.add_system_message(
                    "main",
                    format!("❌ Failed to set group for task {} (not found)", task_id),
                );
            }
        }
        Command::DlGroups => {
            let s = state.lock().await;
            let download_manager = s.download_manager.clone();
            drop(s);

            let groups = download_manager.list_all_groups().await;
            let mut s = state.lock().await;
            if groups.is_empty() {
                s.add_system_message(
                    "main",
                    "📂 No groups defined. Use /dlgroup <task_id> <group> to assign.".to_string(),
                );
            } else {
                let mut lines = "📂 Download Groups:\n".to_string();
                for g in &groups {
                    let count = download_manager.list_tasks_by_group(g).await.len();
                    lines.push_str(&format!("  • {} ({} tasks)\n", g, count));
                }
                s.add_system_message("main", lines.trim_end().to_string());
            }
        }
        Command::DlAutoRule { subcommand, args } => {
            let s = state.lock().await;
            let download_manager = s.download_manager.clone();
            drop(s);

            match subcommand.as_str() {
                "add" => {
                    // /dlarule add <name> <pattern> <tags> [group]
                    // Example: /dlarule add "Video Files" "*.mp4" "video,media" "Media"
                    let arg_str = args.first().map(|s| s.as_str()).unwrap_or("");
                    let parts: Vec<&str> = arg_str.splitn(5, ' ').collect();
                    if parts.len() < 3 {
                        let mut s = state.lock().await;
                        s.add_system_message(
                            "main",
                            "Usage: /dlarule add <name> <pattern> <tags> [group]\nExample: /dlarule add \"Video Files\" \"*.mp4\" \"video,media\" \"Media\"".to_string(),
                        );
                    } else {
                        let name = parts[0].trim_matches('"').to_string();
                        let pattern_str = parts[1].trim_matches('"').to_string();
                        let tags_str = parts[2].trim_matches('"');
                        let tags: Vec<String> = tags_str
                            .split(',')
                            .map(|s| s.trim().to_string())
                            .filter(|s| !s.is_empty())
                            .collect();
                        let group = parts.get(3).map(|s| s.trim_matches('"').to_string());

                        let pattern = if pattern_str.contains('*') || pattern_str.contains('?') {
                            ipmsg_download::auto_categorize::CategorizePattern::Wildcard(
                                pattern_str,
                            )
                        } else {
                            ipmsg_download::auto_categorize::CategorizePattern::Contains(
                                pattern_str,
                            )
                        };

                        let rule = ipmsg_download::auto_categorize::CategorizeRule {
                            id: uuid::Uuid::new_v4().to_string(),
                            name,
                            pattern,
                            match_url: true,
                            match_filename: true,
                            action: ipmsg_download::auto_categorize::CategorizeAction {
                                tags,
                                group,
                            },
                            enabled: true,
                            priority: 0,
                        };

                        match download_manager.add_categorize_rule(rule).await {
                            Ok(()) => {
                                let mut s = state.lock().await;
                                s.add_system_message(
                                    "main",
                                    "✅ Auto-categorization rule added".to_string(),
                                );
                            }
                            Err(e) => {
                                let mut s = state.lock().await;
                                s.add_system_message(
                                    "main",
                                    format!("❌ Failed to add rule: {}", e),
                                );
                            }
                        }
                    }
                }
                "list" | "ls" => {
                    let rules = download_manager.list_categorize_rules().await;
                    let mut s = state.lock().await;
                    if rules.is_empty() {
                        s.add_system_message(
                            "main",
                            "📋 No auto-categorization rules. Use /dlarule add to create one."
                                .to_string(),
                        );
                    } else {
                        let mut lines = "📋 Auto-categorization Rules:\n".to_string();
                        for (i, rule) in rules.iter().enumerate() {
                            let pattern_desc = match &rule.pattern {
                                ipmsg_download::auto_categorize::CategorizePattern::Contains(s) => {
                                    format!("contains '{}'", s)
                                }
                                ipmsg_download::auto_categorize::CategorizePattern::Wildcard(s) => {
                                    format!("wildcard '{}'", s)
                                }
                                ipmsg_download::auto_categorize::CategorizePattern::Exact(s) => {
                                    format!("exact '{}'", s)
                                }
                            };
                            let status = if rule.enabled { "✅" } else { "❌" };
                            lines.push_str(&format!(
                                "  {}. {} [{}] {} → tags: {:?}",
                                i + 1,
                                status,
                                rule.name,
                                pattern_desc,
                                rule.action.tags
                            ));
                            if let Some(ref g) = rule.action.group {
                                lines.push_str(&format!(", group: {}", g));
                            }
                            lines.push('\n');
                        }
                        s.add_system_message("main", lines.trim_end().to_string());
                    }
                }
                "del" | "remove" | "rm" => {
                    // /dlarule del <rule_id>
                    let rule_id = args.first().map(|s| s.trim()).unwrap_or("");
                    if rule_id.is_empty() {
                        let mut s = state.lock().await;
                        s.add_system_message("main", "Usage: /dlarule del <rule_id>".to_string());
                    } else if download_manager.remove_categorize_rule(rule_id).await {
                        let mut s = state.lock().await;
                        s.add_system_message("main", format!("✅ Rule {} removed", rule_id));
                    } else {
                        let mut s = state.lock().await;
                        s.add_system_message("main", format!("❌ Rule {} not found", rule_id));
                    }
                }
                _ => {
                    let mut s = state.lock().await;
                    s.add_system_message(
                        "main",
                        "Usage: /dlarule <add|list|del> [args...]".to_string(),
                    );
                }
            }
        }
        Command::DlMirror { task_id, urls } => {
            let s = state.lock().await;
            let download_manager = s.download_manager.clone();
            drop(s);

            if download_manager.set_mirrors(&task_id, urls.clone()).await {
                let mut s = state.lock().await;
                if urls.is_empty() {
                    s.add_system_message(
                        "main",
                        format!("🪞 Mirrors cleared for task {}", task_id),
                    );
                } else {
                    s.add_system_message(
                        "main",
                        format!("✅ Set {} mirror(s) for task {}", urls.len(), task_id),
                    );
                }
            } else {
                let mut s = state.lock().await;
                s.add_system_message("main", format!("❌ Task {} not found", task_id));
            }
        }
        Command::DlMirrorList { task_id } => {
            let s = state.lock().await;
            let download_manager = s.download_manager.clone();
            drop(s);

            let mut s = state.lock().await;
            match download_manager.get_mirrors(&task_id).await {
                Some(mirrors) if !mirrors.is_empty() => {
                    let mut lines = format!("🪞 Mirrors for task {}:\n", task_id);
                    for (i, url) in mirrors.iter().enumerate() {
                        lines.push_str(&format!("  {}. {}\n", i + 1, url));
                    }
                    s.add_system_message("main", lines.trim_end().to_string());
                }
                Some(_) => {
                    s.add_system_message(
                        "main",
                        format!("🪞 No mirrors configured for task {}", task_id),
                    );
                }
                None => {
                    s.add_system_message("main", format!("❌ Task {} not found", task_id));
                }
            }
        }
        Command::DlChecksum {
            task_id,
            checksum,
            algorithm,
        } => {
            let s = state.lock().await;
            let download_manager = s.download_manager.clone();
            drop(s);

            // Try to detect algorithm from checksum length if not specified
            let algo = if let Some(algo_str) = &algorithm {
                match ipmsg_download::checksum::ChecksumAlgorithm::parse(algo_str) {
                    Some(a) => a,
                    None => {
                        let mut s = state.lock().await;
                        s.add_system_message(
                            "main",
                            format!(
                                "❌ Unknown algorithm '{}'. Supported: md5, sha1, sha256, ed2k",
                                algo_str
                            ),
                        );
                        return;
                    }
                }
            } else {
                // Auto-detect from checksum length
                match ipmsg_download::checksum::detect_algorithm(&checksum) {
                    Some(a) => a,
                    None => {
                        let mut s = state.lock().await;
                        s.add_system_message(
                            "main",
                            "❌ Cannot auto-detect algorithm from checksum length. Specify algorithm: md5, sha1, sha256, ed2k".to_string(),
                        );
                        return;
                    }
                }
            };

            match download_manager
                .set_task_checksum(&task_id, &checksum, algo)
                .await
            {
                Ok(()) => {
                    let mut s = state.lock().await;
                    s.add_system_message(
                        "main",
                        format!(
                            "✅ Checksum set for task {} ({}: {})",
                            task_id,
                            algo.name(),
                            checksum.to_lowercase()
                        ),
                    );
                }
                Err(e) => {
                    let mut s = state.lock().await;
                    s.add_system_message("main", format!("❌ {}", e));
                }
            }
        }
        Command::DlHook { subcommand, args } => {
            let s = state.lock().await;
            let download_manager = s.download_manager.clone();
            drop(s);

            match subcommand.as_str() {
                "list" | "ls" => {
                    let hooks = download_manager.hook_manager().list_hooks();
                    if hooks.is_empty() {
                        let mut s = state.lock().await;
                        s.add_system_message(
                            "main",
                            "No post-download hooks configured".to_string(),
                        );
                    } else {
                        let mut lines = String::from("📋 Post-download hooks:\n");
                        for hook in hooks {
                            let status = if hook.enabled { "✅" } else { "❌" };
                            let event = match hook.event {
                                ipmsg_download::post_hooks::HookEvent::OnComplete => "complete",
                                ipmsg_download::post_hooks::HookEvent::OnFailure => "failure",
                                ipmsg_download::post_hooks::HookEvent::Both => "both",
                            };
                            lines.push_str(&format!(
                                "  {} [{}] {} (event: {}, timeout: {}s)\n",
                                status,
                                &hook.id[..8],
                                hook.name,
                                event,
                                hook.timeout_secs
                            ));
                        }
                        let mut s = state.lock().await;
                        s.add_system_message("main", lines.trim_end().to_string());
                    }
                }
                "add" => {
                    // /dlhook add <event> <name> <command...>
                    // event: complete|failure|both
                    if args.len() < 3 {
                        let mut s = state.lock().await;
                        s.add_system_message(
                            "main",
                            "Usage: /dlhook add <complete|failure|both> <name> <command...>"
                                .to_string(),
                        );
                        return;
                    }
                    let event = match args[0].as_str() {
                        "complete" | "on_complete" => {
                            ipmsg_download::post_hooks::HookEvent::OnComplete
                        }
                        "failure" | "on_failure" => {
                            ipmsg_download::post_hooks::HookEvent::OnFailure
                        }
                        "both" => ipmsg_download::post_hooks::HookEvent::Both,
                        _ => {
                            let mut s = state.lock().await;
                            s.add_system_message(
                                "main",
                                "❌ Invalid event. Use: complete, failure, or both".to_string(),
                            );
                            return;
                        }
                    };
                    let name = args[1].clone();
                    let command = args[2..].join(" ");
                    let hook =
                        ipmsg_download::post_hooks::PostHook::new(name.clone(), event, command);
                    match download_manager.hook_manager().add_hook(hook) {
                        Ok(hook_id) => {
                            let mut s = state.lock().await;
                            s.add_system_message(
                                "main",
                                format!("✅ Added hook '{}' (id: {})", name, &hook_id[..8]),
                            );
                        }
                        Err(e) => {
                            let mut s = state.lock().await;
                            s.add_system_message("main", format!("❌ Failed to add hook: {}", e));
                        }
                    }
                }
                "remove" | "rm" => {
                    if args.is_empty() {
                        let mut s = state.lock().await;
                        s.add_system_message("main", "Usage: /dlhook remove <hook_id>".to_string());
                        return;
                    }
                    let hook_id = &args[0];
                    // Find hook by ID prefix
                    let hooks = download_manager.hook_manager().list_hooks();
                    let found = hooks.iter().find(|h| h.id.starts_with(hook_id));
                    if let Some(hook) = found {
                        match download_manager.hook_manager().remove_hook(&hook.id) {
                            Ok(true) => {
                                let mut s = state.lock().await;
                                s.add_system_message(
                                    "main",
                                    format!("✅ Removed hook '{}'", hook.name),
                                );
                            }
                            Ok(false) => {
                                let mut s = state.lock().await;
                                s.add_system_message("main", "❌ Hook not found".to_string());
                            }
                            Err(e) => {
                                let mut s = state.lock().await;
                                s.add_system_message(
                                    "main",
                                    format!("❌ Failed to remove hook: {}", e),
                                );
                            }
                        }
                    } else {
                        let mut s = state.lock().await;
                        s.add_system_message("main", "❌ Hook not found".to_string());
                    }
                }
                "enable" => {
                    if args.is_empty() {
                        let mut s = state.lock().await;
                        s.add_system_message("main", "Usage: /dlhook enable <hook_id>".to_string());
                        return;
                    }
                    let hook_id = &args[0];
                    let hooks = download_manager.hook_manager().list_hooks();
                    let found = hooks.iter().find(|h| h.id.starts_with(hook_id));
                    if let Some(hook) = found {
                        match download_manager
                            .hook_manager()
                            .set_hook_enabled(&hook.id, true)
                        {
                            Ok(true) => {
                                let mut s = state.lock().await;
                                s.add_system_message(
                                    "main",
                                    format!("✅ Enabled hook '{}'", hook.name),
                                );
                            }
                            Ok(false) => {
                                let mut s = state.lock().await;
                                s.add_system_message("main", "❌ Hook not found".to_string());
                            }
                            Err(e) => {
                                let mut s = state.lock().await;
                                s.add_system_message(
                                    "main",
                                    format!("❌ Failed to enable hook: {}", e),
                                );
                            }
                        }
                    } else {
                        let mut s = state.lock().await;
                        s.add_system_message("main", "❌ Hook not found".to_string());
                    }
                }
                "disable" => {
                    if args.is_empty() {
                        let mut s = state.lock().await;
                        s.add_system_message(
                            "main",
                            "Usage: /dlhook disable <hook_id>".to_string(),
                        );
                        return;
                    }
                    let hook_id = &args[0];
                    let hooks = download_manager.hook_manager().list_hooks();
                    let found = hooks.iter().find(|h| h.id.starts_with(hook_id));
                    if let Some(hook) = found {
                        match download_manager
                            .hook_manager()
                            .set_hook_enabled(&hook.id, false)
                        {
                            Ok(true) => {
                                let mut s = state.lock().await;
                                s.add_system_message(
                                    "main",
                                    format!("✅ Disabled hook '{}'", hook.name),
                                );
                            }
                            Ok(false) => {
                                let mut s = state.lock().await;
                                s.add_system_message("main", "❌ Hook not found".to_string());
                            }
                            Err(e) => {
                                let mut s = state.lock().await;
                                s.add_system_message(
                                    "main",
                                    format!("❌ Failed to disable hook: {}", e),
                                );
                            }
                        }
                    } else {
                        let mut s = state.lock().await;
                        s.add_system_message("main", "❌ Hook not found".to_string());
                    }
                }
                _ => {
                    let mut s = state.lock().await;
                    s.add_system_message(
                        "main",
                        "Usage: /dlhook <list|add|remove|enable|disable> [args...]".to_string(),
                    );
                }
            }
        }
        Command::DlRss { subcommand, args } => {
            let s = state.lock().await;
            let download_manager = s.download_manager.clone();
            drop(s);

            let rss_mgr = download_manager.rss_feed_manager().cloned();
            let rss_mgr = match rss_mgr {
                Some(mgr) => mgr,
                None => {
                    let mut s = state.lock().await;
                    s.add_system_message(
                        "main",
                        "❌ RSS feed manager not initialized. Call /dlrss init first.".to_string(),
                    );
                    return;
                }
            };

            match subcommand.as_str() {
                "list" | "ls" => {
                    let subs = rss_mgr.list().await;
                    if subs.is_empty() {
                        let mut s = state.lock().await;
                        s.add_system_message(
                            "main",
                            "No RSS feed subscriptions configured".to_string(),
                        );
                    } else {
                        let mut lines = String::from("📡 RSS feed subscriptions:\n");
                        for sub in subs {
                            let status = if sub.enabled { "✅" } else { "❌" };
                            let label = sub.label.as_deref().unwrap_or("(no label)");
                            let filter = sub.title_filter.as_deref().unwrap_or("*");
                            let exts = if sub.extensions.is_empty() {
                                "all".to_string()
                            } else {
                                sub.extensions.join(",")
                            };
                            let last_poll = sub
                                .last_poll
                                .map(|t| t.format("%Y-%m-%d %H:%M").to_string())
                                .unwrap_or_else(|| "never".to_string());
                            lines.push_str(&format!(
                                "  {} [{}] {} ({})\n    filter: {} ext: {} interval: {}s last: {}\n",
                                status,
                                &sub.id[..8],
                                label,
                                sub.feed_url,
                                filter,
                                exts,
                                sub.poll_interval_secs,
                                last_poll,
                            ));
                        }
                        let mut s = state.lock().await;
                        s.add_system_message("main", lines.trim_end().to_string());
                    }
                }
                "add" => {
                    // /dlrss add <feed_url> [label] [title_filter] [ext1,ext2,...]
                    if args.is_empty() {
                        let mut s = state.lock().await;
                        s.add_system_message(
                            "main",
                            "Usage: /dlrss add <feed_url> [label] [title_filter] [ext1,ext2,...]"
                                .to_string(),
                        );
                        return;
                    }
                    let feed_url = &args[0];
                    let label = args.get(1).map(|s| s.as_str());
                    let title_filter = args.get(2).map(|s| s.as_str());
                    let extensions: Vec<String> = args
                        .get(3)
                        .map(|s| s.split(',').map(|e| e.trim().to_string()).collect())
                        .unwrap_or_default();
                    match rss_mgr
                        .add_subscription(feed_url, label, title_filter, extensions)
                        .await
                    {
                        Ok(id) => {
                            let mut s = state.lock().await;
                            s.add_system_message(
                                "main",
                                format!("✅ Added RSS subscription (id: {})", &id[..8]),
                            );
                        }
                        Err(e) => {
                            let mut s = state.lock().await;
                            s.add_system_message("main", format!("❌ Failed to add feed: {}", e));
                        }
                    }
                }
                "remove" | "rm" => {
                    if args.is_empty() {
                        let mut s = state.lock().await;
                        s.add_system_message("main", "Usage: /dlrss remove <sub_id>".to_string());
                        return;
                    }
                    let sub_id = &args[0];
                    let subs = rss_mgr.list().await;
                    let found = subs.iter().find(|s| s.id.starts_with(sub_id));
                    if let Some(sub) = found {
                        match rss_mgr.remove_subscription(&sub.id).await {
                            Ok(()) => {
                                let mut s = state.lock().await;
                                s.add_system_message(
                                    "main",
                                    format!("✅ Removed RSS subscription '{}'", sub.id),
                                );
                            }
                            Err(e) => {
                                let mut s = state.lock().await;
                                s.add_system_message("main", format!("❌ Failed to remove: {}", e));
                            }
                        }
                    } else {
                        let mut s = state.lock().await;
                        s.add_system_message("main", "❌ Subscription not found".to_string());
                    }
                }
                "enable" => {
                    if args.is_empty() {
                        let mut s = state.lock().await;
                        s.add_system_message("main", "Usage: /dlrss enable <sub_id>".to_string());
                        return;
                    }
                    let sub_id = &args[0];
                    let subs = rss_mgr.list().await;
                    let found = subs.iter().find(|s| s.id.starts_with(sub_id));
                    if let Some(sub) = found {
                        match rss_mgr.set_enabled(&sub.id, true).await {
                            Ok(()) => {
                                let mut s = state.lock().await;
                                s.add_system_message(
                                    "main",
                                    format!("✅ Enabled RSS subscription '{}'", sub.id),
                                );
                            }
                            Err(e) => {
                                let mut s = state.lock().await;
                                s.add_system_message("main", format!("❌ Failed to enable: {}", e));
                            }
                        }
                    } else {
                        let mut s = state.lock().await;
                        s.add_system_message("main", "❌ Subscription not found".to_string());
                    }
                }
                "disable" => {
                    if args.is_empty() {
                        let mut s = state.lock().await;
                        s.add_system_message("main", "Usage: /dlrss disable <sub_id>".to_string());
                        return;
                    }
                    let sub_id = &args[0];
                    let subs = rss_mgr.list().await;
                    let found = subs.iter().find(|s| s.id.starts_with(sub_id));
                    if let Some(sub) = found {
                        match rss_mgr.set_enabled(&sub.id, false).await {
                            Ok(()) => {
                                let mut s = state.lock().await;
                                s.add_system_message(
                                    "main",
                                    format!("❌ Disabled RSS subscription '{}'", sub.id),
                                );
                            }
                            Err(e) => {
                                let mut s = state.lock().await;
                                s.add_system_message(
                                    "main",
                                    format!("❌ Failed to disable: {}", e),
                                );
                            }
                        }
                    } else {
                        let mut s = state.lock().await;
                        s.add_system_message("main", "❌ Subscription not found".to_string());
                    }
                }
                "poll" => {
                    // /dlrss poll [sub_id] — poll specific sub or all due
                    if args.is_empty() {
                        // Poll all due
                        let results = rss_mgr.poll_all_due().await;
                        let mut s = state.lock().await;
                        if results.is_empty() {
                            s.add_system_message("main", "No feeds due for polling".to_string());
                        } else {
                            let mut lines = String::from("📡 Polled feeds:\n");
                            for (sub_id, items) in results {
                                lines.push_str(&format!(
                                    "  [{}] {} items\n",
                                    &sub_id[..8],
                                    items.len()
                                ));
                                for item in items.iter().take(5) {
                                    lines.push_str(&format!("    • {}\n", item.title));
                                }
                            }
                            s.add_system_message("main", lines.trim_end().to_string());
                        }
                    } else {
                        let sub_id = &args[0];
                        let subs = rss_mgr.list().await;
                        let found = subs.iter().find(|s| s.id.starts_with(sub_id));
                        if let Some(sub) = found {
                            match rss_mgr.poll_feed(&sub.id).await {
                                Ok(items) => {
                                    let mut s = state.lock().await;
                                    if items.is_empty() {
                                        s.add_system_message(
                                            "main",
                                            format!("No items found in feed '{}'", sub.id),
                                        );
                                    } else {
                                        let mut lines = format!(
                                            "📡 Feed '{}' ({} items):\n",
                                            sub.id,
                                            items.len()
                                        );
                                        for item in items.iter().take(10) {
                                            lines.push_str(&format!(
                                                "  • {} ({})\n",
                                                item.title, item.url
                                            ));
                                        }
                                        s.add_system_message("main", lines.trim_end().to_string());
                                    }
                                }
                                Err(e) => {
                                    let mut s = state.lock().await;
                                    s.add_system_message(
                                        "main",
                                        format!("❌ Failed to poll feed: {}", e),
                                    );
                                }
                            }
                        } else {
                            let mut s = state.lock().await;
                            s.add_system_message("main", "❌ Subscription not found".to_string());
                        }
                    }
                }
                _ => {
                    let mut s = state.lock().await;
                    s.add_system_message(
                        "main",
                        "Usage: /dlrss <list|add|remove|enable|disable|poll> [args...]".to_string(),
                    );
                }
            }
        }
        Command::DlEta { task_id } => {
            let s = state.lock().await;
            let download_manager = s.download_manager.clone();
            drop(s);

            if let Some(tid) = task_id {
                // Show ETA for specific task
                let task = download_manager.get_task(&tid).await;
                match task {
                    Some(task) => {
                        if task.state != ipmsg_download::DownloadState::Downloading {
                            let mut s = state.lock().await;
                            s.add_system_message(
                                "main",
                                format!("Task {} is not downloading", tid),
                            );
                            return;
                        }
                        let remaining = task.size.saturating_sub(task.downloaded);
                        match download_manager
                            .eta_estimator()
                            .estimate(&task.id, remaining)
                            .await
                        {
                            Some(est) => {
                                let mut s = state.lock().await;
                                s.add_system_message(
                                    "main",
                                    format!(
                                        "⏱️  ETA for '{}':\n  Estimated: {}\n  Range: {}–{}\n  Confidence: {:?}\n  Speed: {:.1} KB/s (smoothed), {:.1} KB/s (raw)\n  Samples: {}",
                                        task.name,
                                        est.format_eta(),
                                        format_duration(est.optimistic_secs),
                                        format_duration(est.pessimistic_secs),
                                        est.confidence,
                                        est.smoothed_speed_bps / 1024.0,
                                        est.raw_speed_bps / 1024.0,
                                        est.sample_count
                                    ),
                                );
                            }
                            None => {
                                let mut s = state.lock().await;
                                s.add_system_message(
                                    "main",
                                    format!("Insufficient data for ETA estimate (task: {})", tid),
                                );
                            }
                        }
                    }
                    None => {
                        let mut s = state.lock().await;
                        s.add_system_message("main", format!("Task {} not found", tid));
                    }
                }
            } else {
                // Show ETA for all active downloads
                let tasks = download_manager.list_tasks().await;
                let mut lines = String::from("⏱️  ETA estimates for active downloads:\n");
                let mut count = 0;
                for task in tasks {
                    if task.state == ipmsg_download::DownloadState::Downloading
                        && task.speed_bps > 0.0
                    {
                        let remaining = task.size.saturating_sub(task.downloaded);
                        if let Some(est) = download_manager
                            .eta_estimator()
                            .estimate(&task.id, remaining)
                            .await
                        {
                            lines.push_str(&format!(
                                "  {} [{}]: {} ({}–{}) conf={:?} speed={:.1}KB/s\n",
                                task.name,
                                &task.id[..8],
                                est.format_eta(),
                                format_duration(est.optimistic_secs),
                                format_duration(est.pessimistic_secs),
                                est.confidence,
                                est.smoothed_speed_bps / 1024.0
                            ));
                            count += 1;
                        }
                    }
                }
                if count == 0 {
                    lines =
                        "No active downloads with sufficient data for ETA estimation".to_string();
                }
                let mut s = state.lock().await;
                s.add_system_message("main", lines.trim_end().to_string());
            }
        }
        Command::Block { peer } => {
            let _ = cmd_tx.send(SendCommand::BlockPeer {
                peer_id: peer.clone(),
            });
            let mut s = state.lock().await;
            s.add_system_message(
                "main",
                format!("Blocked peer {}", &peer[..8.min(peer.len())]),
            );
        }
        Command::Unblock { peer } => {
            let _ = cmd_tx.send(SendCommand::UnblockPeer {
                peer_id: peer.clone(),
            });
            let mut s = state.lock().await;
            s.add_system_message(
                "main",
                format!("Unblocked peer {}", &peer[..8.min(peer.len())]),
            );
        }
        Command::Fingerprint => {
            let s = state.lock().await;
            let fp = &s.my_fingerprint;
            if fp.is_empty() {
                let mut s = state.lock().await;
                s.add_system_message("main", "Fingerprint not available".to_string());
            } else {
                let mut s = state.lock().await;
                s.add_system_message("main", format!("Your fingerprint:\n{}", fp));
            }
        }
        Command::IpMsg { ip, message } => match ip.parse::<std::net::IpAddr>() {
            Ok(addr) => {
                let _ = cmd_tx.send(SendCommand::SendIpMsg {
                    ip: addr,
                    message: message.clone(),
                });
                let mut s = state.lock().await;
                s.add_system_message("main", format!("Sent IPMSG to {}", ip));
            }
            Err(_) => {
                let mut s = state.lock().await;
                s.add_system_message("main", format!("Invalid IP address: {}", ip));
            }
        },
        Command::IpMsgPeers => {
            let _ = cmd_tx.send(SendCommand::ListIpMsgPeers);
        }
    }
}

/// Handle commands in headless mode (no TUI state needed)
async fn handle_command_headless(
    state: &Arc<Mutex<SharedState>>,
    cmd_tx: &tokio::sync::mpsc::UnboundedSender<SendCommand>,
    cmd: &Command,
    _my_peer_id: &str,
) {
    match cmd {
        Command::Help => {
            println!("Commands: /msg <peer> <text>, /peers, /who, /ping, /quit");
        }
        Command::Msg { target, content } => {
            let full_peer = {
                let s = state.lock().await;
                s.peers.iter().find(|p| p.starts_with(target)).cloned()
            };
            if let Some(peer_id) = full_peer {
                let _ = cmd_tx.send(SendCommand::SendText {
                    to: peer_id,
                    content: content.clone(),
                });
                println!("[sent] -> {}: {}", target, content);
            } else {
                println!("[error] Peer not found: {}", target);
            }
        }
        Command::Peers | Command::Who => {
            let s = state.lock().await;
            if s.peers.is_empty() {
                println!("[peers] No peers connected");
            } else {
                println!("[peers] Connected ({}):", s.peers.len());
                for p in &s.peers {
                    if let Some((uname, _platforms)) = s.peer_details.get(p) {
                        println!("  {} - {}", &p[..8.min(p.len())], uname);
                    } else {
                        println!("  {} - unknown", &p[..8.min(p.len())]);
                    }
                }
            }
        }
        Command::Ping => {
            println!("[pong] Local OK");
        }
        Command::Quit => {
            println!("[quit] Shutting down...");
            std::process::exit(0);
        }
        _ => {
            println!("[error] Command not supported in headless mode");
        }
    }
}

fn setup_terminal() -> io::Result<Terminal<CrosstermBackend<io::Stdout>>> {
    crossterm::terminal::enable_raw_mode()?;
    let mut stdout = stdout();
    crossterm::execute!(stdout, crossterm::terminal::EnterAlternateScreen)?;
    Terminal::new(CrosstermBackend::new(stdout))
}

fn restore_terminal(mut terminal: Terminal<CrosstermBackend<io::Stdout>>) -> io::Result<()> {
    crossterm::terminal::disable_raw_mode()?;
    crossterm::execute!(
        terminal.backend_mut(),
        crossterm::terminal::LeaveAlternateScreen
    )?;
    Ok(())
}

/// Parse speed limit string like "100KB/s", "1MB/s", "0" into bytes per second
fn parse_speed_limit(input: &str) -> Option<u64> {
    let input = input.trim().to_lowercase();

    // Handle "0" or "unlimited"
    if input == "0" || input == "unlimited" || input == "none" {
        return Some(0);
    }

    // Try to parse with unit
    let (num_str, multiplier) = if input.ends_with("kb/s") || input.ends_with("kbs") {
        (input.trim_end_matches("/s").trim_end_matches("s"), 1024.0)
    } else if input.ends_with("mb/s") || input.ends_with("mbs") {
        (
            input.trim_end_matches("/s").trim_end_matches("s"),
            1024.0 * 1024.0,
        )
    } else if input.ends_with("gb/s") || input.ends_with("gbs") {
        (
            input.trim_end_matches("/s").trim_end_matches("s"),
            1024.0 * 1024.0 * 1024.0,
        )
    } else if input.ends_with("b/s") || input.ends_with("bs") {
        (input.trim_end_matches("/s").trim_end_matches("s"), 1.0)
    } else {
        // Try parsing as raw bytes
        (input.as_str(), 1.0)
    };

    // Extract numeric part
    let num_str = num_str.trim();
    let num: f64 = num_str.parse().ok()?;

    if num < 0.0 {
        return None;
    }

    Some((num * multiplier) as u64)
}

/// Parse timeout string like "30s", "5m", "1h", or "0" to disable
fn parse_timeout(input: &str) -> Option<u64> {
    let input = input.trim().to_lowercase();

    // Handle "0" or "off" or "disable"
    if input == "0" || input == "off" || input == "disable" || input == "none" {
        return Some(0);
    }

    // Try to parse with unit
    let (num_str, multiplier) = if input.ends_with('s') {
        (input.trim_end_matches('s'), 1)
    } else if input.ends_with('m') {
        (input.trim_end_matches('m'), 60)
    } else if input.ends_with('h') {
        (input.trim_end_matches('h'), 3600)
    } else {
        // Try parsing as raw seconds
        (input.as_str(), 1)
    };

    // Extract numeric part
    let num_str = num_str.trim();
    let num: f64 = num_str.parse().ok()?;

    if num < 0.0 {
        return None;
    }

    Some((num * multiplier as f64) as u64)
}

/// Generate a sparkline chart from a list of values, fitting into max_width columns
fn generate_sparkline(values: &[f64], max_width: usize) -> String {
    const BARS: &[char] = &['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];
    if values.is_empty() {
        return String::new();
    }
    // Downsample if more values than max_width
    let sampled: Vec<f64> = if values.len() > max_width {
        let step = values.len() as f64 / max_width as f64;
        (0..max_width)
            .map(|i| {
                let start = (i as f64 * step) as usize;
                let end = ((i + 1) as f64 * step) as usize;
                let slice = &values[start..end.min(values.len())];
                slice.iter().sum::<f64>() / slice.len() as f64
            })
            .collect()
    } else {
        values.to_vec()
    };
    let min = sampled.iter().cloned().fold(f64::INFINITY, f64::min);
    let max = sampled.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    let range = max - min;
    sampled
        .iter()
        .map(|&v| {
            let idx = if range <= 0.0 {
                0
            } else {
                ((v - min) / range * (BARS.len() - 1) as f64).round() as usize
            };
            BARS[idx.min(BARS.len() - 1)]
        })
        .collect()
}

/// Format bytes/sec into human-readable speed string
fn format_speed(bps: f64) -> String {
    if bps <= 0.0 {
        return "0 B/s".to_string();
    }
    let units = ["B/s", "KB/s", "MB/s", "GB/s"];
    let mut val = bps;
    let mut idx = 0;
    while val >= 1024.0 && idx < units.len() - 1 {
        val /= 1024.0;
        idx += 1;
    }
    format!("{:.1} {}", val, units[idx])
}

/// Format seconds into human-readable duration
fn format_duration(secs: f64) -> String {
    if secs.is_infinite() || secs.is_nan() || secs < 0.0 {
        return "?".to_string();
    }
    let secs = secs as u64;
    if secs < 60 {
        format!("{}s", secs)
    } else if secs < 3600 {
        format!("{}m {}s", secs / 60, secs % 60)
    } else {
        let hours = secs / 3600;
        let mins = (secs % 3600) / 60;
        format!("{}h {}m", hours, mins)
    }
}

/// Format a list of tasks for display
fn format_task_list(tasks: &[ipmsg_download::DownloadTask]) -> String {
    tasks
        .iter()
        .map(|t| {
            let tags_str = if t.tags.is_empty() {
                String::new()
            } else {
                format!(" [{}]", t.tags.join(", "))
            };
            format!(
                "  {} - {} ({:.1}%) - {}{}",
                t.id,
                t.name,
                t.progress(),
                t.state_label(),
                tags_str
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Format bytes into human-readable size string
fn format_size(bytes: u64) -> String {
    if bytes == 0 {
        return "0 B".to_string();
    }
    let units = ["B", "KB", "MB", "GB", "TB"];
    let mut val = bytes as f64;
    let mut idx = 0;
    while val >= 1024.0 && idx < units.len() - 1 {
        val /= 1024.0;
        idx += 1;
    }
    format!("{:.1} {}", val, units[idx])
}

/// Truncate a name to max_len, adding "..." if truncated
fn truncate_name(name: &str, max_len: usize) -> String {
    if name.len() <= max_len {
        name.to_string()
    } else {
        format!("{}...", &name[..max_len.saturating_sub(3)])
    }
}

fn draw(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    state: &SharedState,
) -> io::Result<()> {
    terminal.draw(|f| {
        let chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(20), Constraint::Percentage(80)].as_ref())
            .split(f.area());

        let right_chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(1), // Status bar
                Constraint::Length(1), // Tabs
                Constraint::Min(3),    // Messages
                Constraint::Length(3), // Downloads
                Constraint::Length(3), // Input
            ])
            .split(chunks[1]);

        // Peer list
        let peer_items: Vec<ListItem> = if state.peers.is_empty() {
            vec![ListItem::new(Line::from(Span::styled(
                "  Waiting...",
                Style::default().fg(Color::DarkGray),
            )))]
        } else {
            state
                .peers
                .iter()
                .map(|p| {
                    let detail = state.peer_details.get(p);
                    let name = match detail {
                        Some((uname, _)) => uname.clone(),
                        None => "unknown".to_string(),
                    };
                    ListItem::new(Line::from(vec![
                        Span::styled("● ", Style::default().fg(Color::Green)),
                        Span::styled(name, Style::default().fg(Color::White)),
                    ]))
                })
                .collect()
        };

        let peers_block = Block::default()
            .title(format!(" Peers ({}) ", state.peers.len()))
            .borders(Borders::ALL)
            .style(Style::default().fg(Color::Cyan));
        f.render_widget(List::new(peer_items).block(peers_block), chunks[0]);

        // Status bar
        let peer_count = state.peers.len();
        let status_icon = if peer_count > 0 { "●" } else { "○" };
        let status_color = if peer_count > 0 {
            Color::Green
        } else {
            Color::DarkGray
        };
        let status_text = format!(
            " {} {} | Peers: {} | {} ",
            status_icon, state.username, peer_count, state.status
        );
        let status_bar = Paragraph::new(Line::from(vec![Span::styled(
            status_text,
            Style::default().fg(status_color).bg(Color::DarkGray),
        )]));
        f.render_widget(status_bar, right_chunks[0]);

        // Tabs
        let tab_titles: Vec<Line> = state
            .tabs
            .iter()
            .enumerate()
            .map(|(i, t)| {
                let style = if i == state.active_tab {
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default().fg(Color::DarkGray)
                };
                Line::from(Span::styled(format!(" {} ", t.name), style))
            })
            .collect();
        f.render_widget(
            Tabs::new(tab_titles).highlight_style(
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            right_chunks[1],
        );

        // Messages
        let tab = state.active_tab();
        let msg_lines: Vec<Line> = tab
            .messages
            .iter()
            .flat_map(|m| {
                let from_color = if m.from == "system" {
                    Color::Yellow
                } else if m.from == state.my_peer_id {
                    Color::Green
                } else {
                    Color::Blue
                };
                let content = match &m.kind {
                    ipmsg_protocol::message::MessageType::Text { content } => content.clone(),
                    _ => format!("[{}]", m.kind.label()),
                };
                let sender = if m.from == state.my_peer_id {
                    "you"
                } else {
                    &m.from
                };
                vec![
                    Line::from(Span::styled(
                        format!("[{}] {} ", m.timestamp.format("%H:%M"), sender),
                        Style::default().fg(Color::DarkGray),
                    )),
                    Line::from(Span::styled(
                        format!("  {}", content),
                        Style::default().fg(from_color),
                    )),
                ]
            })
            .collect();

        let messages_block = Block::default()
            .title(format!(" {} ", tab.name))
            .borders(Borders::ALL)
            .style(Style::default().fg(Color::White));

        let scroll = tab
            .messages
            .len()
            .saturating_sub(right_chunks[2].height.saturating_sub(2) as usize);
        f.render_widget(
            Paragraph::new(msg_lines)
                .block(messages_block)
                .scroll((scroll as u16, 0)),
            right_chunks[2],
        );

        // Downloads
        let download_tasks = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(async { state.download_manager.list_tasks().await })
        });

        let download_lines: Vec<Line> = if download_tasks.is_empty() {
            vec![Line::from(Span::styled(
                "  No downloads",
                Style::default().fg(Color::DarkGray),
            ))]
        } else {
            download_tasks
                .iter()
                .take(3) // Show max 3 downloads
                .map(|task| {
                    let progress = task.progress();
                    let bar_width = 20;
                    let filled = (progress / 100.0 * bar_width as f32) as usize;
                    let bar: String = std::iter::repeat_n('=', filled)
                        .chain(std::iter::repeat_n(' ', bar_width - filled))
                        .collect();

                    let state_icon = match task.state {
                        ipmsg_download::DownloadState::Downloading => "⬇",
                        ipmsg_download::DownloadState::Paused => "⏸",
                        ipmsg_download::DownloadState::Complete => "✓",
                        ipmsg_download::DownloadState::Error => "✗",
                        ipmsg_download::DownloadState::Queued => "⏳",
                    };

                    let state_color = match task.state {
                        ipmsg_download::DownloadState::Downloading => Color::Green,
                        ipmsg_download::DownloadState::Paused => Color::Yellow,
                        ipmsg_download::DownloadState::Complete => Color::Cyan,
                        ipmsg_download::DownloadState::Error => Color::Red,
                        ipmsg_download::DownloadState::Queued => Color::DarkGray,
                    };

                    Line::from(vec![
                        Span::styled(format!("{} ", state_icon), Style::default().fg(state_color)),
                        Span::styled(
                            format!("{:<12} ", &task.name[..12.min(task.name.len())]),
                            Style::default().fg(Color::White),
                        ),
                        Span::styled(format!("[{}] ", bar), Style::default().fg(state_color)),
                        Span::styled(
                            format!("{:.0}%", progress),
                            Style::default().fg(Color::White),
                        ),
                    ])
                })
                .collect()
        };

        let downloads_block = Block::default()
            .title(format!(" Downloads ({}) ", download_tasks.len()))
            .borders(Borders::ALL)
            .style(Style::default().fg(Color::Magenta));
        f.render_widget(
            Paragraph::new(download_lines).block(downloads_block),
            right_chunks[3],
        );

        // Input
        let input_block = Block::default()
            .title(" /cmd or type [Tab/←/→]=switch [Esc]=quit ")
            .borders(Borders::ALL)
            .style(Style::default().fg(Color::White));
        f.render_widget(
            Paragraph::new(format!("> {} ", state.input)).block(input_block),
            right_chunks[4],
        );
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_batch_commands() {
        assert!(matches!(parse_command("/dlpauseall"), Command::DlPauseAll));
        assert!(matches!(
            parse_command("/dl-pause-all"),
            Command::DlPauseAll
        ));
        assert!(matches!(
            parse_command("/dlresumeall"),
            Command::DlResumeAll
        ));
        assert!(matches!(
            parse_command("/dl-resume-all"),
            Command::DlResumeAll
        ));
        assert!(matches!(
            parse_command("/dlrmcompleted"),
            Command::DlRmCompleted
        ));
        assert!(matches!(
            parse_command("/dl-rm-completed"),
            Command::DlRmCompleted
        ));
        assert!(matches!(parse_command("/dlrmfailed"), Command::DlRmFailed));
        assert!(matches!(
            parse_command("/dl-rm-failed"),
            Command::DlRmFailed
        ));
        assert!(matches!(parse_command("/dlstats"), Command::DlStats));
        assert!(matches!(parse_command("/dl-stats"), Command::DlStats));
    }

    #[test]
    fn test_format_speed() {
        assert_eq!(format_speed(0.0), "0 B/s");
        assert_eq!(format_speed(500.0), "500.0 B/s");
        assert_eq!(format_speed(1024.0), "1.0 KB/s");
        assert_eq!(format_speed(1536.0), "1.5 KB/s");
        assert_eq!(format_speed(1048576.0), "1.0 MB/s");
        assert_eq!(format_speed(1073741824.0), "1.0 GB/s");
    }

    #[test]
    fn test_format_size() {
        assert_eq!(format_size(0), "0 B");
        assert_eq!(format_size(512), "512.0 B");
        assert_eq!(format_size(1024), "1.0 KB");
        assert_eq!(format_size(1536), "1.5 KB");
        assert_eq!(format_size(1048576), "1.0 MB");
        assert_eq!(format_size(1073741824), "1.0 GB");
        assert_eq!(format_size(1099511627776), "1.0 TB");
    }

    #[test]
    fn test_help_contains_batch_commands() {
        let help = command_help();
        assert!(help.contains("/dlpauseall"));
        assert!(help.contains("/dlresumeall"));
        assert!(help.contains("/dlrmcompleted"));
        assert!(help.contains("/dlrmfailed"));
        assert!(help.contains("/dlstats"));
    }
}

#[cfg(test)]
mod save_path_tests {
    use super::*;

    #[test]
    fn test_parse_dlpath_command() {
        let cmd = parse_command("/dlpath /home/user/downloads");
        assert!(matches!(cmd, Command::DlPath { path } if path == "/home/user/downloads"));
    }

    #[test]
    fn test_parse_dlpath_command_alias() {
        let cmd = parse_command("/dlsp /data/downloads");
        assert!(matches!(cmd, Command::DlPath { path } if path == "/data/downloads"));
    }

    #[test]
    fn test_parse_dlorganize_on() {
        let cmd = parse_command("/dlorganize on");
        assert!(matches!(cmd, Command::DlOrganize { enabled } if enabled == "on"));
    }

    #[test]
    fn test_parse_dlorganize_off() {
        let cmd = parse_command("/dlorganize off");
        assert!(matches!(cmd, Command::DlOrganize { enabled } if enabled == "off"));
    }

    #[test]
    fn test_parse_dlorganize_alias() {
        let cmd = parse_command("/dlorg yes");
        assert!(matches!(cmd, Command::DlOrganize { enabled } if enabled == "yes"));
    }

    #[test]
    fn test_parse_dlproxy() {
        let cmd = parse_command("/dlproxy socks5://127.0.0.1:1080");
        assert!(matches!(cmd, Command::DlProxy { url } if url == "socks5://127.0.0.1:1080"));
    }

    #[test]
    fn test_parse_dlproxy_none() {
        let cmd = parse_command("/dlproxy none");
        assert!(matches!(cmd, Command::DlProxy { url } if url == "none"));
    }

    #[test]
    fn test_parse_dlproxy_alias() {
        let cmd = parse_command("/dlpx http://proxy:8080");
        assert!(matches!(cmd, Command::DlProxy { url } if url == "http://proxy:8080"));
    }

    #[test]
    fn test_parse_dlproxy_test() {
        let cmd = parse_command("/dlproxy test");
        assert!(matches!(cmd, Command::DlProxyTest));
    }

    #[test]
    fn test_parse_dlproxy_test_alias() {
        let cmd = parse_command("/dlpx test");
        assert!(matches!(cmd, Command::DlProxyTest));
    }

    #[test]
    fn test_help_contains_save_path_commands() {
        let help = command_help();
        assert!(help.contains("/dlpath"));
        assert!(help.contains("/dlorganize"));
        assert!(help.contains("/dlproxy"));
    }

    #[test]
    fn test_parse_dlexport() {
        match parse_command("/dlexport /tmp/tasks.json") {
            Command::DlExport { path, description } => {
                assert_eq!(path, "/tmp/tasks.json");
                assert!(description.is_none());
            }
            other => panic!("Expected DlExport, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_dlexport_with_description() {
        match parse_command("/dlexport /tmp/tasks.json my backup") {
            Command::DlExport { path, description } => {
                assert_eq!(path, "/tmp/tasks.json");
                assert_eq!(description, Some("my backup".to_string()));
            }
            other => panic!("Expected DlExport with description, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_dlimp() {
        match parse_command("/dlimp /tmp/tasks.json") {
            Command::DlImport { path } => {
                assert_eq!(path, "/tmp/tasks.json");
            }
            other => panic!("Expected DlImport, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_dlsegment() {
        match parse_command("/dlsegment https://example.com/file.zip") {
            Command::DlSegment { url } => {
                assert_eq!(url, "https://example.com/file.zip");
            }
            other => panic!("Expected DlSegment, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_dlsegment_alias() {
        match parse_command("/dlseg https://example.com/file.zip") {
            Command::DlSegment { url } => {
                assert_eq!(url, "https://example.com/file.zip");
            }
            other => panic!("Expected DlSegment, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_dlextract() {
        match parse_command("/dlextract /tmp/notes.txt") {
            Command::DlExtract { path } => {
                assert_eq!(path, "/tmp/notes.txt");
            }
            other => panic!("Expected DlExtract, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_dlextract_alias() {
        match parse_command("/dl-extract /tmp/notes.txt") {
            Command::DlExtract { path } => {
                assert_eq!(path, "/tmp/notes.txt");
            }
            other => panic!("Expected DlExtract, got {:?}", other),
        }
    }

    #[test]
    fn test_help_contains_extract() {
        let help = command_help();
        assert!(help.contains("/dlextract"));
    }

    #[test]
    fn test_parse_dlchecksum_with_algorithm() {
        match parse_command(
            "/dlchecksum abc123 deadbeef1234567890abcdef1234567890abcdef1234567890abcdef12345678 sha256",
        ) {
            Command::DlChecksum {
                task_id,
                checksum,
                algorithm,
            } => {
                assert_eq!(task_id, "abc123");
                assert_eq!(
                    checksum,
                    "deadbeef1234567890abcdef1234567890abcdef1234567890abcdef12345678"
                );
                assert_eq!(algorithm, Some("sha256".to_string()));
            }
            other => panic!("Expected DlChecksum, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_dlchecksum_without_algorithm() {
        match parse_command(
            "/dlchecksum abc123 deadbeef1234567890abcdef1234567890abcdef1234567890abcdef12345678",
        ) {
            Command::DlChecksum {
                task_id,
                checksum,
                algorithm,
            } => {
                assert_eq!(task_id, "abc123");
                assert_eq!(
                    checksum,
                    "deadbeef1234567890abcdef1234567890abcdef1234567890abcdef12345678"
                );
                assert_eq!(algorithm, None);
            }
            other => panic!("Expected DlChecksum, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_dlchecksum_alias() {
        match parse_command(
            "/dlcs abc123 deadbeef1234567890abcdef1234567890abcdef1234567890abcdef12345678 md5",
        ) {
            Command::DlChecksum {
                task_id,
                checksum,
                algorithm,
            } => {
                assert_eq!(task_id, "abc123");
                assert_eq!(
                    checksum,
                    "deadbeef1234567890abcdef1234567890abcdef1234567890abcdef12345678"
                );
                assert_eq!(algorithm, Some("md5".to_string()));
            }
            other => panic!("Expected DlChecksum, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_dlchecksum_insufficient_args() {
        match parse_command("/dlchecksum abc123") {
            Command::Unknown(_) => {}
            other => panic!("Expected Unknown for insufficient args, got {:?}", other),
        }
    }

    #[test]
    fn test_help_contains_checksum() {
        let help = command_help();
        assert!(help.contains("/dlchecksum"));
    }
}
