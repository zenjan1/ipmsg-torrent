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
    /// Set download timeout and auto-retry (e.g., "30s", "5m", "0" to disable)
    DlTimeout {
        timeout: String,
        max_retries: u32,
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
        "dlpauseall" | "dl-pause-all" => Command::DlPauseAll,
        "dlresumeall" | "dl-resume-all" => Command::DlResumeAll,
        "dlrmcompleted" | "dl-rm-completed" => Command::DlRmCompleted,
        "dlrmfailed" | "dl-rm-failed" => Command::DlRmFailed,
        "dlstats" | "dl-stats" => Command::DlStats,
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
        "/dlspeed <limit>   - Set download speed limit (e.g., 100KB/s, 1MB/s, 0=unlimited)",
        "/dltimeout <timeout> [max_retries] - Set download timeout (e.g., 30s, 5m, 0=disable)",
        "/dlpauseall      - Pause all running downloads",
        "/dlresumeall     - Resume all paused downloads",
        "/dlrmcompleted   - Remove all completed downloads",
        "/dlrmfailed      - Remove all failed downloads",
        "/dlstats         - Show download statistics",
        "/dltag <id> <tags>   - Add tags to a download (comma-separated)",
        "/dluntag <id> <tags> - Remove tags from a download",
        "/dltags [tag]    - List all tags, or filter tasks by tag",
        "/dlfind [query] [--state=X] [--protocol=X] [--sort=X] [--asc] - Search/filter downloads",
        "/dlpriority <id> <high|normal|low> - Set download task priority",
        "/dlbw <id> <1-10>    - Set bandwidth weight (higher = more bandwidth)",
        "/dlbwmon           - Show bandwidth monitoring dashboard",
        "/dlnotify <action> [value] - Configure notifications (enable/disable/desktop/shell/log/webhook/status)",
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
