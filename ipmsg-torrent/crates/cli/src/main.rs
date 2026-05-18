use clap::Parser;
use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use ipmsg_core::{P2PEvent, P2PEngine};
use ipmsg_protocol::message::{ChannelId, ChatMessage};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, Paragraph, Tabs};
use ratatui::Terminal;
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
    #[arg(long, default_value = "false")]
    headless: bool,
}

/// IRC-style command parser
enum Command {
    Help,
    Nick(String),
    Msg { target: String, content: String },
    Peers,
    Join(String),
    Leave(String),
    GeoJoin(String),
    Who,
    Ping,
    #[allow(dead_code)] // Planned: file transfer support
    File { target: String, path: String },
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
            if parts.len() > 1 { Command::Nick(parts[1].to_string()) } else { Command::Unknown("nick requires a name".to_string()) }
        }
        "msg" | "m" | "dm" => {
            if parts.len() >= 3 { Command::Msg { target: parts[1].to_string(), content: parts[2].to_string() } }
            else { Command::Unknown("/msg <peer> <text>".to_string()) }
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
            } else { Command::Unknown("/join <channel>".to_string()) }
        }
        "leave" | "part" | "l" => {
            if parts.len() > 1 { Command::Leave(parts[1].to_string()) }
            else { Command::Unknown("/leave <channel>".to_string()) }
        }
        "who" | "w" => Command::Who,
        "ping" => Command::Ping,
        "file" | "send" => {
            if parts.len() >= 3 { Command::File { target: parts[1].to_string(), path: parts[2].to_string() } }
            else { Command::Unknown("/file <peer> <path>".to_string()) }
        }
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
        "/clear         - Clear messages",
        "/quit          - Exit",
    ].join("\n")
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
    input: String,
    running: bool,
    username: String,
}

impl SharedState {
    fn new(peer_id: String, username: String) -> Self {
        let main_tab = TabView {
            name: "main".to_string(),
            messages: Vec::new(),
            channel: None,
        };
        Self {
            tabs: vec![main_tab],
            active_tab: 0,
            peers: Vec::new(),
            peer_details: HashMap::new(),
            status: "Ready".to_string(),
            my_peer_id: peer_id,
            input: String::new(),
            running: true,
            username,
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
        self.tabs.iter().position(|t| t.channel.as_ref() == Some(channel))
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
    let mut engine = P2PEngine::new(data_dir)?;
    let peer_id = engine.start(cli.username.clone(), bootstrap).await?;

    let mut event_rx = engine.take_receiver().expect("receiver already taken");

    // Spawn swarm loop
    tokio::spawn(async move {
        engine.run_event_loop().await;
    });

    // Headless mode: just log events to stdout
    if cli.headless {
        println!("P2P engine running in headless mode. Press Ctrl+C to exit.");
        loop {
            tokio::select! {
                Ok(()) = tokio::signal::ctrl_c() => {
                    break;
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
                                    println!("[you] {}", msg.timestamp.format("%H:%M"));
                                }
                                P2PEvent::PeerJoined { peer_id: pid, username: uname, .. } => {
                                    println!("Peer joined: {} ({})", uname, &pid[..8.min(pid.len())]);
                                }
                                P2PEvent::PeerLeft { peer_id: pid } => {
                                    println!("Peer left: {}", &pid[..8.min(pid.len())]);
                                }
                                P2PEvent::Status(st) => { println!("Status: {}", st); }
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
    let state = Arc::new(Mutex::new(SharedState::new(peer_id, username.clone())));

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
                P2PEvent::PeerJoined { peer_id: pid, username: uname, platforms } => {
                    if !s.peers.contains(&pid) {
                        s.peers.push(pid.clone());
                    }
                    s.peer_details.insert(pid.clone(), (uname.clone(), platforms.clone()));
                    let platforms_str = if platforms.is_empty() {
                        String::new()
                    } else {
                        format!(" [{}]", platforms.join(", "))
                    };
                    s.set_status(format!("Peer joined: {}{}{}", uname, platforms_str, &pid[..8.min(pid.len())]));
                }
                P2PEvent::PeerLeft { peer_id: pid } => {
                    s.peers.retain(|p| p != &pid);
                    s.set_status(format!("Peer left: {}", &pid[..8.min(pid.len())]));
                }
                P2PEvent::Typing { from } => {
                    s.set_status(format!("{} is typing...", from));
                }
                P2PEvent::Status(st) => { s.set_status(st); }
                _ => {}
            }
        }

        {
            let s = state.lock().await;
            draw(&mut terminal, &s)?;
            if !s.running { break; }
            drop(s);

            if event::poll(Duration::from_millis(100))? {
                if let Event::Key(key) = event::read()? {
                    if key.kind != KeyEventKind::Press { continue; }
                    let mut s = state.lock().await;
                    match key.code {
                        KeyCode::Enter => {
                            let input = s.input.clone();
                            s.input.clear();
                            drop(s);
                            handle_command(&state, &input).await;
                        }
                        KeyCode::Char(c) => { state.lock().await.input.push(c); }
                        KeyCode::Backspace => { state.lock().await.input.pop(); }
                        KeyCode::Esc => { state.lock().await.running = false; }
                        KeyCode::Tab => {
                            let len = state.lock().await.tabs.len();
                            if len > 1 {
                                let mut s = state.lock().await;
                                s.active_tab = (s.active_tab + 1) % len;
                            }
                        }
                        KeyCode::Left => {
                            let mut s = state.lock().await;
                            if s.active_tab > 0 { s.active_tab -= 1; }
                        }
                        KeyCode::Right => {
                            let mut s = state.lock().await;
                            if s.active_tab + 1 < s.tabs.len() { s.active_tab += 1; }
                        }
                        _ => {}
                    }
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
        let msg = ChatMessage::new_text(
            "system".to_string(),
            None,
            text,
        );
        self.add_message(tab, msg);
    }

    fn set_status(&mut self, text: String) {
        self.status = text;
    }
}

async fn handle_command(state: &Arc<Mutex<SharedState>>, input: &str) {
    if !input.starts_with('/') {
        // Regular message - send to active tab's channel or broadcast
        let s = state.lock().await;
        let content = input.to_string();
        let active = s.active_tab;
        let tab_name = s.tabs[active].name.clone();
        let channel = s.tabs[active].channel.clone();
        drop(s);

        if let Some(_ch) = channel {
            // Send to channel
            // TODO: engine.send_to_channel
        } else if tab_name.starts_with("dm:") {
            // DM to specific peer
            // TODO: engine.send_text
        } else {
            // Broadcast to main
            let _ = content; // Reserved for broadcast
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
            // TODO: send DM
            let mut s = state.lock().await;
            s.add_system_message("main", format!("TODO: DM to {} -> {}", target, content));
        }
        Command::Peers => {
            let s = state.lock().await;
            let peer_list: Vec<String> = s.peers.iter().map(|p| {
                let detail = s.peer_details.get(p);
                match detail {
                    Some((uname, platforms)) => format!("{} - {} [{}]", &p[..8.min(p.len())], uname, platforms.join(", ")),
                    None => format!("{} - unknown", &p[..8.min(p.len())]),
                }
            }).collect();
            let mut s = state.lock().await;
            if peer_list.is_empty() {
                s.add_system_message("main", "No peers connected".to_string());
            } else {
                s.add_system_message("main", format!("Connected peers ({}):\n{}", peer_list.len(), peer_list.join("\n")));
            }
        }
        Command::Join(name) => {
            let channel = ChannelId::Group(name.clone());
            let tab_name = format!("#{}", name);
            let msg_text = format!("Joined channel #{}", name);
            let mut s = state.lock().await;
            let idx = s.find_or_create_tab(&tab_name, Some(channel));
            s.active_tab = idx;
            s.add_system_message(&tab_name, msg_text);
        }
        Command::GeoJoin(hash) => {
            let channel = ChannelId::Geohash(hash.clone());
            let tab_name = format!("@{}", hash);
            let msg_text = format!("Joined geohash channel @{}", hash);
            let mut s = state.lock().await;
            let idx = s.find_or_create_tab(&tab_name, Some(channel));
            s.active_tab = idx;
            s.add_system_message(&tab_name, msg_text);
        }
        Command::Leave(name) => {
            let mut s = state.lock().await;
            if let Some(idx) = s.tabs.iter().position(|t| t.name == format!("#{}", name) || t.name == format!("@{}", name)) {
                let removed = s.tabs.remove(idx).name;
                if s.active_tab >= s.tabs.len() {
                    s.active_tab = s.tabs.len().saturating_sub(1);
                }
                s.add_system_message("main", format!("Left {}", removed));
            }
        }
        Command::Who => {
            let s = state.lock().await;
            let mut lines = vec![format!("Online peers ({}):", s.peers.len())];
            for p in &s.peers {
                if let Some((uname, platforms)) = s.peer_details.get(p) {
                    lines.push(format!("  {} - {} ({})", &p[..8.min(p.len())], uname, platforms.join(", ")));
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
        Command::File { .. } => {
            let mut s = state.lock().await;
            s.add_system_message("main", "TODO: File transfer not yet implemented".to_string());
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
    crossterm::execute!(terminal.backend_mut(), crossterm::terminal::LeaveAlternateScreen)?;
    Ok(())
}

fn draw(terminal: &mut Terminal<CrosstermBackend<io::Stdout>>, state: &SharedState) -> io::Result<()> {
    terminal.draw(|f| {
        let chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(20), Constraint::Percentage(80)].as_ref())
            .split(f.area());

        let right_chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(1),  // Tabs
                Constraint::Min(3),     // Messages
                Constraint::Length(3),  // Input
            ])
            .split(chunks[1]);

        // Peer list
        let peer_items: Vec<ListItem> = if state.peers.is_empty() {
            vec![ListItem::new(Line::from(Span::styled(
                "  Waiting...", Style::default().fg(Color::DarkGray),
            )))]
        } else {
            state.peers.iter().map(|p| {
                let detail = state.peer_details.get(p);
                let name = match detail {
                    Some((uname, _)) => uname.clone(),
                    None => "unknown".to_string(),
                };
                ListItem::new(Line::from(vec![
                    Span::styled("● ", Style::default().fg(Color::Green)),
                    Span::styled(name, Style::default().fg(Color::White)),
                ]))
            }).collect()
        };

        let peers_block = Block::default()
            .title(format!(" Peers ({}) ", state.peers.len()))
            .borders(Borders::ALL)
            .style(Style::default().fg(Color::Cyan));
        f.render_widget(List::new(peer_items).block(peers_block), chunks[0]);

        // Tabs
        let tab_titles: Vec<Line> = state.tabs.iter().enumerate().map(|(i, t)| {
            let style = if i == state.active_tab {
                Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(Color::DarkGray)
            };
            Line::from(Span::styled(format!(" {} ", t.name), style))
        }).collect();
        f.render_widget(Tabs::new(tab_titles).highlight_style(
            Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD),
        ), right_chunks[0]);

        // Messages
        let tab = state.active_tab();
        let msg_lines: Vec<Line> = tab.messages.iter().flat_map(|m| {
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
            let sender = if m.from == state.my_peer_id { "you" } else { &m.from };
            vec![
                Line::from(Span::styled(
                    format!("[{}] {} ", m.timestamp.format("%H:%M"), sender),
                    Style::default().fg(Color::DarkGray),
                )),
                Line::from(Span::styled(format!("  {}", content), Style::default().fg(from_color))),
            ]
        }).collect();

        let messages_block = Block::default()
            .title(format!(" {} ", tab.name))
            .borders(Borders::ALL)
            .style(Style::default().fg(Color::White));

        let scroll = tab.messages.len().saturating_sub(
            right_chunks[1].height.saturating_sub(2) as usize
        );
        f.render_widget(
            Paragraph::new(msg_lines).block(messages_block).scroll((scroll as u16, 0)),
            right_chunks[1],
        );

        // Input
        let input_block = Block::default()
            .title(" /cmd or type [Tab/←/→]=switch [Esc]=quit ")
            .borders(Borders::ALL)
            .style(Style::default().fg(Color::White));
        f.render_widget(
            Paragraph::new(format!("> {} ", state.input)).block(input_block),
            right_chunks[2],
        );
    })?;
    Ok(())
}
