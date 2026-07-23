use ipmsg_core::{P2PEngine, P2PEvent};
use ipmsg_protocol::message::ChannelId;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use tauri::{AppHandle, Emitter, Manager};
use tokio::sync::Mutex;

// ---------------------------------------------------------------------------
// Application state
// ---------------------------------------------------------------------------

pub struct AppState {
    pub engine: Arc<Mutex<P2PEngine>>,
}

// ---------------------------------------------------------------------------
// Command payloads
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct StartArgs {
    pub username: String,
    pub bootstrap_nodes: Vec<String>,
    pub data_dir: String,
}

#[derive(Deserialize)]
pub struct SendMessageArgs {
    pub to: String,
    pub content: String,
}

#[derive(Deserialize)]
pub struct SendChannelArgs {
    pub channel: String,
    pub content: String,
}

#[derive(Deserialize)]
pub struct BroadcastArgs {
    pub content: String,
}

#[derive(Deserialize)]
pub struct HistoryArgs {
    pub peer_id: String,
    pub limit: u32,
}

#[derive(Deserialize)]
pub struct JoinChannelArgs {
    pub name: String,
}

#[derive(Deserialize)]
pub struct LeaveChannelArgs {
    pub name: String,
}

#[derive(Deserialize)]
pub struct ShareFileArgs {
    pub path: String,
    pub tags: Vec<String>,
    pub description: Option<String>,
}

#[derive(Deserialize)]
pub struct SearchFilesArgs {
    pub query: String,
    pub tags: Vec<String>,
}

#[derive(Deserialize)]
pub struct DownloadFileArgs {
    pub file_hash: String,
    pub from_peer: String,
}

#[derive(Deserialize)]
pub struct SearchMessagesArgs {
    pub query: String,
    pub limit: Option<u32>,
}

#[derive(Deserialize)]
pub struct PeerActionArgs {
    pub peer_id: String,
}

// ---------------------------------------------------------------------------
// Serializable event types for frontend
// ---------------------------------------------------------------------------

#[derive(Serialize, Clone)]
#[serde(tag = "type")]
pub enum FrontendEvent {
    PeerJoined {
        peer_id: String,
        username: String,
        platforms: Vec<String>,
    },
    PeerLeft {
        peer_id: String,
    },
    MessageReceived {
        from: String,
        content: Option<String>,
        timestamp: String,
    },
    MessageSent {
        content: Option<String>,
        timestamp: String,
    },
    Typing {
        from: String,
    },
    Status(String),
    Ready { peer_id: String },
    Error(String),
}

fn to_frontend_event(evt: &P2PEvent) -> FrontendEvent {
    match evt {
        P2PEvent::PeerJoined {
            peer_id,
            username,
            platforms,
        } => FrontendEvent::PeerJoined {
            peer_id: peer_id.clone(),
            username: username.clone(),
            platforms: platforms.clone(),
        },
        P2PEvent::PeerLeft { peer_id } => FrontendEvent::PeerLeft {
            peer_id: peer_id.clone(),
        },
        P2PEvent::MessageReceived(msg) => FrontendEvent::MessageReceived {
            from: msg.from.clone(),
            content: msg.text_content().map(|s| s.to_string()),
            timestamp: msg.timestamp.to_rfc3339(),
        },
        P2PEvent::MessageSent(msg) => FrontendEvent::MessageSent {
            content: msg.text_content().map(|s| s.to_string()),
            timestamp: msg.timestamp.to_rfc3339(),
        },
        P2PEvent::Typing { from } => FrontendEvent::Typing {
            from: from.clone(),
        },
        P2PEvent::Status(s) => FrontendEvent::Status(s.clone()),
        P2PEvent::FileOffer { from, file_ref } => FrontendEvent::Status(format!(
            "file offer from {} -> {}",
            from, file_ref.name
        )),
        P2PEvent::MessageDelivered(msg_id) => FrontendEvent::Status(format!("Message delivered: {}", msg_id)),
        P2PEvent::PeerBlocked { peer_id } => FrontendEvent::Status(format!("Peer blocked: {}", peer_id)),
        P2PEvent::PeerVerified { peer_id } => FrontendEvent::Status(format!("Peer verified: {}", peer_id)),
        P2PEvent::FragmentComplete { message_id, .. } => FrontendEvent::Status(format!("Fragment assembled: {}", message_id)),
        _ => FrontendEvent::Status(format!("Event: {:?}", evt)),
    }
}

// ---------------------------------------------------------------------------
// Event loop
// ---------------------------------------------------------------------------

fn spawn_event_loop(app: &AppHandle, engine: Arc<Mutex<P2PEngine>>) {
    let app = app.clone();
    tauri::async_runtime::spawn(async move {
        loop {
            let mut eng = engine.lock().await;
            match eng.next_event().await {
                Some(evt) => {
                    let fe = to_frontend_event(&evt);
                    let _ = app.emit("p2p-event", fe);
                }
                None => break,
            }
        }
    });
}

// ---------------------------------------------------------------------------
// Tauri commands
// ---------------------------------------------------------------------------

#[tauri::command]
async fn p2p_start(
    app: AppHandle,
    state: tauri::State<'_, AppState>,
    args: StartArgs,
) -> Result<String, String> {
    let mut engine = state.engine.lock().await;
    let peer_id = engine
        .start(args.username, args.bootstrap_nodes)
        .await
        .map_err(|e| e.to_string())?;

    // Spawn event loop (pass Arc clone, not State reference)
    spawn_event_loop(&app, state.engine.clone());

    // Emit ready event
    let _ = app.emit(
        "p2p-event",
        FrontendEvent::Ready {
            peer_id: peer_id.clone(),
        },
    );

    Ok(peer_id)
}

#[tauri::command]
async fn p2p_send(
    state: tauri::State<'_, AppState>,
    args: SendMessageArgs,
) -> Result<(), String> {
    let mut engine = state.engine.lock().await;
    engine
        .send_text(&args.to, &args.content)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn p2p_send_channel(
    state: tauri::State<'_, AppState>,
    args: SendChannelArgs,
) -> Result<(), String> {
    let mut engine = state.engine.lock().await;
    let channel = ChannelId::Group(args.channel);
    engine
        .send_to_channel(&channel, &args.content)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn p2p_broadcast(
    state: tauri::State<'_, AppState>,
    args: BroadcastArgs,
) -> Result<(), String> {
    let mut engine = state.engine.lock().await;
    engine
        .broadcast(args.content)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn p2p_get_peers(
    state: tauri::State<'_, AppState>,
) -> Result<String, String> {
    let engine = state.engine.lock().await;
    serde_json::to_string(&engine.list_peers()).map_err(|e| e.to_string())
}

#[tauri::command]
async fn p2p_get_history(
    state: tauri::State<'_, AppState>,
    args: HistoryArgs,
) -> Result<String, String> {
    let engine = state.engine.lock().await;
    serde_json::to_string(&engine.get_history(&args.peer_id, args.limit))
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn p2p_join_channel(
    state: tauri::State<'_, AppState>,
    args: JoinChannelArgs,
) -> Result<(), String> {
    let mut engine = state.engine.lock().await;
    engine.add_channel(ChannelId::Group(args.name));
    Ok(())
}

#[tauri::command]
async fn p2p_leave_channel(
    state: tauri::State<'_, AppState>,
    args: LeaveChannelArgs,
) -> Result<(), String> {
    let mut engine = state.engine.lock().await;
    engine.remove_channel(&ChannelId::Group(args.name));
    Ok(())
}

#[tauri::command]
async fn p2p_peer_id(state: tauri::State<'_, AppState>) -> Result<String, String> {
    Ok(state.engine.lock().await.peer_id_str())
}

#[tauri::command]
async fn p2p_username(state: tauri::State<'_, AppState>) -> Result<String, String> {
    Ok(state.engine.lock().await.username().to_string())
}

#[tauri::command]
async fn p2p_share_file(
    state: tauri::State<'_, AppState>,
    args: ShareFileArgs,
) -> Result<(), String> {
    let mut engine = state.engine.lock().await;
    engine.share_file(PathBuf::from(args.path), args.tags, args.description)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn p2p_unshare_file(
    state: tauri::State<'_, AppState>,
    args: PeerActionArgs,
) -> Result<(), String> {
    let mut engine = state.engine.lock().await;
    engine.unshare_file(&args.peer_id)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn p2p_search_files(
    state: tauri::State<'_, AppState>,
    args: SearchFilesArgs,
) -> Result<(), String> {
    let mut engine = state.engine.lock().await;
    engine.search_files(&args.query, &args.tags)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn p2p_download_file(
    state: tauri::State<'_, AppState>,
    args: DownloadFileArgs,
) -> Result<(), String> {
    let mut engine = state.engine.lock().await;
    engine.download_file(&args.file_hash, &args.from_peer)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn p2p_block_peer(
    state: tauri::State<'_, AppState>,
    args: PeerActionArgs,
) -> Result<(), String> {
    let mut engine = state.engine.lock().await;
    engine.block_peer(&args.peer_id);
    Ok(())
}

#[tauri::command]
async fn p2p_unblock_peer(
    state: tauri::State<'_, AppState>,
    args: PeerActionArgs,
) -> Result<(), String> {
    let mut engine = state.engine.lock().await;
    engine.unblock_peer(&args.peer_id);
    Ok(())
}

#[tauri::command]
async fn p2p_fingerprint(
    state: tauri::State<'_, AppState>,
) -> Result<String, String> {
    let engine = state.engine.lock().await;
    Ok(engine.my_fingerprint())
}

#[tauri::command]
async fn p2p_search_messages(
    state: tauri::State<'_, AppState>,
    args: SearchMessagesArgs,
) -> Result<String, String> {
    let engine = state.engine.lock().await;
    let limit = args.limit.unwrap_or(50);
    let results = engine.get_store().search_messages(&args.query, limit);
    serde_json::to_string(&results).map_err(|e| e.to_string())
}

#[tauri::command]
async fn p2p_start_ipmsg_compat(
    state: tauri::State<'_, AppState>,
) -> Result<(), String> {
    let mut engine = state.engine.lock().await;
    engine.start_ipmsg_compat()
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn p2p_send_ipmsg(
    state: tauri::State<'_, AppState>,
    args: SendMessageArgs,
) -> Result<(), String> {
    let ip: std::net::IpAddr = args.to.parse().map_err(|e: std::net::AddrParseError| e.to_string())?;
    let mut engine = state.engine.lock().await;
    engine.send_ipmsg_message(ip, &args.content)
        .await
        .map_err(|e| e.to_string())
}

// ---------------------------------------------------------------------------
// Tauri plugin
// ---------------------------------------------------------------------------

pub fn init() -> tauri::plugin::TauriPlugin<tauri::Wry> {
    tauri::plugin::Builder::new("ipmsg")
        .invoke_handler(tauri::generate_handler![
            p2p_start,
            p2p_send,
            p2p_send_channel,
            p2p_broadcast,
            p2p_get_peers,
            p2p_get_history,
            p2p_join_channel,
            p2p_leave_channel,
            p2p_peer_id,
            p2p_username,
            p2p_share_file,
            p2p_unshare_file,
            p2p_search_files,
            p2p_download_file,
            p2p_block_peer,
            p2p_unblock_peer,
            p2p_fingerprint,
            p2p_search_messages,
            p2p_start_ipmsg_compat,
            p2p_send_ipmsg,
        ])
        .setup(|app, _api| {
            let data_dir = app
                .path()
                .app_data_dir()
                .unwrap_or_else(|_| PathBuf::from("."));
            std::fs::create_dir_all(&data_dir).ok();

            let engine = P2PEngine::new(data_dir.join("ipmsg")).map_err(|e| e.to_string())?;
            app.manage(AppState {
                engine: Arc::new(Mutex::new(engine)),
            });
            Ok(())
        })
        .build()
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(init())
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
