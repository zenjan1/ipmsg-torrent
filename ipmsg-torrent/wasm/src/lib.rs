use ipmsg_core::{P2PEngine, P2PEvent};
use serde::Serialize;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use tokio::sync::Mutex;
use wasm_bindgen::prelude::*;

// ---------------------------------------------------------------------------
// Serializable event types sent to JavaScript
// ---------------------------------------------------------------------------

#[derive(Serialize)]
#[serde(tag = "type")]
enum JsEvent {
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
}

fn to_js_event(evt: &P2PEvent) -> JsEvent {
    match evt {
        P2PEvent::PeerJoined {
            peer_id,
            username,
            platforms,
        } => JsEvent::PeerJoined {
            peer_id: peer_id.clone(),
            username: username.clone(),
            platforms: platforms.clone(),
        },
        P2PEvent::PeerLeft { peer_id } => JsEvent::PeerLeft {
            peer_id: peer_id.clone(),
        },
        P2PEvent::MessageReceived(msg) => JsEvent::MessageReceived {
            from: msg.from.clone(),
            content: msg.text_content().map(|s| s.to_string()),
            timestamp: msg.timestamp.to_rfc3339(),
        },
        P2PEvent::MessageSent(msg) => JsEvent::MessageSent {
            content: msg.text_content().map(|s| s.to_string()),
            timestamp: msg.timestamp.to_rfc3339(),
        },
        P2PEvent::Typing { from } => JsEvent::Typing { from: from.clone() },
        P2PEvent::Status(s) => JsEvent::Status(s.clone()),
        P2PEvent::FileOffer { from, file_ref } => {
            JsEvent::Status(format!("file offer from {} -> {}", from, file_ref.name))
        }
        P2PEvent::MessageDelivered(msg_id) => {
            JsEvent::Status(format!("Message delivered: {}", msg_id))
        }
        P2PEvent::PeerBlocked { peer_id } => JsEvent::Status(format!("Peer blocked: {}", peer_id)),
        P2PEvent::PeerVerified { peer_id } => {
            JsEvent::Status(format!("Peer verified: {}", peer_id))
        }
        P2PEvent::FragmentComplete { message_id, .. } => {
            JsEvent::Status(format!("Fragment assembled: {}", message_id))
        }
        // Handle remaining event variants as status messages
        P2PEvent::FileShareAnnounce { from, shares } => {
            JsEvent::Status(format!("{} shared {} file(s)", from, shares.len()))
        }
        P2PEvent::FileSearchResponse { from, results } => {
            JsEvent::Status(format!("{} found {} file(s)", from, results.len()))
        }
        P2PEvent::FragmentReceived { .. } => JsEvent::Status("Fragment received".to_string()),
        P2PEvent::FileTransferResponse { from, .. } => {
            JsEvent::Status(format!("File transfer response from {}", from))
        }
        P2PEvent::FileTransferProgress {
            file_hash,
            progress,
            ..
        } => JsEvent::Status(format!("Download {}: {:.1}%", file_hash, progress)),
        P2PEvent::ImageReceived { from, name, .. } => {
            JsEvent::Status(format!("Image received from {}: {}", from, name))
        }
        P2PEvent::ReadReceiptReceived { message_id, .. } => {
            JsEvent::Status(format!("Read receipt: {}", message_id))
        }
        P2PEvent::NearbyPeerDiscovered { peer } => {
            JsEvent::Status(format!("Nearby peer: {} ({})", peer.username, peer.peer_id))
        }
        P2PEvent::SearchResults { query, results } => {
            JsEvent::Status(format!("Search '{}': {} results", query, results.len()))
        }
        P2PEvent::FileTransferRequestReceived { from, .. } => {
            JsEvent::Status(format!("File transfer request from {}", from))
        }
        _ => JsEvent::Status(format!("Event: {:?}", evt)),
    }
}

// ---------------------------------------------------------------------------
// Web client
// ---------------------------------------------------------------------------

#[wasm_bindgen]
pub struct IpmsgClient {
    engine: Arc<Mutex<P2PEngine>>,
    on_event: Rc<RefCell<Option<js_sys::Function>>>,
}

#[wasm_bindgen]
impl IpmsgClient {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Result<IpmsgClient, JsError> {
        console_error_panic_hook::set_once();

        let data_dir = "/ipmsg".to_string();
        let engine = P2PEngine::new(std::path::PathBuf::from(data_dir))
            .map_err(|e| JsError::new(&e.to_string()))?;

        Ok(IpmsgClient {
            engine: Arc::new(Mutex::new(engine)),
            on_event: Rc::new(RefCell::new(None)),
        })
    }

    /// Set a callback that receives events (messages, peer changes, etc.)
    /// The callback receives a JSON string describing the event.
    #[wasm_bindgen(js_name = setEventCallback)]
    pub fn set_event_callback(&self, cb: &js_sys::Function) {
        self.on_event.borrow_mut().replace(cb.clone());
    }

    /// Start the P2P engine and return the local PeerID.
    #[wasm_bindgen]
    pub async fn start(
        &self,
        username: String,
        bootstrap_nodes: Vec<String>,
    ) -> Result<String, JsError> {
        let mut engine = self.engine.lock().await;
        let peer_id = engine
            .start(username.clone(), bootstrap_nodes)
            .await
            .map_err(|e| JsError::new(&e.to_string()))?;

        // Spawn the event loop
        let engine_arc = self.engine.clone();
        let on_event = self.on_event.clone();

        wasm_bindgen_futures::spawn_local(async move {
            loop {
                let mut eng = engine_arc.lock().await;
                match eng.next_event().await {
                    Some(evt) => {
                        if let Some(cb) = &*on_event.borrow() {
                            let js_evt = to_js_event(&evt);
                            let json = serde_json::to_string(&js_evt).unwrap_or_default();
                            let _ = cb.call1(&JsValue::UNDEFINED, &JsValue::from_str(&json));
                        }
                    }
                    None => break,
                }
            }
        });

        Ok(peer_id)
    }

    /// Send a text message to a peer.
    #[wasm_bindgen(js_name = sendText)]
    pub async fn send_text(&self, to: String, content: String) -> Result<(), JsError> {
        let mut engine = self.engine.lock().await;
        engine
            .send_text(&to, &content)
            .await
            .map_err(|e| JsError::new(&e.to_string()))
    }

    /// Broadcast a message to all peers.
    #[wasm_bindgen]
    pub async fn broadcast(&self, content: String) -> Result<(), JsError> {
        let mut engine = self.engine.lock().await;
        engine
            .broadcast(content)
            .await
            .map_err(|e| JsError::new(&e.to_string()))
    }

    /// List connected peers. Returns JSON array.
    #[wasm_bindgen(js_name = getPeers)]
    pub async fn get_peers(&self) -> Result<String, JsError> {
        let engine = self.engine.lock().await;
        let peers = engine.list_peers();
        serde_json::to_string(&peers).map_err(|e| JsError::new(&e.to_string()))
    }

    /// Get chat history with a peer. Returns JSON array of messages.
    #[wasm_bindgen(js_name = getHistory)]
    pub async fn get_history(&self, peer_id: String, limit: u32) -> Result<String, JsError> {
        let engine = self.engine.lock().await;
        let messages = engine.get_history(&peer_id, limit);
        serde_json::to_string(&messages).map_err(|e| JsError::new(&e.to_string()))
    }

    /// Get the local PeerID.
    #[wasm_bindgen(js_name = peerId)]
    pub async fn peer_id(&self) -> String {
        self.engine.lock().await.peer_id_str()
    }

    /// Get the current username.
    #[wasm_bindgen]
    pub async fn username(&self) -> String {
        self.engine.lock().await.username().to_string()
    }

    /// Get the library version string.
    #[wasm_bindgen]
    pub fn version() -> String {
        format!("ipmsg-t wasm v{}", env!("CARGO_PKG_VERSION"))
    }
}

#[wasm_bindgen(start)]
fn init() {
    console_error_panic_hook::set_once();
}
