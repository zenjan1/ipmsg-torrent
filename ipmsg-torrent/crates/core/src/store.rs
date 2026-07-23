use chrono::DateTime;
use ipmsg_protocol::message::ChatMessage;
use std::fmt;

/// Peer info stored locally
#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub peer_id: String,
    pub username: String,
    pub public_key: Vec<u8>,
    pub platforms: String,
    pub last_seen: DateTime<chrono::Utc>,
    pub first_seen: DateTime<chrono::Utc>,
}

/// Store error type (platform-independent)
#[derive(Debug)]
pub struct StoreError(pub String);

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "store error: {}", self.0)
    }
}

impl std::error::Error for StoreError {}

pub type Result<T> = std::result::Result<T, StoreError>;

// ============================================================================
// Native implementation (SQLite via rusqlite)
// ============================================================================
#[cfg(not(target_arch = "wasm32"))]
mod inner {
    use super::*;
    use rusqlite::params;
    use std::path::Path;
    use std::sync::Mutex;

    pub struct MessageStore {
        conn: Mutex<rusqlite::Connection>,
    }

    impl MessageStore {
        pub fn new(path: &Path) -> Result<Self> {
            let conn = rusqlite::Connection::open(path)
                .map_err(|e| StoreError(e.to_string()))?;
            conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")
                .map_err(|e| StoreError(e.to_string()))?;

            conn.execute(
                "CREATE TABLE IF NOT EXISTS peers (
                    peer_id TEXT PRIMARY KEY,
                    username TEXT NOT NULL,
                    public_key BLOB NOT NULL,
                    platforms TEXT DEFAULT '[]',
                    last_seen TIMESTAMP NOT NULL,
                    first_seen TIMESTAMP NOT NULL
                )", [],
            ).map_err(|e| StoreError(e.to_string()))?;

            conn.execute(
                "CREATE TABLE IF NOT EXISTS messages (
                    id TEXT PRIMARY KEY,
                    from_peer TEXT NOT NULL REFERENCES peers(peer_id),
                    to_peer TEXT,
                    channel TEXT,
                    kind TEXT NOT NULL,
                    content BLOB NOT NULL,
                    seq INTEGER DEFAULT 0,
                    timestamp TIMESTAMP NOT NULL,
                    signature BLOB DEFAULT X'',
                    stored_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )", [],
            ).map_err(|e| StoreError(e.to_string()))?;

            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_messages_from ON messages(from_peer, timestamp DESC)", [],
            ).map_err(|e| StoreError(e.to_string()))?;
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_messages_to ON messages(to_peer, timestamp DESC)", [],
            ).map_err(|e| StoreError(e.to_string()))?;
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_messages_channel ON messages(channel, timestamp DESC)", [],
            ).map_err(|e| StoreError(e.to_string()))?;

            Ok(Self { conn: Mutex::new(conn) })
        }

        pub fn save_message(&self, msg: &ChatMessage) -> Result<()> {
            let conn = self.conn.lock().unwrap();
            let content = serde_cbor::to_vec(&msg.kind).unwrap_or_default();
            let channel = msg.channel.as_ref().map(|c| format!("{:?}", c));
            conn.execute(
                "INSERT OR REPLACE INTO messages
                 (id, from_peer, to_peer, channel, kind, content, seq, timestamp, signature)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    msg.id, msg.from, msg.to, channel,
                    msg.kind.label(), content, msg.seq as i64,
                    msg.timestamp.to_rfc3339(), msg.signature,
                ],
            ).map_err(|e| StoreError(e.to_string()))?;
            Ok(())
        }

        pub fn get_messages(&self, peer_id: &str, limit: u32) -> Vec<ChatMessage> {
            let conn = self.conn.lock().unwrap();
            let mut stmt = match conn.prepare(
                "SELECT id, from_peer, to_peer, kind, content, seq, timestamp, signature
                 FROM messages WHERE from_peer = ?1 OR to_peer = ?1
                 ORDER BY timestamp DESC LIMIT ?2",
            ) {
                Ok(s) => s,
                Err(_) => return Vec::new(),
            };
            let rows = match stmt.query_map(params![peer_id, limit], decode_message_row) {
                Ok(r) => r,
                Err(_) => return Vec::new(),
            };
            let mut messages: Vec<ChatMessage> = rows.flatten().collect();
            messages.reverse();
            messages
        }

        pub fn get_channel_messages(&self, channel: &str, limit: u32) -> Vec<ChatMessage> {
            let conn = self.conn.lock().unwrap();
            let mut stmt = match conn.prepare(
                "SELECT id, from_peer, to_peer, kind, content, seq, timestamp, signature
                 FROM messages WHERE channel = ?1
                 ORDER BY timestamp DESC LIMIT ?2",
            ) {
                Ok(s) => s,
                Err(_) => return Vec::new(),
            };
            let rows = match stmt.query_map(params![channel, limit], decode_message_row) {
                Ok(r) => r,
                Err(_) => return Vec::new(),
            };
            let mut messages: Vec<ChatMessage> = rows.flatten().collect();
            messages.reverse();
            messages
        }

        pub fn upsert_peer(&self, info: &PeerInfo) -> Result<()> {
            let conn = self.conn.lock().unwrap();
            conn.execute(
                "INSERT INTO peers (peer_id, username, public_key, platforms, last_seen, first_seen)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(peer_id) DO UPDATE SET
                     username = excluded.username, last_seen = excluded.last_seen",
                params![
                    info.peer_id, info.username, info.public_key, info.platforms,
                    info.last_seen.to_rfc3339(), info.first_seen.to_rfc3339(),
                ],
            ).map_err(|e| StoreError(e.to_string()))?;
            Ok(())
        }

        pub fn get_all_peers(&self) -> Vec<PeerInfo> {
            let conn = self.conn.lock().unwrap();
            let mut stmt = match conn.prepare(
                "SELECT peer_id, username, public_key, platforms, last_seen, first_seen
                 FROM peers ORDER BY last_seen DESC",
            ) {
                Ok(s) => s,
                Err(_) => return Vec::new(),
            };
            let rows = match stmt.query_map([], |row| {
                let last_seen = DateTime::parse_from_rfc3339(&row.get::<_, String>(4)?)
                    .ok().map(|dt| dt.with_timezone(&chrono::Utc)).unwrap_or_else(chrono::Utc::now);
                let first_seen = DateTime::parse_from_rfc3339(&row.get::<_, String>(5)?)
                    .ok().map(|dt| dt.with_timezone(&chrono::Utc)).unwrap_or_else(chrono::Utc::now);
                Ok(PeerInfo {
                    peer_id: row.get(0)?, username: row.get(1)?,
                    public_key: row.get(2)?, platforms: row.get(3)?,
                    last_seen, first_seen,
                })
            }) {
                Ok(r) => r,
                Err(_) => return Vec::new(),
            };
            rows.flatten().collect()
        }

        pub fn cleanup_stale_peers(&self, max_age_secs: i64) -> Result<usize> {
            let conn = self.conn.lock().unwrap();
            let deleted = conn.execute(
                "DELETE FROM peers WHERE last_seen < datetime('now', ?1 || ' seconds')",
                params![format!("-{}", max_age_secs)],
            ).map_err(|e| StoreError(e.to_string()))?;
            Ok(deleted)
        }

        pub fn search_messages(&self, query: &str, limit: u32) -> Vec<ChatMessage> {
            let conn = self.conn.lock().unwrap();
            let pattern = format!("%{}%", query);
            let mut stmt = match conn.prepare(
                "SELECT id, from_peer, to_peer, kind, content, seq, timestamp, signature
                 FROM messages WHERE kind = 'text' AND CAST(content AS TEXT) LIKE ?1
                 ORDER BY timestamp DESC LIMIT ?2",
            ) {
                Ok(s) => s,
                Err(_) => return Vec::new(),
            };
            let rows = match stmt.query_map(params![pattern, limit], decode_message_row) {
                Ok(r) => r,
                Err(_) => return Vec::new(),
            };
            let mut messages: Vec<ChatMessage> = rows.flatten().collect();
            messages.reverse();
            messages
        }
    }

    fn decode_message_row(row: &rusqlite::Row) -> rusqlite::Result<ChatMessage> {
        let id: String = row.get(0)?;
        let from: String = row.get(1)?;
        let to: Option<String> = row.get(2)?;
        let content: Vec<u8> = row.get(4)?;
        let seq: i64 = row.get(5)?;
        let ts_str: String = row.get(6)?;
        let signature: Vec<u8> = row.get(7)?;
        let timestamp = DateTime::parse_from_rfc3339(&ts_str)
            .ok().map(|dt| dt.with_timezone(&chrono::Utc)).unwrap_or_else(chrono::Utc::now);
        let kind = serde_cbor::from_slice(&content).unwrap_or_default();
        Ok(ChatMessage {
            id, from, to, channel: None, seq: seq as u64,
            timestamp, ttl: 0, kind, encrypted_payload: None, signature, reply_to: None,
        })
    }
}

// ============================================================================
// WASM implementation (in-memory)
// ============================================================================
#[cfg(target_arch = "wasm32")]
mod inner {
    use super::*;
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::path::Path;

    pub struct MessageStore {
        messages: RefCell<Vec<ChatMessage>>,
        peers: RefCell<HashMap<String, PeerInfo>>,
    }

    impl MessageStore {
        pub fn new(_path: &Path) -> Result<Self> {
            Ok(Self {
                messages: RefCell::new(Vec::new()),
                peers: RefCell::new(HashMap::new()),
            })
        }

        pub fn save_message(&self, msg: &ChatMessage) -> Result<()> {
            let mut msgs = self.messages.borrow_mut();
            // Replace if same ID exists, otherwise append
            if let Some(pos) = msgs.iter().position(|m| m.id == msg.id) {
                msgs[pos] = msg.clone();
            } else {
                msgs.push(msg.clone());
            }
            Ok(())
        }

        pub fn get_messages(&self, peer_id: &str, limit: u32) -> Vec<ChatMessage> {
            let msgs = self.messages.borrow();
            let mut result: Vec<ChatMessage> = msgs.iter()
                .filter(|m| m.from == peer_id || m.to.as_deref() == Some(peer_id))
                .cloned()
                .collect();
            result.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
            if result.len() > limit as usize {
                result = result[result.len() - limit as usize..].to_vec();
            }
            result
        }

        pub fn get_channel_messages(&self, channel: &str, limit: u32) -> Vec<ChatMessage> {
            let msgs = self.messages.borrow();
            let chan_tag = format!("Group(\"{}\")", channel);
            let mut result: Vec<ChatMessage> = msgs.iter()
                .filter(|m| m.channel.as_ref().map(|c| format!("{:?}", c)) == Some(chan_tag.clone()))
                .cloned()
                .collect();
            result.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
            if result.len() > limit as usize {
                result = result[result.len() - limit as usize..].to_vec();
            }
            result
        }

        pub fn upsert_peer(&self, info: &PeerInfo) -> Result<()> {
            let mut peers = self.peers.borrow_mut();
            if let Some(existing) = peers.get_mut(&info.peer_id) {
                existing.username = info.username.clone();
                existing.last_seen = info.last_seen;
            } else {
                peers.insert(info.peer_id.clone(), info.clone());
            }
            Ok(())
        }

        pub fn get_all_peers(&self) -> Vec<PeerInfo> {
            let peers = self.peers.borrow();
            let mut result: Vec<PeerInfo> = peers.values().cloned().collect();
            result.sort_by(|a, b| b.last_seen.cmp(&a.last_seen));
            result
        }

        pub fn cleanup_stale_peers(&self, max_age_secs: i64) -> Result<usize> {
            let mut peers = self.peers.borrow_mut();
            let cutoff = chrono::Utc::now() - chrono::Duration::seconds(max_age_secs);
            let before = peers.len();
            peers.retain(|_, p| p.last_seen > cutoff);
            Ok(before - peers.len())
        }

        pub fn search_messages(&self, query: &str, limit: u32) -> Vec<ChatMessage> {
            let msgs = self.messages.borrow();
            let query_lower = query.to_lowercase();
            let mut result: Vec<ChatMessage> = msgs.iter()
                .filter(|m| {
                    matches!(m.kind, ipmsg_protocol::message::MessageType::Text(_))
                        && m.text_content().map(|t| t.to_lowercase().contains(&query_lower)).unwrap_or(false)
                })
                .cloned()
                .collect();
            result.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
            result.truncate(limit as usize);
            result.reverse();
            result
        }
    }
}

pub use inner::MessageStore;
