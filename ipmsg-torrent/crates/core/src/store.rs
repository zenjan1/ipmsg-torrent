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
            let conn = rusqlite::Connection::open(path).map_err(|e| StoreError(e.to_string()))?;
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
                )",
                [],
            )
            .map_err(|e| StoreError(e.to_string()))?;

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
                )",
                [],
            )
            .map_err(|e| StoreError(e.to_string()))?;

            conn.execute(
                "CREATE TABLE IF NOT EXISTS peer_addresses (
                    peer_id TEXT NOT NULL,
                    addr TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (peer_id, addr)
                )",
                [],
            )
            .map_err(|e| StoreError(e.to_string()))?;

            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_messages_from ON messages(from_peer, timestamp DESC)", [],
            ).map_err(|e| StoreError(e.to_string()))?;
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_messages_to ON messages(to_peer, timestamp DESC)",
                [],
            )
            .map_err(|e| StoreError(e.to_string()))?;
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_messages_channel ON messages(channel, timestamp DESC)", [],
            ).map_err(|e| StoreError(e.to_string()))?;

            conn.execute(
                "CREATE TABLE IF NOT EXISTS peer_reputation (
                    peer_id TEXT PRIMARY KEY,
                    score REAL NOT NULL DEFAULT 0.5,
                    message_quality REAL NOT NULL DEFAULT 0.5,
                    response_latency_ms INTEGER NOT NULL DEFAULT 0,
                    file_shares INTEGER NOT NULL DEFAULT 0,
                    file_downloads INTEGER NOT NULL DEFAULT 0,
                    uptime_hours REAL NOT NULL DEFAULT 0.0,
                    violations INTEGER NOT NULL DEFAULT 0,
                    total_messages INTEGER NOT NULL DEFAULT 0,
                    valid_messages INTEGER NOT NULL DEFAULT 0,
                    latency_samples INTEGER NOT NULL DEFAULT 0,
                    connected_since TEXT,
                    last_updated TEXT NOT NULL
                )",
                [],
            )
            .map_err(|e| StoreError(e.to_string()))?;

            Ok(Self {
                conn: Mutex::new(conn),
            })
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
                    msg.id,
                    msg.from,
                    msg.to,
                    channel,
                    msg.kind.label(),
                    content,
                    msg.seq as i64,
                    msg.timestamp.to_rfc3339(),
                    msg.signature,
                ],
            )
            .map_err(|e| StoreError(e.to_string()))?;
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
                    .ok()
                    .map(|dt| dt.with_timezone(&chrono::Utc))
                    .unwrap_or_else(chrono::Utc::now);
                let first_seen = DateTime::parse_from_rfc3339(&row.get::<_, String>(5)?)
                    .ok()
                    .map(|dt| dt.with_timezone(&chrono::Utc))
                    .unwrap_or_else(chrono::Utc::now);
                Ok(PeerInfo {
                    peer_id: row.get(0)?,
                    username: row.get(1)?,
                    public_key: row.get(2)?,
                    platforms: row.get(3)?,
                    last_seen,
                    first_seen,
                })
            }) {
                Ok(r) => r,
                Err(_) => return Vec::new(),
            };
            rows.flatten().collect()
        }

        pub fn cleanup_stale_peers(&self, max_age_secs: i64) -> Result<usize> {
            let cutoff =
                (chrono::Utc::now() - chrono::Duration::seconds(max_age_secs)).to_rfc3339();
            let conn = self.conn.lock().unwrap();
            let deleted = conn
                .execute("DELETE FROM peers WHERE last_seen < ?1", params![cutoff])
                .map_err(|e| StoreError(e.to_string()))?;
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

        pub fn save_peer_addresses(&self, peer_id: &str, addrs: &[String]) -> Result<()> {
            let conn = self.conn.lock().unwrap();
            let now = chrono::Utc::now().to_rfc3339();
            for addr in addrs {
                conn.execute(
                    "INSERT INTO peer_addresses (peer_id, addr, updated_at)
                     VALUES (?1, ?2, ?3)
                     ON CONFLICT(peer_id, addr) DO UPDATE SET updated_at = excluded.updated_at",
                    params![peer_id, addr, now],
                )
                .map_err(|e| StoreError(e.to_string()))?;
            }
            Ok(())
        }

        pub fn get_peer_public_key(&self, peer_id: &str) -> Option<Vec<u8>> {
            let conn = self.conn.lock().unwrap();
            let mut stmt = match conn.prepare("SELECT public_key FROM peers WHERE peer_id = ?1") {
                Ok(s) => s,
                Err(_) => return None,
            };
            let result = stmt.query_row(params![peer_id], |row| row.get::<_, Vec<u8>>(0));
            result.ok()
        }

        pub fn get_known_addresses(&self, max_age_days: i64) -> Vec<(String, Vec<String>)> {
            let conn = self.conn.lock().unwrap();
            let mut stmt = match conn.prepare(
                "SELECT peer_id, addr FROM peer_addresses
                 WHERE updated_at >= datetime('now', ?1 || ' days')
                 ORDER BY updated_at DESC",
            ) {
                Ok(s) => s,
                Err(_) => return Vec::new(),
            };
            let rows = match stmt.query_map(params![format!("-{}", max_age_days)], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            }) {
                Ok(r) => r,
                Err(_) => return Vec::new(),
            };
            let mut map: std::collections::HashMap<String, Vec<String>> =
                std::collections::HashMap::new();
            for row in rows.flatten() {
                map.entry(row.0).or_default().push(row.1);
            }
            map.into_iter().collect()
        }

        pub fn save_reputation(&self, rep: &crate::reputation::PeerReputation) -> Result<()> {
            let conn = self.conn.lock().unwrap();
            conn.execute(
                "INSERT OR REPLACE INTO peer_reputation
                 (peer_id, score, message_quality, response_latency_ms, file_shares,
                  file_downloads, uptime_hours, violations, total_messages, valid_messages,
                  latency_samples, connected_since, last_updated)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                params![
                    rep.peer_id,
                    rep.score,
                    rep.message_quality,
                    rep.response_latency_ms as i64,
                    rep.file_shares as i64,
                    rep.file_downloads as i64,
                    rep.uptime_hours,
                    rep.violations as i64,
                    rep.total_messages() as i64,
                    rep.valid_messages() as i64,
                    rep.latency_samples() as i64,
                    rep.connected_since().map(|t| t.to_rfc3339()),
                    rep.last_updated.to_rfc3339(),
                ],
            )
            .map_err(|e| StoreError(e.to_string()))?;
            Ok(())
        }

        pub fn load_all_reputation(&self) -> Result<Vec<crate::reputation::PeerReputation>> {
            let conn = self.conn.lock().unwrap();
            let mut stmt = conn
                .prepare(
                    "SELECT peer_id, score, message_quality, response_latency_ms, file_shares,
                        file_downloads, uptime_hours, violations, total_messages, valid_messages,
                        latency_samples, connected_since, last_updated
                 FROM peer_reputation",
                )
                .map_err(|e| StoreError(e.to_string()))?;

            let rows = stmt
                .query_map([], |row| {
                    let connected_since_str: Option<String> = row.get(11)?;
                    let connected_since = connected_since_str
                        .and_then(|s| chrono::DateTime::parse_from_rfc3339(&s).ok())
                        .map(|dt| dt.with_timezone(&chrono::Utc));

                    Ok(crate::reputation::PeerReputation::from_db(
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get::<_, i64>(3)? as u64,
                        row.get::<_, i64>(4)? as u32,
                        row.get::<_, i64>(5)? as u32,
                        row.get(6)?,
                        row.get::<_, i64>(7)? as u32,
                        row.get::<_, i64>(8)? as u64,
                        row.get::<_, i64>(9)? as u64,
                        row.get::<_, i64>(10)? as u64,
                        connected_since,
                        row.get::<_, String>(12)?
                            .parse()
                            .unwrap_or_else(|_| chrono::Utc::now()),
                    ))
                })
                .map_err(|e| StoreError(e.to_string()))?;

            let mut result = Vec::new();
            for row in rows {
                result.push(row.map_err(|e| StoreError(e.to_string()))?);
            }
            Ok(result)
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
            .ok()
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .unwrap_or_else(chrono::Utc::now);
        let kind = serde_cbor::from_slice(&content).unwrap_or_default();
        Ok(ChatMessage {
            id,
            from,
            to,
            channel: None,
            seq: seq as u64,
            timestamp,
            ttl: 0,
            kind,
            encrypted_payload: None,
            signature,
            reply_to: None,
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
        peer_addresses: RefCell<HashMap<String, Vec<(String, chrono::DateTime<chrono::Utc>)>>>,
    }

    impl MessageStore {
        pub fn new(_path: &Path) -> Result<Self> {
            Ok(Self {
                messages: RefCell::new(Vec::new()),
                peers: RefCell::new(HashMap::new()),
                peer_addresses: RefCell::new(HashMap::new()),
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
            let mut result: Vec<ChatMessage> = msgs
                .iter()
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
            let mut result: Vec<ChatMessage> = msgs
                .iter()
                .filter(|m| {
                    m.channel.as_ref().map(|c| format!("{:?}", c)) == Some(chan_tag.clone())
                })
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
            let mut result: Vec<ChatMessage> = msgs
                .iter()
                .filter(|m| {
                    matches!(m.kind, ipmsg_protocol::message::MessageType::Text(_))
                        && m.text_content()
                            .map(|t| t.to_lowercase().contains(&query_lower))
                            .unwrap_or(false)
                })
                .cloned()
                .collect();
            result.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
            result.truncate(limit as usize);
            result.reverse();
            result
        }

        pub fn save_peer_addresses(&self, peer_id: &str, addrs: &[String]) -> Result<()> {
            let mut all = self.peer_addresses.borrow_mut();
            let now = chrono::Utc::now();
            let entry = all.entry(peer_id.to_string()).or_default();
            for addr in addrs {
                if let Some(existing) = entry.iter_mut().find(|(a, _)| a == addr) {
                    existing.1 = now;
                } else {
                    entry.push((addr.clone(), now));
                }
            }
            Ok(())
        }

        pub fn get_known_addresses(&self, max_age_days: i64) -> Vec<(String, Vec<String>)> {
            let all = self.peer_addresses.borrow();
            let cutoff = chrono::Utc::now() - chrono::Duration::days(max_age_days);
            all.iter()
                .map(|(peer_id, addrs)| {
                    let filtered: Vec<String> = addrs
                        .iter()
                        .filter(|(_, ts)| *ts > cutoff)
                        .map(|(a, _)| a.clone())
                        .collect();
                    (peer_id.clone(), filtered)
                })
                .filter(|(_, addrs)| !addrs.is_empty())
                .collect()
        }

        pub fn save_reputation(&self, rep: &crate::reputation::PeerReputation) -> Result<()> {
            let conn = self.conn.lock().unwrap();
            conn.execute(
                "INSERT OR REPLACE INTO peer_reputation
                 (peer_id, score, message_quality, response_latency_ms, file_shares,
                  file_downloads, uptime_hours, violations, total_messages, valid_messages,
                  latency_samples, connected_since, last_updated)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                params![
                    rep.peer_id,
                    rep.score,
                    rep.message_quality,
                    rep.response_latency_ms as i64,
                    rep.file_shares as i64,
                    rep.file_downloads as i64,
                    rep.uptime_hours,
                    rep.violations as i64,
                    rep.total_messages() as i64,
                    rep.valid_messages() as i64,
                    rep.latency_samples() as i64,
                    rep.connected_since().map(|t| t.to_rfc3339()),
                    rep.last_updated.to_rfc3339(),
                ],
            )
            .map_err(|e| StoreError(e.to_string()))?;
            Ok(())
        }

        pub fn load_all_reputation(&self) -> Result<Vec<crate::reputation::PeerReputation>> {
            let conn = self.conn.lock().unwrap();
            let mut stmt = conn
                .prepare(
                    "SELECT peer_id, score, message_quality, response_latency_ms, file_shares,
                        file_downloads, uptime_hours, violations, total_messages, valid_messages,
                        latency_samples, connected_since, last_updated
                 FROM peer_reputation",
                )
                .map_err(|e| StoreError(e.to_string()))?;

            let rows = stmt
                .query_map([], |row| {
                    let connected_since_str: Option<String> = row.get(11)?;
                    let connected_since = connected_since_str
                        .and_then(|s| chrono::DateTime::parse_from_rfc3339(&s).ok())
                        .map(|dt| dt.with_timezone(&chrono::Utc));

                    Ok(crate::reputation::PeerReputation::from_db(
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get::<_, i64>(3)? as u64,
                        row.get::<_, i64>(4)? as u32,
                        row.get::<_, i64>(5)? as u32,
                        row.get(6)?,
                        row.get::<_, i64>(7)? as u32,
                        row.get::<_, i64>(8)? as u64,
                        row.get::<_, i64>(9)? as u64,
                        row.get::<_, i64>(10)? as u64,
                        connected_since,
                        row.get::<_, String>(12)?
                            .parse()
                            .unwrap_or_else(|_| chrono::Utc::now()),
                    ))
                })
                .map_err(|e| StoreError(e.to_string()))?;

            let mut result = Vec::new();
            for row in rows {
                result.push(row.map_err(|e| StoreError(e.to_string()))?);
            }
            Ok(result)
        }
    }
}

pub use inner::MessageStore;

#[cfg(test)]
mod tests {
    use super::*;
    use ipmsg_protocol::message::{ChatMessage, MessageType};
    use tempfile::TempDir;

    fn make_store(dir: &TempDir) -> MessageStore {
        let db_path = dir.path().join("test.db");
        MessageStore::new(&db_path).unwrap()
    }

    fn make_peer(id: &str, name: &str) -> PeerInfo {
        PeerInfo {
            peer_id: id.to_string(),
            username: name.to_string(),
            public_key: vec![1, 2, 3],
            platforms: "[]".to_string(),
            last_seen: chrono::Utc::now(),
            first_seen: chrono::Utc::now(),
        }
    }

    fn make_text_msg(id: &str, from: &str, to: Option<&str>, text: &str) -> ChatMessage {
        ChatMessage {
            id: id.to_string(),
            from: from.to_string(),
            to: to.map(|s| s.to_string()),
            channel: None,
            seq: 0,
            timestamp: chrono::Utc::now(),
            ttl: 0,
            kind: MessageType::Text {
                content: text.to_string(),
            },
            encrypted_payload: None,
            signature: vec![],
            reply_to: None,
        }
    }

    #[test]
    fn test_create_store() {
        let dir = TempDir::new().unwrap();
        let _store = make_store(&dir);
    }

    #[test]
    fn test_create_store_invalid_path() {
        let result = MessageStore::new(std::path::Path::new("/nonexistent/dir/test.db"));
        assert!(result.is_err());
    }

    #[test]
    fn test_save_and_get_message() {
        let dir = TempDir::new().unwrap();
        let store = make_store(&dir);
        // Create peer first to satisfy foreign key constraint
        store.upsert_peer(&make_peer("peer_a", "Alice")).unwrap();
        let msg = make_text_msg("msg1", "peer_a", Some("peer_b"), "hello");
        store.save_message(&msg).unwrap();
        let msgs = store.get_messages("peer_a", 10);
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].id, "msg1");
    }

    #[test]
    fn test_get_messages_by_to_peer() {
        let dir = TempDir::new().unwrap();
        let store = make_store(&dir);
        // Create peer first to satisfy foreign key constraint
        store.upsert_peer(&make_peer("peer_a", "Alice")).unwrap();
        let msg = make_text_msg("msg2", "peer_a", Some("peer_b"), "hi");
        store.save_message(&msg).unwrap();
        let msgs = store.get_messages("peer_b", 10);
        assert_eq!(msgs.len(), 1);
    }

    #[test]
    fn test_get_messages_limit() {
        let dir = TempDir::new().unwrap();
        let store = make_store(&dir);
        // Create peer first to satisfy foreign key constraint
        store.upsert_peer(&make_peer("peer", "Peer")).unwrap();
        for i in 0..5 {
            let msg = make_text_msg(
                &format!("m{}", i),
                "peer",
                Some("other"),
                &format!("msg {}", i),
            );
            store.save_message(&msg).unwrap();
        }
        let msgs = store.get_messages("peer", 3);
        assert_eq!(msgs.len(), 3);
    }

    #[test]
    fn test_get_messages_empty() {
        let dir = TempDir::new().unwrap();
        let store = make_store(&dir);
        let msgs = store.get_messages("nonexistent", 10);
        assert!(msgs.is_empty());
    }

    #[test]
    fn test_upsert_and_get_peer() {
        let dir = TempDir::new().unwrap();
        let store = make_store(&dir);
        let peer = make_peer("p1", "alice");
        store.upsert_peer(&peer).unwrap();
        let peers = store.get_all_peers();
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].peer_id, "p1");
        assert_eq!(peers[0].username, "alice");
    }

    #[test]
    fn test_upsert_peer_updates_existing() {
        let dir = TempDir::new().unwrap();
        let store = make_store(&dir);
        let peer1 = make_peer("p1", "alice");
        store.upsert_peer(&peer1).unwrap();
        let peer2 = make_peer("p1", "alice_updated");
        store.upsert_peer(&peer2).unwrap();
        let peers = store.get_all_peers();
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].username, "alice_updated");
    }

    #[test]
    fn test_get_all_peers_sorted_by_last_seen() {
        let dir = TempDir::new().unwrap();
        let store = make_store(&dir);
        let mut p1 = make_peer("p1", "old");
        p1.last_seen = chrono::Utc::now() - chrono::Duration::hours(2);
        let mut p2 = make_peer("p2", "new");
        p2.last_seen = chrono::Utc::now();
        store.upsert_peer(&p1).unwrap();
        store.upsert_peer(&p2).unwrap();
        let peers = store.get_all_peers();
        assert_eq!(peers.len(), 2);
        assert_eq!(peers[0].peer_id, "p2"); // most recent first
    }

    #[test]
    fn test_cleanup_stale_peers() {
        let dir = TempDir::new().unwrap();
        let store = make_store(&dir);
        let mut old = make_peer("old_peer", "old");
        old.last_seen = chrono::Utc::now() - chrono::Duration::seconds(1000);
        store.upsert_peer(&old).unwrap();
        let mut fresh = make_peer("fresh_peer", "fresh");
        fresh.last_seen = chrono::Utc::now();
        store.upsert_peer(&fresh).unwrap();
        let deleted = store.cleanup_stale_peers(500).unwrap();
        assert_eq!(deleted, 1);
        let peers = store.get_all_peers();
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].peer_id, "fresh_peer");
    }

    #[test]
    fn test_search_messages() {
        let dir = TempDir::new().unwrap();
        let store = make_store(&dir);
        // Create peer first to satisfy foreign key constraint
        store.upsert_peer(&make_peer("p1", "P1")).unwrap();
        let msg1 = make_text_msg("s1", "p1", None, "hello world");
        let msg2 = make_text_msg("s2", "p1", None, "goodbye world");
        let msg3 = make_text_msg("s3", "p1", None, "hello rust");
        store.save_message(&msg1).unwrap();
        store.save_message(&msg2).unwrap();
        store.save_message(&msg3).unwrap();
        let results = store.search_messages("hello", 10);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_search_messages_limit() {
        let dir = TempDir::new().unwrap();
        let store = make_store(&dir);
        // Create peer first to satisfy foreign key constraint
        store.upsert_peer(&make_peer("p", "P")).unwrap();
        for i in 0..5 {
            let msg = make_text_msg(&format!("sl{}", i), "p", None, &format!("hello {}", i));
            store.save_message(&msg).unwrap();
        }
        let results = store.search_messages("hello", 2);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_search_messages_empty() {
        let dir = TempDir::new().unwrap();
        let store = make_store(&dir);
        let results = store.search_messages("nonexistent", 10);
        assert!(results.is_empty());
    }

    #[test]
    fn test_save_and_get_peer_public_key() {
        let dir = TempDir::new().unwrap();
        let store = make_store(&dir);
        let peer = make_peer("pk_peer", "bob");
        store.upsert_peer(&peer).unwrap();
        let key = store.get_peer_public_key("pk_peer");
        assert_eq!(key, Some(vec![1, 2, 3]));
        assert!(store.get_peer_public_key("nonexistent").is_none());
    }

    #[test]
    fn test_save_peer_addresses() {
        let dir = TempDir::new().unwrap();
        let store = make_store(&dir);
        let addrs = vec![
            "/ip4/1.2.3.4/tcp/4001".to_string(),
            "/ip4/5.6.7.8/udp/4001/quic-v1".to_string(),
        ];
        store.save_peer_addresses("peer1", &addrs).unwrap();
        let known = store.get_known_addresses(7);
        assert!(!known.is_empty());
        let (pid, addr_list) = &known[0];
        assert_eq!(pid, "peer1");
        assert_eq!(addr_list.len(), 2);
    }

    #[test]
    fn test_save_peer_addresses_upsert() {
        let dir = TempDir::new().unwrap();
        let store = make_store(&dir);
        let addr = vec!["/ip4/1.2.3.4/tcp/4001".to_string()];
        store.save_peer_addresses("peer1", &addr).unwrap();
        store.save_peer_addresses("peer1", &addr).unwrap(); // upsert
        let known = store.get_known_addresses(7);
        let (_, addr_list) = &known[0];
        assert_eq!(addr_list.len(), 1); // no duplicate
    }

    #[test]
    fn test_get_known_addresses_empty() {
        let dir = TempDir::new().unwrap();
        let store = make_store(&dir);
        let known = store.get_known_addresses(7);
        assert!(known.is_empty());
    }

    #[test]
    fn test_store_error_display() {
        let err = StoreError("something went wrong".to_string());
        assert_eq!(format!("{}", err), "store error: something went wrong");
    }
}
