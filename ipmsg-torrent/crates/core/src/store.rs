use chrono::DateTime;
use ipmsg_protocol::message::ChatMessage;
use rusqlite::{params, Connection, Result};
use std::path::Path;
use std::sync::Mutex;

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

/// SQLite-backed message and peer store
pub struct MessageStore {
    conn: Mutex<Connection>,
}

impl MessageStore {
    pub fn new(path: &Path) -> Result<Self> {
        let conn = Connection::open(path)?;

        // Enable WAL mode for better performance
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")?;

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
        )?;

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
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_messages_from ON messages(from_peer, timestamp DESC)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_messages_to ON messages(to_peer, timestamp DESC)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_messages_channel ON messages(channel, timestamp DESC)",
            [],
        )?;

        Ok(Self { conn: Mutex::new(conn) })
    }

    pub fn save_message(&self, msg: &ChatMessage) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let content = serde_cbor::to_vec(&msg.kind).unwrap_or_default();
        let channel = msg.channel.as_ref().map(|c| format!("{:?}", c));

        // Try INSERT, if conflict (duplicate ID) do UPDATE
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
        )?;
        Ok(())
    }

    pub fn get_messages(&self, peer_id: &str, limit: u32) -> Vec<ChatMessage> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = match conn.prepare(
            "SELECT id, from_peer, to_peer, kind, content, seq, timestamp, signature
             FROM messages
             WHERE from_peer = ?1 OR to_peer = ?1
             ORDER BY timestamp DESC
             LIMIT ?2",
        ) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };

        let rows = match stmt.query_map(params![peer_id, limit], |row| {
            let id: String = row.get(0)?;
            let from: String = row.get(1)?;
            let to: Option<String> = row.get(2)?;
            let _kind: String = row.get(3)?;
            let content: Vec<u8> = row.get(4)?;
            let seq: i64 = row.get(5)?;
            let ts_str: String = row.get(6)?;
            let signature: Vec<u8> = row.get(7)?;

            let timestamp = DateTime::parse_from_rfc3339(&ts_str)
                .ok()
                .map(|dt| dt.with_timezone(&chrono::Utc))
                .unwrap_or(chrono::Utc::now());

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
        }) {
            Ok(r) => r,
            Err(_) => return Vec::new(),
        };

        let mut messages: Vec<ChatMessage> = Vec::new();
        for row in rows {
            if let Ok(msg) = row {
                messages.push(msg);
            }
        }
        messages.reverse();
        messages
    }

    /// Get messages for a specific channel
    pub fn get_channel_messages(&self, channel: &str, limit: u32) -> Vec<ChatMessage> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = match conn.prepare(
            "SELECT id, from_peer, to_peer, kind, content, seq, timestamp, signature
             FROM messages
             WHERE channel = ?1
             ORDER BY timestamp DESC
             LIMIT ?2",
        ) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };

        let rows = match stmt.query_map(params![channel, limit], |row| {
            let id: String = row.get(0)?;
            let from: String = row.get(1)?;
            let to: Option<String> = row.get(2)?;
            let _kind: String = row.get(3)?;
            let content: Vec<u8> = row.get(4)?;
            let seq: i64 = row.get(5)?;
            let ts_str: String = row.get(6)?;
            let signature: Vec<u8> = row.get(7)?;

            let timestamp = DateTime::parse_from_rfc3339(&ts_str)
                .ok()
                .map(|dt| dt.with_timezone(&chrono::Utc))
                .unwrap_or(chrono::Utc::now());

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
        }) {
            Ok(r) => r,
            Err(_) => return Vec::new(),
        };

        let mut messages: Vec<ChatMessage> = Vec::new();
        for row in rows {
            if let Ok(msg) = row {
                messages.push(msg);
            }
        }
        messages.reverse();
        messages
    }

    pub fn upsert_peer(&self, info: &PeerInfo) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO peers (peer_id, username, public_key, platforms, last_seen, first_seen)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(peer_id) DO UPDATE SET
                 username = excluded.username,
                 last_seen = excluded.last_seen",
            params![
                info.peer_id,
                info.username,
                info.public_key,
                info.platforms,
                info.last_seen.to_rfc3339(),
                info.first_seen.to_rfc3339(),
            ],
        )?;
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
            let peer_id: String = row.get(0)?;
            let username: String = row.get(1)?;
            let public_key: Vec<u8> = row.get(2)?;
            let platforms: String = row.get(3)?;
            let last_seen_str: String = row.get(4)?;
            let first_seen_str: String = row.get(5)?;

            let last_seen = DateTime::parse_from_rfc3339(&last_seen_str)
                .ok()
                .map(|dt| dt.with_timezone(&chrono::Utc))
                .unwrap_or(chrono::Utc::now());
            let first_seen = DateTime::parse_from_rfc3339(&first_seen_str)
                .ok()
                .map(|dt| dt.with_timezone(&chrono::Utc))
                .unwrap_or(chrono::Utc::now());

            Ok(PeerInfo {
                peer_id,
                username,
                public_key,
                platforms,
                last_seen,
                first_seen,
            })
        }) {
            Ok(r) => r,
            Err(_) => return Vec::new(),
        };

        let mut peers = Vec::new();
        for row in rows {
            if let Ok(peer) = row {
                peers.push(peer);
            }
        }
        peers
    }

    pub fn cleanup_stale_peers(&self, max_age_secs: i64) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let deleted = conn.execute(
            "DELETE FROM peers WHERE last_seen < datetime('now', ?1 || ' seconds')",
            params![format!("-{}", max_age_secs)],
        )?;
        Ok(deleted)
    }

    /// Search messages by text content
    pub fn search_messages(&self, query: &str, limit: u32) -> Vec<ChatMessage> {
        let conn = self.conn.lock().unwrap();
        let pattern = format!("%{}%", query);
        let mut stmt = match conn.prepare(
            "SELECT id, from_peer, to_peer, kind, content, seq, timestamp, signature
             FROM messages
             WHERE kind = 'text' AND CAST(content AS TEXT) LIKE ?1
             ORDER BY timestamp DESC
             LIMIT ?2",
        ) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };

        let rows = match stmt.query_map(params![pattern, limit], |row| {
            let id: String = row.get(0)?;
            let from: String = row.get(1)?;
            let to: Option<String> = row.get(2)?;
            let content: Vec<u8> = row.get(3)?;
            let seq: i64 = row.get(4)?;
            let ts_str: String = row.get(5)?;
            let signature: Vec<u8> = row.get(6)?;

            let timestamp = DateTime::parse_from_rfc3339(&ts_str)
                .ok()
                .map(|dt| dt.with_timezone(&chrono::Utc))
                .unwrap_or(chrono::Utc::now());

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
        }) {
            Ok(r) => r,
            Err(_) => return Vec::new(),
        };

        let mut messages: Vec<ChatMessage> = Vec::new();
        for row in rows {
            if let Ok(msg) = row {
                messages.push(msg);
            }
        }
        messages.reverse();
        messages
    }
}
