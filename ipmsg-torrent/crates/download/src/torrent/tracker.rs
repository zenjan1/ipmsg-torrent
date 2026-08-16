//! HTTP Tracker protocol implementation

use super::meta::TorrentMeta;
use reqwest::Client;
use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;
use url::Url;

#[derive(Debug, thiserror::Error)]
pub enum TrackerError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("invalid tracker response: {0}")]
    InvalidResponse(String),
    #[error("tracker returned error: {0}")]
    ServerError(String),
    #[error("URL parse error: {0}")]
    UrlParse(#[from] url::ParseError),
}

/// Peer information from tracker
#[derive(Debug, Clone)]
pub struct TrackerPeer {
    pub peer_id: Option<[u8; 20]>,
    pub ip: IpAddr,
    pub port: u16,
}

/// Tracker announce response
#[derive(Debug, Clone)]
pub struct AnnounceResponse {
    /// Interval in seconds between regular announces
    pub interval: u64,
    /// Minimum interval (optional)
    pub min_interval: Option<u64>,
    /// Tracker ID (optional, for private torrents)
    pub tracker_id: Option<String>,
    /// Number of seeders (optional)
    pub complete: Option<u64>,
    /// Number of leechers (optional)
    pub incomplete: Option<u64>,
    /// List of peers
    pub peers: Vec<TrackerPeer>,
}

/// Announce event type
#[derive(Debug, Clone, Copy)]
pub enum AnnounceEvent {
    Started,
    Stopped,
    Completed,
    None,
}

/// HTTP Tracker client
pub struct HttpTracker {
    client: Client,
    peer_id: [u8; 20],
    port: u16,
    uploaded: u64,
    downloaded: u64,
    left: u64,
}

impl HttpTracker {
    pub fn new(peer_id: [u8; 20], port: u16) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("Failed to create HTTP client");

        Self {
            client,
            peer_id,
            port,
            uploaded: 0,
            downloaded: 0,
            left: 0,
        }
    }

    /// Announce to tracker
    pub async fn announce(
        &mut self,
        meta: &TorrentMeta,
        event: AnnounceEvent,
    ) -> Result<AnnounceResponse, TrackerError> {
        let tracker_url = meta
            .announce
            .as_ref()
            .ok_or_else(|| TrackerError::InvalidResponse("no announce URL".to_string()))?;

        let mut url = Url::parse(tracker_url)?;

        // Build query parameters
        {
            let mut query = url.query_pairs_mut();

            // Info hash (20 bytes, URL-encoded)
            query.append_pair("info_hash", &Self::encode_bytes(&meta.info_hash));

            // Peer ID
            query.append_pair("peer_id", &Self::encode_bytes(&self.peer_id));

            // Port
            query.append_pair("port", &self.port.to_string());

            // Uploaded/downloaded/left
            query.append_pair("uploaded", &self.uploaded.to_string());
            query.append_pair("downloaded", &self.downloaded.to_string());
            query.append_pair("left", &self.left.to_string());

            // Compact response (1 = yes)
            query.append_pair("compact", "1");

            // Event
            match event {
                AnnounceEvent::Started => {
                    query.append_pair("event", "started");
                }
                AnnounceEvent::Stopped => {
                    query.append_pair("event", "stopped");
                }
                AnnounceEvent::Completed => {
                    query.append_pair("event", "completed");
                }
                AnnounceEvent::None => {}
            }

            // Numwant (request up to 50 peers)
            query.append_pair("numwant", "50");
        }

        // Make HTTP request
        let response = self.client.get(url.clone()).send().await?;
        let bytes = response.bytes().await?;

        // Parse bencode response
        self.parse_announce_response(&bytes)
    }

    fn encode_bytes(bytes: &[u8]) -> String {
        bytes
            .iter()
            .map(|&b| {
                if b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.' || b == b'~' {
                    (b as char).to_string()
                } else {
                    format!("%{:02X}", b)
                }
            })
            .collect()
    }

    fn parse_announce_response(&self, data: &[u8]) -> Result<AnnounceResponse, TrackerError> {
        let bencode = super::bencode::decode(data)
            .map_err(|e| TrackerError::InvalidResponse(format!("bencode error: {}", e)))?;

        let dict = bencode
            .as_dict()
            .ok_or_else(|| TrackerError::InvalidResponse("not a dictionary".to_string()))?;

        // Check for error
        if let Some(error) = dict.get("failure reason") {
            return Err(TrackerError::ServerError(
                error
                    .as_string()
                    .unwrap_or_else(|| "unknown error".to_string()),
            ));
        }

        // Parse interval
        let interval = dict
            .get("interval")
            .and_then(|v| v.as_integer())
            .ok_or_else(|| TrackerError::InvalidResponse("missing interval".to_string()))?
            as u64;

        let min_interval = dict
            .get("min interval")
            .and_then(|v| v.as_integer())
            .map(|v| v as u64);
        let tracker_id = dict.get("tracker id").and_then(|v| v.as_string());
        let complete = dict
            .get("complete")
            .and_then(|v| v.as_integer())
            .map(|v| v as u64);
        let incomplete = dict
            .get("incomplete")
            .and_then(|v| v.as_integer())
            .map(|v| v as u64);

        // Parse peers (compact or dictionary model)
        let peers = if let Some(peers_bytes) = dict.get("peers").and_then(|v| v.as_bytes()) {
            // Compact model: 6 bytes per peer (4 IP + 2 port)
            self.parse_compact_peers(peers_bytes)?
        } else if let Some(peers_list) = dict.get("peers").and_then(|v| v.as_list()) {
            // Dictionary model
            self.parse_dict_peers(peers_list)?
        } else {
            Vec::new()
        };

        Ok(AnnounceResponse {
            interval,
            min_interval,
            tracker_id,
            complete,
            incomplete,
            peers,
        })
    }

    fn parse_compact_peers(&self, data: &[u8]) -> Result<Vec<TrackerPeer>, TrackerError> {
        let mut peers = Vec::new();

        // Each peer is 6 bytes: 4 bytes IP + 2 bytes port
        for chunk in data.chunks(6) {
            if chunk.len() == 6 {
                let ip = Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]);
                let port = u16::from_be_bytes([chunk[4], chunk[5]]);

                peers.push(TrackerPeer {
                    peer_id: None,
                    ip: IpAddr::V4(ip),
                    port,
                });
            }
        }

        Ok(peers)
    }

    fn parse_dict_peers(
        &self,
        list: &[super::bencode::Bencode],
    ) -> Result<Vec<TrackerPeer>, TrackerError> {
        let mut peers = Vec::new();

        for peer_dict in list {
            if let Some(dict) = peer_dict.as_dict() {
                let ip_str = dict
                    .get("ip")
                    .and_then(|v| v.as_string())
                    .ok_or_else(|| TrackerError::InvalidResponse("peer missing ip".to_string()))?;

                let ip: IpAddr = ip_str.parse().map_err(|_| {
                    TrackerError::InvalidResponse(format!("invalid IP: {}", ip_str))
                })?;

                let port = dict
                    .get("port")
                    .and_then(|v| v.as_integer())
                    .ok_or_else(|| TrackerError::InvalidResponse("peer missing port".to_string()))?
                    as u16;

                let peer_id = dict
                    .get("peer id")
                    .and_then(|v| v.as_bytes())
                    .and_then(|b| {
                        if b.len() == 20 {
                            let mut id = [0u8; 20];
                            id.copy_from_slice(b);
                            Some(id)
                        } else {
                            None
                        }
                    });

                peers.push(TrackerPeer { peer_id, ip, port });
            }
        }

        Ok(peers)
    }

    /// Update transfer statistics
    pub fn update_stats(&mut self, uploaded: u64, downloaded: u64, left: u64) {
        self.uploaded = uploaded;
        self.downloaded = downloaded;
        self.left = left;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    // ── Helper: build bencode dict ─────────────────────────────────────

    fn make_bencode_dict(pairs: Vec<(&str, super::super::bencode::Bencode)>) -> Vec<u8> {
        let mut map = BTreeMap::new();
        for (k, v) in pairs {
            map.insert(k.to_string(), v);
        }
        super::super::bencode::encode(&super::super::bencode::Bencode::Dict(map))
    }

    fn bencode_int(n: i64) -> super::super::bencode::Bencode {
        super::super::bencode::Bencode::Integer(n)
    }

    fn bencode_bytes(b: Vec<u8>) -> super::super::bencode::Bencode {
        super::super::bencode::Bencode::Bytes(b)
    }

    fn bencode_str(s: &str) -> super::super::bencode::Bencode {
        super::super::bencode::Bencode::Bytes(s.as_bytes().to_vec())
    }

    fn bencode_list(items: Vec<super::super::bencode::Bencode>) -> super::super::bencode::Bencode {
        super::super::bencode::Bencode::List(items)
    }

    // ── TrackerError Display ───────────────────────────────────────────

    #[test]
    fn test_error_display_http() {
        // We can't easily construct a reqwest::Error, so just verify the variant exists
        let e = TrackerError::InvalidResponse("bad data".to_string());
        assert_eq!(format!("{}", e), "invalid tracker response: bad data");
    }

    #[test]
    fn test_error_display_server() {
        let e = TrackerError::ServerError("rate limited".to_string());
        assert_eq!(format!("{}", e), "tracker returned error: rate limited");
    }

    #[test]
    fn test_error_display_url_parse() {
        let url_err = url::Url::parse("://bad").unwrap_err();
        let e = TrackerError::from(url_err);
        let msg = format!("{}", e);
        assert!(msg.contains("URL parse error"));
    }

    #[test]
    fn test_error_display_all_variants_distinct() {
        let e1 = TrackerError::InvalidResponse("a".to_string());
        let e2 = TrackerError::ServerError("a".to_string());
        assert_ne!(format!("{}", e1), format!("{}", e2));
    }

    // ── TrackerError Debug ─────────────────────────────────────────────

    #[test]
    fn test_error_debug() {
        let e = TrackerError::ServerError("test".to_string());
        let dbg = format!("{:?}", e);
        assert!(dbg.contains("ServerError"));
    }

    // ── TrackerError From ──────────────────────────────────────────────

    #[test]
    fn test_error_from_url_parse() {
        let url_err = url::Url::parse("not a url").unwrap_err();
        let e: TrackerError = TrackerError::from(url_err);
        let msg = format!("{}", e);
        assert!(msg.contains("URL parse error"));
    }

    // ── TrackerPeer Clone/Debug ────────────────────────────────────────

    #[test]
    fn test_tracker_peer_clone() {
        let peer = TrackerPeer {
            peer_id: Some([1u8; 20]),
            ip: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            port: 8080,
        };
        let cloned = peer.clone();
        assert_eq!(cloned.port, 8080);
        assert_eq!(cloned.ip, IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)));
        assert_eq!(cloned.peer_id, Some([1u8; 20]));
    }

    #[test]
    fn test_tracker_peer_clone_none_peer_id() {
        let peer = TrackerPeer {
            peer_id: None,
            ip: IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
            port: 6881,
        };
        let cloned = peer.clone();
        assert!(cloned.peer_id.is_none());
    }

    #[test]
    fn test_tracker_peer_debug() {
        let peer = TrackerPeer {
            peer_id: None,
            ip: IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)),
            port: 80,
        };
        let dbg = format!("{:?}", peer);
        assert!(dbg.contains("TrackerPeer"));
        assert!(dbg.contains("192.168.1.1"));
    }

    // ── AnnounceResponse Clone/Debug ───────────────────────────────────

    #[test]
    fn test_announce_response_clone() {
        let resp = AnnounceResponse {
            interval: 1800,
            min_interval: Some(900),
            tracker_id: Some("abc".to_string()),
            complete: Some(10),
            incomplete: Some(5),
            peers: vec![TrackerPeer {
                peer_id: None,
                ip: IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4)),
                port: 5000,
            }],
        };
        let cloned = resp.clone();
        assert_eq!(cloned.interval, 1800);
        assert_eq!(cloned.min_interval, Some(900));
        assert_eq!(cloned.tracker_id, Some("abc".to_string()));
        assert_eq!(cloned.complete, Some(10));
        assert_eq!(cloned.incomplete, Some(5));
        assert_eq!(cloned.peers.len(), 1);
    }

    #[test]
    fn test_announce_response_debug() {
        let resp = AnnounceResponse {
            interval: 600,
            min_interval: None,
            tracker_id: None,
            complete: None,
            incomplete: None,
            peers: vec![],
        };
        let dbg = format!("{:?}", resp);
        assert!(dbg.contains("AnnounceResponse"));
        assert!(dbg.contains("600"));
    }

    // ── AnnounceEvent Clone/Copy/Debug ─────────────────────────────────

    #[test]
    fn test_announce_event_clone_copy() {
        let ev = AnnounceEvent::Started;
        let cloned = ev.clone();
        let copied = ev;
        // AnnounceEvent is Copy, so ev is still usable
        assert!(matches!(ev, AnnounceEvent::Started));
        assert!(matches!(cloned, AnnounceEvent::Started));
        assert!(matches!(copied, AnnounceEvent::Started));
    }

    #[test]
    fn test_announce_event_all_variants() {
        let variants = [
            AnnounceEvent::Started,
            AnnounceEvent::Stopped,
            AnnounceEvent::Completed,
            AnnounceEvent::None,
        ];
        // All are distinct
        for (i, a) in variants.iter().enumerate() {
            for (j, b) in variants.iter().enumerate() {
                if i == j {
                    assert!(matches!(
                        (a, b),
                        (AnnounceEvent::Started, AnnounceEvent::Started)
                            | (AnnounceEvent::Stopped, AnnounceEvent::Stopped)
                            | (AnnounceEvent::Completed, AnnounceEvent::Completed)
                            | (AnnounceEvent::None, AnnounceEvent::None)
                    ));
                }
            }
        }
    }

    #[test]
    fn test_announce_event_debug() {
        let dbg = format!("{:?}", AnnounceEvent::Completed);
        assert!(dbg.contains("Completed"));
    }

    // ── HttpTracker::new ───────────────────────────────────────────────

    #[test]
    fn test_new_basic() {
        let tracker = HttpTracker::new([0u8; 20], 6881);
        assert_eq!(tracker.port, 6881);
        assert_eq!(tracker.peer_id, [0u8; 20]);
        assert_eq!(tracker.uploaded, 0);
        assert_eq!(tracker.downloaded, 0);
        assert_eq!(tracker.left, 0);
    }

    #[test]
    fn test_new_with_non_zero_peer_id() {
        let peer_id = [42u8; 20];
        let tracker = HttpTracker::new(peer_id, 8080);
        assert_eq!(tracker.peer_id, [42u8; 20]);
        assert_eq!(tracker.port, 8080);
    }

    #[test]
    fn test_new_port_zero() {
        let tracker = HttpTracker::new([0u8; 20], 0);
        assert_eq!(tracker.port, 0);
    }

    #[test]
    fn test_new_port_max() {
        let tracker = HttpTracker::new([0u8; 20], u16::MAX);
        assert_eq!(tracker.port, u16::MAX);
    }

    // ── encode_bytes ───────────────────────────────────────────────────

    #[test]
    fn test_encode_bytes_alphanumeric() {
        let bytes = b"test";
        assert_eq!(HttpTracker::encode_bytes(bytes), "test");
    }

    #[test]
    fn test_encode_bytes_special_chars() {
        let bytes = vec![0xFF, 0x00, 0x41];
        assert_eq!(HttpTracker::encode_bytes(&bytes), "%FF%00A");
    }

    #[test]
    fn test_encode_bytes_dash_underscore_dot_tilde() {
        let bytes = b"a-b_c.d~e";
        assert_eq!(HttpTracker::encode_bytes(bytes), "a-b_c.d~e");
    }

    #[test]
    fn test_encode_bytes_empty() {
        let bytes = b"";
        assert_eq!(HttpTracker::encode_bytes(bytes), "");
    }

    #[test]
    fn test_encode_bytes_all_printable() {
        let bytes = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        assert_eq!(
            HttpTracker::encode_bytes(bytes),
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
        );
    }

    #[test]
    fn test_encode_bytes_mixed() {
        let bytes = vec![b'A', 0x00, b'Z', 0xFF];
        assert_eq!(HttpTracker::encode_bytes(&bytes), "A%00Z%FF");
    }

    #[test]
    fn test_encode_bytes_space_encoded() {
        // Space (0x20) is not alphanumeric/dash/underscore/dot/tilde
        let bytes = b"a b";
        assert_eq!(HttpTracker::encode_bytes(bytes), "a%20b");
    }

    #[test]
    fn test_encode_bytes_slash_encoded() {
        let bytes = b"a/b";
        assert_eq!(HttpTracker::encode_bytes(bytes), "a%2Fb");
    }

    // ── parse_compact_peers ────────────────────────────────────────────

    #[test]
    fn test_parse_compact_peers_basic() {
        let tracker = HttpTracker::new([0u8; 20], 6881);
        let data = vec![
            192, 168, 1, 1, 0x1A, 0xE1, // 192.168.1.1:6881
            10, 0, 0, 1, 0x1A, 0xE2, // 10.0.0.1:6882
        ];
        let peers = tracker.parse_compact_peers(&data).unwrap();
        assert_eq!(peers.len(), 2);
        assert_eq!(peers[0].ip, IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)));
        assert_eq!(peers[0].port, 6881);
        assert_eq!(peers[1].ip, IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        assert_eq!(peers[1].port, 6882);
    }

    #[test]
    fn test_parse_compact_peers_empty() {
        let tracker = HttpTracker::new([0u8; 20], 6881);
        let peers = tracker.parse_compact_peers(&[]).unwrap();
        assert_eq!(peers.len(), 0);
    }

    #[test]
    fn test_parse_compact_peers_single() {
        let tracker = HttpTracker::new([0u8; 20], 6881);
        let data = vec![127, 0, 0, 1, 0x1A, 0xE1]; // 127.0.0.1:6881
        let peers = tracker.parse_compact_peers(&data).unwrap();
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].ip, IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)));
        assert_eq!(peers[0].port, 6881);
        assert!(peers[0].peer_id.is_none());
    }

    #[test]
    fn test_parse_compact_peers_truncated_chunk() {
        // Less than 6 bytes → truncated, should be ignored
        let tracker = HttpTracker::new([0u8; 20], 6881);
        let data = vec![192, 168, 1, 1, 0x1A]; // only 5 bytes
        let peers = tracker.parse_compact_peers(&data).unwrap();
        assert_eq!(peers.len(), 0);
    }

    #[test]
    fn test_parse_compact_peers_truncated_remainder() {
        // 6 valid bytes + 3 truncated bytes → only 1 peer
        let tracker = HttpTracker::new([0u8; 20], 6881);
        let data = vec![
            10, 0, 0, 1, 0x1A, 0xE1, // valid peer
            10, 0, 0, // truncated
        ];
        let peers = tracker.parse_compact_peers(&data).unwrap();
        assert_eq!(peers.len(), 1);
    }

    #[test]
    fn test_parse_compact_peers_port_zero() {
        let tracker = HttpTracker::new([0u8; 20], 6881);
        let data = vec![10, 0, 0, 1, 0x00, 0x00]; // port 0
        let peers = tracker.parse_compact_peers(&data).unwrap();
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].port, 0);
    }

    #[test]
    fn test_parse_compact_peers_port_max() {
        let tracker = HttpTracker::new([0u8; 20], 6881);
        let data = vec![10, 0, 0, 1, 0xFF, 0xFF]; // port 65535
        let peers = tracker.parse_compact_peers(&data).unwrap();
        assert_eq!(peers[0].port, 65535);
    }

    #[test]
    fn test_parse_compact_peers_all_zeros_ip() {
        let tracker = HttpTracker::new([0u8; 20], 6881);
        let data = vec![0, 0, 0, 0, 0x1A, 0xE1]; // 0.0.0.0:6881
        let peers = tracker.parse_compact_peers(&data).unwrap();
        assert_eq!(peers[0].ip, IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)));
    }

    #[test]
    fn test_parse_compact_peers_all_255_ip() {
        let tracker = HttpTracker::new([0u8; 20], 6881);
        let data = vec![255, 255, 255, 255, 0x1A, 0xE1]; // 255.255.255.255:6881
        let peers = tracker.parse_compact_peers(&data).unwrap();
        assert_eq!(peers[0].ip, IpAddr::V4(Ipv4Addr::new(255, 255, 255, 255)));
    }

    #[test]
    fn test_parse_compact_peers_many() {
        let tracker = HttpTracker::new([0u8; 20], 6881);
        let mut data = Vec::new();
        for i in 0..50u8 {
            data.extend_from_slice(&[10, 0, 0, i, 0x1A, 0xE1]);
        }
        let peers = tracker.parse_compact_peers(&data).unwrap();
        assert_eq!(peers.len(), 50);
    }

    // ── parse_dict_peers ───────────────────────────────────────────────

    #[test]
    fn test_parse_dict_peers_basic() {
        let tracker = HttpTracker::new([0u8; 20], 6881);
        let mut peer_map = BTreeMap::new();
        peer_map.insert("ip".to_string(), bencode_str("192.168.1.1"));
        peer_map.insert("port".to_string(), bencode_int(6881));
        let list = vec![super::super::bencode::Bencode::Dict(peer_map)];
        let peers = tracker.parse_dict_peers(&list).unwrap();
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].ip, IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)));
        assert_eq!(peers[0].port, 6881);
        assert!(peers[0].peer_id.is_none());
    }

    #[test]
    fn test_parse_dict_peers_with_peer_id() {
        let tracker = HttpTracker::new([0u8; 20], 6881);
        let peer_id = [42u8; 20];
        let mut peer_map = BTreeMap::new();
        peer_map.insert("ip".to_string(), bencode_str("10.0.0.1"));
        peer_map.insert("port".to_string(), bencode_int(8080));
        peer_map.insert("peer id".to_string(), bencode_bytes(peer_id.to_vec()));
        let list = vec![super::super::bencode::Bencode::Dict(peer_map)];
        let peers = tracker.parse_dict_peers(&list).unwrap();
        assert_eq!(peers[0].peer_id, Some([42u8; 20]));
    }

    #[test]
    fn test_parse_dict_peers_short_peer_id_ignored() {
        let tracker = HttpTracker::new([0u8; 20], 6881);
        let mut peer_map = BTreeMap::new();
        peer_map.insert("ip".to_string(), bencode_str("10.0.0.1"));
        peer_map.insert("port".to_string(), bencode_int(8080));
        peer_map.insert("peer id".to_string(), bencode_bytes(vec![1, 2, 3])); // too short
        let list = vec![super::super::bencode::Bencode::Dict(peer_map)];
        let peers = tracker.parse_dict_peers(&list).unwrap();
        assert!(peers[0].peer_id.is_none());
    }

    #[test]
    fn test_parse_dict_peers_long_peer_id_ignored() {
        let tracker = HttpTracker::new([0u8; 20], 6881);
        let mut peer_map = BTreeMap::new();
        peer_map.insert("ip".to_string(), bencode_str("10.0.0.1"));
        peer_map.insert("port".to_string(), bencode_int(8080));
        peer_map.insert("peer id".to_string(), bencode_bytes(vec![0u8; 30])); // too long
        let list = vec![super::super::bencode::Bencode::Dict(peer_map)];
        let peers = tracker.parse_dict_peers(&list).unwrap();
        assert!(peers[0].peer_id.is_none());
    }

    #[test]
    fn test_parse_dict_peers_missing_ip() {
        let tracker = HttpTracker::new([0u8; 20], 6881);
        let mut peer_map = BTreeMap::new();
        peer_map.insert("port".to_string(), bencode_int(8080));
        let list = vec![super::super::bencode::Bencode::Dict(peer_map)];
        let result = tracker.parse_dict_peers(&list);
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("peer missing ip"));
    }

    #[test]
    fn test_parse_dict_peers_missing_port() {
        let tracker = HttpTracker::new([0u8; 20], 6881);
        let mut peer_map = BTreeMap::new();
        peer_map.insert("ip".to_string(), bencode_str("10.0.0.1"));
        let list = vec![super::super::bencode::Bencode::Dict(peer_map)];
        let result = tracker.parse_dict_peers(&list);
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("peer missing port"));
    }

    #[test]
    fn test_parse_dict_peers_invalid_ip() {
        let tracker = HttpTracker::new([0u8; 20], 6881);
        let mut peer_map = BTreeMap::new();
        peer_map.insert("ip".to_string(), bencode_str("not.an.ip"));
        peer_map.insert("port".to_string(), bencode_int(8080));
        let list = vec![super::super::bencode::Bencode::Dict(peer_map)];
        let result = tracker.parse_dict_peers(&list);
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("invalid IP"));
    }

    #[test]
    fn test_parse_dict_peers_multiple() {
        let tracker = HttpTracker::new([0u8; 20], 6881);
        let mut map1 = BTreeMap::new();
        map1.insert("ip".to_string(), bencode_str("1.2.3.4"));
        map1.insert("port".to_string(), bencode_int(1111));
        let mut map2 = BTreeMap::new();
        map2.insert("ip".to_string(), bencode_str("5.6.7.8"));
        map2.insert("port".to_string(), bencode_int(2222));
        let list = vec![
            super::super::bencode::Bencode::Dict(map1),
            super::super::bencode::Bencode::Dict(map2),
        ];
        let peers = tracker.parse_dict_peers(&list).unwrap();
        assert_eq!(peers.len(), 2);
        assert_eq!(peers[0].port, 1111);
        assert_eq!(peers[1].port, 2222);
    }

    #[test]
    fn test_parse_dict_peers_empty_list() {
        let tracker = HttpTracker::new([0u8; 20], 6881);
        let list: Vec<super::super::bencode::Bencode> = vec![];
        let peers = tracker.parse_dict_peers(&list).unwrap();
        assert_eq!(peers.len(), 0);
    }

    #[test]
    fn test_parse_dict_peers_non_dict_entry_skipped() {
        let tracker = HttpTracker::new([0u8; 20], 6881);
        let mut peer_map = BTreeMap::new();
        peer_map.insert("ip".to_string(), bencode_str("10.0.0.1"));
        peer_map.insert("port".to_string(), bencode_int(8080));
        let list = vec![
            super::super::bencode::Bencode::Integer(42), // not a dict, skipped
            super::super::bencode::Bencode::Dict(peer_map),
        ];
        let peers = tracker.parse_dict_peers(&list).unwrap();
        assert_eq!(peers.len(), 1); // non-dict skipped
    }

    #[test]
    fn test_parse_dict_peers_ipv6() {
        let tracker = HttpTracker::new([0u8; 20], 6881);
        let mut peer_map = BTreeMap::new();
        peer_map.insert("ip".to_string(), bencode_str("::1"));
        peer_map.insert("port".to_string(), bencode_int(8080));
        let list = vec![super::super::bencode::Bencode::Dict(peer_map)];
        let peers = tracker.parse_dict_peers(&list).unwrap();
        assert_eq!(peers[0].ip, IpAddr::V6(std::net::Ipv6Addr::LOCALHOST));
    }

    // ── parse_announce_response ────────────────────────────────────────

    #[test]
    fn test_parse_announce_response_minimal() {
        let tracker = HttpTracker::new([0u8; 20], 6881);
        let data = make_bencode_dict(vec![("interval", bencode_int(1800))]);
        let resp = tracker.parse_announce_response(&data).unwrap();
        assert_eq!(resp.interval, 1800);
        assert!(resp.min_interval.is_none());
        assert!(resp.tracker_id.is_none());
        assert!(resp.complete.is_none());
        assert!(resp.incomplete.is_none());
        assert!(resp.peers.is_empty());
    }

    #[test]
    fn test_parse_announce_response_all_fields() {
        let tracker = HttpTracker::new([0u8; 20], 6881);
        let data = make_bencode_dict(vec![
            ("interval", bencode_int(1800)),
            ("min interval", bencode_int(900)),
            ("tracker id", bencode_str("tracker-abc")),
            ("complete", bencode_int(50)),
            ("incomplete", bencode_int(10)),
        ]);
        let resp = tracker.parse_announce_response(&data).unwrap();
        assert_eq!(resp.interval, 1800);
        assert_eq!(resp.min_interval, Some(900));
        assert_eq!(resp.tracker_id, Some("tracker-abc".to_string()));
        assert_eq!(resp.complete, Some(50));
        assert_eq!(resp.incomplete, Some(10));
    }

    #[test]
    fn test_parse_announce_response_compact_peers() {
        let tracker = HttpTracker::new([0u8; 20], 6881);
        let peer_bytes = vec![
            10, 0, 0, 1, 0x1A, 0xE1, // 10.0.0.1:6881
            192, 168, 0, 1, 0x1F, 0x90, // 192.168.0.1:8080
        ];
        let data = make_bencode_dict(vec![
            ("interval", bencode_int(1800)),
            ("peers", bencode_bytes(peer_bytes)),
        ]);
        let resp = tracker.parse_announce_response(&data).unwrap();
        assert_eq!(resp.peers.len(), 2);
        assert_eq!(resp.peers[0].ip, IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        assert_eq!(resp.peers[1].ip, IpAddr::V4(Ipv4Addr::new(192, 168, 0, 1)));
    }

    #[test]
    fn test_parse_announce_response_dict_peers() {
        let tracker = HttpTracker::new([0u8; 20], 6881);
        let mut peer_map = BTreeMap::new();
        peer_map.insert("ip".to_string(), bencode_str("172.16.0.1"));
        peer_map.insert("port".to_string(), bencode_int(51413));
        let peers_list = bencode_list(vec![super::super::bencode::Bencode::Dict(peer_map)]);
        let data = make_bencode_dict(vec![("interval", bencode_int(1800)), ("peers", peers_list)]);
        let resp = tracker.parse_announce_response(&data).unwrap();
        assert_eq!(resp.peers.len(), 1);
        assert_eq!(resp.peers[0].ip, IpAddr::V4(Ipv4Addr::new(172, 16, 0, 1)));
        assert_eq!(resp.peers[0].port, 51413);
    }

    #[test]
    fn test_parse_announce_response_failure_reason() {
        let tracker = HttpTracker::new([0u8; 20], 6881);
        let data = make_bencode_dict(vec![("failure reason", bencode_str("torrent not found"))]);
        let result = tracker.parse_announce_response(&data);
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("torrent not found"));
    }

    #[test]
    fn test_parse_announce_response_missing_interval() {
        let tracker = HttpTracker::new([0u8; 20], 6881);
        let data = make_bencode_dict(vec![("complete", bencode_int(10))]);
        let result = tracker.parse_announce_response(&data);
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("missing interval"));
    }

    #[test]
    fn test_parse_announce_response_not_dict() {
        let tracker = HttpTracker::new([0u8; 20], 6881);
        let data = super::super::bencode::encode(&bencode_int(42));
        let result = tracker.parse_announce_response(&data);
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("not a dictionary"));
    }

    #[test]
    fn test_parse_announce_response_invalid_bencode() {
        let tracker = HttpTracker::new([0u8; 20], 6881);
        let result = tracker.parse_announce_response(b"garbage");
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("bencode error"));
    }

    #[test]
    fn test_parse_announce_response_empty_peers_bytes() {
        let tracker = HttpTracker::new([0u8; 20], 6881);
        let data = make_bencode_dict(vec![
            ("interval", bencode_int(1800)),
            ("peers", bencode_bytes(vec![])),
        ]);
        let resp = tracker.parse_announce_response(&data).unwrap();
        assert!(resp.peers.is_empty());
    }

    #[test]
    fn test_parse_announce_response_empty_peers_list() {
        let tracker = HttpTracker::new([0u8; 20], 6881);
        let data = make_bencode_dict(vec![
            ("interval", bencode_int(1800)),
            ("peers", bencode_list(vec![])),
        ]);
        let resp = tracker.parse_announce_response(&data).unwrap();
        assert!(resp.peers.is_empty());
    }

    #[test]
    fn test_parse_announce_response_interval_zero() {
        let tracker = HttpTracker::new([0u8; 20], 6881);
        let data = make_bencode_dict(vec![("interval", bencode_int(0))]);
        let resp = tracker.parse_announce_response(&data).unwrap();
        assert_eq!(resp.interval, 0);
    }

    #[test]
    fn test_parse_announce_response_large_interval() {
        let tracker = HttpTracker::new([0u8; 20], 6881);
        let data = make_bencode_dict(vec![("interval", bencode_int(u64::MAX as i64))]);
        let resp = tracker.parse_announce_response(&data).unwrap();
        // Since i64 → u64 cast, large i64 values get cast
        assert!(resp.interval > 0);
    }

    #[test]
    fn test_parse_announce_response_unicode_failure_reason() {
        let tracker = HttpTracker::new([0u8; 20], 6881);
        let data = make_bencode_dict(vec![("failure reason", bencode_str("种子不存在"))]);
        let result = tracker.parse_announce_response(&data);
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("种子不存在"));
    }

    // ── update_stats ───────────────────────────────────────────────────

    #[test]
    fn test_update_stats() {
        let mut tracker = HttpTracker::new([0u8; 20], 6881);
        tracker.update_stats(1000, 2000, 3000);
        assert_eq!(tracker.uploaded, 1000);
        assert_eq!(tracker.downloaded, 2000);
        assert_eq!(tracker.left, 3000);
    }

    #[test]
    fn test_update_stats_zero() {
        let mut tracker = HttpTracker::new([0u8; 20], 6881);
        tracker.update_stats(0, 0, 0);
        assert_eq!(tracker.uploaded, 0);
        assert_eq!(tracker.downloaded, 0);
        assert_eq!(tracker.left, 0);
    }

    #[test]
    fn test_update_stats_large_values() {
        let mut tracker = HttpTracker::new([0u8; 20], 6881);
        tracker.update_stats(u64::MAX, u64::MAX, u64::MAX);
        assert_eq!(tracker.uploaded, u64::MAX);
        assert_eq!(tracker.downloaded, u64::MAX);
        assert_eq!(tracker.left, u64::MAX);
    }

    #[test]
    fn test_update_stats_multiple_calls() {
        let mut tracker = HttpTracker::new([0u8; 20], 6881);
        tracker.update_stats(100, 200, 300);
        tracker.update_stats(500, 600, 700);
        assert_eq!(tracker.uploaded, 500);
        assert_eq!(tracker.downloaded, 600);
        assert_eq!(tracker.left, 700);
    }

    #[test]
    fn test_update_stats_overwrite() {
        let mut tracker = HttpTracker::new([0u8; 20], 6881);
        tracker.update_stats(100, 200, 300);
        tracker.update_stats(0, 0, 0);
        assert_eq!(tracker.uploaded, 0);
        assert_eq!(tracker.downloaded, 0);
        assert_eq!(tracker.left, 0);
    }

    // ── Integration / lifecycle ────────────────────────────────────────

    #[test]
    fn test_lifecycle_new_update_parse() {
        let mut tracker = HttpTracker::new([1u8; 20], 6881);
        tracker.update_stats(1024, 2048, 4096);

        // Build a valid response
        let peer_bytes = vec![10, 0, 0, 1, 0x1A, 0xE1];
        let data = make_bencode_dict(vec![
            ("interval", bencode_int(1800)),
            ("min interval", bencode_int(900)),
            ("complete", bencode_int(5)),
            ("incomplete", bencode_int(2)),
            ("peers", bencode_bytes(peer_bytes)),
        ]);
        let resp = tracker.parse_announce_response(&data).unwrap();
        assert_eq!(resp.interval, 1800);
        assert_eq!(resp.peers.len(), 1);
        assert_eq!(tracker.uploaded, 1024);
    }

    #[test]
    fn test_multiple_trackers_independent() {
        let t1 = HttpTracker::new([1u8; 20], 6881);
        let t2 = HttpTracker::new([2u8; 20], 6882);
        assert_eq!(t1.peer_id, [1u8; 20]);
        assert_eq!(t2.peer_id, [2u8; 20]);
        assert_eq!(t1.port, 6881);
        assert_eq!(t2.port, 6882);
    }

    // ── Unicode ────────────────────────────────────────────────────────

    #[test]
    fn test_encode_bytes_unicode_not_special() {
        // Non-ASCII bytes should be percent-encoded
        let bytes = "你好".as_bytes();
        let encoded = HttpTracker::encode_bytes(bytes);
        assert!(encoded.contains('%'));
        assert!(!encoded.contains("你"));
    }

    #[test]
    fn test_tracker_peer_debug_unicode() {
        let peer = TrackerPeer {
            peer_id: None,
            ip: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            port: 80,
        };
        let dbg = format!("{:?}", peer);
        assert!(dbg.contains("TrackerPeer"));
    }

    // ── Edge cases ─────────────────────────────────────────────────────

    #[test]
    fn test_parse_announce_response_no_peers_key() {
        let tracker = HttpTracker::new([0u8; 20], 6881);
        let data = make_bencode_dict(vec![
            ("interval", bencode_int(1800)),
            ("complete", bencode_int(5)),
        ]);
        let resp = tracker.parse_announce_response(&data).unwrap();
        assert!(resp.peers.is_empty());
        assert_eq!(resp.complete, Some(5));
    }

    #[test]
    fn test_parse_announce_response_compact_empty_peers() {
        let tracker = HttpTracker::new([0u8; 20], 6881);
        let data = make_bencode_dict(vec![
            ("interval", bencode_int(600)),
            ("peers", bencode_bytes(vec![])),
        ]);
        let resp = tracker.parse_announce_response(&data).unwrap();
        assert_eq!(resp.peers.len(), 0);
    }

    #[test]
    fn test_parse_compact_peers_exactly_6_bytes() {
        let tracker = HttpTracker::new([0u8; 20], 6881);
        let data = vec![1, 2, 3, 4, 0x00, 0x50]; // 1.2.3.4:80
        let peers = tracker.parse_compact_peers(&data).unwrap();
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].ip, IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4)));
        assert_eq!(peers[0].port, 80);
    }

    #[test]
    fn test_parse_compact_peers_7_bytes_one_full_one_truncated() {
        let tracker = HttpTracker::new([0u8; 20], 6881);
        let data = vec![1, 2, 3, 4, 0x00, 0x50, 0xFF]; // 1 full + 1 byte leftover
        let peers = tracker.parse_compact_peers(&data).unwrap();
        assert_eq!(peers.len(), 1);
    }

    #[test]
    fn test_parse_compact_peers_12_bytes_two_peers() {
        let tracker = HttpTracker::new([0u8; 20], 6881);
        let data = vec![
            1, 2, 3, 4, 0x00, 0x50, // peer 1
            5, 6, 7, 8, 0x00, 0x51, // peer 2
        ];
        let peers = tracker.parse_compact_peers(&data).unwrap();
        assert_eq!(peers.len(), 2);
        assert_eq!(peers[0].port, 80);
        assert_eq!(peers[1].port, 81);
    }

    #[test]
    fn test_parse_dict_peers_exact_20_byte_peer_id() {
        let tracker = HttpTracker::new([0u8; 20], 6881);
        let peer_id = [0xAB; 20];
        let mut peer_map = BTreeMap::new();
        peer_map.insert("ip".to_string(), bencode_str("10.0.0.1"));
        peer_map.insert("port".to_string(), bencode_int(8080));
        peer_map.insert("peer id".to_string(), bencode_bytes(peer_id.to_vec()));
        let list = vec![super::super::bencode::Bencode::Dict(peer_map)];
        let peers = tracker.parse_dict_peers(&list).unwrap();
        assert_eq!(peers[0].peer_id, Some([0xAB; 20]));
    }

    #[test]
    fn test_parse_dict_peers_zero_length_peer_id() {
        let tracker = HttpTracker::new([0u8; 20], 6881);
        let mut peer_map = BTreeMap::new();
        peer_map.insert("ip".to_string(), bencode_str("10.0.0.1"));
        peer_map.insert("port".to_string(), bencode_int(8080));
        peer_map.insert("peer id".to_string(), bencode_bytes(vec![])); // 0 bytes
        let list = vec![super::super::bencode::Bencode::Dict(peer_map)];
        let peers = tracker.parse_dict_peers(&list).unwrap();
        assert!(peers[0].peer_id.is_none()); // not exactly 20
    }

    #[test]
    fn test_parse_announce_response_extra_fields_ignored() {
        let tracker = HttpTracker::new([0u8; 20], 6881);
        let data = make_bencode_dict(vec![
            ("interval", bencode_int(1800)),
            ("extra_field", bencode_str("ignored")),
            ("another", bencode_int(999)),
        ]);
        let resp = tracker.parse_announce_response(&data).unwrap();
        assert_eq!(resp.interval, 1800);
    }

    #[test]
    fn test_parse_announce_response_peers_wrong_type_ignored() {
        // peers as integer (wrong type) → should be treated as no peers
        let tracker = HttpTracker::new([0u8; 20], 6881);
        let data = make_bencode_dict(vec![
            ("interval", bencode_int(1800)),
            ("peers", bencode_int(42)), // wrong type
        ]);
        let resp = tracker.parse_announce_response(&data).unwrap();
        assert!(resp.peers.is_empty());
    }
}
