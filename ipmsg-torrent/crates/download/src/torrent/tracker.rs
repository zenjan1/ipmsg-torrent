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

    #[test]
    fn test_encode_bytes() {
        let bytes = b"test";
        let encoded = HttpTracker::encode_bytes(bytes);
        assert_eq!(encoded, "test");

        let bytes = vec![0xFF, 0x00, 0x41];
        let encoded = HttpTracker::encode_bytes(&bytes);
        assert_eq!(encoded, "%FF%00A");
    }

    #[test]
    fn test_parse_compact_peers() {
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
}
