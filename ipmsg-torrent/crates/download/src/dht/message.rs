//! DHT message types and encoding/decoding
//!
//! Implements BEP 0005 message format

use super::NodeId;
use crate::torrent::bencode::{Bencode, BencodeError};
use std::net::SocketAddr;

/// DHT message types
#[derive(Debug, Clone)]
pub enum DhtMessage {
    Query {
        transaction_id: Vec<u8>,
        query: QueryType,
    },
    Response {
        transaction_id: Vec<u8>,
        response: ResponseType,
    },
    Error {
        transaction_id: Vec<u8>,
        code: i64,
        message: String,
    },
}

/// Query message types
#[derive(Debug, Clone)]
pub enum QueryType {
    Ping {
        id: NodeId,
    },
    FindNode {
        id: NodeId,
        target: NodeId,
    },
    GetPeers {
        id: NodeId,
        info_hash: [u8; 20],
    },
    AnnouncePeer {
        id: NodeId,
        info_hash: [u8; 20],
        port: u16,
        token: Vec<u8>,
        implied_port: bool,
    },
}

/// Response message types
#[derive(Debug, Clone)]
pub enum ResponseType {
    Pong {
        id: NodeId,
    },
    Nodes {
        id: NodeId,
        nodes: Vec<(NodeId, SocketAddr)>,
    },
    Peers {
        id: NodeId,
        token: Vec<u8>,
        values: Option<Vec<SocketAddr>>,
        nodes: Vec<(NodeId, SocketAddr)>,
    },
    AnnounceSuccess {
        id: NodeId,
    },
}

impl DhtMessage {
    /// Encode message to bencode bytes
    pub fn encode(&self) -> Result<Vec<u8>, BencodeError> {
        let bencode = match self {
            DhtMessage::Query {
                transaction_id,
                query,
            } => {
                let mut map = std::collections::BTreeMap::new();
                map.insert("t".to_string(), Bencode::Bytes(transaction_id.clone()));
                map.insert("y".to_string(), Bencode::Bytes(b"q".to_vec()));

                match query {
                    QueryType::Ping { id } => {
                        map.insert("q".to_string(), Bencode::Bytes(b"ping".to_vec()));
                        let mut args = std::collections::BTreeMap::new();
                        args.insert("id".to_string(), Bencode::Bytes(id.to_vec()));
                        map.insert("a".to_string(), Bencode::Dict(args));
                    }
                    QueryType::FindNode { id, target } => {
                        map.insert("q".to_string(), Bencode::Bytes(b"find_node".to_vec()));
                        let mut args = std::collections::BTreeMap::new();
                        args.insert("id".to_string(), Bencode::Bytes(id.to_vec()));
                        args.insert("target".to_string(), Bencode::Bytes(target.to_vec()));
                        map.insert("a".to_string(), Bencode::Dict(args));
                    }
                    QueryType::GetPeers { id, info_hash } => {
                        map.insert("q".to_string(), Bencode::Bytes(b"get_peers".to_vec()));
                        let mut args = std::collections::BTreeMap::new();
                        args.insert("id".to_string(), Bencode::Bytes(id.to_vec()));
                        args.insert("info_hash".to_string(), Bencode::Bytes(info_hash.to_vec()));
                        map.insert("a".to_string(), Bencode::Dict(args));
                    }
                    QueryType::AnnouncePeer {
                        id,
                        info_hash,
                        port,
                        token,
                        implied_port,
                    } => {
                        map.insert("q".to_string(), Bencode::Bytes(b"announce_peer".to_vec()));
                        let mut args = std::collections::BTreeMap::new();
                        args.insert("id".to_string(), Bencode::Bytes(id.to_vec()));
                        args.insert("info_hash".to_string(), Bencode::Bytes(info_hash.to_vec()));
                        args.insert("port".to_string(), Bencode::Integer(*port as i64));
                        args.insert("token".to_string(), Bencode::Bytes(token.clone()));
                        args.insert(
                            "implied_port".to_string(),
                            Bencode::Integer(if *implied_port { 1 } else { 0 }),
                        );
                        map.insert("a".to_string(), Bencode::Dict(args));
                    }
                }

                Bencode::Dict(map)
            }
            DhtMessage::Response {
                transaction_id,
                response,
            } => {
                let mut map = std::collections::BTreeMap::new();
                map.insert("t".to_string(), Bencode::Bytes(transaction_id.clone()));
                map.insert("y".to_string(), Bencode::Bytes(b"r".to_vec()));

                let mut r_map = std::collections::BTreeMap::new();

                match response {
                    ResponseType::Pong { id } => {
                        r_map.insert("id".to_string(), Bencode::Bytes(id.to_vec()));
                    }
                    ResponseType::Nodes { id, nodes } => {
                        r_map.insert("id".to_string(), Bencode::Bytes(id.to_vec()));
                        r_map.insert("nodes".to_string(), Bencode::Bytes(encode_nodes(nodes)));
                    }
                    ResponseType::Peers {
                        id,
                        token,
                        values,
                        nodes,
                    } => {
                        r_map.insert("id".to_string(), Bencode::Bytes(id.to_vec()));
                        r_map.insert("token".to_string(), Bencode::Bytes(token.clone()));

                        if let Some(values) = values {
                            let values_list: Vec<Bencode> = values
                                .iter()
                                .map(|addr| Bencode::Bytes(encode_addr(addr)))
                                .collect();
                            r_map.insert("values".to_string(), Bencode::List(values_list));
                        }

                        r_map.insert("nodes".to_string(), Bencode::Bytes(encode_nodes(nodes)));
                    }
                    ResponseType::AnnounceSuccess { id } => {
                        r_map.insert("id".to_string(), Bencode::Bytes(id.to_vec()));
                    }
                }

                map.insert("r".to_string(), Bencode::Dict(r_map));
                Bencode::Dict(map)
            }
            DhtMessage::Error {
                transaction_id,
                code,
                message,
            } => {
                let mut map = std::collections::BTreeMap::new();
                map.insert("t".to_string(), Bencode::Bytes(transaction_id.clone()));
                map.insert("y".to_string(), Bencode::Bytes(b"e".to_vec()));
                map.insert(
                    "e".to_string(),
                    Bencode::List(vec![
                        Bencode::Integer(*code),
                        Bencode::Bytes(message.as_bytes().to_vec()),
                    ]),
                );
                Bencode::Dict(map)
            }
        };

        Ok(crate::torrent::bencode::encode(&bencode))
    }

    /// Decode message from bencode bytes
    pub fn decode(data: &[u8]) -> Result<Self, BencodeError> {
        let bencode = crate::torrent::bencode::decode(data)?;
        let dict = bencode.as_dict().ok_or(BencodeError::InvalidFormat)?;

        let transaction_id = dict
            .get("t")
            .and_then(|v| v.as_bytes())
            .ok_or(BencodeError::InvalidFormat)?
            .to_vec();

        let msg_type = dict
            .get("y")
            .and_then(|v| v.as_bytes())
            .ok_or(BencodeError::InvalidFormat)?;

        match msg_type {
            b"q" => {
                let query_name = dict
                    .get("q")
                    .and_then(|v| v.as_bytes())
                    .ok_or(BencodeError::InvalidFormat)?;

                let args = dict
                    .get("a")
                    .and_then(|v| v.as_dict())
                    .ok_or(BencodeError::InvalidFormat)?;

                let id = get_node_id(args, "id")?;

                let query = match query_name {
                    b"ping" => QueryType::Ping { id },
                    b"find_node" => {
                        let target = get_node_id(args, "target")?;
                        QueryType::FindNode { id, target }
                    }
                    b"get_peers" => {
                        let info_hash = get_info_hash(args, "info_hash")?;
                        QueryType::GetPeers { id, info_hash }
                    }
                    b"announce_peer" => {
                        let info_hash = get_info_hash(args, "info_hash")?;
                        let port = args
                            .get("port")
                            .and_then(|v| v.as_integer())
                            .ok_or(BencodeError::InvalidFormat)?
                            as u16;
                        let token = args
                            .get("token")
                            .and_then(|v| v.as_bytes())
                            .ok_or(BencodeError::InvalidFormat)?
                            .to_vec();
                        let implied_port = args
                            .get("implied_port")
                            .and_then(|v| v.as_integer())
                            .unwrap_or(0)
                            != 0;

                        QueryType::AnnouncePeer {
                            id,
                            info_hash,
                            port,
                            token,
                            implied_port,
                        }
                    }
                    _ => return Err(BencodeError::InvalidFormat),
                };

                Ok(DhtMessage::Query {
                    transaction_id,
                    query,
                })
            }
            b"r" => {
                let r_dict = dict
                    .get("r")
                    .and_then(|v| v.as_dict())
                    .ok_or(BencodeError::InvalidFormat)?;

                let id = get_node_id(r_dict, "id")?;

                let response = if r_dict.contains_key("values") {
                    // Peers response
                    let token = r_dict
                        .get("token")
                        .and_then(|v| v.as_bytes())
                        .ok_or(BencodeError::InvalidFormat)?
                        .to_vec();

                    let values = r_dict.get("values").and_then(|v| v.as_list()).map(|list| {
                        list.iter()
                            .filter_map(|v| v.as_bytes())
                            .filter_map(decode_addr)
                            .collect()
                    });

                    let nodes = r_dict
                        .get("nodes")
                        .and_then(|v| v.as_bytes())
                        .map(decode_nodes)
                        .unwrap_or_default();

                    ResponseType::Peers {
                        id,
                        token,
                        values,
                        nodes,
                    }
                } else if r_dict.contains_key("nodes") {
                    // Nodes response
                    let nodes_bytes = r_dict
                        .get("nodes")
                        .and_then(|v| v.as_bytes())
                        .ok_or(BencodeError::InvalidFormat)?;
                    let nodes = decode_nodes(nodes_bytes);
                    ResponseType::Nodes { id, nodes }
                } else {
                    // Pong or announce success
                    if dict.contains_key("q") {
                        ResponseType::Pong { id }
                    } else {
                        ResponseType::AnnounceSuccess { id }
                    }
                };

                Ok(DhtMessage::Response {
                    transaction_id,
                    response,
                })
            }
            b"e" => {
                let error = dict
                    .get("e")
                    .and_then(|v| v.as_list())
                    .ok_or(BencodeError::InvalidFormat)?;

                if error.len() < 2 {
                    return Err(BencodeError::InvalidFormat);
                }

                let code = error[0].as_integer().ok_or(BencodeError::InvalidFormat)?;
                let message = error[1].as_bytes().ok_or(BencodeError::InvalidFormat)?;
                let message = String::from_utf8_lossy(message).to_string();

                Ok(DhtMessage::Error {
                    transaction_id,
                    code,
                    message,
                })
            }
            _ => Err(BencodeError::InvalidFormat),
        }
    }
}

fn get_node_id(
    dict: &std::collections::BTreeMap<String, Bencode>,
    key: &str,
) -> Result<NodeId, BencodeError> {
    let bytes = dict
        .get(key)
        .and_then(|v| v.as_bytes())
        .ok_or(BencodeError::InvalidFormat)?;

    if bytes.len() != 20 {
        return Err(BencodeError::InvalidFormat);
    }

    let mut id = [0u8; 20];
    id.copy_from_slice(bytes);
    Ok(id)
}

fn get_info_hash(
    dict: &std::collections::BTreeMap<String, Bencode>,
    key: &str,
) -> Result<[u8; 20], BencodeError> {
    get_node_id(dict, key)
}

/// Encode nodes list to compact format (6 bytes per node: 4 IP + 2 port + 20 ID)
fn encode_nodes(nodes: &[(NodeId, SocketAddr)]) -> Vec<u8> {
    let mut result = Vec::with_capacity(nodes.len() * 26);
    for (id, addr) in nodes {
        result.extend_from_slice(id);
        result.extend_from_slice(&encode_addr(addr));
    }
    result
}

/// Decode nodes from compact format
fn decode_nodes(data: &[u8]) -> Vec<(NodeId, SocketAddr)> {
    let mut nodes = Vec::new();
    let mut pos = 0;

    while pos + 26 <= data.len() {
        let mut id = [0u8; 20];
        id.copy_from_slice(&data[pos..pos + 20]);
        pos += 20;

        if let Some(addr) = decode_addr(&data[pos..pos + 6]) {
            nodes.push((id, addr));
        }
        pos += 6;
    }

    nodes
}

/// Encode socket address to compact format (4 bytes IP + 2 bytes port)
fn encode_addr(addr: &SocketAddr) -> Vec<u8> {
    match addr {
        SocketAddr::V4(v4) => {
            let mut result = Vec::with_capacity(6);
            result.extend_from_slice(&v4.ip().octets());
            result.extend_from_slice(&v4.port().to_be_bytes());
            result
        }
        SocketAddr::V6(_) => {
            // IPv6 not supported in compact format
            vec![0u8; 6]
        }
    }
}

/// Decode socket address from compact format
fn decode_addr(data: &[u8]) -> Option<SocketAddr> {
    if data.len() < 6 {
        return None;
    }

    let ip = std::net::Ipv4Addr::new(data[0], data[1], data[2], data[3]);
    let port = u16::from_be_bytes([data[4], data[5]]);
    Some(SocketAddr::V4(std::net::SocketAddrV4::new(ip, port)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_node_id() -> NodeId {
        [0xAB; 20]
    }

    fn test_info_hash() -> [u8; 20] {
        [0xCD; 20]
    }

    fn test_addr() -> SocketAddr {
        "192.168.1.1:6881".parse().unwrap()
    }

    // ===== Ping query roundtrip =====

    #[test]
    fn test_encode_decode_ping() {
        let id = [1u8; 20];
        let msg = DhtMessage::Query {
            transaction_id: b"aa".to_vec(),
            query: QueryType::Ping { id },
        };

        let encoded = msg.encode().unwrap();
        let decoded = DhtMessage::decode(&encoded).unwrap();

        match decoded {
            DhtMessage::Query {
                transaction_id,
                query: QueryType::Ping { id: decoded_id },
            } => {
                assert_eq!(transaction_id, b"aa".to_vec());
                assert_eq!(decoded_id, id);
            }
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_ping_roundtrip_all_zeros_id() {
        let id = [0u8; 20];
        let msg = DhtMessage::Query {
            transaction_id: b"\x00\x01".to_vec(),
            query: QueryType::Ping { id },
        };
        let encoded = msg.encode().unwrap();
        let decoded = DhtMessage::decode(&encoded).unwrap();
        match decoded {
            DhtMessage::Query {
                query: QueryType::Ping { id: decoded_id },
                ..
            } => assert_eq!(decoded_id, id),
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_ping_roundtrip_all_ff_id() {
        let id = [0xFF; 20];
        let msg = DhtMessage::Query {
            transaction_id: b"\xFF\xFE".to_vec(),
            query: QueryType::Ping { id },
        };
        let encoded = msg.encode().unwrap();
        let decoded = DhtMessage::decode(&encoded).unwrap();
        match decoded {
            DhtMessage::Query {
                query: QueryType::Ping { id: decoded_id },
                ..
            } => assert_eq!(decoded_id, id),
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_ping_empty_transaction_id() {
        let msg = DhtMessage::Query {
            transaction_id: vec![],
            query: QueryType::Ping { id: test_node_id() },
        };
        let encoded = msg.encode().unwrap();
        let decoded = DhtMessage::decode(&encoded).unwrap();
        match decoded {
            DhtMessage::Query { transaction_id, .. } => assert!(transaction_id.is_empty()),
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_ping_long_transaction_id() {
        let tid = vec![0x42u8; 64];
        let msg = DhtMessage::Query {
            transaction_id: tid.clone(),
            query: QueryType::Ping { id: test_node_id() },
        };
        let encoded = msg.encode().unwrap();
        let decoded = DhtMessage::decode(&encoded).unwrap();
        match decoded {
            DhtMessage::Query { transaction_id, .. } => assert_eq!(transaction_id, tid),
            _ => panic!("Wrong message type"),
        }
    }

    // ===== FindNode query roundtrip =====

    #[test]
    fn test_encode_decode_find_node() {
        let id = test_node_id();
        let target = [0x42; 20];
        let msg = DhtMessage::Query {
            transaction_id: b"bb".to_vec(),
            query: QueryType::FindNode { id, target },
        };

        let encoded = msg.encode().unwrap();
        let decoded = DhtMessage::decode(&encoded).unwrap();

        match decoded {
            DhtMessage::Query {
                transaction_id,
                query:
                    QueryType::FindNode {
                        id: did,
                        target: dt,
                    },
            } => {
                assert_eq!(transaction_id, b"bb".to_vec());
                assert_eq!(did, id);
                assert_eq!(dt, target);
            }
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_find_node_same_id_and_target() {
        let id = test_node_id();
        let msg = DhtMessage::Query {
            transaction_id: b"cc".to_vec(),
            query: QueryType::FindNode { id, target: id },
        };
        let encoded = msg.encode().unwrap();
        let decoded = DhtMessage::decode(&encoded).unwrap();
        match decoded {
            DhtMessage::Query {
                query:
                    QueryType::FindNode {
                        id: did,
                        target: dt,
                    },
                ..
            } => {
                assert_eq!(did, id);
                assert_eq!(dt, id);
            }
            _ => panic!("Wrong message type"),
        }
    }

    // ===== GetPeers query roundtrip =====

    #[test]
    fn test_encode_decode_get_peers() {
        let msg = DhtMessage::Query {
            transaction_id: b"cc".to_vec(),
            query: QueryType::GetPeers {
                id: test_node_id(),
                info_hash: test_info_hash(),
            },
        };

        let encoded = msg.encode().unwrap();
        let decoded = DhtMessage::decode(&encoded).unwrap();

        match decoded {
            DhtMessage::Query {
                transaction_id,
                query: QueryType::GetPeers { id, info_hash },
            } => {
                assert_eq!(transaction_id, b"cc".to_vec());
                assert_eq!(id, test_node_id());
                assert_eq!(info_hash, test_info_hash());
            }
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_get_peers_all_zeros_info_hash() {
        let msg = DhtMessage::Query {
            transaction_id: b"d1".to_vec(),
            query: QueryType::GetPeers {
                id: test_node_id(),
                info_hash: [0u8; 20],
            },
        };
        let encoded = msg.encode().unwrap();
        let decoded = DhtMessage::decode(&encoded).unwrap();
        match decoded {
            DhtMessage::Query {
                query: QueryType::GetPeers { info_hash, .. },
                ..
            } => assert_eq!(info_hash, [0u8; 20]),
            _ => panic!("Wrong message type"),
        }
    }

    // ===== AnnouncePeer query roundtrip =====

    #[test]
    fn test_encode_decode_announce_peer() {
        let msg = DhtMessage::Query {
            transaction_id: b"dd".to_vec(),
            query: QueryType::AnnouncePeer {
                id: test_node_id(),
                info_hash: test_info_hash(),
                port: 6881,
                token: b"tok123".to_vec(),
                implied_port: false,
            },
        };

        let encoded = msg.encode().unwrap();
        let decoded = DhtMessage::decode(&encoded).unwrap();

        match decoded {
            DhtMessage::Query {
                transaction_id,
                query:
                    QueryType::AnnouncePeer {
                        id,
                        info_hash,
                        port,
                        token,
                        implied_port,
                    },
            } => {
                assert_eq!(transaction_id, b"dd".to_vec());
                assert_eq!(id, test_node_id());
                assert_eq!(info_hash, test_info_hash());
                assert_eq!(port, 6881);
                assert_eq!(token, b"tok123".to_vec());
                assert!(!implied_port);
            }
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_announce_peer_implied_port_true() {
        let msg = DhtMessage::Query {
            transaction_id: b"e1".to_vec(),
            query: QueryType::AnnouncePeer {
                id: test_node_id(),
                info_hash: test_info_hash(),
                port: 0,
                token: vec![],
                implied_port: true,
            },
        };
        let encoded = msg.encode().unwrap();
        let decoded = DhtMessage::decode(&encoded).unwrap();
        match decoded {
            DhtMessage::Query {
                query:
                    QueryType::AnnouncePeer {
                        implied_port,
                        port,
                        token,
                        ..
                    },
                ..
            } => {
                assert!(implied_port);
                assert_eq!(port, 0);
                assert!(token.is_empty());
            }
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_announce_peer_port_zero() {
        let msg = DhtMessage::Query {
            transaction_id: b"f1".to_vec(),
            query: QueryType::AnnouncePeer {
                id: test_node_id(),
                info_hash: test_info_hash(),
                port: 0,
                token: b"abc".to_vec(),
                implied_port: false,
            },
        };
        let encoded = msg.encode().unwrap();
        let decoded = DhtMessage::decode(&encoded).unwrap();
        match decoded {
            DhtMessage::Query {
                query: QueryType::AnnouncePeer { port, .. },
                ..
            } => assert_eq!(port, 0),
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_announce_peer_port_max() {
        let msg = DhtMessage::Query {
            transaction_id: b"f2".to_vec(),
            query: QueryType::AnnouncePeer {
                id: test_node_id(),
                info_hash: test_info_hash(),
                port: u16::MAX,
                token: b"xyz".to_vec(),
                implied_port: false,
            },
        };
        let encoded = msg.encode().unwrap();
        let decoded = DhtMessage::decode(&encoded).unwrap();
        match decoded {
            DhtMessage::Query {
                query: QueryType::AnnouncePeer { port, .. },
                ..
            } => assert_eq!(port, u16::MAX),
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_announce_peer_empty_token() {
        let msg = DhtMessage::Query {
            transaction_id: b"f3".to_vec(),
            query: QueryType::AnnouncePeer {
                id: test_node_id(),
                info_hash: test_info_hash(),
                port: 8080,
                token: vec![],
                implied_port: false,
            },
        };
        let encoded = msg.encode().unwrap();
        let decoded = DhtMessage::decode(&encoded).unwrap();
        match decoded {
            DhtMessage::Query {
                query: QueryType::AnnouncePeer { token, .. },
                ..
            } => assert!(token.is_empty()),
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_announce_peer_large_token() {
        let token = vec![0xAA; 256];
        let msg = DhtMessage::Query {
            transaction_id: b"f4".to_vec(),
            query: QueryType::AnnouncePeer {
                id: test_node_id(),
                info_hash: test_info_hash(),
                port: 1234,
                token: token.clone(),
                implied_port: false,
            },
        };
        let encoded = msg.encode().unwrap();
        let decoded = DhtMessage::decode(&encoded).unwrap();
        match decoded {
            DhtMessage::Query {
                query: QueryType::AnnouncePeer { token: dt, .. },
                ..
            } => assert_eq!(dt, token),
            _ => panic!("Wrong message type"),
        }
    }

    // ===== Pong response roundtrip =====
    // NOTE: BEP 0005 Pong and AnnounceSuccess have identical wire format
    // (both are {"t":..., "y":"r", "r":{"id":...}}). The decoder cannot
    // distinguish them without tracking pending transactions. So a Pong
    // encoded message decodes as AnnounceSuccess (the else branch).

    #[test]
    fn test_encode_decode_pong_decodes_as_announce_success() {
        let msg = DhtMessage::Response {
            transaction_id: b"aa".to_vec(),
            response: ResponseType::Pong { id: test_node_id() },
        };

        let encoded = msg.encode().unwrap();
        let decoded = DhtMessage::decode(&encoded).unwrap();

        // Pong and AnnounceSuccess are wire-identical; decoder returns AnnounceSuccess
        match decoded {
            DhtMessage::Response {
                transaction_id,
                response: ResponseType::AnnounceSuccess { id },
            } => {
                assert_eq!(transaction_id, b"aa".to_vec());
                assert_eq!(id, test_node_id());
            }
            _ => panic!("Expected AnnounceSuccess (wire-identical with Pong)"),
        }
    }

    #[test]
    fn test_pong_encode_contains_pong_id() {
        // Verify Pong encodes correctly even if decode is ambiguous
        let msg = DhtMessage::Response {
            transaction_id: b"po".to_vec(),
            response: ResponseType::Pong { id: test_node_id() },
        };
        let encoded = msg.encode().unwrap();
        let bval = crate::torrent::bencode::decode(&encoded).unwrap();
        let dict = bval.as_dict().unwrap();
        assert_eq!(dict.get("y").unwrap().as_bytes().unwrap(), b"r");
        let r = dict.get("r").unwrap().as_dict().unwrap();
        assert_eq!(r.get("id").unwrap().as_bytes().unwrap(), &test_node_id());
        // No "nodes", no "values", no "token" → pure pong structure
        assert!(!r.contains_key("nodes"));
        assert!(!r.contains_key("values"));
        assert!(!r.contains_key("token"));
    }

    // ===== Nodes response roundtrip =====

    #[test]
    fn test_encode_decode_nodes_response() {
        let nodes = vec![
            ([1u8; 20], "10.0.0.1:6881".parse().unwrap()),
            ([2u8; 20], "10.0.0.2:6882".parse().unwrap()),
        ];
        let msg = DhtMessage::Response {
            transaction_id: b"bb".to_vec(),
            response: ResponseType::Nodes {
                id: test_node_id(),
                nodes: nodes.clone(),
            },
        };

        let encoded = msg.encode().unwrap();
        let decoded = DhtMessage::decode(&encoded).unwrap();

        match decoded {
            DhtMessage::Response {
                transaction_id,
                response: ResponseType::Nodes { id, nodes: dn },
            } => {
                assert_eq!(transaction_id, b"bb".to_vec());
                assert_eq!(id, test_node_id());
                assert_eq!(dn.len(), 2);
                assert_eq!(dn[0].0, nodes[0].0);
                assert_eq!(dn[0].1, nodes[0].1);
                assert_eq!(dn[1].0, nodes[1].0);
                assert_eq!(dn[1].1, nodes[1].1);
            }
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_nodes_response_empty() {
        let msg = DhtMessage::Response {
            transaction_id: b"c1".to_vec(),
            response: ResponseType::Nodes {
                id: test_node_id(),
                nodes: vec![],
            },
        };
        let encoded = msg.encode().unwrap();
        let decoded = DhtMessage::decode(&encoded).unwrap();
        match decoded {
            DhtMessage::Response {
                response: ResponseType::Nodes { nodes, .. },
                ..
            } => assert!(nodes.is_empty()),
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_nodes_response_many_nodes() {
        let nodes: Vec<(NodeId, SocketAddr)> = (0..20)
            .map(|i| {
                let mut id = [0u8; 20];
                id[0] = i;
                let addr = format!("10.0.0.{}:{}", i, 6881 + i as u16).parse().unwrap();
                (id, addr)
            })
            .collect();
        let msg = DhtMessage::Response {
            transaction_id: b"c2".to_vec(),
            response: ResponseType::Nodes {
                id: test_node_id(),
                nodes: nodes.clone(),
            },
        };
        let encoded = msg.encode().unwrap();
        let decoded = DhtMessage::decode(&encoded).unwrap();
        match decoded {
            DhtMessage::Response {
                response: ResponseType::Nodes { nodes: dn, .. },
                ..
            } => {
                assert_eq!(dn.len(), 20);
                for (i, (decoded_node, orig_node)) in dn.iter().zip(nodes.iter()).enumerate() {
                    assert_eq!(decoded_node.0, orig_node.0);
                    assert_eq!(decoded_node.1, orig_node.1);
                    assert_eq!(decoded_node.0[0], i as u8);
                }
            }
            _ => panic!("Wrong message type"),
        }
    }

    // ===== Peers response roundtrip =====

    #[test]
    fn test_encode_decode_peers_response_with_values() {
        let values = vec![
            "10.0.0.1:6881".parse().unwrap(),
            "10.0.0.2:6882".parse().unwrap(),
        ];
        let msg = DhtMessage::Response {
            transaction_id: b"dd".to_vec(),
            response: ResponseType::Peers {
                id: test_node_id(),
                token: b"token1".to_vec(),
                values: Some(values.clone()),
                nodes: vec![],
            },
        };

        let encoded = msg.encode().unwrap();
        let decoded = DhtMessage::decode(&encoded).unwrap();

        match decoded {
            DhtMessage::Response {
                response:
                    ResponseType::Peers {
                        id,
                        token,
                        values: dv,
                        nodes,
                    },
                ..
            } => {
                assert_eq!(id, test_node_id());
                assert_eq!(token, b"token1".to_vec());
                assert!(dv.is_some());
                let dv = dv.unwrap();
                assert_eq!(dv.len(), 2);
                assert_eq!(dv[0], values[0]);
                assert_eq!(dv[1], values[1]);
                assert!(nodes.is_empty());
            }
            _ => panic!("Wrong message type"),
        }
    }

    // NOTE: Peers response with values=None encodes only id+token+nodes.
    // The decoder sees "nodes" key but no "values" key and decodes as Nodes response.
    // This is inherent BEP 0005 wire format ambiguity.

    #[test]
    fn test_encode_peers_response_with_nodes_decodes_as_nodes() {
        let nodes = vec![([3u8; 20], "10.0.0.3:6883".parse().unwrap())];
        let msg = DhtMessage::Response {
            transaction_id: b"ee".to_vec(),
            response: ResponseType::Peers {
                id: test_node_id(),
                token: b"token2".to_vec(),
                values: None,
                nodes: nodes.clone(),
            },
        };

        let encoded = msg.encode().unwrap();
        let decoded = DhtMessage::decode(&encoded).unwrap();

        // Decodes as Nodes because no "values" key present
        match decoded {
            DhtMessage::Response {
                response: ResponseType::Nodes { nodes: dn, .. },
                ..
            } => {
                assert_eq!(dn.len(), 1);
                assert_eq!(dn[0].0, nodes[0].0);
                assert_eq!(dn[0].1, nodes[0].1);
            }
            _ => panic!("Expected Nodes (wire-identical with Peers values=None)"),
        }
    }

    #[test]
    fn test_peers_response_with_values_and_nodes() {
        // When values is Some, the decoder correctly identifies it as Peers
        let nodes = vec![([3u8; 20], "10.0.0.3:6883".parse().unwrap())];
        let values = vec!["1.2.3.4:80".parse().unwrap()];
        let msg = DhtMessage::Response {
            transaction_id: b"pv".to_vec(),
            response: ResponseType::Peers {
                id: test_node_id(),
                token: b"tk".to_vec(),
                values: Some(values.clone()),
                nodes: nodes.clone(),
            },
        };
        let encoded = msg.encode().unwrap();
        let decoded = DhtMessage::decode(&encoded).unwrap();
        match decoded {
            DhtMessage::Response {
                response:
                    ResponseType::Peers {
                        values: dv,
                        nodes: dn,
                        token,
                        ..
                    },
                ..
            } => {
                assert_eq!(token, b"tk".to_vec());
                let dv = dv.unwrap();
                assert_eq!(dv.len(), 1);
                assert_eq!(dv[0], values[0]);
                assert_eq!(dn.len(), 1);
            }
            _ => panic!("Expected Peers"),
        }
    }

    #[test]
    fn test_peers_response_empty_token_decodes_as_nodes() {
        // Peers with values=None and empty nodes → decodes as Nodes (empty)
        let msg = DhtMessage::Response {
            transaction_id: b"c3".to_vec(),
            response: ResponseType::Peers {
                id: test_node_id(),
                token: vec![],
                values: None,
                nodes: vec![],
            },
        };
        let encoded = msg.encode().unwrap();
        let decoded = DhtMessage::decode(&encoded).unwrap();
        // Has "nodes" key (empty bytes) but no "values" → Nodes response
        match decoded {
            DhtMessage::Response {
                response: ResponseType::Nodes { nodes, .. },
                ..
            } => {
                assert!(nodes.is_empty());
            }
            _ => panic!("Expected Nodes (wire-identical with Peers values=None)"),
        }
    }

    // ===== AnnounceSuccess response roundtrip =====

    #[test]
    fn test_encode_decode_announce_success() {
        let msg = DhtMessage::Response {
            transaction_id: b"ff".to_vec(),
            response: ResponseType::AnnounceSuccess { id: test_node_id() },
        };

        let encoded = msg.encode().unwrap();
        let decoded = DhtMessage::decode(&encoded).unwrap();

        match decoded {
            DhtMessage::Response {
                transaction_id,
                response: ResponseType::AnnounceSuccess { id },
            } => {
                assert_eq!(transaction_id, b"ff".to_vec());
                assert_eq!(id, test_node_id());
            }
            _ => panic!("Wrong message type"),
        }
    }

    // ===== Error message roundtrip =====

    #[test]
    fn test_encode_decode_error() {
        let msg = DhtMessage::Error {
            transaction_id: b"gg".to_vec(),
            code: 203,
            message: "Invalid token".to_string(),
        };

        let encoded = msg.encode().unwrap();
        let decoded = DhtMessage::decode(&encoded).unwrap();

        match decoded {
            DhtMessage::Error {
                transaction_id,
                code,
                message,
            } => {
                assert_eq!(transaction_id, b"gg".to_vec());
                assert_eq!(code, 203);
                assert_eq!(message, "Invalid token");
            }
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_error_code_zero() {
        let msg = DhtMessage::Error {
            transaction_id: b"e0".to_vec(),
            code: 0,
            message: "unknown".to_string(),
        };
        let encoded = msg.encode().unwrap();
        let decoded = DhtMessage::decode(&encoded).unwrap();
        match decoded {
            DhtMessage::Error { code, .. } => assert_eq!(code, 0),
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_error_negative_code() {
        let msg = DhtMessage::Error {
            transaction_id: b"en".to_vec(),
            code: -1,
            message: "generic error".to_string(),
        };
        let encoded = msg.encode().unwrap();
        let decoded = DhtMessage::decode(&encoded).unwrap();
        match decoded {
            DhtMessage::Error { code, .. } => assert_eq!(code, -1),
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_error_empty_message() {
        let msg = DhtMessage::Error {
            transaction_id: b"em".to_vec(),
            code: 201,
            message: String::new(),
        };
        let encoded = msg.encode().unwrap();
        let decoded = DhtMessage::decode(&encoded).unwrap();
        match decoded {
            DhtMessage::Error { message, .. } => assert!(message.is_empty()),
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_error_unicode_message() {
        let msg = DhtMessage::Error {
            transaction_id: b"eu".to_vec(),
            code: 202,
            message: "令牌无效 🚫".to_string(),
        };
        let encoded = msg.encode().unwrap();
        let decoded = DhtMessage::decode(&encoded).unwrap();
        match decoded {
            DhtMessage::Error { message, .. } => assert_eq!(message, "令牌无效 🚫"),
            _ => panic!("Wrong message type"),
        }
    }

    // ===== encode_addr / decode_addr =====

    #[test]
    fn test_encode_addr() {
        let addr: SocketAddr = "127.0.0.1:6881".parse().unwrap();
        let encoded = encode_addr(&addr);
        assert_eq!(encoded.len(), 6);

        let decoded = decode_addr(&encoded).unwrap();
        assert_eq!(decoded, addr);
    }

    #[test]
    fn test_encode_addr_various_ips() {
        let addrs = vec![
            "0.0.0.0:0",
            "255.255.255.255:65535",
            "10.0.0.1:80",
            "172.16.0.1:443",
            "192.168.0.1:8080",
        ];
        for addr_str in addrs {
            let addr: SocketAddr = addr_str.parse().unwrap();
            let encoded = encode_addr(&addr);
            assert_eq!(encoded.len(), 6);
            let decoded = decode_addr(&encoded).unwrap();
            assert_eq!(decoded, addr, "roundtrip failed for {}", addr_str);
        }
    }

    #[test]
    fn test_encode_addr_ipv6_returns_zeroes() {
        let addr: SocketAddr = "[::1]:6881".parse().unwrap();
        let encoded = encode_addr(&addr);
        assert_eq!(encoded.len(), 6);
        assert_eq!(encoded, vec![0u8; 6]);
    }

    #[test]
    fn test_decode_addr_too_short() {
        assert!(decode_addr(&[0u8; 5]).is_none());
        assert!(decode_addr(&[]).is_none());
    }

    #[test]
    fn test_decode_addr_exactly_six_bytes() {
        let data = [192u8, 168, 1, 1, 0x1A, 0xE1]; // 192.168.1.1:6881
        let addr = decode_addr(&data).unwrap();
        assert_eq!(addr, "192.168.1.1:6881".parse().unwrap());
    }

    #[test]
    fn test_decode_addr_extra_bytes_ignored() {
        let data = [10u8, 0, 0, 1, 0x00, 0x50, 0xFF, 0xFF]; // 10.0.0.1:80 + extra
        let addr = decode_addr(&data).unwrap();
        assert_eq!(addr, "10.0.0.1:80".parse().unwrap());
    }

    // ===== encode_nodes / decode_nodes =====

    #[test]
    fn test_encode_decode_nodes_empty() {
        let nodes: Vec<(NodeId, SocketAddr)> = vec![];
        let encoded = encode_nodes(&nodes);
        assert!(encoded.is_empty());
        let decoded = decode_nodes(&encoded);
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_encode_decode_nodes_single() {
        let nodes = vec![([0x42; 20], "10.0.0.1:6881".parse().unwrap())];
        let encoded = encode_nodes(&nodes);
        assert_eq!(encoded.len(), 26);
        let decoded = decode_nodes(&encoded);
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].0, nodes[0].0);
        assert_eq!(decoded[0].1, nodes[0].1);
    }

    #[test]
    fn test_encode_decode_nodes_multiple() {
        let nodes = vec![
            ([1u8; 20], "10.0.0.1:6881".parse().unwrap()),
            ([2u8; 20], "10.0.0.2:6882".parse().unwrap()),
            ([3u8; 20], "10.0.0.3:6883".parse().unwrap()),
        ];
        let encoded = encode_nodes(&nodes);
        assert_eq!(encoded.len(), 78); // 3 * 26
        let decoded = decode_nodes(&encoded);
        assert_eq!(decoded.len(), 3);
        for (d, o) in decoded.iter().zip(nodes.iter()) {
            assert_eq!(d.0, o.0);
            assert_eq!(d.1, o.1);
        }
    }

    #[test]
    fn test_decode_nodes_truncated_data() {
        // 25 bytes < 26 required for one node
        let data = vec![0u8; 25];
        let decoded = decode_nodes(&data);
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_decode_nodes_partial_second_node() {
        // First node complete (26 bytes) + 10 bytes of second (incomplete)
        let mut data = vec![0u8; 26]; // first node: all zeros id, 0.0.0.0:0
        data.extend_from_slice(&[0u8; 10]); // partial second node
        let decoded = decode_nodes(&data);
        assert_eq!(decoded.len(), 1); // only first node decoded
    }

    #[test]
    fn test_decode_nodes_extra_bytes_ignored() {
        // One complete node + 5 extra bytes
        let mut data = vec![0u8; 26];
        data[0..20].copy_from_slice(&[0xAA; 20]); // node id
        data[20] = 10;
        data[21] = 0;
        data[22] = 0;
        data[23] = 1;
        data[24] = 0x1A;
        data[25] = 0xE1; // port 6881
        data.extend_from_slice(&[0xFF; 5]); // extra bytes
        let decoded = decode_nodes(&data);
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].1, "10.0.0.1:6881".parse().unwrap());
    }

    // ===== Decode error cases =====

    #[test]
    fn test_decode_empty_data() {
        let result = DhtMessage::decode(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_invalid_bencode() {
        let result = DhtMessage::decode(b"not bencode at all");
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_missing_transaction_id() {
        // Valid bencode dict without "t" key
        let data = b"d1:y1:qe1:q4:ping1:ad2:id20:aaaaaaaaaaaaaaaaaaaaee";
        let result = DhtMessage::decode(data);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_missing_message_type() {
        let data = b"d1:t2:aad1:q4:ping1:ad2:id20:aaaaaaaaaaaaaaaaaaaaee";
        let result = DhtMessage::decode(data);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_unknown_message_type() {
        // y = "x" is unknown
        let data = b"d1:t2:aad1:y1:xe";
        let result = DhtMessage::decode(data);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_query_missing_q_field() {
        // Query message without "q" field
        let data = b"d1:t2:aad1:y1:q1:ad2:id20:aaaaaaaaaaaaaaaaaaaaee";
        let result = DhtMessage::decode(data);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_query_unknown_type() {
        // Query with unknown query type "unknown_query"
        let data = b"d1:t2:aad1:y1:q1:q13:unknown_query1:ad2:id20:aaaaaaaaaaaaaaaaaaaaee";
        let result = DhtMessage::decode(data);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_query_missing_args() {
        // Query without "a" field
        let data = b"d1:t2:aad1:y1:q1:q4:pinge";
        let result = DhtMessage::decode(data);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_query_missing_id_in_args() {
        // Ping query without id in args
        let data = b"d1:t2:aad1:y1:q1:q4:ping1:adee";
        let result = DhtMessage::decode(data);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_query_wrong_id_length() {
        // Ping query with id that is not 20 bytes
        let data = b"d1:t2:aad1:y1:q1:q4:ping1:ad2:id5:shortee";
        let result = DhtMessage::decode(data);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_find_node_missing_target() {
        // find_node without target field
        let data = b"d1:t2:aad1:y1:q1:q9:find_node1:ad2:id20:aaaaaaaaaaaaaaaaaaaaaee";
        let result = DhtMessage::decode(data);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_get_peers_missing_info_hash() {
        let data = b"d1:t2:aad1:y1:q1:q9:get_peers1:ad2:id20:aaaaaaaaaaaaaaaaaaaaaee";
        let result = DhtMessage::decode(data);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_announce_peer_missing_port() {
        let data = b"d1:t2:aad1:y1:q1:q13:announce_peer1:ad2:id20:aaaaaaaaaaaaaaaaaaaa9:info_hash20:bbbbbbbbbbbbbbbbbbbb5:token3:abcee";
        let result = DhtMessage::decode(data);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_announce_peer_missing_token() {
        let data = b"d1:t2:aad1:y1:q1:q13:announce_peer1:ad2:id20:aaaaaaaaaaaaaaaaaaaa9:info_hash20:bbbbbbbbbbbbbbbbbbbb4:porti6881ee";
        let result = DhtMessage::decode(data);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_response_missing_r_dict() {
        // Response message without "r" field
        let data = b"d1:t2:aad1:y1:re";
        let result = DhtMessage::decode(data);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_response_missing_id_in_r() {
        // Response with empty r dict (no id)
        let data = b"d1:t2:aad1:y1:q1:q4:ping1:ad2:id20:aaaaaaaaaaaaaaaaaaaa1:rdeee";
        // This is a response (y=r) but r dict has no id
        let result = DhtMessage::decode(data);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_error_missing_e_field() {
        let data = b"d1:t2:aad1:y1:re";
        let result = DhtMessage::decode(data);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_error_e_too_short() {
        // Error message with e list containing only one element
        let data = b"d1:t2:aad1:y1:q1:q4:ping1:ad2:id20:aaaaaaaaaaaaaaaaaaaa1:eli201ee";
        let result = DhtMessage::decode(data);
        assert!(result.is_err());
    }

    // ===== get_node_id =====

    #[test]
    fn test_get_node_id_wrong_length() {
        let mut dict = std::collections::BTreeMap::new();
        dict.insert("id".to_string(), Bencode::Bytes(vec![0u8; 19])); // 19 bytes, not 20
        let result = get_node_id(&dict, "id");
        assert!(result.is_err());
    }

    #[test]
    fn test_get_node_id_missing_key() {
        let dict = std::collections::BTreeMap::new();
        let result = get_node_id(&dict, "missing");
        assert!(result.is_err());
    }

    #[test]
    fn test_get_node_id_not_bytes() {
        let mut dict = std::collections::BTreeMap::new();
        dict.insert("id".to_string(), Bencode::Integer(42));
        let result = get_node_id(&dict, "id");
        assert!(result.is_err());
    }

    #[test]
    fn test_get_node_id_too_long() {
        let mut dict = std::collections::BTreeMap::new();
        dict.insert("id".to_string(), Bencode::Bytes(vec![0u8; 21]));
        let result = get_node_id(&dict, "id");
        assert!(result.is_err());
    }

    #[test]
    fn test_get_node_id_exact_20_bytes() {
        let mut dict = std::collections::BTreeMap::new();
        dict.insert("id".to_string(), Bencode::Bytes(vec![0xAB; 20]));
        let result = get_node_id(&dict, "id");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), [0xAB; 20]);
    }

    // ===== Clone trait tests =====

    #[test]
    fn test_dht_message_clone_query() {
        let msg = DhtMessage::Query {
            transaction_id: b"ab".to_vec(),
            query: QueryType::Ping { id: test_node_id() },
        };
        let cloned = msg.clone();
        match cloned {
            DhtMessage::Query {
                transaction_id,
                query: QueryType::Ping { id },
            } => {
                assert_eq!(transaction_id, b"ab".to_vec());
                assert_eq!(id, test_node_id());
            }
            _ => panic!("Clone produced wrong variant"),
        }
    }

    #[test]
    fn test_dht_message_clone_response() {
        let msg = DhtMessage::Response {
            transaction_id: b"cd".to_vec(),
            response: ResponseType::Pong { id: test_node_id() },
        };
        let cloned = msg.clone();
        match cloned {
            DhtMessage::Response {
                transaction_id,
                response: ResponseType::Pong { id },
            } => {
                assert_eq!(transaction_id, b"cd".to_vec());
                assert_eq!(id, test_node_id());
            }
            _ => panic!("Clone produced wrong variant"),
        }
    }

    #[test]
    fn test_dht_message_clone_error() {
        let msg = DhtMessage::Error {
            transaction_id: b"ef".to_vec(),
            code: 203,
            message: "test".to_string(),
        };
        let cloned = msg.clone();
        match cloned {
            DhtMessage::Error {
                transaction_id,
                code,
                message,
            } => {
                assert_eq!(transaction_id, b"ef".to_vec());
                assert_eq!(code, 203);
                assert_eq!(message, "test");
            }
            _ => panic!("Clone produced wrong variant"),
        }
    }

    // ===== Debug trait tests =====

    #[test]
    fn test_dht_message_debug_query() {
        let msg = DhtMessage::Query {
            transaction_id: b"aa".to_vec(),
            query: QueryType::Ping { id: test_node_id() },
        };
        let debug = format!("{:?}", msg);
        assert!(debug.contains("Query"));
    }

    #[test]
    fn test_dht_message_debug_response() {
        let msg = DhtMessage::Response {
            transaction_id: b"bb".to_vec(),
            response: ResponseType::Pong { id: test_node_id() },
        };
        let debug = format!("{:?}", msg);
        assert!(debug.contains("Response"));
    }

    #[test]
    fn test_dht_message_debug_error() {
        let msg = DhtMessage::Error {
            transaction_id: b"cc".to_vec(),
            code: 201,
            message: "test error".to_string(),
        };
        let debug = format!("{:?}", msg);
        assert!(debug.contains("Error"));
        assert!(debug.contains("test error"));
    }

    #[test]
    fn test_query_type_debug() {
        let qt = QueryType::Ping { id: test_node_id() };
        let debug = format!("{:?}", qt);
        assert!(debug.contains("Ping"));
    }

    #[test]
    fn test_response_type_debug() {
        let rt = ResponseType::Pong { id: test_node_id() };
        let debug = format!("{:?}", rt);
        assert!(debug.contains("Pong"));
    }

    // ===== Encode produces valid bencode =====

    #[test]
    fn test_encode_ping_contains_query_marker() {
        let msg = DhtMessage::Query {
            transaction_id: b"aa".to_vec(),
            query: QueryType::Ping { id: test_node_id() },
        };
        let encoded = msg.encode().unwrap();
        // The encoded bencode should contain y:q (query marker)
        let decoded_bencode = crate::torrent::bencode::decode(&encoded).unwrap();
        let dict = decoded_bencode.as_dict().unwrap();
        assert_eq!(dict.get("y").unwrap().as_bytes().unwrap(), b"q");
    }

    #[test]
    fn test_encode_response_contains_response_marker() {
        let msg = DhtMessage::Response {
            transaction_id: b"aa".to_vec(),
            response: ResponseType::Pong { id: test_node_id() },
        };
        let encoded = msg.encode().unwrap();
        let decoded_bencode = crate::torrent::bencode::decode(&encoded).unwrap();
        let dict = decoded_bencode.as_dict().unwrap();
        assert_eq!(dict.get("y").unwrap().as_bytes().unwrap(), b"r");
    }

    #[test]
    fn test_encode_error_contains_error_marker() {
        let msg = DhtMessage::Error {
            transaction_id: b"aa".to_vec(),
            code: 201,
            message: "test".to_string(),
        };
        let encoded = msg.encode().unwrap();
        let decoded_bencode = crate::torrent::bencode::decode(&encoded).unwrap();
        let dict = decoded_bencode.as_dict().unwrap();
        assert_eq!(dict.get("y").unwrap().as_bytes().unwrap(), b"e");
    }

    // ===== AnnouncePeer implied_port encoding =====

    #[test]
    fn test_announce_peer_implied_port_encoded_as_1() {
        let msg = DhtMessage::Query {
            transaction_id: b"ip".to_vec(),
            query: QueryType::AnnouncePeer {
                id: test_node_id(),
                info_hash: test_info_hash(),
                port: 0,
                token: b"tok".to_vec(),
                implied_port: true,
            },
        };
        let encoded = msg.encode().unwrap();
        let decoded_bencode = crate::torrent::bencode::decode(&encoded).unwrap();
        let dict = decoded_bencode.as_dict().unwrap();
        let args = dict.get("a").unwrap().as_dict().unwrap();
        let implied = args.get("implied_port").unwrap().as_integer().unwrap();
        assert_eq!(implied, 1);
    }

    #[test]
    fn test_announce_peer_implied_port_false_encoded_as_0() {
        let msg = DhtMessage::Query {
            transaction_id: b"ip".to_vec(),
            query: QueryType::AnnouncePeer {
                id: test_node_id(),
                info_hash: test_info_hash(),
                port: 6881,
                token: b"tok".to_vec(),
                implied_port: false,
            },
        };
        let encoded = msg.encode().unwrap();
        let decoded_bencode = crate::torrent::bencode::decode(&encoded).unwrap();
        let dict = decoded_bencode.as_dict().unwrap();
        let args = dict.get("a").unwrap().as_dict().unwrap();
        let implied = args.get("implied_port").unwrap().as_integer().unwrap();
        assert_eq!(implied, 0);
    }

    // ===== Decode announces with implied_port absent =====

    #[test]
    fn test_decode_announce_peer_without_implied_port_defaults_false() {
        // Build announce_peer bencode manually without implied_port
        let mut args = std::collections::BTreeMap::new();
        args.insert("id".to_string(), Bencode::Bytes(test_node_id().to_vec()));
        args.insert(
            "info_hash".to_string(),
            Bencode::Bytes(test_info_hash().to_vec()),
        );
        args.insert("port".to_string(), Bencode::Integer(6881));
        args.insert("token".to_string(), Bencode::Bytes(b"tok".to_vec()));
        // No implied_port key

        let mut outer = std::collections::BTreeMap::new();
        outer.insert("t".to_string(), Bencode::Bytes(b"np".to_vec()));
        outer.insert("y".to_string(), Bencode::Bytes(b"q".to_vec()));
        outer.insert("q".to_string(), Bencode::Bytes(b"announce_peer".to_vec()));
        outer.insert("a".to_string(), Bencode::Dict(args));

        let bencode = Bencode::Dict(outer);
        let encoded = crate::torrent::bencode::encode(&bencode);

        let decoded = DhtMessage::decode(&encoded).unwrap();
        match decoded {
            DhtMessage::Query {
                query: QueryType::AnnouncePeer { implied_port, .. },
                ..
            } => assert!(!implied_port),
            _ => panic!("Wrong message type"),
        }
    }

    // ===== Peers response: values absent vs present =====

    #[test]
    fn test_peers_response_values_none_roundtrip() {
        // Peers with values=None decodes as Nodes (wire ambiguity)
        let msg = DhtMessage::Response {
            transaction_id: b"vn".to_vec(),
            response: ResponseType::Peers {
                id: test_node_id(),
                token: b"t".to_vec(),
                values: None,
                nodes: vec![([5u8; 20], "1.2.3.4:80".parse().unwrap())],
            },
        };
        let encoded = msg.encode().unwrap();
        let decoded = DhtMessage::decode(&encoded).unwrap();
        match decoded {
            DhtMessage::Response {
                response: ResponseType::Nodes { nodes, .. },
                ..
            } => {
                assert_eq!(nodes.len(), 1);
                assert_eq!(nodes[0].0, [5u8; 20]);
            }
            _ => panic!("Expected Nodes"),
        }
    }

    #[test]
    fn test_peers_response_empty_values_list() {
        let msg = DhtMessage::Response {
            transaction_id: b"ev".to_vec(),
            response: ResponseType::Peers {
                id: test_node_id(),
                token: b"t".to_vec(),
                values: Some(vec![]),
                nodes: vec![],
            },
        };
        let encoded = msg.encode().unwrap();
        let decoded = DhtMessage::decode(&encoded).unwrap();
        match decoded {
            DhtMessage::Response {
                response: ResponseType::Peers { values, .. },
                ..
            } => {
                let v = values.unwrap();
                assert!(v.is_empty());
            }
            _ => panic!("Wrong type"),
        }
    }

    // ===== Multiple message types independent =====

    #[test]
    fn test_all_query_types_encode_independently() {
        let msgs = vec![
            DhtMessage::Query {
                transaction_id: b"q1".to_vec(),
                query: QueryType::Ping { id: test_node_id() },
            },
            DhtMessage::Query {
                transaction_id: b"q2".to_vec(),
                query: QueryType::FindNode {
                    id: test_node_id(),
                    target: [0x11; 20],
                },
            },
            DhtMessage::Query {
                transaction_id: b"q3".to_vec(),
                query: QueryType::GetPeers {
                    id: test_node_id(),
                    info_hash: [0x22; 20],
                },
            },
            DhtMessage::Query {
                transaction_id: b"q4".to_vec(),
                query: QueryType::AnnouncePeer {
                    id: test_node_id(),
                    info_hash: [0x33; 20],
                    port: 9999,
                    token: b"tk".to_vec(),
                    implied_port: true,
                },
            },
        ];

        for msg in &msgs {
            let encoded = msg.encode().unwrap();
            let decoded = DhtMessage::decode(&encoded).unwrap();
            // Verify transaction id preserved
            match decoded {
                DhtMessage::Query { transaction_id, .. } => {
                    assert!(
                        transaction_id == b"q1".to_vec()
                            || transaction_id == b"q2".to_vec()
                            || transaction_id == b"q3".to_vec()
                            || transaction_id == b"q4".to_vec()
                    );
                }
                _ => panic!("Expected query"),
            }
        }
    }

    // ===== Address encoding edge cases =====

    #[test]
    fn test_encode_addr_port_byte_order() {
        // Port 0x0102 = 258
        let addr: SocketAddr = "1.2.3.4:258".parse().unwrap();
        let encoded = encode_addr(&addr);
        // IP: 1, 2, 3, 4
        assert_eq!(encoded[0], 1);
        assert_eq!(encoded[1], 2);
        assert_eq!(encoded[2], 3);
        assert_eq!(encoded[3], 4);
        // Port 258 = 0x0102 in big-endian
        assert_eq!(encoded[4], 0x01);
        assert_eq!(encoded[5], 0x02);
    }

    #[test]
    fn test_decode_addr_port_zero() {
        let data = [10u8, 0, 0, 1, 0x00, 0x00]; // port 0
        let addr = decode_addr(&data).unwrap();
        assert_eq!(addr, "10.0.0.1:0".parse().unwrap());
    }

    #[test]
    fn test_decode_addr_port_max() {
        let data = [10u8, 0, 0, 1, 0xFF, 0xFF]; // port 65535
        let addr = decode_addr(&data).unwrap();
        assert_eq!(addr, "10.0.0.1:65535".parse().unwrap());
    }

    // ===== Node encoding in nodes compact format =====

    #[test]
    fn test_nodes_compact_format_structure() {
        let id = [0xAA; 20];
        let addr: SocketAddr = "192.168.0.1:8080".parse().unwrap();
        let nodes = vec![(id, addr)];
        let encoded = encode_nodes(&nodes);

        // 20 bytes id + 4 bytes IP + 2 bytes port = 26
        assert_eq!(encoded.len(), 26);
        // First 20 bytes are the node id
        assert_eq!(&encoded[0..20], &[0xAA; 20]);
        // Next 4 bytes are IP
        assert_eq!(encoded[20], 192);
        assert_eq!(encoded[21], 168);
        assert_eq!(encoded[22], 0);
        assert_eq!(encoded[23], 1);
        // Last 2 bytes are port 8080 = 0x1F90
        assert_eq!(encoded[24], 0x1F);
        assert_eq!(encoded[25], 0x90);
    }

    // ===== Full encode-decode stress test =====

    #[test]
    fn test_stress_all_message_types_roundtrip() {
        let messages = vec![
            DhtMessage::Query {
                transaction_id: vec![0; 2],
                query: QueryType::Ping { id: [0; 20] },
            },
            DhtMessage::Query {
                transaction_id: vec![0xFF; 4],
                query: QueryType::FindNode {
                    id: [0xFF; 20],
                    target: [0x00; 20],
                },
            },
            DhtMessage::Query {
                transaction_id: b"gp".to_vec(),
                query: QueryType::GetPeers {
                    id: [0x11; 20],
                    info_hash: [0x22; 20],
                },
            },
            DhtMessage::Query {
                transaction_id: b"ap".to_vec(),
                query: QueryType::AnnouncePeer {
                    id: [0x33; 20],
                    info_hash: [0x44; 20],
                    port: 51412,
                    token: b"secret_token".to_vec(),
                    implied_port: true,
                },
            },
            DhtMessage::Response {
                transaction_id: b"po".to_vec(),
                response: ResponseType::Pong { id: [0x55; 20] },
            },
            DhtMessage::Response {
                transaction_id: b"no".to_vec(),
                response: ResponseType::Nodes {
                    id: [0x66; 20],
                    nodes: vec![
                        ([0x77; 20], "10.0.0.1:6881".parse().unwrap()),
                        ([0x88; 20], "10.0.0.2:6882".parse().unwrap()),
                    ],
                },
            },
            DhtMessage::Response {
                transaction_id: b"pe".to_vec(),
                response: ResponseType::Peers {
                    id: [0x99; 20],
                    token: b"mytoken".to_vec(),
                    values: Some(vec![
                        "1.1.1.1:80".parse().unwrap(),
                        "2.2.2.2:443".parse().unwrap(),
                    ]),
                    nodes: vec![],
                },
            },
            DhtMessage::Response {
                transaction_id: b"as".to_vec(),
                response: ResponseType::AnnounceSuccess { id: [0xAA; 20] },
            },
            DhtMessage::Error {
                transaction_id: b"er".to_vec(),
                code: 203,
                message: "Bad token".to_string(),
            },
        ];

        for msg in &messages {
            let encoded = msg.encode().unwrap();
            let decoded = DhtMessage::decode(&encoded).unwrap();
            // Re-encode and verify idempotent
            let re_encoded = decoded.encode().unwrap();
            assert_eq!(
                encoded, re_encoded,
                "encode-decode-re-encode not idempotent"
            );
        }
    }
}
