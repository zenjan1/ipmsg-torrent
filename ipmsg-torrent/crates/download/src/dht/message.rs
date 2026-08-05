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
            DhtMessage::Query { transaction_id, query } => {
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
                    QueryType::AnnouncePeer { id, info_hash, port, token, implied_port } => {
                        map.insert("q".to_string(), Bencode::Bytes(b"announce_peer".to_vec()));
                        let mut args = std::collections::BTreeMap::new();
                        args.insert("id".to_string(), Bencode::Bytes(id.to_vec()));
                        args.insert("info_hash".to_string(), Bencode::Bytes(info_hash.to_vec()));
                        args.insert("port".to_string(), Bencode::Integer(*port as i64));
                        args.insert("token".to_string(), Bencode::Bytes(token.clone()));
                        args.insert("implied_port".to_string(), Bencode::Integer(if *implied_port { 1 } else { 0 }));
                        map.insert("a".to_string(), Bencode::Dict(args));
                    }
                }
                
                Bencode::Dict(map)
            }
            DhtMessage::Response { transaction_id, response } => {
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
                    ResponseType::Peers { id, token, values, nodes } => {
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
            DhtMessage::Error { transaction_id, code, message } => {
                let mut map = std::collections::BTreeMap::new();
                map.insert("t".to_string(), Bencode::Bytes(transaction_id.clone()));
                map.insert("y".to_string(), Bencode::Bytes(b"e".to_vec()));
                map.insert("e".to_string(), Bencode::List(vec![
                    Bencode::Integer(*code),
                    Bencode::Bytes(message.as_bytes().to_vec()),
                ]));
                Bencode::Dict(map)
            }
        };
        
        Ok(crate::torrent::bencode::encode(&bencode))
    }

    /// Decode message from bencode bytes
    pub fn decode(data: &[u8]) -> Result<Self, BencodeError> {
        let bencode = crate::torrent::bencode::decode(data)?;
        let dict = bencode.as_dict().ok_or(BencodeError::InvalidFormat)?;
        
        let transaction_id = dict.get("t")
            .and_then(|v| v.as_bytes())
            .ok_or(BencodeError::InvalidFormat)?
            .to_vec();
        
        let msg_type = dict.get("y")
            .and_then(|v| v.as_bytes())
            .ok_or(BencodeError::InvalidFormat)?;
        
        match msg_type {
            b"q" => {
                let query_name = dict.get("q")
                    .and_then(|v| v.as_bytes())
                    .ok_or(BencodeError::InvalidFormat)?;
                
                let args = dict.get("a")
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
                        let port = args.get("port")
                            .and_then(|v| v.as_integer())
                            .ok_or(BencodeError::InvalidFormat)? as u16;
                        let token = args.get("token")
                            .and_then(|v| v.as_bytes())
                            .ok_or(BencodeError::InvalidFormat)?
                            .to_vec();
                        let implied_port = args.get("implied_port")
                            .and_then(|v| v.as_integer())
                            .unwrap_or(0) != 0;
                        
                        QueryType::AnnouncePeer { id, info_hash, port, token, implied_port }
                    }
                    _ => return Err(BencodeError::InvalidFormat),
                };
                
                Ok(DhtMessage::Query { transaction_id, query })
            }
            b"r" => {
                let r_dict = dict.get("r")
                    .and_then(|v| v.as_dict())
                    .ok_or(BencodeError::InvalidFormat)?;
                
                let id = get_node_id(r_dict, "id")?;
                
                let response = if r_dict.contains_key("values") {
                    // Peers response
                    let token = r_dict.get("token")
                        .and_then(|v| v.as_bytes())
                        .ok_or(BencodeError::InvalidFormat)?
                        .to_vec();
                    
                    let values = r_dict.get("values")
                        .and_then(|v| v.as_list())
                        .map(|list| {
                            list.iter()
                                .filter_map(|v| v.as_bytes())
                                .filter_map(|b| decode_addr(b))
                                .collect()
                        });
                    
                    let nodes = r_dict.get("nodes")
                        .and_then(|v| v.as_bytes())
                        .map(|b| decode_nodes(b))
                        .unwrap_or_default();
                    
                    ResponseType::Peers { id, token, values, nodes }
                } else if r_dict.contains_key("nodes") {
                    // Nodes response
                    let nodes_bytes = r_dict.get("nodes")
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
                
                Ok(DhtMessage::Response { transaction_id, response })
            }
            b"e" => {
                let error = dict.get("e")
                    .and_then(|v| v.as_list())
                    .ok_or(BencodeError::InvalidFormat)?;
                
                if error.len() < 2 {
                    return Err(BencodeError::InvalidFormat);
                }
                
                let code = error[0].as_integer().ok_or(BencodeError::InvalidFormat)?;
                let message = error[1].as_bytes()
                    .ok_or(BencodeError::InvalidFormat)?;
                let message = String::from_utf8_lossy(message).to_string();
                
                Ok(DhtMessage::Error { transaction_id, code, message })
            }
            _ => Err(BencodeError::InvalidFormat),
        }
    }
}

fn get_node_id(dict: &std::collections::BTreeMap<String, Bencode>, key: &str) -> Result<NodeId, BencodeError> {
    let bytes = dict.get(key)
        .and_then(|v| v.as_bytes())
        .ok_or(BencodeError::InvalidFormat)?;
    
    if bytes.len() != 20 {
        return Err(BencodeError::InvalidFormat);
    }
    
    let mut id = [0u8; 20];
    id.copy_from_slice(bytes);
    Ok(id)
}

fn get_info_hash(dict: &std::collections::BTreeMap<String, Bencode>, key: &str) -> Result<[u8; 20], BencodeError> {
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
            DhtMessage::Query { transaction_id, query: QueryType::Ping { id: decoded_id } } => {
                assert_eq!(transaction_id, b"aa".to_vec());
                assert_eq!(decoded_id, id);
            }
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_encode_addr() {
        let addr: SocketAddr = "127.0.0.1:6881".parse().unwrap();
        let encoded = encode_addr(&addr);
        assert_eq!(encoded.len(), 6);
        
        let decoded = decode_addr(&encoded).unwrap();
        assert_eq!(decoded, addr);
    }
}
