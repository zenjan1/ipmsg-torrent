//! eDonkey/eMule protocol implementation

mod client;
mod engine;
mod peer_cache;
mod protocol;
mod server_cache;

pub use client::{Ed2kClient, Ed2kClientError, SearchType};
pub use engine::{Ed2kDownloadError, Ed2kEngine};
pub use peer_cache::{PeerCacheError, load_peers, peer_cache_path, remove_peer_cache, save_peers};
pub use protocol::{Ed2kFileHash, Ed2kPeer};
pub use server_cache::{
    ServerCacheError, load_servers, remove_server_cache, save_servers, server_cache_path,
};
