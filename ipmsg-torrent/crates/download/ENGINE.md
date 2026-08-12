# Download Engine Architecture

## Overview

The ipmsg-torrent download engine is a multi-protocol download manager supporting:
- **BitTorrent** (.torrent files, magnet links)
- **eDonkey/eMule** (ed2k:// links)
- **Xunlei P2SP** (HTTP/FTP with P2P acceleration)
- **Direct HTTP/HTTPS/FTP** URLs

## Crate Structure

```
ipmsg-download (crates/download/)
├── lib.rs              # DownloadManager - central orchestrator (23,000+ lines)
├── torrent/            # BitTorrent protocol
│   ├── engine.rs       # TorrentEngine - piece management, peer coordination
│   ├── tracker.rs      # HTTP tracker announce/scrape
│   ├── meta.rs         # .torrent file parser (bencode → TorrentMeta)
│   ├── bencode.rs      # Bencode encoder/decoder
│   ├── peer.rs         # Peer wire protocol (handshake, bitfield, piece messages)
│   ├── metadata.rs     # Metadata fetcher for magnet links
│   └── file_selection.rs # Multi-file torrent selection
├── ed2k/               # eDonkey protocol
│   ├── engine.rs       # Ed2kEngine - chunk management, server communication
│   ├── client.rs       # Ed2k client protocol (server login, search, peer exchange)
│   ├── protocol.rs     # Ed2k message types and serialization
│   ├── server_cache.rs # Persistent server list
│   └── peer_cache.rs   # Persistent peer list
├── xunlei/             # Xunlei P2SP protocol
│   ├── engine.rs       # XunleiEngine - multi-source segmented download
│   ├── protocol.rs     # Source types (HTTP, FTP, P2P)
│   └── peer.rs         # P2P peer management
├── progress.rs         # Resume support - bitmap persistence
├── connection_pool.rs  # TCP connection reuse, DNS caching
├── adaptive_concurrency.rs # RTT-based connection tuning
├── segment_download.rs # Bandwidth-adaptive segmentation
└── ... (140+ modules for features like scheduling, analytics, etc.)
```

## Core APIs

### DownloadManager

```rust
// Add downloads
pub async fn add_torrent(path: PathBuf) -> Result<String, DownloadManagerError>
pub async fn add_magnet(uri: &str) -> Result<String, DownloadManagerError>
pub async fn add_ed2k(hash, size, name, servers) -> Result<String, DownloadManagerError>
pub async fn add_xunlei(name, size, sources) -> Result<String, DownloadManagerError>
pub async fn add_url(url: &str) -> Result<String, DownloadManagerError>

// Task management
pub async fn list_tasks() -> Vec<DownloadTask>
pub async fn pause_task(task_id: &str) -> bool
pub async fn resume_task(task_id: &str) -> bool
pub async fn remove_task(task_id: &str) -> bool

// Speed control
pub async fn set_global_speed_limit(bps: u64)
pub async fn set_task_speed_limit(task_id: &str, bps: u64)

// Dashboard
pub async fn generate_dashboard() -> DashboardSnapshot
```

### DownloadTask

```rust
pub struct DownloadTask {
    pub id: String,
    pub name: String,
    pub protocol: DownloadProtocol,  // Torrent | Ed2k | Xunlei | Magnet | P2P
    pub size: u64,
    pub downloaded: u64,
    pub state: DownloadState,        // Queued | Downloading | Paused | Complete | Error
    pub speed_bps: f64,
    pub save_path: PathBuf,
    pub tags: Vec<String>,
    pub priority: DownloadPriority,
    // ... 30+ more fields for scheduling, retry, proxy, etc.
}
```

## Protocol Engines

### TorrentEngine

- **Piece selection**: Rarest-first (default) or sequential (for streaming)
- **Block size**: 16KB (BitTorrent standard)
- **Peer scoring**: Based on speed, response time, reliability
- **Endgame mode**: Activates when <5% pieces remain
- **Resume**: Progress bitmap saved to `.filename.progress`
- **Multi-tracker**: Supports announce-list tiers
- **File selection**: Download specific files from multi-file torrents

### Ed2kEngine

- **Chunk size**: 9.28MB (eDonkey standard, MD4 hashed)
- **Server protocol**: Login → search → get source list → download from peers
- **Peer exchange**: Source sharing between connected peers
- **Server cache**: Persistent list in `ed2k_servers.json`
- **Peer cache**: Persistent list in `ed2k_peers.json`
- **Resume**: Chunk-level bitmap persistence

### XunleiEngine

- **Segmentation**: Dynamic block sizing based on bandwidth
- **Multi-source**: HTTP/FTP + P2P hybrid
- **Mirror discovery**: Automatic fallback URL detection
- **Buffered I/O**: Pre-allocated file with buffered writes
- **Source quality**: Long-term reliability scoring per domain

## Performance Optimizations

See [PERFORMANCE.md](src/PERFORMANCE.md) for details.

Key optimizations:
1. **Adaptive concurrency**: EWMA-smoothed RTT, BBR-inspired bandwidth estimation
2. **Connection pool**: TCP parameter optimization, DNS caching, pre-connect
3. **Dynamic block sizing**: Bandwidth-adaptive segmentation for Xunlei
4. **Buffered I/O**: Reduced small writes with write coalescing

## Resume Support

Progress is persisted using a custom binary format (v1):

```
| magic (4B) | version (1B) | file_hash (20B) | file_size (8B) |
| piece_size (8B) | total_pieces (4B) | bitmap_len (4B) |
| bitmap (N bytes) | downloaded (8B) |
```

File path: `<download_dir>/.<filename>.progress`

On resume, the engine loads the bitmap and skips completed pieces/chunks.

## Testing

```bash
# Unit tests (in-module)
cargo test -p ipmsg-download

# Integration tests
cargo test -p ipmsg-download --test integration_test
cargo test -p ipmsg-download --test progress_test

# Specific test
cargo test -p ipmsg-download test_progress_save_and_load
```

## CLI Usage

```bash
# Start CLI
./target/debug/ipmsg-cli --username user

# Download commands
/dl <torrent|ed2k|url>     # Add download
/dls                        # List active downloads (with progress bars)
/dlp <task_id>              # Pause
/dlr <task_id>              # Resume
/dldetail <task_id>         # Detailed info
/dllog <task_id>            # Activity log
/dlsearch <keyword>         # Search tasks
/dlhelp                     # All download commands
```

## Configuration

Key settings (via CLI or REST API):
- Global speed limit
- Max concurrent downloads
- Timeout and retry policy
- Proxy configuration
- Bandwidth scheduling
- Auto-cleanup rules
- Disk space monitoring

## Extension Points

- **Post-download hooks**: Run scripts after completion
- **Automation rules**: IFTTT-style triggers
- **Webhooks**: HTTP notifications on events
- **RSS feeds**: Auto-import from feed URLs
- **Watch folders**: Monitor directories for new files
