# IPMsg-Torrent

Decentralized P2P chat application built on [libp2p](https://libp2p.io/), with torrent-style file transfer and classic IPMSG/FeiQ compatibility.

## Features

- **P2P Chat** — Gossipsub pub/sub messaging with CBOR serialization
- **Peer Discovery** — mDNS (LAN) + Kademlia DHT (WAN) + bootstrap nodes
- **Direct & Group Messaging** — DMs, named channels, geohash location channels
- **Ed25519 Signing** — All messages cryptographically signed
- **Noise XX E2E Encryption** — ChaCha20-Poly1305 with automatic re-keying
- **Torrent File Transfer** — 256KB chunked transfers with resume support
- **File Sharing & Search** — Share/unshare files, broadcast announcements, search by name/tags
- **Message Fragmentation** — Large message splitting for small MTU links
- **LZ4 Compression** — Optional payload compression
- **Traffic Analysis Resistance** — Fixed-size padding (256–2048 bytes)
- **NAT Traversal** — Relay, DCUtR hole punching, AutoNAT
- **Legacy IPMSG Compat** — UDP port 2425, interoperable with FeiQ/IPMSG desktop clients
- **Social Trust** — Block/unblock peers, favorites, fingerprint verification
- **Rate Limiting** — Sliding window: 10 msgs / 5s per peer
- **SQLite Storage** — Persistent message store with WAL mode and full-text search

## Architecture

```
ipmsg-protocol    Wire format (CBOR messages, types)
       ↓
  ipmsg-core      P2P engine (libp2p, crypto, storage, file transfer)
     ↓   ↓   ↓
   cli  tauri  wasm
  (TUI) (Desktop/Mobile) (Browser)
```

### Crates

| Crate | Description |
|-------|-------------|
| `ipmsg-protocol` | Message types, CBOR codec |
| `ipmsg-core` | P2P networking engine, E2E encryption, SQLite store, file transfer, IPMSG compat |
| `ipmsg-cli` | Terminal UI client (ratatui + crossterm) |
| `ipmsg-app` | Desktop/mobile app (Tauri v2) |
| `ipmsg-wasm` | Browser client (WebAssembly) |

## Download Engine

Multi-protocol download manager with resume support:

| Protocol | Description | Engine |
|----------|-------------|--------|
| **BitTorrent** | .torrent files, magnet links, rarest-first piece selection | `TorrentEngine` |
| **eDonkey/eMule** | ed2k:// links, 9.28MB chunks, MD4 verification | `Ed2kEngine` |
| **Xunlei P2SP** | HTTP/FTP + P2P hybrid, dynamic segmentation | `XunleiEngine` |
| **HTTP/HTTPS/FTP** | Direct URL downloads with range requests | `XunleiEngine` |

Key features:
- **Resume support** — Bitmap-based progress persistence (`.progress` files)
- **Adaptive concurrency** — RTT-based connection tuning (BBR-inspired)
- **Connection pool** — TCP reuse, DNS caching, per-domain limits
- **Dashboard** — Real-time speed, ETA, health monitoring
- **Scheduling** — Time windows, bandwidth budgets, auto-pause
- **Analytics** — Speed history, cost tracking, source quality scoring

See [ENGINE.md](crates/download/ENGINE.md) for architecture details.

## Installation

### Prerequisites

- **Rust 1.85+** (edition 2024) — install via [rustup](https://rustup.rs/)
- **System dependencies** (Linux): `sudo apt install build-essential pkg-config libssl-dev`
- **For Tauri desktop app**: [Tauri CLI prerequisites](https://tauri.app/start/prerequisites/)
- **For WASM**: [wasm-pack](https://rustwasm.github.io/wasm-pack/installer/)

### Building from Source

```bash
git clone <repo-url> ipmsg-torrent
cd ipmsg-torrent

# Build all crates
cargo build --release

# Binaries are placed in target/release/
```

### Platform-Specific Notes

#### Linux

```bash
# Install system dependencies
sudo apt install build-essential pkg-config libssl-dev

# Build and run CLI
cargo run -p ipmsg-cli --release -- --username alice
```

#### macOS

```bash
# Xcode command-line tools required
xcode-select --install

# Build and run CLI
cargo run -p ipmsg-cli --release -- --username alice
```

#### Windows

```powershell
# Requires Visual Studio Build Tools (C++ workload)
# Build and run CLI
cargo run -p ipmsg-cli --release -- --username alice
```

### Docker (Container Deployment)

```dockerfile
# Example Dockerfile
FROM rust:1.85 AS builder
WORKDIR /app
COPY . .
RUN cargo build --release -p ipmsg-cli

FROM debian:bookworm-slim
RUN apt update && apt install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/ipmsg-cli /usr/local/bin/ipmsg
EXPOSE 4001/tcp 4001/udp 2425/udp
ENTRYPOINT ["ipmsg"]
```

```bash
docker build -t ipmsg-torrent .
docker run -it --net=host ipmsg-torrent --username alice
```

## Quick Start

### CLI Client

```bash
cargo run -p ipmsg-cli -- --username alice
```

Commands (IRC-style):

| Command | Description |
|---------|-------------|
| `/nick <name>` | Change username |
| `/msg <peer> <text>` | Direct message |
| `/join <channel>` | Join channel |
| `/peers` | List connected peers |
| `/share <path>` | Share a file |
| `/search <query>` | Search files |
| `/download <hash>` | Download file |
| `/block <peer>` | Block a peer |
| `/ipmsg` | Start legacy IPMSG compat |
| `/help` | Show all commands |

### Desktop App (Tauri)

```bash
cargo tauri dev --manifest-path crates/app/src-tauri/Cargo.toml
```

### WASM (Browser)

```bash
wasm-pack build wasm --target web
```

## Configuration

### Configuration File

IPMsg-Torrent uses a TOML configuration file located at:

| Platform | Path |
|----------|------|
| Linux | `~/.config/ipmsg-torrent/config.toml` |
| macOS | `~/Library/Application Support/ipmsg-torrent/config.toml` |
| Windows | `%APPDATA%\ipmsg-torrent\config.toml` |

The config file is created automatically on first run with sensible defaults.

### Configuration Options

```toml
[network]
# TCP/UDP port to listen on (0 = random available port)
port = 0

# Bootstrap nodes for initial peer discovery
# Format: /ip4/{addr}/udp/{port}/quic-v1/p2p/{peer_id}
bootstrap_nodes = [
    "/dns4/bootstrap1.libp2p.io/tcp/4001/p2p/QmNnooDu7bfjPFoVaZY5cukYfR3oKQeRgZp3zWzrKzGVyP",
    "/dns4/bootstrap2.libp2p.io/tcp/4001/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
]

# Enable mDNS for automatic LAN peer discovery
enable_mdns = true

# Enable relay client for NAT traversal
enable_relay = true

# Connection timeout in seconds
connection_timeout = 30

[storage]
# Data directory (messages, keys, downloads)
# Linux default: ~/.local/share/ipmsg-torrent/
data_dir = "~/.local/share/ipmsg-torrent"

# Maximum message history to keep in memory
max_history = 10000

# Enable message deduplication via Bloom filter
enable_dedup = true

[ui]
# Default username
username = "Anonymous"

# Theme: "dark" or "light"
theme = "dark"

# Show timestamps next to messages
show_timestamps = true

# Show peer IDs in chat (useful for debugging)
show_peer_ids = false

[download]
# Maximum concurrent downloads
max_concurrent = 5

# Download directory
download_dir = "~/.local/share/ipmsg-torrent/downloads"

# Enable P2SP multi-source download (HTTP + P2P hybrid)
enable_p2sp = true
```

### Environment Variables

Environment variables override config file values:

| Variable | Description |
|----------|-------------|
| `IPMSG_PORT` | Override listening port |
| `IPMSG_USERNAME` | Override username |
| `IPMSG_DATA_DIR` | Override data directory path |

### Network Settings

**Ports:**
- **TCP/UDP 4001** — Default libp2p transport (configurable via `network.port`)
- **UDP 2425** — Legacy IPMSG/FeiQ compatibility (fixed)

**Peer Discovery:**
- **mDNS** — Automatic LAN discovery via `_ipmsg._udp.local.` multicast
- **Kademlia DHT** — WAN peer discovery via bootstrap nodes
- **Bootstrap interval** — 300 seconds between DHT refresh cycles

**NAT Traversal:**
- **Relay** — Circuit relay through intermediate peers
- **DCUtR** — Direct Connection Upgrade through Relay (hole punching)
- **AutoNAT** — Automatic NAT type detection

## Security Model

### Encryption

All peer-to-peer communication is encrypted using the **Noise Protocol Framework**:

- **Handshake**: Noise XX pattern (3-message mutual authentication)
- **Cipher suite**: `Noise_XX_25519_ChaChaPoly_SHA256`
  - **Key exchange**: X25519 (Curve25519 Diffie-Hellman)
  - **Symmetric encryption**: ChaCha20-Poly1305 (AEAD)
  - **Hash**: SHA-256
- **Automatic re-keying**: Sessions re-key after a configurable message threshold
- **Forward secrecy**: Compromised long-term keys don't reveal past session keys

### Message Signing

All messages are cryptographically signed using **Ed25519**:

- Each node generates a unique Ed25519 keypair on first run
- Keys are persisted in the data directory (`identity.key`)
- Every message includes a signature verifiable by any peer
- PeerID is derived from the public key (libp2p identity)

### Trust Model

**Peer Verification:**
- Peers are identified by their PeerID (derived from Ed25519 public key)
- Fingerprints can be verified out-of-band for high-security scenarios
- Blocked peers are stored locally and excluded from all communication

**Social Trust Features:**
- `/block <peer>` — Block a peer (prevents all communication)
- `/favorite <peer>` — Mark trusted peers as favorites
- Fingerprint verification for identity confirmation

### Privacy Features

**Traffic Analysis Resistance:**
- Fixed-size padding (256–2048 bytes) on all messages
- Prevents observers from determining message length
- Random padding size within range for additional obfuscation

**Additional Privacy:**
- No central server — fully decentralized
- No message persistence on relay nodes
- Local SQLite storage with WAL mode (no remote sync)

## Project Structure

```
Cargo.toml              Workspace root
crates/
  protocol/src/         Message types & CBOR codec
  core/src/
    lib.rs              P2PEngine orchestrator
    transport.rs        libp2p Swarm & NetworkBehaviour
    identity.rs         Ed25519 key management
    noise.rs            Noise XX E2E encryption
    store.rs            SQLite store (native) / in-memory (WASM)
    file_transfer.rs    Torrent-style chunked transfer
    file_sharing.rs     File sharing manager
    fragment.rs         Message fragmentation + LZ4 + padding
    ipmsg_compat.rs     Classic IPMSG/FeiQ protocol (native only)
    bloom.rs            Bloom filter dedup cache
    discovery.rs        Bootstrap & mDNS constants
    messaging.rs        Topic constants & peer info
    config.rs           Configuration management
    scoring.rs          Peer scoring/reputation
    stats.rs            Network statistics
  cli/src/main.rs       TUI client (ratatui + crossterm)
  app/
    src-tauri/src/      Tauri backend
    dist/               Frontend (HTML/CSS/JS)
  wasm/src/             WASM bindings
  download/src/         Multi-protocol download engine
```

## Development Guide

### Running Tests

```bash
# Run all tests
cargo test --workspace

# Run tests for a specific crate
cargo test -p ipmsg-core
cargo test -p ipmsg-cli

# Run with output
cargo test -- --nocapture

# Run specific test
cargo test -p ipmsg-core test_handshake_roundtrip
```

### Code Style

- **Formatter**: `cargo fmt` (rustfmt with default settings)
- **Linter**: `cargo clippy --workspace -- -D warnings`
- **Edition**: Rust 2024
- **Error handling**: Use `thiserror` for library errors, `anyhow` for applications
- **Async**: Tokio runtime with `async/await`

### Code Conventions

- Module documentation with `//!` at the top of each file
- Public APIs documented with `///` and examples where helpful
- Unit tests in `#[cfg(test)] mod tests` within each module
- Integration tests in `crates/*/tests/`
- Use `tracing` for structured logging

### Adding a New Feature

1. Create a branch: `git checkout -b feature/my-feature`
2. Implement in the appropriate crate (`core` for protocol, `cli` for UI, etc.)
3. Add tests for new functionality
4. Run `cargo fmt` and `cargo clippy`
5. Submit a pull request

### Architecture Decisions

- **CBOR over JSON**: Compact binary format, faster parsing, smaller messages
- **SQLite over flat files**: Concurrent access, transactions, full-text search
- **libp2p over custom networking**: Battle-tested P2P primitives, NAT traversal
- **Noise over TLS**: Simpler handshake, better for P2P, forward secrecy by default
- **Ed25519 over RSA**: Smaller keys, faster signatures, modern cryptography

## Advanced Usage

### Setting Up a Private Network

To create a private chat network with custom bootstrap nodes:

1. **Generate a bootstrap node identity**:
   ```bash
   # On the bootstrap node machine
   cargo run -p ipmsg-cli -- --username bootstrap-node
   # Note the PeerID from startup output
   ```

2. **Configure other nodes to use your bootstrap**:
   ```toml
   # config.toml for all peers in your private network
   [network]
   bootstrap_nodes = [
       "/ip4/192.168.1.100/udp/4001/quic-v1/p2p/12D3KooW..."
   ]
   enable_mdns = true  # Still useful for LAN discovery
   ```

3. **Disable relay for full isolation** (optional):
   ```toml
   [network]
   enable_relay = false
   ```

### Integrating with Existing IPMSG Clients

IPMsg-Torrent is compatible with classic IPMSG and FeiQ clients:

```bash
# Start IPMSG compatibility mode
/ipmsg

# This enables UDP port 2425 listener
# Classic IPMSG/FeiQ clients on the LAN will see you automatically
```

**Compatibility notes:**
- Receives and sends messages in IPMSG UDP format
- File attachments work with both protocols
- Group messages are not supported in IPMSG compat mode
- The IPMSG protocol uses plaintext (no encryption) — use native mode for E2E

### Custom Bootstrap Nodes

Add your own relay/bootstrap nodes:

```toml
[network]
bootstrap_nodes = [
    # Your public relay node
    "/ip4/203.0.113.1/udp/4001/quic-v1/p2p/12D3KooW...",
    # Backup node (TCP transport also supported)
    "/dns4/relay.example.com/tcp/4001/p2p/12D3KooW...",
]
```

**Running a public bootstrap node:**

```bash
# On your public server
cargo run -p ipmsg-cli -- --username relay-node --port 4001

# Ensure port 4001 TCP+UDP is open in firewall
# Share the multiaddr with your network
```

### File Sharing Workflows

**Share a file with the network:**

```bash
/share /path/to/document.pdf
# File is chunked (256KB pieces), hashed, and announced to peers
# Others can discover it via /search
```

**Search and download:**

```bash
/search document
# Returns: HASH  document.pdf  (2.4 MB, 3 peers seeding)

/download <hash>
# Downloads via torrent-style chunked transfer
# Supports resume if interrupted
```

**Multi-protocol downloads:**

The download engine supports multiple protocols simultaneously:

```bash
# BitTorrent
cargo run -p ipmsg-cli -- download "magnet:?xt=urn:btih:..."

# HTTP/FTP
cargo run -p ipmsg-cli -- download "https://example.com/file.zip"

# ed2k
cargo run -p ipmsg-cli -- download "ed2k://|file|...|"
```

## Troubleshooting

### Common Issues

**"No peers found"**

- Ensure port 4001 (TCP+UDP) is not blocked by firewall
- Check that mDNS is enabled (`enable_mdns = true`)
- Verify bootstrap nodes are reachable
- On LAN, peers should discover automatically within seconds

**"Connection refused" or "Handshake failed"**

- Check that the remote peer is running the same protocol version
- Verify the multiaddr format is correct
- Ensure both peers have relay enabled if behind NAT

**"Messages not delivered"**

- Check rate limiting: max 10 messages per 5 seconds per peer
- Verify the recipient hasn't blocked you
- Check network connectivity with `/peers`

**"Database locked" errors**

- SQLite WAL mode allows concurrent reads but only one writer
- Ensure no other instance is using the same data directory
- Check file permissions on the data directory

### Network Connectivity

**Diagnosing connection issues:**

```bash
# Check listening ports
ss -tlnp | grep ipmsg  # Linux
netstat -an | grep 4001  # macOS/Windows

# Test bootstrap node connectivity
curl -v /ip4/<bootstrap-ip>/udp/4001/quic-v1  # Should timeout, not refuse

# Enable verbose logging
RUST_LOG=debug cargo run -p ipmsg-cli -- --username alice
```

**Firewall configuration:**

```bash
# Linux (ufw)
sudo ufw allow 4001/tcp
sudo ufw allow 4001/udp
sudo ufw allow 2425/udp  # For IPMSG compatibility

# Linux (firewalld)
sudo firewall-cmd --add-port=4001/tcp --permanent
sudo firewall-cmd --add-port=4001/udp --permanent
sudo firewall-cmd --add-port=2425/udp --permanent
sudo firewall-cmd --reload
```

### Performance Tuning

**For high-throughput file transfers:**

```toml
[download]
max_concurrent = 10  # Increase parallel downloads
enable_p2sp = true   # Enable multi-source downloads
```

**For low-bandwidth environments:**

```toml
[network]
connection_timeout = 60  # Longer timeout for slow connections

[download]
max_concurrent = 2  # Limit parallel downloads
```

**Reducing resource usage:**

```toml
[storage]
max_history = 1000  # Keep fewer messages in memory
enable_dedup = true  # Bloom filter reduces duplicate processing
```

### Debugging

Enable detailed logging:

```bash
# Debug level
RUST_LOG=debug cargo run -p ipmsg-cli

# Trace level (very verbose)
RUST_LOG=trace cargo run -p ipmsg-cli

# Filter to specific module
RUST_LOG=ipmsg_core::noise=debug cargo run -p ipmsg-cli
```

## Platform Support

| Platform | Status | Notes |
|----------|--------|-------|
| Linux | ✅ Full support | x86_64, aarch64 |
| macOS | ✅ Full support | Intel, Apple Silicon |
| Windows | ✅ Full support | x86_64 |
| Android | ✅ Tauri mobile | Via `crates/app/src-tauri` |
| iOS | 🔄 Experimental | Via Tauri (not yet tested) |
| Browser (WASM) | ✅ Core P2P | No SQLite persistence (in-memory only) |

## License

This project is licensed under the **GNU General Public License v3.0 or later (GPL-3.0-or-later)**.

See [LICENSE](../LICENSE) for the full text.

## Related Projects & Inspiration

IPMsg-Torrent draws inspiration from and is related to the following projects:

### P2P Chat / Messaging

| Project | Description | What we learned |
|---------|-------------|------------------|
| [bitchat](https://github.com/bitchat/bitchat) | Bluetooth mesh chat with Noise XX encryption, Bloom filter dedup, fixed-size padding | Noise XX handshake, Bloom filter dedup, message fragmentation, padding for traffic analysis resistance |
| [qaul.net](https://github.com/qaul/qaul.net) | Internet-independent wireless mesh communication app (Rust) | Mesh networking patterns, offline-first design |
| [iroh-messenger](https://github.com/n0-computer/iroh) | Rust P2P chat using iroh (QUIC-based) | QUIC transport, gossip protocols |
| [Iron Messenger](https://github.com/anthdm/iron-messenger) | Rust CLI P2P chat using iroh-gossip | Terminal UI patterns, gossipsub usage |
| [tor-chat](https://github.com/) | E2E-encrypted (MLS) chat over Tor onion services (Rust + Tauri 2) | Tauri 2 architecture, MLS encryption concepts |
| [PKARR chat](https://github.com/) | Decentralized P2P chat with Rust and PKARR | Censorship-resistant peer discovery |

### libp2p Ecosystem

| Project | Description | What we learned |
|---------|-------------|------------------|
| [rust-libp2p](https://github.com/libp2p/rust-libp2p) | The Rust implementation of the libp2p networking stack | Core transport, swarm, NetworkBehaviour patterns |
| [IPFS / Filecoin](https://github.com/ipfs/ipfs) | Content-addressed storage and P2P file sharing | Torrent-style chunked transfer, DHT usage |
| [Locutus](https://github.com/freenet/locutus) | Global decentralized key-value store (Rust) | Contract-based state management |
| [Substrate](https://github.com/paritytech/substrate) | Blockchain framework built on libp2p | Grandpa/BABE consensus, peer scoring |

### IPMSG / FeiQ Compatibility

| Project | Description | What we learned |
|---------|-------------|------------------|
| [IPMsg for Android](https://github.com/) | Android IPMSG client compatible with FeiQ | UDP broadcast protocol, file attachment handling |
| [CodeMsg](https://github.com/) | VS Code extension with IPMsg compatibility | IDE integration patterns |
| Original IPMSG (ipmsg.org) | The classic Windows LAN messenger by H.Bito | The UDP protocol format (VERSION:PACKET_NO:USER:HOST:CMD_NO:EXTRA) |

### Technologies Used

- **[libp2p](https://libp2p.io/)** — P2P networking stack (transport, discovery, pubsub)
- **[Noise Protocol](https://noiseprotocol.org/)** — E2E encryption (XX handshake, ChaCha20-Poly1305)
- **[CBOR](https://cbor.io/)** — Compact binary serialization
- **[Tauri](https://tauri.app/)** — Cross-platform desktop/mobile apps
- **[Ed25519](https://ed25519.cr.yp.to/)** — Digital signatures
- **[SQLite](https://www.sqlite.org/)** — Local message storage
