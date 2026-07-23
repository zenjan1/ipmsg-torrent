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

## Quick Start

### Prerequisites

- Rust 1.85+ (edition 2024)
- For Tauri app: [Tauri CLI](https://tauri.app/start/prerequisites/)

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
  cli/src/main.rs       TUI client
  app/
    src-tauri/src/      Tauri backend
    dist/               Frontend (HTML/CSS/JS)
  wasm/src/             WASM bindings
```

## Platform Support

| Platform | Status |
|----------|--------|
| Linux | ✅ Full support |
| macOS | ✅ Full support |
| Windows | ✅ Full support |
| Android | ✅ Tauri mobile (via src-tauri) |
| Browser (WASM) | ✅ Core P2P (no SQLite persistence) |

## License

MIT
