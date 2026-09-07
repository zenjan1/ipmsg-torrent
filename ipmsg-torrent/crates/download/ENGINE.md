# Download Engine Architecture

## Overview

The ipmsg-torrent download engine is a multi-protocol download manager supporting:
- **BitTorrent** (.torrent files, magnet links)
- **eDonkey/eMule** (ed2k:// links)
- **Xunlei P2SP** (HTTP/FTP with P2P acceleration)
- **Direct HTTP/HTTPS/FTP** URLs

Built as a single crate (`ipmsg-download`) with 142+ modules, the engine provides a unified `DownloadManager` that orchestrates protocol-specific engines, shared infrastructure (connection pooling, bandwidth control, progress tracking), and a rich feature layer (scheduling, analytics, automation, dashboards).

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
├── error_recovery.rs   # Error classification and auto-recovery
├── dynamic_priority.rs # Multi-factor priority adjustment
├── retry_quota.rs      # Daily retry budget
├── bandwidth_schedule.rs # Time-based speed limit rules
├── priority_queue.rs   # Priority-aware task queue
├── dashboard.rs        # Real-time dashboard snapshots
└── ... (140+ modules for scheduling, analytics, automation, etc.)
```

## Multi-Protocol Architecture

The engine uses a **strategy pattern** where `DownloadManager` holds protocol-specific engine instances and delegates protocol-level operations to them. Each engine implements a common lifecycle:

```
                    ┌─────────────────────┐
                    │   DownloadManager   │
                    │  (orchestrator)     │
                    └─────────┬───────────┘
                              │
            ┌─────────────────┼─────────────────┐
            ▼                 ▼                  ▼
   ┌────────────────┐ ┌──────────────┐ ┌────────────────┐
   │ TorrentEngine  │ │  Ed2kEngine  │ │ XunleiEngine   │
   │ (BitTorrent)   │ │  (eDonkey)   │ │ (P2SP hybrid)  │
   └────────────────┘ └──────────────┘ └────────────────┘
            │                 │                  │
            └─────────────────┼──────────────────┘
                              ▼
                   ┌─────────────────────┐
                   │  Shared Infra       │
                   │  • ConnectionPool   │
                   │  • ProgressTracker  │
                   │  • BandwidthControl │
                   │  • ErrorRecovery    │
                   │  • AdaptiveConcurrency │
                   └─────────────────────┘
```

### Protocol Dispatch

When a download is added, `DownloadManager` inspects the input and routes to the correct engine:

| Input Format | Protocol | Engine |
|---|---|---|
| `.torrent` file path | BitTorrent | `TorrentEngine` |
| `magnet:?xt=urn:btih:...` | BitTorrent (metadata fetch) | `TorrentEngine` |
| `ed2k://|file|...` | eDonkey | `Ed2kEngine` |
| Xunlei sources (HTTP/FTP + P2P) | P2SP | `XunleiEngine` |
| Plain `http(s)://` or `ftp://` URL | Direct HTTP | `XunleiEngine` (single-source mode) |

## Protocol Engine Details

### TorrentEngine

Handles BitTorrent protocol downloads with full piece management and peer coordination.

**Core mechanics:**
- **Piece selection**: Rarest-first (default) or sequential (for streaming playback)
- **Block size**: 16KB blocks (BitTorrent standard), pieces typically 256KB–4MB
- **Peer wire protocol**: Handshake → bitfield exchange → piece requests → verification
- **Peer scoring**: Composite score based on throughput, response latency, and reliability history
- **Endgame mode**: Activates when <5% of pieces remain — requests outstanding pieces from all available peers to avoid tail stalls
- **Multi-tracker**: Supports `announce-list` tiers with fallback between tracker tiers
- **File selection**: Download specific files from multi-file torrents without fetching the entire content
- **DHT support**: Distributed Hash Table for trackerless torrents (module `dht/`)
- **Metadata fetch**: For magnet links, fetches `.torrent` metadata from the swarm before downloading content

**Resume**: Progress bitmap persisted to `<download_dir>/.<filename>.progress` — on restart, completed pieces are verified against piece hashes and skipped.

### Ed2kEngine

Implements the eDonkey2000/eMule protocol for downloading from the ed2k network.

**Core mechanics:**
- **Chunk size**: 9.28MB chunks (eDonkey standard), each hashed with MD4
- **Server workflow**: Connect → login → receive server list → search/get source list → download from peers
- **Peer exchange**: Sources shared between connected peers (source sharing protocol)
- **Server cache**: Persistent server list in `ed2k_servers.json` — survives restarts
- **Peer cache**: Persistent peer list in `ed2k_peers.json` — reconnect to known good peers
- **Chunk-level resume**: Each 9.28MB chunk tracked independently; partial chunks re-downloaded from the correct offset

**Protocol messages** (defined in `ed2k/protocol.rs`):
- Server login/identification
- Search requests (filename, hash, size filters)
- Get source list (peer discovery)
- Peer-to-peer chunk request/response
- Callback requests for firewalled clients

### XunleiEngine

Implements Xunlei's P2SP (Peer-to-Server-and-Peer) protocol — a multi-source segmented download approach that combines HTTP/FTP mirrors with P2P peers.

**Core mechanics:**
- **Dynamic segmentation**: Files split into segments sized adaptively based on available bandwidth and source count
- **Multi-source parallel**: Each segment can be downloaded from a different HTTP/FTP mirror or P2P peer simultaneously
- **Mirror discovery**: Automatic fallback URL detection — when one mirror is slow or dead, the engine tries alternatives
- **Source quality scoring**: Long-term reliability tracking per domain/mirror (success rate, average speed, uptime)
- **Buffered I/O**: Pre-allocated output file with buffered writes — reduces fragmentation and small-write overhead
- **P2P hybrid**: When P2P peers are available for a file, they supplement HTTP sources for additional throughput

**Source types** (defined in `xunlei/protocol.rs`):
- `HttpSource` — direct HTTP/HTTPS mirror
- `FtpSource` — FTP mirror with credential support
- `P2PSource` — peer from P2P network

## Download Task State Machine

Every download task progresses through a well-defined state machine:

```
                    add_torrent/add_url/add_ed2k
                              │
                              ▼
                        ┌──────────┐
                        │ Queued   │◄─────── pause (from Downloading)
                        └────┬─────┘              ▲
                             │ start              │
                             ▼                    │
                        ┌───────────┐    pause    │
                   ┌───►│Downloading ├────────────┘
                   │    └─────┬─────┘
                   │          │
                   │    ┌─────┼──────────┐
                   │    │     │          │
                   │    ▼     ▼          ▼
                   │ ┌────────┐ ┌──────────┐
                   │ │Complete│ │  Error   │──── retry ────┐
                   │ └────────┘ └────┬─────┘               │
                   │                 │                      │
                   │          auto-recover                  │
                   │                 │                      │
                   │                 ▼                      │
                   │          ┌───────────┐                │
                   │          │ Cooldown  │────────────────┘
                   │          └───────────┘  (backoff expired)
                   │
                   └── (resume from Complete = re-verify)
```

### State Definitions

| State | Description |
|---|---|
| `Queued` | Task added, waiting for a slot (respects `max_concurrent_downloads`) |
| `Downloading` | Active download in progress |
| `Paused` | User-paused or auto-paused; progress persisted, resources released |
| `Complete` | All data verified and written to disk |
| `Error` | Failed with an error; classified by `ErrorCategory` for recovery |

### Task Lifecycle

1. **Creation**: Input parsed → protocol detected → engine selected → `DownloadTask` struct created with unique ID
2. **Queueing**: Task enters priority queue; respects concurrency limits and bandwidth schedule
3. **Execution**: Engine allocates connections via `ConnectionPool`, begins segmented/piece download
4. **Progress tracking**: Bitmap updated on each piece/chunk completion; persisted periodically
5. **Completion**: Final integrity verification → post-download hooks triggered → notification sent
6. **Cleanup**: Optional auto-cleanup rules apply (move files, remove from list, etc.)

### DownloadTask Structure

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
    // ... 30+ fields covering:
    //   retry policy, proxy config, scheduling,
    //   speed limits, auto-actions, deadlines, etc.
}
```

## Bandwidth Management & Priority Scheduling

### Global Speed Control

```rust
// Global limit (applies to all tasks combined)
pub async fn set_global_speed_limit(bytes_per_sec: u64)

// Per-task limit (individual task cap)
pub async fn set_task_speed_limit_per_task(task_id: &str, limit: Option<u64>)

// Effective limit = min(global, per_task, schedule_rule)
pub async fn get_effective_speed_limit() -> Option<u64>
```

Bandwidth is distributed across active tasks using a **weighted fair queuing** algorithm — tasks with higher priority get proportionally more bandwidth, but every active task gets a minimum share to prevent starvation.

### Time-Based Bandwidth Scheduling

The `BandwidthScheduleManager` applies automatic speed limits based on time-of-day rules:

```rust
pub struct BandwidthScheduleRule {
    pub id: String,
    pub name: String,
    pub start_hour: u32,        // 0-23
    pub start_minute: u32,      // 0-59
    pub end_hour: u32,
    pub end_minute: u32,
    pub speed_limit_bps: u64,   // 0 = unlimited
    pub days_of_week: Vec<Weekday>,
    pub priority: i32,          // higher = evaluated first
    pub enabled: bool,
}
```

Rules are evaluated in priority order; the first matching rule is applied. Example use cases:
- Limit to 1 MB/s during work hours (9am–6pm weekdays)
- Unlimited during night hours (10pm–8am)
- 5 MB/s during evening hours

### Dynamic Priority Adjustment

The `DynamicPriorityManager` automatically adjusts task priorities based on a composite score from multiple factors:

| Factor | Effect | Weight (default) |
|---|---|---|
| Speed performance | Slow tasks → lower priority | 1.0 |
| Wait time | Long-waiting tasks → boosted | 1.0 |
| Progress % | Near-complete tasks → boosted | 1.0 |
| Retry count | Frequently failing → lowered | 1.0 |
| File size | Small files → boosted (clear queue faster) | 1.0 |

Each factor's weight is configurable via `FactorWeights`. This works alongside the static `DownloadPriority` (Low/Normal/High) set by the user — dynamic adjustment modulates but does not override user intent.

### Priority Queue

The `PriorityQueue` module provides a heap-based scheduling structure:
- Tasks sorted by effective priority (static × dynamic adjustment)
- FIFO within the same priority level (fairness)
- Supports priority promotion/demotion without full re-sort

### Speed Burst & Speed Boost

Two mechanisms for temporary speed increases:

- **Speed Burst**: Per-task temporary speed limit increase (time-bounded)
- **Speed Boost**: Global temporary override — lifts or raises the global limit for all tasks

Both support presets (named configurations) and scheduled boost windows.

## Error Recovery & Retry Mechanism

### Error Classification

The `ErrorRecovery` module classifies errors into categories and applies targeted recovery strategies:

| Category | Detection Signals | Recovery Strategy |
|---|---|---|
| `Network` | DNS failure, connection refused/reset, timeout, unreachable | Exponential backoff retry |
| `Disk` | No space left, permission denied, file locked | Pause task, alert user |
| `Authentication` | HTTP 401/403, invalid credentials | Pause task, request credential update |
| `Server` | HTTP 5xx, service unavailable | Retry with backoff, try mirror |
| `NotFound` | HTTP 404, file removed | Mark error, no auto-retry |
| `RateLimited` | HTTP 429, too many requests | Respect Retry-After header, cooldown |
| `Certificate` | SSL/TLS handshake failure, invalid cert | Pause task, alert user |
| `Protocol` | Invalid torrent, bad ed2k link, malformed data | Mark error, no auto-retry |
| `Unknown` | Unclassified | Generic retry with backoff |

### Retry Quota System

The `RetryQuotaManager` prevents runaway retries from consuming resources:

```rust
pub struct RetryQuotaConfig {
    pub enabled: bool,              // default: false
    pub max_retries_per_day: u32,   // default: 100
    pub window_secs: u64,           // default: 86400 (24h rolling window)
}
```

When the quota is exhausted, failed tasks remain in `Error` state until:
- The rolling window slides enough to free a retry slot
- The user manually resets the quota
- The next calendar day (automatic rollover)

### Cooldown & Backoff

The `DownloadCooldown` module implements intelligent retry timing:
- **Exponential backoff**: Each consecutive failure doubles the wait time
- **Jitter**: Random variation prevents thundering herd when many tasks fail simultaneously
- **Category-aware**: Network errors back off; disk/auth errors pause immediately (no point retrying)
- **Cooldown state**: Task enters a `Cooldown` sub-state between retries, visible in the dashboard

### Integrity Verification

The `IntegrityVerification` module validates downloaded data:
- BitTorrent: SHA-1 hash per piece (16KB blocks)
- Ed2k: MD4 hash per 9.28MB chunk
- HTTP/Xunlei: Optional Content-Length and ETag validation
- On mismatch: corrupted piece/chunk re-downloaded from a different source when possible

## Performance Optimizations

### Adaptive Concurrency

The `AdaptiveConcurrency` module automatically tunes connection count per task:

```rust
pub struct AdaptiveConcurrencyConfig {
    pub enabled: bool,               // default: true
    pub min_connections: u32,        // default: 1
    pub max_connections: u32,        // default: 16
    pub initial_connections: u32,    // default: 4
    pub target_response_ms: u64,     // default: 200ms
    pub high_latency_threshold_ms: u64, // default: 1000ms
    pub error_rate_threshold: f64,   // default: 0.1 (10%)
    pub sample_window: u32,          // default: 10 samples
    pub adjustment_cooldown_secs: u64, // default: 30s
    pub increase_factor: f64,        // default: 1.5×
    pub decrease_factor: f64,        // default: 0.7×
}
```

**Algorithm:**
1. **EWMA smoothing**: Response times smoothed with Exponentially Weighted Moving Average
2. **BBR-inspired estimation**: Tracks minimum RTT as baseline, estimates available bandwidth from best recent samples
3. **Increase**: When smoothed RTT < target and error rate < threshold → multiply connections by `increase_factor`
4. **Decrease**: When RTT > high_latency_threshold or error rate > threshold → multiply by `decrease_factor`
5. **Hysteresis**: Minimum cooldown between adjustments prevents oscillation
6. **Per-domain limiting**: Tracks concurrency per domain to avoid overloading a single server

### Connection Pool

The `ConnectionPool` module provides TCP connection reuse:
- **Keep-alive**: HTTP connections reused across segments from the same host
- **DNS caching**: Resolved addresses cached to avoid repeated lookups
- **Pre-connect**: Speculative connection establishment for upcoming segments
- **TCP parameter tuning**: Socket buffer sizes, TCP_NODELAY, keepalive intervals

### Dynamic Block Sizing

For Xunlei/HTTP downloads, segment sizes adapt to conditions:
- High bandwidth → larger segments (fewer HTTP requests, less overhead)
- Low bandwidth → smaller segments (finer-grained source switching)
- Adjusted per-source based on individual mirror throughput

### Buffered I/O

- Pre-allocated output files (avoid fragmentation)
- Write coalescing: small piece completions buffered before disk flush
- Async I/O via Tokio for non-blocking disk operations

### Additional Performance Modules

| Module | Purpose |
|---|---|
| `speed_history` | Track speed over time for trend analysis |
| `speed_trend` | Detect improving/degrading download speeds |
| `speed_benchmark` | Periodic speed tests to measure available bandwidth |
| `speed_anomaly` | Detect sudden speed drops (possible throttling) |
| `speed_heatmap` | Visualize speed across time segments |
| `completion_probability` | Estimate likelihood of task completing within deadline |
| `eta_estimator` | Estimated time remaining based on recent throughput |
| `progress_prediction` | Predict final completion time |
| `intelligent_source_selector` | Score and rank download sources |
| `mirror_health` | Monitor mirror availability and response times |
| `network_monitor` | Track overall network conditions |
| `network_aware` | Adapt behavior to network type (WiFi/cellular/metered) |

## Resume Support

Progress is persisted using a custom binary format (v1):

```
| magic (4B) | version (1B) | file_hash (20B) | file_size (8B) |
| piece_size (8B) | total_pieces (4B) | bitmap_len (4B) |
| bitmap (N bytes) | downloaded (8B) |
```

File path: `<download_dir>/.<filename>.progress`

On resume, the engine:
1. Loads the bitmap from the progress file
2. Verifies the file hash matches (detects if the source file changed)
3. Skips completed pieces/chunks
4. Re-validates partial pieces against their hash
5. Resumes from the next incomplete piece

## Dashboard & Monitoring

The engine provides multiple monitoring surfaces:

- **`DashboardSnapshot`**: Real-time aggregate of all task states, speeds, and resource usage
- **`HealthDashboard`**: Task health scores combining speed, progress, and error history
- **`QueueHealth`**: Queue-level metrics — wait times, throughput, bottleneck detection
- **`DownloadAnalytics`**: Historical analytics — daily/weekly throughput, success rates, popular sources
- **`DownloadStats`**: Per-task and aggregate statistics

## Extension Points

### Post-Download Hooks (`post_hooks`)

Run arbitrary scripts or commands after a download completes:
```rust
// Configure via DownloadManager
pub async fn add_post_hook(hook: PostHook) -> String
```

### Automation Rules (`automation_rules`)

IFTTT-style triggers: "when download completes → do action":
- Auto-categorize based on file type/size/name patterns
- Auto-move files to organized directories (`path_organizer`, `path_rules`)
- Auto-delete torrents after completion
- Trigger external scripts or webhooks

### Webhooks (`event_webhook`)

HTTP notifications on download events:
- Task started, paused, completed, failed
- Configurable URL, headers, retry policy
- Payload includes task metadata and event details

### RSS Feed Auto-Import (`rss_feed`)

Monitor RSS/Atom feeds and automatically download matching content:
- Filter by title pattern, size range, file type
- Polling interval configurable per feed
- Deduplication against download history

### Watch Folders

Monitor directories for new `.torrent`, `.magnet`, or link files:
- Automatic import on file creation
- Configurable scan interval
- Processed files moved to archive or deleted

### Download Templates & Presets

- **Templates** (`download_templates`): Pre-configured download settings (speed limit, save path, priority)
- **Presets** (`download_presets`): Named configurations applicable to tasks
- Apply presets to existing tasks to change their behavior

### Custom Protocol Interface

To add a new protocol engine:

1. Create a module under `src/<protocol>/`
2. Implement engine struct with methods for:
   - `start()` — begin downloading
   - `pause()` — pause and release resources
   - `resume()` — resume from last progress
   - `status()` — return current progress/speed/state
   - `cancel()` — abort and clean up partial files
3. Add a variant to `DownloadProtocol` enum
4. Register the engine in `DownloadManager::new_with_restore()`
5. Add dispatch logic in the task creation path

The engine should use `ConnectionPool` for TCP connections and `ProgressTracker` for resume support to stay consistent with existing protocols.

## Testing

```bash
# Unit tests (in-module)
cargo test -p ipmsg-download

# Integration tests
cargo test -p ipmsg-download --test integration_test
cargo test -p ipmsg-download --test progress_test

# Specific test
cargo test -p ipmsg-download test_progress_save_and_load

# Compile check
cargo check --lib -p ipmsg-download
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
- **Speed control**: Global limit, per-task limit, burst/boost presets
- **Scheduling**: Bandwidth schedule rules, time-limited downloads, TTL
- **Concurrency**: Max concurrent downloads, adaptive concurrency tuning
- **Retry policy**: Retry quota, cooldown backoff, error recovery strategies
- **Storage**: Save path rules, disk space monitoring, auto-cleanup
- **Proxy**: HTTP/SOCKS proxy configuration per-task or global
- **Automation**: Post-hooks, RSS feeds, watch folders, webhooks
- **Budget**: Download quotas, data caps, cost tracking
- **Notifications**: Event notifications, milestone alerts, completion sounds
