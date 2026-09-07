# API 使用指南

本文档介绍 ipmsg-torrent 的核心公共 API，包括 P2P 引擎和下载管理器。

## 目录

- [P2PEngine](#p2pengine)
- [DownloadManager](#downloadmanager)
- [事件系统](#事件系统)
- [常见用例](#常见用例)

---

## P2PEngine

P2PEngine 是 P2P 网络的核心引擎，负责消息收发、文件共享、节点发现等。

### 创建与启动

```rust
use ipmsg_core::{P2PEngine, P2PEvent, SendCommand};
use std::path::PathBuf;

// 创建引擎实例
let mut engine = P2PEngine::new(PathBuf::from("~/.local/share/ipmsg-torrent"))?;

// 启动引擎（连接到网络）
let peer_id = engine.start(
    "alice".to_string(),           // 用户名
    vec![                          // 引导节点（可选）
        "/dns4/bootstrap1.libp2p.io/tcp/4001/p2p/QmNnooDu...".to_string(),
    ],
    4001,                          // 监听端口（0 = 随机）
).await?;

println!("本地 Peer ID: {}", peer_id);
```

### 主要方法

#### 消息发送

```rust
// 发送私聊消息
engine.send_text(&peer_id, "你好！").await?;

// 发送到频道
use ipmsg_protocol::message::ChannelId;
engine.send_to_channel(&ChannelId::Named("general".into()), "大家好").await?;

// 广播到所有节点
engine.broadcast("系统公告".to_string()).await?;
```

#### 节点管理

```rust
// 获取已连接节点列表
let peers = engine.list_peers();
for peer in peers {
    println!("{} ({}) - {:?}", peer.peer_id, peer.username, peer.platforms);
}

// 获取聊天历史
let history = engine.get_history(&peer_id, 50);

// 获取频道历史
let channel_history = engine.get_channel_history("general", 100);
```

#### 频道管理

```rust
// 加入频道
engine.add_channel(ChannelId::Named("general".into()));

// 离开频道
engine.remove_channel(&ChannelId::Named("general".into()));

// 获取已加入的频道列表
let channels = engine.joined_channels();
```

#### 社交信任

```rust
// 屏蔽节点
engine.block_peer(&peer_id);

// 取消屏蔽
engine.unblock_peer(&peer_id);

// 检查是否已屏蔽
if engine.is_blocked(&peer_id) { /* ... */ }

// 收藏节点
engine.mark_favorite(&peer_id);

// 获取指纹（用于带外验证）
let fingerprint = engine.my_fingerprint();
```

#### 文件共享

```rust
use std::path::PathBuf;

// 共享文件
engine.share_file(
    PathBuf::from("/path/to/file.zip"),
    vec!["archive".into(), "tools".into()],  // 标签
    Some("一个有用的工具".to_string()),        // 描述
).await?;

// 取消共享
engine.unshare_file("file_hash_here").await?;

// 搜索文件
engine.search_files("tool", &[]).await?;

// 列出所有共享文件
engine.list_files().await?;

// 从节点下载文件
engine.download_file("file_hash", "peer_id").await?;
```

#### 统计信息

```rust
// 获取网络统计摘要
let summary = engine.stats_summary();

// 获取管理器引用
let scores = engine.peer_scores();
let stats = engine.stats();
let sharing = engine.file_sharing();
```

#### IPMSG 兼容（仅限 native 平台）

```rust
// 启动经典 IPMSG 兼容服务器（UDP 2425 端口）
engine.start_ipmsg_compat().await?;

// 向经典 IPMSG 节点发送消息
use std::net::IpAddr;
engine.send_ipmsg_message("192.168.1.100".parse()?, "你好").await?;
```

### 事件接收

```rust
// 方式一：逐个接收事件
loop {
    if let Some(event) = engine.next_event().await {
        handle_event(event);
    }
}

// 方式二：取出 receiver 自行处理
let rx = engine.take_receiver();
// 在另一个 task 中 recv()

// 方式三：取出 command sender 发送命令
let cmd_tx = engine.take_command_sender();
cmd_tx.send(SendCommand::Broadcast { content: "hello".into() }).unwrap();
```

---

## DownloadManager

DownloadManager 是多协议下载管理器，支持 BitTorrent、eDonkey、Xunlei P2SP 和 HTTP/FTP。

### 创建

```rust
use ipmsg_download::DownloadManager;
use std::path::PathBuf;

let manager = DownloadManager::new(PathBuf::from("~/.local/share/ipmsg-torrent"));
```

### 添加下载任务

```rust
// BitTorrent（.torrent 文件）
let task_id = manager.add_torrent(PathBuf::from("ubuntu.torrent")).await?;

// Magnet 链接
let task_id = manager.add_magnet("magnet:?xt=urn:btih:...").await?;

// eDonkey 链接
use ipmsg_download::ed2k::Ed2kFileHash;
let hash = Ed2kFileHash::from_hex("abc123...")?;
let task_id = manager.add_ed2k(
    hash,
    1_000_000_000,                    // 文件大小（字节）
    "large_file.iso".to_string(),
    vec!["ed2k://|server|host|4661|/".parse()?],
).await?;

// HTTP/FTP URL
let task_id = manager.add_url("https://example.com/file.zip").await?;

// HTTP 多段下载
let task_id = manager.add_http_multisegment("https://example.com/large.zip").await?;

// Xunlei P2SP（多源混合下载）
use ipmsg_download::xunlei::XunleiSource;
let sources = vec![
    XunleiSource::Http {
        url: "https://mirror1.com/file.zip".into(),
        cookies: None,
        referer: None,
    },
    XunleiSource::Http {
        url: "https://mirror2.com/file.zip".into(),
        cookies: None,
        referer: None,
    },
];
let task_id = manager.add_xunlei("file.zip".into(), 0, sources).await?;

// P2P 文件下载（来自 IPMSG 网络节点）
let task_id = manager.add_p2p(
    "file_hash".into(),
    "filename.zip".into(),
    500_000_000,
    "peer_id".into(),
).await?;
```

### 任务管理

```rust
// 列出所有任务
let tasks = manager.list_tasks().await;
for task in &tasks {
    println!("{} - {} [{:.1}%] ({})",
        &task.id[..8],
        task.name,
        task.progress(),
        task.state_label(),
    );
}

// 按条件过滤和排序
use ipmsg_download::{TaskFilter, TaskSortBy};
let active = manager.list_tasks_filtered(
    TaskFilter::Active,
    Some(TaskSortBy::Speed),
).await;

// 获取单个任务
if let Some(task) = manager.get_task(&task_id).await {
    println!("进度: {:.1}%", task.progress());
    if let Some(eta) = task.eta_seconds() {
        println!("预计剩余: {:.0} 秒", eta);
    }
}

// 暂停任务
manager.pause_task(&task_id).await;

// 恢复任务
manager.resume_task(&task_id).await;

// 检查是否可以恢复
if manager.can_resume(&task_id).await { /* ... */ }
```

### 速度控制

```rust
// 设置全局速度限制（字节/秒）
manager.set_global_speed_limit(1_000_000).await; // 1 MB/s

// 设置单任务速度限制
manager.set_task_speed_limit_per_task(&task_id, Some(500_000)).await;

// 获取任务速度限制
if let Some(limit) = manager.get_task_speed_limit(&task_id).await {
    println!("限速: {} B/s", limit);
}

// 设置最大并发下载数
manager.set_max_concurrent(5);
```

### 事件订阅

```rust
// 订阅下载事件流
let mut rx = manager.subscribe();

tokio::spawn(async move {
    while let Ok(event) = rx.recv().await {
        match event {
            TaskEvent::Updated { task } => {
                println!("任务更新: {} - {:.1}%", task.name, task.progress);
            }
            TaskEvent::Completed { task_id } => {
                println!("下载完成: {}", task_id);
            }
            // ...
        }
    }
});
```

### 带宽监控

```rust
// 获取带宽监控器
let bw = manager.bandwidth_monitor();

// 获取数据目录
let data_dir = manager.data_dir();
```

---

## 事件系统

### P2PEvent 枚举

P2PEngine 通过 `P2PEvent` 枚举通知所有网络事件：

```rust
use ipmsg_core::P2PEvent;

fn handle_event(event: P2PEvent) {
    match event {
        // 节点事件
        P2PEvent::PeerJoined { peer_id, username, platforms } => {
            println!("节点加入: {} ({})", username, peer_id);
        }
        P2PEvent::PeerLeft { peer_id } => {
            println!("节点离开: {}", peer_id);
        }

        // 消息事件
        P2PEvent::MessageReceived(msg) => {
            if let Some(text) = msg.text_content() {
                println!("[{}] {}: {}", msg.from, msg.timestamp, text);
            }
        }
        P2PEvent::MessageSent(msg) => {
            println!("消息已发送: {}", msg.id);
        }
        P2PEvent::MessageDelivered(message_id) => {
            println!("消息已送达: {}", message_id);
        }

        // 文件事件
        P2PEvent::FileOffer { from, file_ref } => {
            println!("{} 提供文件: {}", from, file_ref.name);
        }
        P2PEvent::FileShareAnnounce { from, shares } => {
            println!("{} 共享了 {} 个文件", from, shares.len());
        }
        P2PEvent::FileSearchResponse { from, results } => {
            println!("搜索到 {} 个结果", results.len());
        }
        P2PEvent::FileTransferProgress { file_hash, progress, chunk_index, total_chunks } => {
            println!("下载进度: {:.1}% ({}/{})", progress * 100.0, chunk_index, total_chunks);
        }

        // 社交事件
        P2PEvent::PeerBlocked { peer_id } => { /* ... */ }
        P2PEvent::PeerVerified { peer_id } => { /* ... */ }

        // 图片消息
        P2PEvent::ImageReceived { from, data, mime_type, name } => {
            println!("收到图片: {} ({} bytes)", name, data.len());
        }

        // 已读回执
        P2PEvent::ReadReceiptReceived { from, message_id } => {
            println!("{} 已读消息 {}", from, message_id);
        }

        // 附近节点发现
        P2PEvent::NearbyPeerDiscovered { peer } => {
            println!("发现附近节点: {}", peer.username);
        }

        // 搜索结果
        P2PEvent::SearchResults { query, results } => {
            println!("搜索 '{}' 找到 {} 条结果", query, results.len());
        }

        // IPMSG 兼容事件（native only）
        #[cfg(not(target_arch = "wasm32"))]
        P2PEvent::LegacyPeerDiscovered { name, host, ip } => {
            println!("发现 IPMSG 节点: {}@{} ({})", name, host, ip);
        }
        #[cfg(not(target_arch = "wasm32"))]
        P2PEvent::LegacyMessageReceived { from, ip, content, has_attachment } => {
            println!("[IPMSG] {}: {}", from, content);
        }

        // 状态消息
        P2PEvent::Status(msg) => {
            println!("状态: {}", msg);
        }

        _ => {}
    }
}
```

### SendCommand 枚举

通过 `SendCommand` 向引擎发送命令（适用于分离的事件循环）：

```rust
use ipmsg_core::SendCommand;
use std::path::PathBuf;

// 常用命令
let commands = vec![
    SendCommand::SendText { to: peer_id.into(), content: "你好".into() },
    SendCommand::Broadcast { content: "公告".into() },
    SendCommand::ShareFile { path: PathBuf::from("file.zip"), tags: vec![], description: None },
    SendCommand::SearchFiles { query: "tool".into(), tags: vec![] },
    SendCommand::DownloadFile { file_hash: "hash".into(), from_peer: peer_id },
    SendCommand::BlockPeer { peer_id: peer_id.into() },
    SendCommand::DownloadTorrent { path: PathBuf::from("file.torrent") },
    SendCommand::DownloadUrl { url: "https://example.com/file.zip".into() },
    SendCommand::ListDownloads,
    SendCommand::PauseDownload { task_id: "id".into() },
    SendCommand::ResumeDownload { task_id: "id".into() },
];
```

---

## 常见用例

### 1. 最小化 P2P 聊天客户端

```rust
use ipmsg_core::{P2PEngine, P2PEvent};
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut engine = P2PEngine::new(PathBuf::from("./data"))?;
    let peer_id = engine.start("alice".into(), vec![], 4001).await?;
    println!("已启动，Peer ID: {}", peer_id);

    // 接收事件
    while let Some(event) = engine.next_event().await {
        match event {
            P2PEvent::PeerJoined { username, .. } => {
                println!("{} 加入了聊天", username);
            }
            P2PEvent::MessageReceived(msg) => {
                if let Some(text) = msg.text_content() {
                    println!("{}: {}", msg.from, text);
                }
            }
            _ => {}
        }
    }
    Ok(())
}
```

### 2. 批量下载管理

```rust
use ipmsg_download::DownloadManager;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manager = DownloadManager::new(PathBuf::from("./downloads"));

    // 添加多个下载
    let urls = vec![
        "https://releases.ubuntu.com/24.04/ubuntu-24.04-desktop.iso",
        "https://example.com/file1.zip",
        "https://example.com/file2.tar.gz",
    ];

    for url in urls {
        match manager.add_url(url).await {
            Ok(id) => println!("已添加: {}", &id[..8]),
            Err(e) => eprintln!("添加失败: {}", e),
        }
    }

    // 订阅事件
    let mut rx = manager.subscribe();
    tokio::spawn(async move {
        while let Ok(event) = rx.recv().await {
            // 处理完成事件
        }
    });

    // 监控进度
    loop {
        let tasks = manager.list_tasks().await;
        let all_done = tasks.iter().all(|t| t.state == ipmsg_download::DownloadState::Complete);
        for task in &tasks {
            println!("{} - {:.1}%", task.name, task.progress());
        }
        if all_done { break; }
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
    Ok(())
}
```

### 3. 文件共享与搜索

```rust
use ipmsg_core::{P2PEngine, P2PEvent};
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut engine = P2PEngine::new(PathBuf::from("./data"))?;
    engine.start("filesharer".into(), vec![], 0).await?;

    // 共享文件
    engine.share_file(
        PathBuf::from("./documents/report.pdf"),
        vec!["pdf".into(), "report".into()],
        Some("月度报告".into()),
    ).await?;

    // 搜索文件
    engine.search_files("report", &[]).await?;

    // 处理搜索结果
    while let Some(event) = engine.next_event().await {
        match event {
            P2PEvent::FileSearchResponse { from, results } => {
                for file in results {
                    println!("找到: {} (来自 {})", file.file_ref.name, from);
                    // 下载文件
                    engine.download_file(&file.file_ref.hash, &from).await?;
                }
            }
            P2PEvent::FileTransferProgress { progress, .. } => {
                println!("下载进度: {:.1}%", progress * 100.0);
            }
            _ => {}
        }
    }
    Ok(())
}
```

### 4. 配置管理

```rust
use ipmsg_core::config::Config;
use std::path::Path;

// 加载配置
let mut config = Config::load_or_default();

// 修改设置
config.network.port = 4001;
config.network.enable_mdns = true;
config.ui.username = "alice".into();
config.download.max_concurrent = 5;
config.download.download_dir = Path::new("/home/alice/downloads").into();

// 应用环境变量覆盖
config.apply_env_overrides();

// 保存配置
let config_path = Config::default_config_path();
config.save(&config_path)?;
```

### 5. 错误处理

```rust
use ipmsg_core::{P2PEngine, P2PError};

match P2PEngine::new(path) {
    Ok(engine) => { /* ... */ }
    Err(P2PError::Identity(e)) => eprintln!("身份错误: {}", e),
    Err(P2PError::Transport(e)) => eprintln!("传输错误: {}", e),
    Err(P2PError::Store(e)) => eprintln!("存储错误: {}", e),
    Err(P2PError::PeerNotFound(e)) => eprintln!("节点未找到: {}", e),
    Err(P2PError::Network(e)) => eprintln!("网络错误: {}", e),
    Err(P2PError::Io(e)) => eprintln!("IO 错误: {}", e),
}
```
