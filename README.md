# PPX - 去中心化P2P聊天应用

<div align="center">

![Version](https://img.shields.io/badge/version-1.2.0-green)
![License](https://img.shields.io/badge/license-MIT-blue)
![Platform](https://img.shields.io/badge/platform-Web%20%7C%20Android%20%7C%20Windows%20%7C%20Linux-blue)

**去中心化 · 无服务器 · 即时通讯**

</div>

## 简介

PPX 是一款完全去中心化的 P2P 聊天应用，无需任何服务器即可实现即时通讯。基于 BroadcastChannel 和 WebRTC 技术，支持多平台（Web、Android、Windows、Linux），集成文件传输、下载管理、媒体播放等功能。

## ✨ 特性

### 核心功能

- 🚀 **去中心化通信** - 无需服务器，P2P 直连
- 💬 **即时聊天** - 实时消息收发
- 📁 **文件传输** - 支持图片、视频、音频、文档等
- 🎬 **媒体播放** - 内置图片/视频/音频预览
- ⬇️ **下载管理** - 完整的下载进度和状态管理
- 🔔 **通知系统** - 消息通知和未读计数
- ⌨️ **正在输入提示** - 显示对方输入状态
- ✅ **消息已读回执** - 消息状态反馈

### 用户体验

- 📱 **响应式设计** - 完美适配移动端和桌面端
- 🎨 **微信风格 UI** - 简洁清晰的界面
- 💾 **本地数据持久化** - 聊天记录保存
- 🔍 **消息搜索** - 快速查找历史消息
- 📤 **文件分享** - 支持各类文件格式

### 跨平台支持

- 🌐 **Web 版本** - 纯浏览器运行
- 🤖 **Android** - 原生 APK 安装包
- 🪟 **Windows** - 便携版应用
- 🐧 **Linux** - tar.gz 压缩包

## 🏗️ 技术架构

| 层级 | 技术方案 |
|------|----------|
| **通信核心** | BroadcastChannel + WebRTC |
| **前端框架** | 原生 JavaScript (ES6+) |
| **桌面端** | Electron + Vite |
| **移动端** | Capacitor + Android |
| **构建工具** | Vite |
| **打包工具** | electron-builder + Gradle |

## 📥 快速开始

### 环境要求

- Node.js >= 16.0.0
- npm >= 8.0.0
- Java JDK >= 11 (Android 构建)

### 安装依赖

```bash
npm install
```

### 开发模式

**Web 开发:**
```bash
npm run dev
```

**Electron 开发:**
```bash
npm run electron:dev
```

## 🔨 构建应用

### 单平台构建

**Web 版本:**
```bash
npm run build:web
```

**Windows 便携版:**
```bash
npm run build:win
```

**Linux 版本:**
```bash
npm run build:linux
```

**Android APK:**
```bash
npm run android:build    # 同步资源
npm run android:debug    # 构建 Debug APK
npm run android:apk      # 构建 Release APK
```

### 全平台构建

```bash
npm run build:all
```

## 📱 Android 构建详细步骤

详细说明请参考 [ANDROID_BUILD.md](./ANDROID_BUILD.md)

### 快速构建

```bash
# 1. 安装依赖
npm install

# 2. 构建并同步 Web 资源
npm run android:build

# 3. 构建 APK
cd android
./gradlew assembleRelease
```

### 输出位置

- Debug APK: `android/app/build/outputs/apk/debug/app-debug.apk`
- Release APK: `android/app/build/outputs/apk/release/app-release.apk`

## 📦 项目结构

```
ppx-chat/
├── src/                      # 源代码目录
│   ├── core/
│   │   └── p2p.js           # P2P 通信核心
│   ├── utils/
│   │   └── uuid.js          # UUID 工具函数
│   ├── app.js              # 主应用逻辑
│   ├── index.html          # HTML 结构
│   └── style.css           # 样式文件
├── electron/                # Electron 配置
│   ├── main.js             # 主进程
│   └── preload.js          # 预加载脚本
├── android/                 # Android 项目
│   ├── app/
│   │   ├── src/
│   │   │   └── main/
│   │   │       ├── assets/      # 资源文件
│   │   │       ├── java/        # Java 代码
│   │   │       ├── res/         # Android 资源
│   │   │       └── AndroidManifest.xml
│   │   └── build.gradle
│   ├── build.gradle        # 项目构建配置
│   ├── settings.gradle     # Gradle 设置
│   └── init.gradle         # 阿里云镜像源配置
├── build/                   # 构建资源
│   ├── icons/             # 应用图标
│   └── icon.png
├── public/                  # 静态资源
│   └── manifest.json
├── dist/                    # Web 构建输出
├── dist-electron/           # Electron 构建输出
├── release/                 # 最终安装包
├── package.json
├── vite.config.js
├── capacitor.config.json
├── electron-builder.yml
├── ANDROID_BUILD.md        # Android 构建文档
├── QUICKSTART.md           # 快速开始指南
└── README.md
```

## 🎯 核心模块说明

### P2P 通信 ([src/core/p2p.js](src/core/p2p.js))

```javascript
import { P2PChat } from './core/p2p.js';

const p2p = new P2PChat();

// 初始化
p2p.init('我的用户名').then(info => {
  console.log('上线成功:', info);
});

// 监听事件
p2p.on('peers-updated', peers => { /* 在线用户更新 */ });
p2p.on('message', msg => { /* 收到消息 */ });
p2p.on('peer-joined', peer => { /* 用户加入 */ });
p2p.on('peer-left', peer => { /* 用户离开 */ });

// 发送消息
p2p.sendMessage(peerId, '你好！', 'text');

// 发送文件
p2p.sendFileOffer(peerId, fileInfo);
```

### 主应用 ([src/app.js](src/app.js))

主要功能类：
- `PPXApp` - 主应用类
- 聊天界面管理
- 文件管理
- 下载管理
- 用户设置

## ⚙️ 配置说明

### P2P 通信频道

在 [src/core/p2p.js](src/core/p2p.js) 中修改：

```javascript
this.BROADCAST_CHANNEL = 'ppx-discovery';  // 发现频道
this.MESSAGE_CHANNEL = 'ppx-messages';    // 消息频道
```

### 应用信息

在 [package.json](package.json) 中修改：
- 应用名称: `name`
- 版本号: `version`
- 应用 ID: `build.appId`

### Android 配置

在 [capacitor.config.json](capacitor.config.json) 中修改：
- 应用名称
- 应用 ID
- 图标配置

## 🔧 故障排除

### Android 构建失败

1. 确保 Java JDK >= 11 已安装
2. 确保 Android SDK 已安装
3. 运行 `npx cap doctor` 检查环境
4. 检查网络连接（需要下载依赖）

### Electron 构建失败

1. 确保网络连接正常
2. 设置国内镜像源：
   ```bash
   export ELECTRON_MIRROR=https://npmmirror.com/mirrors/electron/
   ```

### 文件传输问题

1. 确保设备在同一网络下
2. 检查防火墙设置
3. 确认 BroadcastChannel 支持

## 📝 开发计划

- [ ] WebRTC 数据通道支持
- [ ] 端到端加密
- [ ] 群组聊天
- [ ] 消息撤回
- [ ] 更多平台支持（iOS、macOS）
- [ ] 插件系统

## 🤝 贡献

欢迎 Issue 和 Pull Request！

## 📄 许可证

MIT License - 详见 [LICENSE](LICENSE) 文件

## 🙏 致谢

- [Electron](https://electronjs.org/)
- [Capacitor](https://capacitorjs.com/)
- [Vite](https://vitejs.dev/)

---

**PPX** - 让通信更自由！
