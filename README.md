# PPX - 去中心化P2P聊天应用

<div align="center">

![Version](https://img.shields.io/badge/version-1.0.0-green)
![License](https://img.shields.io/badge/license-MIT-blue)
![Platform](https://img.shields.io/badge/platform-Web%7CAndroid%7CWindows%7CLinux-blue)

</div>

## 📱 简介

PPX 是一款去中心化的 P2P 聊天应用，基于 BroadcastChannel 和 WebRTC 技术实现，无需服务器即可在多个平台间进行即时通讯。支持文件传输、下载管理、媒体播放等功能。

### ✨ 特性

- 🚀 **去中心化** - 无需服务器，P2P直连通信
- 🌐 **跨平台** - 支持 Web、Android、Windows、Linux
- 🔒 **隐私安全** - 本地数据存储，消息不经过第三方
- 📁 **文件传输** - 支持图片、视频、音频、文档等文件
- ⬇️ **下载管理** - 集成下载管理器，支持断点续传
- 🎬 **媒体播放** - 支持图片预览、视频/音频播放
- 💬 **微信风格** - 简洁清晰的微信式UI设计
- 📦 **体积小巧** - Web版仅约 40KB (gzip)

## 🏗️ 技术架构

| 层级 | 技术 |
|------|------|
| 通信 | BroadcastChannel + WebRTC |
| 框架 | 原生 JavaScript (ES6+) |
| 桌面端 | Electron |
| 移动端 | Capacitor (Android) |
| 构建 | Vite + electron-builder |

## 📥 下载安装

### Web 版
直接打开 `dist/index.html` 或部署到任意静态服务器

### Windows
下载 `PPX-x.x.x-win-portable.exe` 便携版，双击即用

### Linux
下载 `PPX-x.x.x-linux-x64.tar.gz`，解压后运行

### Android
在有网络的环境下构建：
```bash
npm run android:debug
```

## 🛠️ 开发

### 环境要求
- Node.js >= 16.0.0
- npm >= 8.0.0

### 安装依赖
```bash
npm install
```

### 开发预览
```bash
npm run dev
```

### 构建
```bash
# 构建 Web 版
npm run build:web

# 构建 Windows 便携版
npm run build:win

# 构建 Linux 版
npm run build:linux

# 构建全部
npm run build:all
```

### Android 构建
```bash
# 初始化 Capacitor（如需要）
npm run android:init

# 同步并构建
npm run android:debug
```

## 📁 项目结构

```
ppx-chat/
├── src/
│   ├── core/
│   │   └── p2p.js          # P2P通信核心
│   ├── app.js              # 应用主逻辑
│   ├── index.html          # 页面入口
│   └── style.css           # 样式
├── electron/
│   ├── main.js             # Electron主进程
│   └── preload.js           # 预加载脚本
├── android/                 # Android项目
├── dist/                    # 构建输出
├── package.json
└── vite.config.js
```

## 📱 功能说明

### 聊天
- 自动发现附近在线用户
- 实时消息发送接收
- 消息已读回执
- 正在输入提示

### 文件传输
- 支持图片、视频、音频、文档等
- 文件预览（图片/视频/音频）
- 一键保存到本地

### 下载管理
- 下载进度显示
- 下载速度统计
- 完成后自动归档

## 🔧 配置

P2P 通信频道可在 `src/core/p2p.js` 中修改：
```javascript
this.BROADCAST_CHANNEL = 'ppx-discovery';
this.MESSAGE_CHANNEL = 'ppx-messages';
```

## ⚠️ 注意事项

1. **同源策略**: BroadcastChannel 仅在同一浏览器标签页间通信
2. **跨标签页**: 需要在同一浏览器中打开多个标签页
3. **局域网**: 同一网络下的设备可以相互发现

## 📄 许可证

MIT License

## 🙏 致谢

- [Electron](https://electronjs.org/)
- [Capacitor](https://capacitorjs.com/)
- [Vite](https://vitejs.dev/)
