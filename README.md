# IPMsg Torrent - 去中心化P2P聊天应用

## 📋 项目简介

IPMsg Torrent 是一款去中心化的 P2P 聊天应用，基于 BroadcastChannel 和 WebRTC 技术实现，无需服务器即可在多个平台间进行即时通讯。

### ✨ 特性

- 🚀 **去中心化** - 无需服务器，P2P直连通信
- 🌐 **跨平台** - 支持 Web、Android、鸿蒙、Windows、Linux、macOS
- 🔒 **隐私安全** - 本地数据存储，消息不经过第三方
- 📁 **文件传输** - 支持端到端文件分享
- 💬 **实时聊天** - 即时消息传递
- 👥 **多人聊天** - 自动发现附近在线用户
- 📱 **移动端适配** - 响应式设计，完美支持移动端

## 🛠️ 技术架构

### 核心技术

- **P2P通信**: BroadcastChannel API + WebRTC
- **前端框架**: 原生 JavaScript ES6+ 模块化架构
- **桌面端**: Electron
- **移动端**: Capacitor (Android + 鸿蒙)
- **构建工具**: Vite + electron-builder

### 平台支持

| 平台 | 状态 | 构建方式 |
|------|------|----------|
| Web | ✅ 完成 | Vite |
| Android | ✅ 完成 | Capacitor + Gradle |
| 鸿蒙 | ✅ 完成 | Capacitor (兼容) |
| Windows | ✅ 完成 | Electron + NSIS |
| Linux | ✅ 完成 | Electron + AppImage/deb |
| macOS | ✅ 完成 | Electron + DMG |

## 🚀 快速开始

### 环境要求

- Node.js >= 16.0.0
- npm >= 8.0.0
- Java JDK >= 11 (Android构建)
- Android SDK (Android构建)

### 安装依赖

```bash
npm install
```

### 开发模式

**Web开发:**
```bash
npm run dev
```

**Electron开发:**
```bash
npm run electron:dev
```

### 构建应用

**构建所有平台:**
```bash
npm run build:all
```

**分别构建:**
```bash
# Web版
npm run build:web

# Windows
npm run build:win

# Linux
npm run build:linux

# macOS
npm run build:mac

# Android
npm run android:build
```

### 使用构建脚本

```bash
# 赋予执行权限
chmod +x build.sh build-*.sh

# 构建所有平台
./build.sh all

# 仅构建Windows
./build.sh --electron win

# 仅构建Android
./build.sh --android
```

## 📱 Android / 鸿蒙 构建详细步骤

### 国内源配置（可选，推荐）

如果网络较慢，可配置使用国内镜像源加速下载：

修改 `android/build.gradle` 中的 repositories：

```gradle
buildscript {
    repositories {
        maven { url 'http://maven.aliyun.com/repository/google'; allowInsecureProtocol = true }
        maven { url 'http://maven.aliyun.com/repository/central'; allowInsecureProtocol = true }
        maven { url 'http://maven.aliyun.com/repository/public'; allowInsecureProtocol = true }
        maven { url 'http://maven.aliyun.com/repository/gradle-plugin'; allowInsecureProtocol = true }
        google()
        mavenCentral()
        gradlePluginPortal()
    }
}

allprojects {
    repositories {
        maven { url 'http://maven.aliyun.com/repository/google'; allowInsecureProtocol = true }
        maven { url 'http://maven.aliyun.com/repository/central'; allowInsecureProtocol = true }
        maven { url 'http://maven.aliyun.com/repository/public'; allowInsecureProtocol = true }
        google()
        mavenCentral()
    }
}
```

修改 `android/settings.gradle` 中的 pluginManagement：

```gradle
pluginManagement {
    repositories {
        maven { url 'http://maven.aliyun.com/repository/google'; allowInsecureProtocol = true }
        maven { url 'http://maven.aliyun.com/repository/central'; allowInsecureProtocol = true }
        maven { url 'http://maven.aliyun.com/repository/public'; allowInsecureProtocol = true }
        maven { url 'http://maven.aliyun.com/repository/gradle-plugin'; allowInsecureProtocol = true }
        google()
        mavenCentral()
        gradlePluginPortal()
    }
}
```

### 1. 初始化Capacitor

```bash
npm run android:init
```

### 2. 构建Web应用

```bash
npm run build:web
```

### 3. 复制到Android

```bash
npx cap copy android
npx cap sync android
```

### 4. 命令行构建APK

```bash
cd android
./gradlew assembleDebug
```

或在 Android Studio 中打开 `android` 目录进行构建

### 5. 生成签名APK

#### 5.1 生成签名密钥

```bash
cd android
./generate-keystore.sh
```

或手动生成:

```bash
keytool -genkey -v -keystore release.keystore -alias ipmsg-release -keyalg RSA -keysize 2048 -validity 10000
```

#### 5.2 配置签名信息

在 `android/gradle.properties` 中添加:

```properties
RELEASE_STORE_FILE=./build/release.keystore
RELEASE_STORE_PASSWORD=你的密码
RELEASE_KEY_ALIAS=ipmsg-release
RELEASE_KEY_PASSWORD=你的密码
```

#### 5.3 构建Release APK

```bash
cd android
./gradlew assembleRelease
```

APK位置: `android/app/build/outputs/apk/release/`

### 调试APK

```bash
npm run android:debug
```

APK位置: `android/app/build/outputs/apk/debug/`

## 💻 Windows 构建

### 构建安装包

```bash
npm run build:win
```

生成文件:
- `IPMsg-Torrent-Setup-1.0.0.exe` - NSIS安装包
- `IPMsg-Torrent-1.0.0-portable.exe` - 便携版

安装包位置: `release/`

## 🐧 Linux 构建

### 构建AppImage和deb包

```bash
npm run build:linux
```

生成文件:
- `IPMsg-Torrent-1.0.0-amd64.AppImage` - AppImage包
- `IPMsg-Torrent-1.0.0-amd64.deb` - Debian包

安装包位置: `release/`

## 📦 项目结构

```
ipmsg-torrent/
├── src/
│   ├── core/
│   │   └── p2p.js          # P2P通信核心模块
│   ├── utils/
│   │   └── uuid.js         # UUID工具
│   ├── components/         # UI组件
│   ├── app.js             # 主应用逻辑
│   └── index.html         # HTML入口
├── electron/
│   ├── main.js            # Electron主进程
│   └── preload.js         # 预加载脚本
├── android/               # Android项目
│   ├── app/               # 应用代码
│   ├── build.gradle       # Gradle配置
│   └── ...
├── public/                # 静态资源
├── build/                 # 构建资源
├── dist/                  # Vite构建输出
├── dist-electron/         # Electron构建输出
├── release/              # 最终安装包
├── package.json
├── vite.config.js
├── capacitor.config.json
└── electron-builder.yml
```

## 🔧 配置说明

### P2P通信配置

在 `src/core/p2p.js` 中可以修改:

```javascript
this.BROADCAST_CHANNEL = 'ipmsg-torrent-discovery';  // 发现频道
this.MESSAGE_CHANNEL = 'ipmsg-torrent-messages';    // 消息频道
this.peerTimeout = 10000;                           // 节点超时(ms)
```

### Electron配置

在 `package.json` 的 `build` 部分配置:

```json
"build": {
  "appId": "com.ipmsg.torrent",
  "productName": "IPMsg Torrent",
  "directories": {
    "output": "release"
  }
}
```

### Capacitor配置

在 `capacitor.config.json` 中配置应用信息和插件。

## 🎨 自定义

### 修改应用图标

1. **Windows**: 替换 `build/icon.ico`
2. **macOS**: 替换 `build/icon.icns`
3. **Linux**: 替换 `build/icons/` 目录下的图标
4. **Android**: 替换 `android/app/src/main/res/mipmap-*/` 下的图标

图标尺寸要求:
- Windows: 256x256 (ico格式)
- macOS: 512x512 (icns格式)
- Linux: 多种尺寸 (png格式)
- Android: 48x48, 72x72, 96x96, 144x144, 192x192 (png格式)

### 修改主题色

在 `index.html` 和 `android/app/src/main/res/values/colors.xml` 中修改:

```css
background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
```

## 📥 下载安装

直接从 GitHub Releases 下载对应平台的安装包：

- **Windows**: `IPMsg-Torrent-1.0.0-portable.exe` (便携版，双击即用)
- **Linux**: `IPMsg-Torrent-1.0.0-amd64.AppImage` 或 `.tar.gz`
- **Web**: 直接打开 `web-dist/index.html` 或部署到任意静态服务器
- **Android**: 从 Releases 下载 APK（需自行构建或从其他渠道获取）

## 🐛 常见问题

### 1. Android无法连接

确保在 `AndroidManifest.xml` 中添加了网络权限:

```xml
<uses-permission android:name="android.permission.INTERNET" />
```

### 2. BroadcastChannel不可用

在某些旧浏览器中可能不支持 BroadcastChannel，应用会自动降级。

### 3. 文件传输失败

文件过大会导致传输问题，建议在良好网络环境下传输，或压缩后分块传输。

### 4. Android构建失败

确保:
- Java JDK版本 >= 11
- Android SDK已安装
- Gradle版本正确

### 5. 签名APK无法安装

确保使用正确的签名密钥，或先卸载已安装的调试版本。

## 📄 许可证

MIT License

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

## 🙏 致谢

- Electron Team
- Capacitor Team
- Vite Team
- 所有开源贡献者
