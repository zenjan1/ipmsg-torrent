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

## 📦 完整构建指南

### 前置准备

在开始构建之前，请确保：

1. **安装必要工具**
   ```bash
   # Node.js (>= 16.0.0)
   # npm (>= 8.0.0)
   npm install -g npm@latest
   ```

2. **对于 Android 构建**
   - Java JDK >= 11
   - Android SDK
   - 安装 Android Studio (推荐)

3. **对于 Electron 构建**
   - 无需额外工具，npm 会自动安装依赖

### 构建命令速查

| 平台 | 命令 | 输出位置 |
|------|------|----------|
| **Web 版本** | `npm run build:web` | `dist/` |
| **Windows** | `npm run build:win` | `release/*.exe` |
| **Linux** | `npm run build:linux` | `release/*.AppImage, *.deb` |
| **macOS** | `npm run build:mac` | `release/*.dmg` |
| **Android (Debug)** | `npm run android:debug` | `android/app/build/outputs/apk/debug/` |
| **Android (Release)** | `npm run android:apk` | `android/app/build/outputs/apk/release/` |
| **所有平台** | `npm run build:all` | 各对应目录 |

### 🖥️ 各平台详细构建步骤

#### 1️⃣ Web 版本构建

Web 版本是最基础的，适用于浏览器直接访问：

```bash
# 1. 安装依赖
npm install

# 2. 开发模式（预览）
npm run dev

# 3. 生产构建
npm run build:web
```

**输出内容：**
- `dist/index.html` - 主 HTML 文件
- `dist/assets/` - 编译后的 JS/CSS 资源
- `dist/manifest.json` - PWA 配置

**部署方式：**
```bash
# 直接部署到任何静态服务器
# 或使用本地预览
npm run preview
```

#### 2️⃣ Windows 安装包构建

```bash
# 1. 确保安装了所有依赖
npm install

# 2. 构建 Web 应用（会自动运行）
npm run build:win
```

**生成文件：**
- `release/IPMsg-Torrent-Setup-1.0.0.exe` - 完整安装包（推荐）
- `release/IPMsg-Torrent-1.0.0-portable.exe` - 便携版，无需安装

**安装包特性：**
- 支持 x64 和 ia32 架构
- 自动添加开始菜单和桌面快捷方式
- 支持自定义安装目录
- 提供卸载程序

#### 3️⃣ Linux 安装包构建

```bash
# 1. 安装依赖
npm install

# 2. 构建
npm run build:linux
```

**生成文件：**
- `release/IPMsg-Torrent-1.0.0-amd64.AppImage` - AppImage（推荐，通用格式）
- `release/IPMsg-Torrent-1.0.0-amd64.deb` - Debian/Ubuntu 专用包
- `release/IPMsg-Torrent-1.0.0.tar.gz` - 通用压缩包

**Linux 使用说明：**
```bash
# AppImage（无需安装，直接运行）
chmod +x IPMsg-Torrent-1.0.0-amd64.AppImage
./IPMsg-Torrent-1.0.0-amd64.AppImage

# DEB 包（Debian/Ubuntu）
sudo dpkg -i IPMsg-Torrent-1.0.0-amd64.deb
sudo apt-get install -f  # 如有依赖缺失
```

#### 4️⃣ macOS 构建

```bash
# 1. 安装依赖
npm install

# 2. 构建
npm run build:mac
```

**生成文件：**
- `release/IPMsg-Torrent-1.0.0.dmg` - DMG 安装包
- `release/IPMsg-Torrent-1.0.0-mac.zip` - 压缩版

---

### 📱 Android / 鸿蒙 构建详细步骤

#### 5️⃣ Android Debug 版本构建（最简单）

无需签名，快速构建测试：

```bash
# 1. 初始化 Capacitor（首次构建需要）
npm run android:init

# 2. 构建 Web 应用
npm run build:web

# 3. 复制到 Android 项目
npx cap copy android
npx cap sync android

# 4. 构建 Debug APK
npm run android:debug
```

**输出位置：**
`android/app/build/outputs/apk/debug/app-debug.apk

**特性：**
- 可以直接安装到 Android 设备
- 兼容鸿蒙系统
- 无需签名配置

---

#### 6️⃣ Android Release 签名版本构建

#### 6.1 初始化 Capacitor

```bash
npm run android:init
```

#### 6.2 构建 Web 应用

```bash
npm run build:web
```

#### 6.3 复制代码到 Android 项目

```bash
npx cap copy android
npx cap sync android
```

#### 6.4 生成签名密钥

```bash
cd android
./generate-keystore.sh
```

或手动生成：

```bash
keytool -genkey -v -keystore release.keystore -alias ipmsg-release -keyalg RSA -keysize 2048 -validity 10000
```

#### 6.5 配置签名信息

编辑 `android/gradle.properties`，添加：

```properties
RELEASE_STORE_FILE=./build/release.keystore
RELEASE_STORE_PASSWORD=你的密码
RELEASE_KEY_ALIAS=ipmsg-release
RELEASE_KEY_PASSWORD=你的密码
```

#### 6.6 构建 Release APK

```bash
cd android
./gradlew assembleRelease
```

**输出位置：**
`android/app/build/outputs/apk/release/app-release.apk`

---

### 🤖 鸿蒙系统兼容性说明

应用通过 Android APK 可以在鸿蒙系统上直接安装运行，因为鸿蒙系统兼容 Android 应用。

**推荐配置优化（可选）：**
```bash
# 在鸿蒙系统可通过鸿蒙 DevEco Studio 中导入项目
# 在 Android 或直接使用生成的 APK
```

---

## 🚀 自动化构建脚本

为了简化构建流程，项目提供了自动化构建脚本。

### 使用统一构建脚本

#### 方式一：使用交互式菜单

```bash
# 1. 赋予执行权限
chmod +x build-all.sh

# 2. 运行菜单选择要构建的平台
./build-all.sh
```

#### 方式二：直接选择平台构建

```bash
# Web 版本
./build.sh web

# Windows
./build-win.sh

# Linux
./build-linux.sh

# Android
./build-android.sh
```

#### 构建脚本说明

| 脚本 | 功能 |
|------|------|
| `build-all.sh` | 交互式菜单，选择构建平台 |
| `build-win.sh` | 仅构建 Windows |
| `build-linux.sh` | 仅构建 Linux |
| `build-android.sh` | 仅构建 Android |
| `build.sh` | 完整构建脚本（原版本） |

---

## 📦 完整构建流程图

```
┌─────────────────┐
│ npm install   │
└───────┬─────────┘
        │
        ▼
┌───────────────────────────────┐
│ 选择目标平台         │
└──┬───────────────────────┘
   │
   ├─→ Web → npm run build:web → dist/
   │
   ├─→ Windows → npm run build:win → release/*.exe
   │
   ├─→ Linux → npm run build:linux → release/*.AppImage/*.deb
   │
   ├─→ Android (Debug) → npm run android:debug → android/app/build/outputs/apk/debug/app-debug.apk
   │
   └─→ Android (Release) → 配置签名 → npm run android:apk → android/app/build/outputs/apk/release/app-release.apk
```

---

## 🔧 高级构建配置

### 修改应用信息

编辑 `package.json`：

```json
{
  "name": "ipmsg-torrent",
  "version": "1.0.0",
  "productName": "IPMsg Torrent",
  "description": "去中心化聊天软件"
}
```

### 修改版本号

修改 `package.json` 中的 `version` 字段：

```json
"version": "1.0.0"
```

构建时版本号会自动应用到所有平台的安装包中。

### 修改应用名称和图标

参考 [自定义](#🎨-自定义) 部分的详细说明。

---

## 📋 构建清单

构建前检查：

- [ ] 已安装 Node.js >= 16.0.0
- [ ] 已运行 npm install
- [ ] Android 构建已安装 Java JDK >= 11
- [ ] Android 构建已配置 Android SDK
- [ ] Release 版本已配置签名密钥
- [ ] 修改了应用版本号（如需）

构建完成后检查：

- [ ] Web 版本在 dist/ 目录
- [ ] 安装包在 release/ 目录
- [ ] Android APK 在 android/app/build/outputs/apk/ 目录

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
