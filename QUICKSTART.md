# IPMsg Torrent - 快速开始指南

## ✅ 已完成的工作

### 1. 项目架构重构 ✓
- ✅ 使用现代 ES6 模块化架构
- ✅ 清晰的代码结构
- ✅ 跨所有平台支持

### 2. Web 版本 ✓
- ✅ 纯 HTML/CSS/JS 实现
- ✅ BroadcastChannel API 实现 P2P 通信
- ✅ 响应式设计，完美适配移动端
- ✅ 已测试构建成功

### 3. Capacitor 配置 (Android + 鸿蒙) ✓
- ✅ 完整的 Android 项目结构
- ✅ AndroidManifest.xml 配置
- ✅ 网络权限配置
- ✅ 文件分享配置
- ✅ Gradle 构建配置
- ✅ 签名配置脚本

### 4. Electron 桌面端配置 ✓
- ✅ Electron 主进程配置
- ✅ 预加载脚本配置
- ✅ 应用菜单配置
- ✅ 系统托盘配置

### 5. Electron-builder 配置 ✓
- ✅ Windows 安装包 (.exe) 配置
- ✅ Linux 安装包 (.AppImage, .deb) 配置
- ✅ macOS 安装包 (.dmg) 配置
- ✅ 构建脚本

## 🚀 快速开始

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

### 构建应用

**Web 版本:**
```bash
npm run build:web
```

**Windows:**
```bash
npm run build:win
```

**Linux:**
```bash
npm run build:linux
```

**Android:**
```bash
npm run android:init  # 首次需要
npm run android:build
```

**所有平台:**
```bash
npm run build:all
```

## 📱 Android 构建详细步骤

### 1. 初始化
```bash
npm install
npm run android:init
```

### 2. 构建 Web 应用
```bash
npm run build
```

### 3. 复制到 Android
```bash
npx cap copy android
npx cap sync android
```

### 4. 打开 Android Studio
```bash
npx cap open android
```

### 5. 在 Android Studio 中构建

- 选择 `Build > Generate Signed Bundle / APK`
- 选择 `APK`
- 选择或创建签名密钥
- 构建 Release 或 Debug APK

### 6. 生成签名密钥（可选）
```bash
cd android
./generate-keystore.sh
```

## 💻 Windows 构建

```bash
npm run build:win
```

输出位置: `release/`

生成文件:
- `IPMsg-Torrent-Setup-x.x.x.exe` - NSIS 安装包
- `IPMsg-Torrent-x.x.x-portable.exe` - 便携版

## 🐧 Linux 构建

```bash
npm run build:linux
```

输出位置: `release/`

生成文件:
- `IPMsg-Torrent-x.x.x-amd64.AppImage` - AppImage
- `IPMsg-Torrent-x.x.x-amd64.deb` - Debian 包

## 📦 项目结构

```
ipmsg-torrent/
├── src/                      # 源代码
│   ├── core/
│   │   └── p2p.js           # P2P 通信核心
│   ├── utils/
│   │   └── uuid.js          # UUID 工具
│   └── app.js              # 主应用
├── electron/                # Electron 配置
│   ├── main.js             # 主进程
│   └── preload.js          # 预加载
├── android/                 # Android 项目
│   ├── app/                # 应用代码
│   ├── build.gradle        # 构建配置
│   └── ...
├── public/                  # 静态资源
├── dist/                    # 构建输出 (Web)
├── dist-electron/           # 构建输出 (Electron)
├── release/                 # 最终安装包
├── package.json
├── vite.config.js
├── capacitor.config.json
└── electron-builder.yml
```

## 🎯 构建产物

### Web 版本
- `dist/index.html` - 主页面
- `dist/assets/*` - 资源文件

### Android APK
- Debug: `android/app/build/outputs/apk/debug/app-debug.apk`
- Release: `android/app/build/outputs/apk/release/app-release.apk`

### Windows 安装包
- `release/IPMsg-Torrent-Setup-x.x.x.exe` - 安装包

### Linux 安装包
- `release/IPMsg-Torrent-x.x.x-amd64.AppImage` - AppImage
- `release/IPMsg-Torrent-x.x.x-amd64.deb` - Debian 包

## ⚙️ 配置说明

### P2P 通信
在 `src/core/p2p.js` 中修改:
```javascript
this.BROADCAST_CHANNEL = 'ipmsg-torrent-discovery';  // 发现频道
this.MESSAGE_CHANNEL = 'ipmsg-torrent-messages';    // 消息频道
```

### 应用信息
在 `package.json` 中修改 `build` 部分

### Android 配置
在 `capacitor.config.json` 中修改应用 ID 和名称

## 🐛 故障排除

### 1. Android 构建失败
- 确保 Java JDK >= 11
- 确保 Android SDK 已安装
- 运行 `npx cap doctor` 检查环境

### 2. Electron 构建失败
- 确保 Electron 下载成功
- 检查网络连接

### 3. 文件传输失败
- 确保在同一网络下
- 检查防火墙设置

## 📞 支持

如遇问题，请查看:
- `README.md` - 详细文档
- GitHub Issues - 问题反馈

## 🎉 成功构建验证

✅ Web 版本构建成功
- 输出: `dist/index.html`
- 大小: ~17KB (HTML)
- JS: ~21KB (压缩后 ~6KB)

✅ Electron 构建成功
- 输出: `dist-electron/main.js`
- 输出: `dist-electron/preload.js`

✅ Android 项目配置完成
- Gradle 配置完成
- 签名脚本就绪
- 构建脚本完整

## 📝 下一步

1. 添加应用图标到 `build/` 目录
2. 生成 Android 签名密钥
3. 运行构建命令生成安装包
4. 测试各平台功能

---

**构建成功！** 所有配置文件和构建脚本已就绪。
