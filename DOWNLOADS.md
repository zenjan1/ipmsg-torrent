# IPMsg Torrent - 各平台下载说明

## 📦 已构建的安装包

### ✅ Web 版本
- **位置**: `dist/`
- **文件**:
  - `index.html` - 主页面
  - `assets/main-*.js` - 编译后的 JavaScript
- **使用方式**: 直接部署到任何 Web 服务器
- **大小**: ~17KB + 21KB assets

### ✅ Windows 便携版
- **位置**: `release/win-unpacked/IPMsg Torrent.exe`
- **大小**: ~217MB
- **使用方式**: 直接运行 exe 文件，无需安装
- **说明**: 这是完整的便携版本，包含所有依赖

### ✅ Linux 版本
#### AppImage（推荐）
- **位置**: `release/IPMsg Torrent-1.0.0-x86_64-linux.AppImage`
- **大小**: 116MB
- **使用方式**:
  ```bash
  chmod +x "IPMsg Torrent-1.0.0-x86_64-linux.AppImage"
  ./"IPMsg Torrent-1.0.0-x86_64-linux.AppImage"
  ```

#### DEB 包（Debian/Ubuntu）
- **位置**: `release/IPMsg Torrent-1.0.0-amd64-linux.deb`
- **大小**: 91MB
- **使用方式**:
  ```bash
  sudo dpkg -i "IPMsg Torrent-1.0.0-amd64-linux.deb"
  ```

#### TAR.GZ 压缩包
- **位置**: `release/IPMsg Torrent-1.0.0-x64-linux.tar.gz`
- **大小**: 110MB
- **使用方式**:
  ```bash
  tar -xzf "IPMsg Torrent-1.0.0-x64-linux.tar.gz"
  cd "IPMsg Torrent-1.0.0-x64-linux"
  ./IPMsg\ Torrent
  ```

### ⚠️ Windows NSIS 安装包
- **状态**: 在当前环境无法构建（需要 wine）
- **替代方案**: 使用 `release/win-unpacked/` 便携版
- **如需安装包**: 在 Windows 系统上运行 `npm run build:win` 重新构建

### ❌ Android APK
- **状态**: 当前环境网络超时，无法下载依赖
- **替代方案**:
  1. 在具有稳定网络的环境中重新构建
  2. 使用 Web 版本在浏览器中测试

---

## 🔨 Android APK 重新构建步骤

如果你需要生成 Android APK，请在具有稳定网络的环境中运行：

```bash
# 1. 安装依赖
npm install

# 2. 初始化 Android 项目（如果首次构建）
npm run android:init

# 3. 构建 Web 版本
npm run build:web

# 4. 复制到 Android 项目
npx cap copy android
npx cap sync android

# 5. 构建 Debug APK
cd android
./gradlew assembleDebug
```

**输出位置**: `android/app/build/outputs/apk/debug/app-debug.apk`

**如果需要 Release 签名 APK**:
```bash
# 1. 生成签名密钥
cd android
./generate-keystore.sh

# 2. 配置签名信息（在 gradle.properties 中）
# 3. 构建 Release APK
./gradlew assembleRelease
```

**输出位置**: `android/app/build/outputs/apk/release/app-release.apk`

---

## 📋 各平台测试建议

### 1. Web 版本测试
```bash
# 方法一：使用 npm preview
npm run preview

# 方法二：使用 Python 简单服务器
cd dist
python3 -m http.server 8080

# 然后在浏览器访问 http://localhost:8080
```

### 2. Windows 便携版测试
1. 下载 `release/win-unpacked/` 整个目录
2. 运行 `IPMsg Torrent.exe`
3. 测试基本功能

### 3. Linux 版本测试
推荐使用 AppImage：
1. 下载 `IPMsg Torrent-1.0.0-x86_64-linux.AppImage`
2. 添加执行权限
3. 双击运行

### 4. P2P 功能测试
在不同设备/浏览器标签页中打开应用：
1. 打开第一个实例，设置用户名"用户A"
2. 打开第二个实例，设置用户名"用户B"
3. 确认两个实例都能发现对方
4. 测试发送消息

---

## 📊 构建状态汇总

| 平台 | 状态 | 文件位置 | 大小 |
|------|------|---------|------|
| Web | ✅ 完成 | `dist/` | ~38KB |
| Windows 便携版 | ✅ 完成 | `release/win-unpacked/` | ~217MB |
| Linux AppImage | ✅ 完成 | `release/` | 116MB |
| Linux DEB | ✅ 完成 | `release/` | 91MB |
| Linux TAR.GZ | ✅ 完成 | `release/` | 110MB |
| Windows NSIS | ⚠️ 需 Windows 环境 | - | - |
| Android APK | ⚠️ 需网络环境 | - | - |

---

## 🐛 常见问题

### Q: Windows 便携版和安装包有什么区别？
**A**: 便携版无需安装，直接运行 exe 即可。安装包会注册系统信息，创建开始菜单快捷方式。

### Q: 为什么推荐 AppImage 而不是 DEB？
**A**: AppImage 是通用格式，无需安装，兼容大多数 Linux 发行版。DEB 主要用于 Debian/Ubuntu 系统。

### Q: Web 版本如何使用？
**A**: 将 `dist/` 目录部署到任何 Web 服务器即可。也可以直接在浏览器打开 `dist/index.html`（部分功能可能受限）。

### Q: 如何获取 Android APK？
**A**: 在具有稳定网络的环境中按照上面的步骤重新构建，或联系开发者获取预编译版本。

---

## 📞 获取帮助

如有问题，请查看：
- [README.md](README.md) - 完整项目文档
- [QUICKSTART.md](QUICKSTART.md) - 快速开始指南
- GitHub Issues: https://github.com/zenjan1/ipmsg-torrent/issues

---

**最后更新**: 2026-05-06
**版本**: 1.0.0
