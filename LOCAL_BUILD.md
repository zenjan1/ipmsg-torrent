# IPMsg Torrent - 本地构建指南

## ⚠️ 当前环境说明

当前沙盒环境存在网络限制，无法完成以下构建：
- Android APK（需要访问 Maven 仓库下载依赖）

**好消息是**：所有代码和配置已准备就绪，你可以在本地环境快速完成 Android APK 构建！

---

## ✅ 已构建完成的版本

| 平台 | 状态 | 文件位置 | 下载方式 |
|------|------|----------|----------|
| **Web 版本** | ✅ 已构建 | `dist/` | 直接部署或浏览器打开 |
| **Windows 便携版** | ✅ 已构建 | `release/win-unpacked/` | 下载整个目录 |
| **Linux AppImage** | ✅ 已构建 | `release/` | 下载 .AppImage 文件 |
| **Linux DEB** | ✅ 已构建 | `release/` | 下载 .deb 文件 |
| **Linux TAR.GZ** | ✅ 已构建 | `release/` | 下载 .tar.gz 文件 |
| **Android APK** | ⏳ 待本地构建 | - | 见下方说明 |

---

## 📱 Android APK 本地构建步骤（5分钟完成）

### 前提条件
确保本地环境已安装：
- Node.js >= 16.0.0
- Java JDK >= 11
- Android SDK

### 步骤 1：克隆项目
```bash
git clone https://github.com/zenjan1/ipmsg-torrent.git
cd ipmsg-torrent
```

### 步骤 2：安装依赖
```bash
npm install
```

### 步骤 3：构建 Web 版本
```bash
npm run build:web
```

### 步骤 4：同步到 Android
```bash
npx cap sync android
```

### 步骤 5：构建 Debug APK
```bash
cd android
./gradlew assembleDebug
```

**或使用简化脚本**：
```bash
chmod +x build-android.sh
./build-android.sh
```

---

## 📦 构建输出位置

### Android APK
```
android/app/build/outputs/apk/debug/app-debug.apk
```

### Release 签名 APK（可选）
```bash
# 1. 生成签名密钥
cd android
./generate-keystore.sh

# 2. 配置签名（在 gradle.properties 中填写密码）

# 3. 构建 Release APK
./gradlew assembleRelease
```

**输出**：`android/app/build/outputs/apk/release/app-release.apk`

---

## 🎯 一键构建脚本

项目提供了多个自动化构建脚本：

### `build-android.sh` - Android 专用
```bash
./build-android.sh
```
自动完成：Web 构建 → 同步 → APK 构建

### `build-all.sh` - 交互式菜单
```bash
./build-all.sh
```
交互式选择要构建的平台

### `build-win.sh` - Windows
```bash
./build-win.sh
```

### `build-linux.sh` - Linux
```bash
./build-linux.sh
```

---

## 🔧 常见问题解决

### 问题 1：Gradle 下载失败
**解决**：检查网络连接，或配置镜像源
```bash
# 在 android/build.gradle 中已配置阿里云镜像
```

### 问题 2：Android SDK 找不到
**解决**：配置 ANDROID_HOME 环境变量
```bash
export ANDROID_HOME=/path/to/android/sdk
```

### 问题 3：Java 版本不兼容
**解决**：确保 Java >= 11
```bash
java -version  # 检查 Java 版本
```

---

## 📥 如何获取所有安装包

### 方法 1：下载 release 目录
GitHub release 页面（需要创建 releases）：
https://github.com/zenjan1/ipmsg-torrent/releases

### 方法 2：本地构建后打包
构建完成后，在项目根目录执行：
```bash
mkdir -p downloads
cp -r dist downloads/web
cp -r release/downloads/downloads/windows
cp release/*.AppImage downloads/downloads/
cp release/*.deb downloads/downloads/
```

### 方法 3：使用 GitHub Actions 自动构建
可以配置 GitHub Actions，在每次代码更新时自动构建所有平台。

---

## 🚀 发布到 GitHub Releases

构建完成后，可以使用 GitHub CLI 创建发布：

```bash
# 安装 GitHub CLI（如未安装）
brew install gh  # macOS
# 或参考 https://github.com/cli/cli#installation

# 登录
gh auth login

# 创建发布
gh release create v1.0.0 \
  --title "IPMsg Torrent v1.0.0" \
  --notes "多平台去中心化聊天软件" \
  release/*.exe \
  release/*.AppImage \
  release/*.deb \
  android/app/build/outputs/apk/debug/app-debug.apk
```

---

## 📊 版本信息

- **当前版本**: 1.0.0
- **项目地址**: https://github.com/zenjan1/ipmsg-torrent
- **最后更新**: 2026-05-07

---

## 💡 建议的下一步

1. ✅ **本地构建 Android APK** - 按照上面的步骤操作
2. 📦 **创建 GitHub Releases** - 方便用户下载
3. 🔄 **配置 GitHub Actions** - 自动构建所有平台
4. 📱 **上传到应用商店** - Google Play、华为应用市场等
5. 📖 **完善文档** - 添加使用教程和截图

---

如有任何问题，请在 GitHub Issues 中提问：
https://github.com/zenjan1/ipmsg-torrent/issues
