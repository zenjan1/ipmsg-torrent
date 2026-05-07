#!/bin/bash

# IPMsg Torrent - Android APK 构建脚本
# 使用阿里云镜像源

set -e

cd "$(dirname "$0")"

echo "======================================"
echo "IPMsg Torrent - Android 构建"
echo "使用阿里云镜像源（抖音旗下）"
echo "======================================"
echo ""

# 1. 构建 Web 版本
echo "[1/5] 构建 Web 版本..."
cd ..
npm run build:web
cd android

# 2. 清理之前的构建
echo ""
echo "[2/5] 清理构建缓存..."
rm -rf .gradle build app/build

# 3. 同步 Web 资源到 Android
echo ""
echo "[3/5] 同步 Web 资源..."
cd ..
npx cap sync android
cd android

# 4. 构建 Debug APK
echo ""
echo "[4/5] 构建 Debug APK（使用阿里云镜像）..."
echo "如果需要使用 init.gradle，执行: ./gradlew --init-script init.gradle assembleDebug"
echo ""

# 尝试构建 - 先检查网络连接
echo "检查镜像源连接..."
./gradlew tasks --all > /dev/null 2>&1 || true

echo ""
echo "开始构建 APK..."

# 使用默认的阿里云镜像源（已在 build.gradle 和 settings.gradle 中配置）
./gradlew assembleDebug --no-daemon

echo ""
echo "[5/5] 构建完成！"
echo ""

APK_PATH="app/build/outputs/apk/debug/app-debug.apk"

if [ -f "$APK_PATH" ]; then
    APK_SIZE=$(ls -lh "$APK_PATH" | awk '{print $5}')
    echo "✅ APK 构建成功！"
    echo ""
    echo "📦 APK 文件: $APK_PATH"
    echo "📏 文件大小: $APK_SIZE"
    echo ""
    echo "📥 你可以直接下载并安装这个 APK！"
else
    echo "❌ APK 构建失败，请检查错误信息"
    exit 1
fi
