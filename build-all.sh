#!/bin/bash

echo "======================================"
echo "IPMsg Torrent 多平台构建脚本"
echo "======================================"
echo ""

show_menu() {
    echo "请选择要构建的平台："
    echo "1) Windows (.exe)"
    echo "2) Linux (.AppImage, .deb)"
    echo "3) Android (.apk)"
    echo "4) Web 版本"
    echo "5) 全部平台"
    echo "0) 退出"
    echo ""
    read -p "请输入选项: " choice
}

build_web() {
    echo "正在构建 Web 版本..."
    npm run build:web
    if [ $? -eq 0 ]; then
        echo "✅ Web 版本构建成功！"
        echo "📦 输出目录: dist/"
    else
        echo "❌ Web 版本构建失败！"
        return 1
    fi
}

build_windows() {
    echo "正在构建 Windows 安装包..."
    npm run build:win
    if [ $? -eq 0 ]; then
        echo "✅ Windows 安装包构建成功！"
        echo "📦 输出目录: release/"
    else
        echo "❌ Windows 构建失败！"
        return 1
    fi
}

build_linux() {
    echo "正在构建 Linux 安装包..."
    npm run build:linux
    if [ $? -eq 0 ]; then
        echo "✅ Linux 安装包构建成功！"
        echo "📦 输出目录: release/"
    else
        echo "❌ Linux 构建失败！"
        return 1
    fi
}

build_android() {
    echo "正在构建 Android APK..."
    echo "首次构建可能需要较长时间下载 Gradle..."
    npm run android:build
    if [ $? -eq 0 ]; then
        echo "✅ Android APK 构建成功！"
        echo "📦 Debug APK: android/app/build/outputs/apk/debug/app-debug.apk"
        echo "📦 Release APK: android/app/build/outputs/apk/release/app-release.apk"
    else
        echo "❌ Android 构建失败！"
        return 1
    fi
}

if [ $# -eq 0 ]; then
    show_menu
else
    choice=$1
fi

case $choice in
    1)
        build_web
        build_windows
        ;;
    2)
        build_web
        build_linux
        ;;
    3)
        build_web
        build_android
        ;;
    4)
        build_web
        ;;
    5)
        echo "正在构建所有平台..."
        build_web
        build_windows
        build_linux
        build_android
        echo ""
        echo "======================================"
        echo "🎉 所有平台构建完成！"
        echo "======================================"
        ;;
    0)
        echo "退出构建"
        exit 0
        ;;
    *)
        echo "无效选项"
        exit 1
        ;;
esac

echo ""
echo "构建日志已保存到 build.log"
