#!/bin/bash

set -e

echo "======================================"
echo "IPMsg Torrent - Build Script"
echo "======================================"
echo ""

check_command() {
    if ! command -v $1 &> /dev/null; then
        echo "Error: $1 is not installed"
        exit 1
    fi
}

echo "Checking prerequisites..."
check_command node
check_command npm
check_command java

echo "✓ All prerequisites installed"
echo ""

if [ ! -d "node_modules" ]; then
    echo "Installing npm dependencies..."
    npm install
    echo ""
fi

echo "======================================"
echo "Building Web Application"
echo "======================================"
npm run build:web
echo ""

if [ "$1" = "--electron" ] || [ "$1" = "all" ]; then
    echo "======================================"
    echo "Building Electron Application"
    echo "======================================"

    if [ "$2" = "win" ] || [ "$2" = "all" ]; then
        echo "Building Windows executable..."
        npm run build:win
    fi

    if [ "$2" = "linux" ] || [ "$2" = "all" ]; then
        echo "Building Linux packages..."
        npm run build:linux
    fi

    if [ "$2" = "mac" ] || [ "$2" = "all" ]; then
        echo "Building macOS packages..."
        npm run build:mac
    fi

    echo ""
fi

if [ "$1" = "--android" ] || [ "$1" = "all" ]; then
    echo "======================================"
    echo "Building Android Application"
    echo "======================================"

    if [ ! -d "android" ]; then
        echo "Initializing Capacitor..."
        npm run android:init
    fi

    echo "Building Android APK..."
    npm run android:build

    echo ""
    echo "APK location: android/app/build/outputs/apk/"
fi

echo "======================================"
echo "Build completed successfully!"
echo "======================================"
