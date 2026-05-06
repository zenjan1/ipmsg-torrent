#!/bin/bash

set -e

echo "======================================"
echo "IPMsg Torrent - Android Build"
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
check_command gradle

echo "✓ All prerequisites installed"
echo ""

if [ ! -d "node_modules" ]; then
    echo "Installing npm dependencies..."
    npm install
    echo ""
fi

echo "Building web application..."
npm run build:web
echo ""

if [ ! -d "android" ]; then
    echo "Initializing Capacitor..."
    npm run android:init
    echo ""
fi

echo "Copying web assets to Android..."
npx cap copy android
echo ""

echo "Syncing Capacitor..."
npx cap sync android
echo ""

echo "Building Android APK..."
cd android

if [ "$1" = "release" ]; then
    echo "Building release APK..."
    ./gradlew assembleRelease
    APK_DIR="app/build/outputs/apk/release"
else
    echo "Building debug APK..."
    ./gradlew assembleDebug
    APK_DIR="app/build/outputs/apk/debug"
fi

cd ..

echo ""
echo "======================================"
echo "Build completed successfully!"
echo "APK location: android/$APK_DIR/"
echo "======================================"

ls -lh android/$APK_DIR/*.apk 2>/dev/null || echo "APK files:"
find android -name "*.apk" -type f 2>/dev/null | head -5
