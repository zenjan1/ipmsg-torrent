#!/bin/bash

set -e

echo "======================================"
echo "IPMsg Torrent - Linux Build"
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

echo "Building web application..."
npm run build:web
echo ""

echo "Building Electron application for Linux..."
npx vite build && npx electron-builder --linux --config
echo ""

echo "======================================"
echo "Build completed successfully!"
echo "Executable location: release/"
echo "======================================"
