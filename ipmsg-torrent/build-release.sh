#!/bin/bash

set -e

echo "========================================"
echo "PPX Release 构建脚本"
echo "========================================"

# 设置环境变量
export CARGO_PROFILE_RELEASE_LTO=fat
export CARGO_PROFILE_RELEASE_STRIP=true
export CARGO_PROFILE_RELEASE_OPT_LEVEL=z
export CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1

# 清理
echo "清理构建..."
cargo clean

# 检查代码
echo "检查代码..."
cargo check --release

# 构建 CLI 版本
echo "构建 CLI..."
cargo build --release --bin ipmsg-cli

# 构建 Tauri 应用
echo "构建 Tauri 应用..."
cd crates/app/src-tauri
cargo tauri build --target x86_64-pc-windows-msvc 2>&1 || cargo tauri build --target x86_64-unknown-linux-gnu 2>&1 || cargo tauri build

echo "========================================"
echo "构建完成！"
echo "========================================"
