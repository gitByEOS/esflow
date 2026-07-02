#!/usr/bin/env bash
# 检测:语法编译 + 全量测试。静默,仅报错时显示详情。
set -euo pipefail
cd "$(dirname "$0")/.."

echo "[check] 编译检测..."
log=$(python3 -m compileall -q easyflow tests examples 2>&1) || {
    echo "$log"
    echo "[check] 编译失败"
    exit 1
}

echo "[check] 测试..."
log=$(pytest -q --tb=short 2>&1) || {
    echo "$log"
    echo "[check] 测试失败"
    exit 1
}

echo "[check] 全部通过"
