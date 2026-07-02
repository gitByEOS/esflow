#!/usr/bin/env bash
# 构建 easyflow 发布包(sdist + wheel)。
set -euo pipefail
cd "$(dirname "$0")/.."

# 确保 build 模块可用(纯构建工具,不污染运行时依赖)
python3 -c "import build" 2>/dev/null || python3 -m pip install --quiet build

rm -rf dist/ build/ easyflow.egg-info/
log=$(python3 -m build 2>&1) || {
    echo "$log"
    echo "[build] 失败"
    exit 1
}

echo "[build] 产物:"
ls -1 dist/
