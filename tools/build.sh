#!/usr/bin/env bash
# 构建 esflow 发布包:检查 -> 清理 -> sdist/wheel -> 临时安装验证。
set -euo pipefail
cd "$(dirname "$0")/.."

VENV_DIR="${TMPDIR:-/tmp}/esflow-build-venv"

echo "[build] 运行项目检查..."
bash tools/check.sh

# 确保 build 模块可用(纯构建工具,不污染运行时依赖)
echo "[build] 检查构建工具..."
python3 -c "import build" 2>/dev/null || python3 -m pip install --quiet build

echo "[build] 清理旧构建产物..."
rm -rf dist/ build/ esflow.egg-info/

echo "[build] 构建 sdist 和 wheel..."
log=$(python3 -m build 2>&1) || {
    echo "$log"
    echo "[build] 失败"
    exit 1
}

echo "[build] 创建临时安装环境..."
rm -rf "$VENV_DIR"
python3 -m venv "$VENV_DIR"

echo "[build] 安装 wheel 验证..."
"$VENV_DIR/bin/pip" install --quiet dist/*.whl
"$VENV_DIR/bin/esflow" --help >/dev/null

echo "[build] 产物:"
ls -1 dist/

echo "[build] 全部通过"
