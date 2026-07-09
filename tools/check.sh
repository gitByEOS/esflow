#!/usr/bin/env bash
# 检测:语法编译 + 全量测试。静默,仅报错时显示详情。
set -euo pipefail
cd "$(dirname "$0")/.."

log_dir="${TMPDIR:-/tmp}/esflow-check"
mkdir -p "$log_dir"
compile_log="$log_dir/compile.log"
test_log="$log_dir/pytest.log"
max_lines=50

echo "[check] 编译检测..."
python3 -m compileall -q esflow tests examples >"$compile_log" 2>&1 || {
    tail -n "$max_lines" "$compile_log"
    echo "[check] 完整日志:$compile_log"
    echo "[check] 编译失败"
    exit 1
}

echo "[check] 测试..."
pytest -q --tb=short >"$test_log" 2>&1 || {
    tail -n "$max_lines" "$test_log"
    echo "[check] 完整日志:$test_log"
    echo "[check] 测试失败"
    exit 1
}

echo "[check] 全部通过"
