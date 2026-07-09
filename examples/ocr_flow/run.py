"""ocr_flow skill 入口:先预检再启动 runner。

预检失败时聚合输出红色 reason + 绿色 fix,不启动 runner。
环境变量 OCR_BASE 指定 OCR 服务地址,默认 http://localhost:11434(ollama 默认端口)。

跑:
    python examples/ocr_flow/run.py [--out DIR] [--resume DIR]
    OCR_BASE=http://<host>:<port> python examples/ocr_flow/run.py
"""

import asyncio
import os
from pathlib import Path

from esflow import run_flow_script
from esflow.check import CheckResult

OCR_BASE = os.environ.get("OCR_BASE", "http://localhost:11434")


def check_ocr_service() -> CheckResult | None:
    """OCR 服务 /health 是否返回 healthy。"""
    import json
    import urllib.request

    try:
        with urllib.request.urlopen(OCR_BASE + "/health", timeout=3) as resp:
            status = json.loads(resp.read().decode("utf-8")).get("status")
    except Exception as exc:  # noqa: BLE001 健康检查要兜住连接/解析任何异常
        return CheckResult(
            reason=f"OCR 服务不可达: {OCR_BASE} ({type(exc).__name__}: {exc})",
            fix=f"确认服务已启动,或设置正确的 OCR_BASE:\n"
                f"    OCR_BASE=http://<host>:<port> python examples/ocr_flow/run.py",
        )
    if status != "healthy":
        return CheckResult(reason=f"OCR 服务状态异常: status={status}")
    return None


def check_image_lib() -> CheckResult | None:
    """Pillow 是否可用(preprocess 节点放大 + 锐化依赖)。"""
    try:
        import PIL  # noqa: F401
    except ImportError:
        return CheckResult(
            reason="未安装 Pillow",
            fix="pip install pillow",
        )
    return None


async def main() -> int:
    return await run_flow_script(
        Path(__file__).parent,
        checks=(check_ocr_service, check_image_lib),
    )


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
