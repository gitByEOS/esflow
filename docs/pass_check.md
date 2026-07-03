# 启动预检 `pass_check`

skill 的 `run.py` 在 `runner.run()` 前用 `pass_check` 跑检查函数,任一失败聚合抛 `FlowCheckError`,不启动 runner。检查函数签名 `() -> None | str | CheckResult`:`None` 通过,`str` 视为只有 `reason` 的快捷,`CheckResult` 可带 `fix` 修复指引。

```python
from easyflow import Runner, easyflow_event
from easyflow.check import pass_check, CheckResult, FlowCheckError

def check_image_lib() -> CheckResult | None:
    """Pillow 是否可用(节点放大 + 锐化依赖)。"""
    try:
        import PIL
    except ImportError:
        return CheckResult(reason="未安装 Pillow", fix="pip install pillow")
    return None

def check_ocr_service() -> CheckResult | None:
    """OCR 服务 /health 是否返回 healthy。"""
    import json, urllib.request
    try:
        with urllib.request.urlopen(OCR_BASE + "/health", timeout=3) as resp:
            status = json.loads(resp.read().decode("utf-8")).get("status")
    except Exception as exc:
        return CheckResult(
            reason=f"OCR 服务不可达: {OCR_BASE}",
            fix="OCR_BASE=http://<host>:<port> python run.py",
        )
    return None if status == "healthy" else CheckResult(reason=f"状态异常: {status}")

async def main():
    try:
        pass_check(check_ocr_service, check_image_lib)
    except FlowCheckError as exc:
        print(exc, file=sys.stderr); return 1
    runner = Runner.load(str(Path(__file__).parent))
    async for event in runner.run():
        easyflow_event(event)
    return 0 if runner.state.status != "error" else 1
```

失败时聚合输出(`reason` 红、`fix` 绿):

```
预检失败:
- check_ocr_service: OCR 服务不可达: http://localhost:11434
  修复:
    OCR_BASE=http://<host>:<port> python run.py
- check_image_lib: 未安装 Pillow
  修复:
    pip install pillow
```

节点里的外部依赖 import 放 `run` 方法内(非模块顶部),让预检先于依赖加载执行——否则 loader 加载 `nodes/*.py` 时就会因 `ImportError` 失败,预检无从触发。完整示例见 [`examples/ocr_flow/`](../examples/ocr_flow)。
