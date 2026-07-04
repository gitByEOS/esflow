# CheckResult / pass_check / FlowCheckError

## 模块

`easyflow.check` — `from easyflow.check import pass_check, CheckResult, FlowCheckError`

## 职责

启动预检。skill 的 `run.py` 在 [`Runner.run()`](Runner.md) 之前显式调用 `pass_check`，跑所有检查函数，任一失败聚合抛 `FlowCheckError`，不启动 runner。

## CheckResult

```python
@dataclass
class CheckResult:
    reason: str
    fix: str | None = None
```

检查结果。

| 字段 | 类型 | 用途 |
|---|---|---|
| `reason` | `str` | 失败原因 |
| `fix` | `str \| None` | 修复指引（多行字符串，可选），聚合时绿色高亮 |

## pass_check

```python
def pass_check(*checks: Callable[[], object]) -> None
```

跑所有检查函数，全过才返回，否则聚合抛 `FlowCheckError`。

### 检查函数签名

`() -> None | str | CheckResult`，抛异常也算失败：

| 返回 | 含义 |
|---|---|
| `None` | 通过 |
| `str` | 失败，视为只有 `reason` 的快捷 |
| `CheckResult` | 失败，可带 `fix` |

### 用法

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
        print(exc, file=sys.stderr)
        return 1
    runner = Runner.load(str(Path(__file__).parent))
    async for event in runner.run():
        easyflow_event(event)
```

### 失败输出

所有失败原因**聚合后一次性抛**，不逐个抛。`reason` 红色、`fix` 绿色高亮：

```text
预检失败:
- check_ocr_service: OCR 服务不可达: http://localhost:11434
  修复:
    OCR_BASE=http://<host>:<port> python run.py
- check_image_lib: 未安装 Pillow
  修复:
    pip install pillow
```

## FlowCheckError

```python
class FlowCheckError(Exception):
    failures: list[str]
```

预检失败异常。`failures` 是已格式化的失败条目列表（含 ANSI 颜色），`str(exc)` 输出带换行的聚合信息。

## 注意

节点里的**外部依赖 import 放 `run` 方法内**（非模块顶部），让预检先于依赖加载执行——否则 loader 加载 `nodes/*.py` 时就会因 `ImportError` 失败，预检无从触发。

## 相关

- [`Runner`](Runner.md) — 预检通过后启动
- 完整示例见 [../pass_check.md](../pass_check.md) 与 `examples/ocr_flow/`
