# Checkpoint

## 模块

`esflow.node` — `from esflow import Checkpoint`

## 职责

节点暂停点枚举。`Node.checkpoint` 类属性取它，决定节点在哪个时机暂停整 job 等外部确认。

## 定义

```python
class Checkpoint(str, Enum):
    NONE = "none"
    AFTER = "after"
```

继承 `str`，兼容字符串字面量比较。

## 取值

| 名称 | 值 | 行为 |
|---|---|---|
| `NONE` | `"none"` | 默认，不暂停 |
| `AFTER` | `"after"` | `run` 完成后暂停，展示 artifact 等外部 [`Runner.resume()`](Runner.md#resume) / `retry(from_node)` / `abort()` |

## 用法

```python
from esflow import Node, Checkpoint


class GenSrt(Node):
    id = "gen_srt"
    checkpoint = Checkpoint.AFTER

    def run(self, ctx) -> dict:
        ...
        return {"srt": srt_path}
```

跑到 `gen_srt` 完成后 emit `checkpoint` 事件，整 job 暂停，等控制信号：

- `Runner.resume()` — 确认继续，paused 节点转 done，跑下一轮
- `Runner.retry(from_node)` — 清 `from_node` 及下游重跑，上游复用
- `Runner.abort()` — 中止 job

CLI 入口在 checkpoint 时从 stdin 读短命令：`c` continue / `r` retry / `a` abort。

- `c` 等价 `runner.resume()`
- `r` 等价 `runner.retry(from_node=<当前 checkpoint 节点>)` — 清当前节点及下游重跑,上游复用
- `a` 等价 `runner.abort()`

## 两种暂停点来源

`checkpoint` 事件有两个触发来源，职责不同，可共存：

| 来源 | 声明方式 | 触发时机 | 适用场景 |
|---|---|---|---|
| `Checkpoint.AFTER` | 节点类属性（声明式） | `run` 完成、`deliver` 通过后 | 产物需人工确认的固有暂停（如生成字幕需人工校对后才往下走） |
| `break_before={"X"}` | `run()` 入参（命令式） | X 就绪后、`run` 执行前 | 调试时临时打断点，观察上游产物再决定是否跑 X |

**声明式仅支持 `AFTER`;`BEFORE` 语义只能通过 `break_before` 命令式触发**——节点没有 `Checkpoint.BEFORE` 类属性,因为"run 前暂停"通常是调试行为而非节点固有属性。

共存时**发两次** `checkpoint` 事件：`break_before` 在 run 前发一次(artifact=None,resume 后节点才执行),`AFTER` 在 run 后发一次(artifact=实际值,resume 后节点转 done)。调用方需处理两次 `checkpoint`,各调一次 `resume()`。

## 限制

- 动态扇出副本节点（`split` / `worker#i`）不挂 checkpoint
- 扇入节点（如 `merge`）可以挂 checkpoint

## 相关

- [`Node`](Node.md#checkpoint) — `checkpoint` 类属性
- [`Runner`](Runner.md) — `resume` / `retry` / `abort` 控制信号
- [`JobEvent`](JobEvent.md) — `checkpoint` 事件
