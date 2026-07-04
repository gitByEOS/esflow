# Checkpoint

## 模块

`easyflow.node` — `from easyflow import Checkpoint`

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
from easyflow import Node, Checkpoint


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

## 限制

- 动态扇出副本节点（`split` / `worker#i`）不挂 checkpoint
- 扇入节点（如 `merge`）可以挂 checkpoint

## 相关

- [`Node`](Node.md#checkpoint) — `checkpoint` 类属性
- [`Runner`](Runner.md) — `resume` / `retry` / `abort` 控制信号
- [`JobEvent`](JobEvent.md) — `checkpoint` 事件
