# Checkpoint

## 模块

`esflow.node` — `from esflow import Checkpoint`

## 职责

节点暂停点枚举。`Node.checkpoint` 类属性取它,决定节点在哪个时机暂停、交给谁处理。

## 定义

```python
class Checkpoint(str, Enum):
    NONE = "none"
    TO_HUMAN = "to_human"
    TO_AGENT = "to_agent"
```

继承 `str`,兼容字符串字面量比较。维度统一为"暂停后交给谁":`NONE` 不暂停 / `TO_HUMAN` 等人确认 / `TO_AGENT` 等外部 agent 写产物。

## 取值

| 名称 | 值 | 行为 |
|---|---|---|
| `NONE` | `"none"` | 默认,不暂停 |
| `TO_HUMAN` | `"to_human"` | `run` 完成后暂停,展示 artifact 等人机确认 [`Runner.resume()`](Runner.md#resume) / `retry(from_node)` / `abort()` |
| `TO_AGENT` | `"to_agent"` | 不调 `run`,就绪即 emit checkpoint 退出进程,产物由外部 agent 写入 `output_dir`,`--resume` 时框架扫文件构造 artifact + `deliver` 校验 |

## 用法 - TO_HUMAN(人机确认)

```python
from esflow import Node, Checkpoint


class GenSrt(Node):
    id = "gen_srt"
    checkpoint = Checkpoint.TO_HUMAN

    def run(self, ctx) -> dict:
        ...
        return {"srt": srt_path}
```

跑到 `gen_srt` 完成后 emit `checkpoint` 事件,整 job 暂停,等控制信号:

- `Runner.resume()` — 确认继续,paused 节点转 done,跑下一轮
- `Runner.retry(from_node)` — 清 `from_node` 及下游重跑,上游复用
- `Runner.abort()` — 中止 job

CLI 入口在 checkpoint 时从 stdin 读短命令:`c` continue / `r` retry / `a` abort。

- `c` 等价 `runner.resume()`
- `r` 等价 `runner.retry(from_node=<当前 checkpoint 节点>)` — 清当前节点及下游重跑,上游复用
- `a` 等价 `runner.abort()`

## 用法 - TO_AGENT(agent 介入)

```python
from esflow import Node, Checkpoint


class AgentSummary(Node):
    id = "agent_summary"
    checkpoint = Checkpoint.TO_AGENT

    def deliver(self, artifact) -> bool:
        return "summary.txt" in artifact.get("files", [])
```

节点不实现 `run`。链路:

1. `esflow run <flow> --out <path>` → 跑到 TO_AGENT 节点 emit checkpoint(artifact=上游产物集合),写 `_break_to_agent.json`,进程退出(exit 2)
2. 外部 agent 读 stderr 拿上游产物 → 写产物文件到 `<path>/<to_agent 节点>/`
3. `esflow run --resume <path>` → 框架扫文件构造 `artifact={"output_dir", "files"}` + `deliver` 校验 + 转 DONE + 跑下游

agent 契约零 JSON:只读 stderr + 写产物文件,不碰 artifact.json。

## 两种暂停点来源

`checkpoint` 事件有三个触发来源,职责不同,可共存:

| 来源 | 声明方式 | 触发时机 | 适用场景 |
|---|---|---|---|
| `Checkpoint.TO_HUMAN` | 节点类属性(声明式) | `run` 完成、`deliver` 通过后 | 产物需人工确认的固有暂停 |
| `Checkpoint.TO_AGENT` | 节点类属性(声明式) | 节点就绪、`run` 执行前 | 产物由外部 agent 写入 |
| `break_before={"X"}` | `run()` 入参(命令式) | X 就绪后、`run` 执行前 | 调试时临时打断点,观察上游产物再决定是否跑 X |

**声明式仅支持 `TO_HUMAN` / `TO_AGENT`;通用 `BEFORE` 语义只能通过 `break_before` 命令式触发**——节点没有 `Checkpoint.BEFORE` 类属性,因为"run 前暂停"通常是调试行为或 agent 接管,有专用枚举值。

`break_before + TO_HUMAN` 共存时**发两次** `checkpoint` 事件:`break_before` 在 run 前发一次(artifact=None,resume 后节点才执行),`TO_HUMAN` 在 run 后发一次(artifact=实际值,resume 后节点转 done)。调用方需处理两次 `checkpoint`,各调一次 `resume()`。

## 限制

- 动态扇出副本节点(`split` / `worker#i`)不挂 checkpoint
- 扇入节点(如 `merge`)可以挂 checkpoint
- `TO_AGENT` 节点的 `run` 不被框架调用,产物纯外部写入

## 相关

- [`Node`](Node.md#checkpoint) — `checkpoint` 类属性
- [`Runner`](Runner.md) — `resume` / `retry` / `abort` 控制信号;`--resume` CLI 续跑 TO_AGENT
- [`JobEvent`](JobEvent.md) — `checkpoint` 事件
