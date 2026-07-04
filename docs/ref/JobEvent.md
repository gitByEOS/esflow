# JobEvent

## 模块

`esflow.event` — `from esflow import JobEvent, trace, delta, checkpoint, final, error, end, esflow_event`

## 职责

统一事件信封。[`Runner.run()`](Runner.md#run) 产出的唯一事件流，`type` 字段决定哪些字段有效。事件经 [`apply_event`](JobState.md#apply_event) 折叠成内存 [`JobState`](JobState.md) 供视图消费。

## 定义

```python
@dataclass
class JobEvent:
    type: Literal["trace", "delta", "checkpoint", "final", "error", "end"]
    at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    run_id: str | None = None
    status: TraceStatus | None = None
    detail: str | None = None
    text: str | None = None
    artifact: Any | None = None
    message: str | None = None
```

### 字段

| 字段 | 类型 | 出现在哪些 type | 用途 |
|---|---|---|---|
| `type` | `Literal` | 全部 | 事件类型，决定哪些字段有效 |
| `at` | `str` | 全部 | ISO UTC 时间戳，默认自动填 |
| `run_id` | `str \| None` | 除 `end` | 标识一次节点运行,即 [`Node.replica_id`](Node.md#运行时字段)（普通节点 = base id，副本 = `worker#2`） |
| `status` | `TraceStatus \| None` | `trace` | `queued` / `running` / `done` / `error` / `skipped` |
| `detail` | `str \| None` | `trace` | 状态描述，如 `"就绪:抓取数据"` |
| `text` | `str \| None` | `delta` | 节点产出增量文本 |
| `artifact` | `Any \| None` | `checkpoint` / `final` | 节点 artifact |
| `message` | `str \| None` | `error` | 错误信息 |

### TraceStatus

```python
TraceStatus = Literal["queued", "running", "done", "error", "skipped"]
```

`trace` 事件 `status` 字段值域，与 [`NodeStatus`](JobState.md#nodestatus) 重叠但不含 `idle`/`paused`：

- 不含 `idle` — idle 是节点初始状态,不发事件
- 不含 `paused` — 暂停由 `checkpoint` 事件体现,trace 不重复发

## 事件类型

| type | 含义 | 触发时机 |
|---|---|---|
| `trace` | 节点状态变更（`queued`/`running`/`done`/`error`/`skipped`） | runner 推进节点状态时 |
| `delta` | 节点产出增量文本 | 目前 runner **不产出** delta 事件,事件类型保留供未来 streaming 节点使用;事件消费代码应忽略或累积 `text` 字段 |
| `checkpoint` | 节点到暂停点，等外部 `resume`/`retry`/`abort` | `checkpoint TO_HUMAN` 节点 `run` 完成后；`TO_AGENT` 节点就绪后；`break_before` 节点就绪后 |
| `final` | 节点最终 artifact | `run` 完成、`deliver` 通过后 |
| `error` | 错误（含接手/脱手确认失败、节点异常、`aborted`） | `accept`/`run`/`deliver` 异常或返回 `False`、`abort()` |
| `end` | job 结束 | 无就绪节点、error 后、abort 后 |

## 节点生命周期事件时序

一个节点从就绪到结束，正常路径事件序列：

```text
trace(queued)  →  trace(running)  →  [checkpoint]  →  final  →  (下一轮)
                                  ↑                ↑
                            break_before       Checkpoint.TO_HUMAN
                            (run 前暂停)        (run 后暂停)
```

- 无暂停点时：`trace(queued) → trace(running) → final`
- `break_before + Checkpoint.TO_HUMAN` 共存：**发两次** checkpoint
  - run 前：`trace(queued) → trace(running) → checkpoint(artifact=None)` → resume → run 执行
  - run 后：`trace(running) → checkpoint(artifact=值)` → resume → `final`
- `accept` 抛异常：`trace(queued) → error → end`(无 running)
- `accept` 返回 `False`：`trace(queued) → trace(running) → trace(skipped)`（无 `final`，artifact 为 `None`）
- `run` 返回 `FanOut`：`trace(queued) → trace(running) → trace(done)`（无 `final`,不产 artifact,详见 [`FanOut`](FanOut.md)）
- `run`/`deliver` 抛异常或 `deliver` 返回 `False`：`trace(running) → error → end`（job 结束）
- `abort()`：当前节点 `error`，整 job `end`

`end` 事件一定出现在 job 终止时（无就绪节点 / error / abort），不带 `run_id`。

## 构造函数

便捷构造，避免每次写 `JobEvent(type=..., ...)`：

```python
def trace(run_id: str, status: TraceStatus, detail: str = "") -> JobEvent
def delta(run_id: str, text: str) -> JobEvent
def checkpoint(run_id: str, artifact: Any) -> JobEvent
def final(run_id: str, artifact: Any) -> JobEvent
def error(run_id: str | None, message: str) -> JobEvent
def end() -> JobEvent
```

```python
from esflow import trace, final

ev = trace("fetch", "running", "开始:抓取数据")
ev = final("fetch", {"items": [1, 2, 3]})
```

## esflow_event

```python
def esflow_event(event: JobEvent) -> None
```

按事件类型打印一行，CLI / skill `run.py` / view 共用的统一消费入口。`checkpoint` 只打印 artifact，不打印交互提示（交互由调用方自行处理）。

```python
from esflow import Runner, esflow_event

runner = Runner.load("./my_flow")
async for event in runner.run():
    esflow_event(event)
```

输出形如：

```text
[fetch] running: 开始:抓取数据
[fetch] artifact: {'items': [1, 2, 3]}
[end]
```

## 相关

- [`Runner`](Runner.md) — 事件产出方
- [`JobState`](JobState.md) — 事件折叠状态
- [`Checkpoint`](Checkpoint.md) — `checkpoint` 事件触发条件
