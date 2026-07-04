# JobState / RunState / NodeStatus / JobStatus / apply_event

## 模块

`esflow.state` — `from esflow import JobState, RunState, NodeStatus, apply_event`

## 职责

把 [`JobEvent`](JobEvent.md) 流折叠成可读的内存状态，供 view/CLI 渲染进度。不落库，每次 `run()` 重建。

## NodeStatus

```python
class NodeStatus(str, Enum):
    IDLE = "idle"
    QUEUED = "queued"
    RUNNING = "running"
    PAUSED = "paused"
    DONE = "done"
    ERROR = "error"
    SKIPPED = "skipped"
```

节点运行状态。继承 `str`，兼容 `== "done"` 与 dict 字符串 key。

## JobStatus

```python
class JobStatus(str, Enum):
    RUNNING = "running"
    PAUSED = "paused"
    DONE = "done"
    ERROR = "error"
```

job 整体状态，`NodeStatus` 的运行子集，不含节点级 `idle`/`queued`/`skipped`。

## RunState

```python
@dataclass
class RunState:
    run_id: str
    status: NodeStatus = NodeStatus.IDLE
    artifact: Any | None = None
    detail: str = ""
    text: str = ""
```

单次节点运行（per-replica）的事件折叠状态。`text` 累积 `delta` 事件的增量文本。

## JobState

```python
@dataclass
class JobState:
    flow_id: str
    runs: dict[str, RunState] = field(default_factory=dict)
    status: JobStatus = JobStatus.RUNNING
    finished: bool = False
```

job 整体折叠状态，`runs` 按 `run_id` 索引各节点。`runner.state` 就是它。

## apply_event

```python
def apply_event(state: JobState, event: JobEvent) -> JobState
```

把 event 折进 state，返回同一 state（**就地更新**）。runner 每产出一个 event 调一次。

折叠规则：

| event.type | 行为 |
|---|---|
| `trace` | 对应 `RunState.status` 更新为 `event.status`，`detail` 非空则覆盖 |
| `delta` | `RunState.text += event.text` |
| `checkpoint` | `RunState.status = PAUSED`、`artifact = event.artifact`，`JobState.status = PAUSED` |
| `final` | `RunState.artifact = event.artifact`、`status = DONE` |
| `error` | 对应 `RunState.status = ERROR`，`JobState.status = ERROR` |
| `end` | `JobState.finished = True`，未 error 则 `status = DONE` |

## 用法

通常不直接调用，由 [`Runner`](Runner.md) 内部维护。需要自己消费事件流时：

```python
from esflow import Runner, JobState, apply_event

runner = Runner.load("./my_flow")
state = runner.state          # runner 已在跑过程中维护同一份 state
async for event in runner.run():
    apply_event(state, event) # 已由 runner 调用，这里仅示意
    print(state.status, state.runs[event.run_id].status if event.run_id else "")
```

## 相关

- [`JobEvent`](JobEvent.md) — 折叠输入
- [`Runner`](Runner.md) — 维护 `runner.state`
