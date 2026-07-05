# JobEvent

`esflow.event` — `from esflow import JobEvent, trace, delta, checkpoint, final, error, end, esflow_event`

统一事件信封。[`Runner.run()`](Runner.md#run) 产出的唯一事件流,`type` 字段决定哪些字段有效。事件经 [`apply_event`](JobState.md#apply_event) 折叠成 [`JobState`](JobState.md) 供视图消费。

## 事件类型

| type | 含义 | 触发 |
|---|---|---|
| `trace` | 节点状态变更(`queued`/`running`/`done`/`error`/`skipped`) | runner 推进节点状态时 |
| `delta` | 节点产出增量文本 | **目前 runner 不产出**,保留供未来 streaming 节点;消费代码应忽略或累积 `text` |
| `checkpoint` | 节点到暂停点,等外部 `resume`/`retry`/`abort` | `TO_HUMAN` 节点 `run` 后;`TO_AGENT` 节点就绪后;`break_before` 节点就绪后 |
| `final` | 节点最终 artifact | `run` 完成、`deliver` 通过后 |
| `error` | 错误(接手/脱手失败、节点异常、`aborted`) | `accept`/`run`/`deliver` 异常或返回 `False`、`abort()` |
| `end` | job 结束 | 无就绪节点 / error 后 / abort 后,不带 `run_id` |

## 定义

```python
@dataclass
class JobEvent:
    type: Literal["trace", "delta", "checkpoint", "final", "error", "end"]
    at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    run_id: str | None = None
    status: TraceStatus | None = None       # trace
    detail: str | None = None               # trace
    text: str | None = None                 # delta
    artifact: Any | None = None             # checkpoint / final
    resume_hint: dict[str, Any] | None = None   # checkpoint (TO_AGENT)
    message: str | None = None              # error
    exc_attrs: dict[str, Any] | None = None # error,跨进程 as_exception 回填用
    exc: BaseException | None = field(default=None, repr=False)  # error 同进程,不进 json
    exc_type: str | None = None             # error 类全名,跨进程 as_exception import 用
```

`TraceStatus = Literal["queued", "running", "done", "error", "skipped"]`,与 [`NodeStatus`](JobState.md#nodestatus) 重叠但不含 `idle`/`paused`(idle 不发事件,暂停由 `checkpoint` 体现)。

## as_exception() -> BaseException

还原成原异常。三段降级:

1. **同进程**(有 `exc`):返回 `exc`,类型/属性/身份全保留
2. **跨进程**(无 `exc`,有 `exc_type` + `exc_attrs`):按 `exc_type` 全名 import 类,`__new__` 绕过 `__init__` 实例化,回填 `__dict__` + 设 `args=(message,)`;import 失败降级
3. **完全降级**: `RuntimeError(message)`

skill 用 `raise event.as_exception()` 一行替代手拼 `__dict__` 还原,同进程/跨进程统一。

## 构造函数

```python
def trace(run_id, status, detail="") -> JobEvent
def delta(run_id, text) -> JobEvent
def checkpoint(run_id, artifact) -> JobEvent
def final(run_id, artifact) -> JobEvent
def error(run_id, message, exc_attrs=None, exc=None, exc_type=None) -> JobEvent
def end() -> JobEvent
```

## esflow_event(event) -> None

按事件类型打印一行,CLI / skill `run.py` / view 共用的统一消费入口。所有输出走 stderr,保留 stdout 给管道数据。`checkpoint` 只打印 artifact,不打印交互提示。

```python
from esflow import Runner, esflow_event

runner = Runner.load("./my_flow")
async for event in runner.run():
    esflow_event(event)
```

输出形如:

```text
[fetch] running: 开始:抓取数据
[fetch] artifact: {'items': [1, 2, 3]}
[end]
```

## 节点事件时序

正常路径:`trace(queued) → trace(running) → [checkpoint] → final`

- 无暂停点:`trace(queued) → trace(running) → final`
- `break_before + TO_HUMAN` 共存:发两次 checkpoint(run 前暂停 + run 后暂停)
- `accept` 抛异常:`trace(queued) → error → end`(无 running)
- `accept` 返回 `False`:`trace(queued) → trace(running) → trace(skipped)`(无 final,artifact 为 None)
- `run` 返回 `FanOut`:`trace(queued) → trace(running) → trace(done)`(无 final,不产 artifact)
- `run`/`deliver` 异常或 `deliver` False:`trace(running) → error → end`
- `abort()`:当前节点 `error`,整 job `end`

详见 [`Node` 核心差异表](Node.md#核心差异写节点前必看)。

## 相关

- [`Runner`](Runner.md) — 事件产出方
- [`JobState`](JobState.md) — 事件折叠状态
- [`Checkpoint`](Checkpoint.md) — `checkpoint` 事件触发条件
