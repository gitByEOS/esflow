# Runner

## 模块

`easyflow.runner` — `from easyflow import Runner`

## 职责

加载并执行一个 flow，产出 [`JobEvent`](JobEvent.md) 事件流，支持并行调度、静态副本/动态扇出、人机协作控制循环（暂停/重试/中止）、单点调试、定点续跑。

## 类定义

```python
class Runner:
    def __init__(
        self,
        flow: FlowDefine,
        runs: dict[str, Node],
        node_classes: dict[str, type[Node]],
        output_root: Path = DEFAULT_OUTPUT_ROOT,
        debug: bool = False,
        job_dir: Path | None = None,
    ) -> None: ...

    @classmethod
    def load(
        cls,
        flow_dir: str,
        output_root: Path = DEFAULT_OUTPUT_ROOT,
        debug: bool = False,
        job_dir: Path | str | None = None,
    ) -> "Runner": ...
```

### 实例属性

| 属性 | 类型 | 用途 |
|---|---|---|
| `flow` | [`FlowDefine`](FlowDefine.md) | 展开后的 flow 定义（动态扇出后边会改写） |
| `runs` | `dict[str, Node]` | 当前所有运行实例 `{run_id: Node}`（动态扇出后会新增副本） |
| `node_classes` | `dict[str, type[Node]]` | 全量 base 类，供动态扇出实例化副本 |
| `state` | [`JobState`](JobState.md) | 事件折叠状态，view/CLI 订阅渲染 |
| `artifacts` | `dict[str, Any]` | 当前内存中所有节点 artifact |
| `debug` | `bool` | 是否 debug 模式（job_dir 固定、artifact 持久化） |
| `job_id` | `str` | 时间戳 + 4 位 hash，debug 模式无 |
| `job_dir` | `Path` | 产物根目录 |

### job_dir 推导

| 模式 | job_dir |
|---|---|
| 默认 run | `output_root/<flow_id>/<job_id>/` |
| `--out DIR` | `DIR/`（无 job_id 层） |
| debug | `DEBUG_OUTPUT_ROOT/<flow_id>/`（固定，累积复用） |

## 构造方法

### `Runner.load(flow_dir, output_root=..., debug=False, job_dir=None) -> Runner`

从目录加载 flow（内部调 `load_flow`），构造 `Runner`。最常用入口。

```python
runner = Runner.load("./my_flow")
runner = Runner.load("./my_flow", debug=True)
runner = Runner.load("./my_flow", job_dir=Path("./runs/a"))
```

## 执行方法

### `run(only=None, break_before=None, nodes=None, from_node=None, from_depth=None) -> AsyncGenerator[JobEvent]`

执行 flow，产出事件流。五个互斥入参（优先级 `from_node > from_depth > nodes > only > 默认`）决定跑哪些节点与加载策略。

| 参数 | 行为 | 加载策略 |
|---|---|---|
| 默认 | 全跑 | `load_all`：加载已有产物，跳过已完成节点；`--out` 显式指定时 `invalidate_all`：清空重跑 |
| `only={"X"}` | 跑 X 及其必需上游 | `load_all`：跑必需闭包 |
| `nodes={"X"}` | 只跑 X 本身，上游必须已完成 | `load_skip_target`：加载但跳过 target |
| `from_node="X"` | 重跑 X 及其下游，上游复用 | `load_skip_target_then_invalidate` |
| `from_depth=N` | 重跑 `depth >= N` 的所有节点，上游 `depth < N` 复用 | `load_skip_target_then_invalidate` |
| `break_before={"X"}` | X 就绪后不立即执行，emit `checkpoint` 暂停等 `resume` | —— |

```python
runner = Runner.load("./my_flow")
async for event in runner.run():
    if event.type == "checkpoint":
        runner.resume()        # 或 retry("review") / abort()
```

单调试：

```python
# run 模式：跑 X 及其上游
async for event in runner.run(only={"worker#2"}):
    ...

# debug 模式：只跑 X，上游从磁盘复用，X 前暂停等 resume
async for event in runner.run(nodes={"worker#2"}, break_before={"worker#2"}):
    if event.type == "checkpoint":
        runner.resume()
```

定点续跑：

```python
# 按节点：上游产物已在 job_dir，只重跑 X 及下游
async for event in runner.run(from_node="translate"):
    ...

# 按拓扑深度：重跑 depth >= 2 的所有节点，上游 depth < 2 复用
async for event in runner.run(from_depth=2):
    ...
```

`from_depth` 越界（`< 0` 或 `> runner.max_depth`）抛 `RuntimeError`，可用 `runner.max_depth` 查询最大深度。

## 控制信号

checkpoint 暂停时外部调用，唤醒等待中的 `run()`。

### `resume() -> None`

确认继续。paused 节点转 done（`checkpoint AFTER`）或转 idle（`break_before`），跑下一轮就绪节点。

### `retry(from_node: str) -> None`

从 `from_node` 重跑：清 `from_node` 及下游的 artifact 与状态，上游复用。debug 模式额外清磁盘 `artifact.json` 防止下次启动加载到旧产物。

### `abort() -> None`

中止 job。checkpoint 时立即生效，emit `error("aborted")` + `end`。

## 调试辅助方法

### `clear_debug() -> None`

debug 模式：清空 `job_dir` 下持久化产物，下次 run 从头跑。非 debug 模式无操作。等价于 CLI `--clear`。

### `missing_upstream(nodes: set[str]) -> list[str]`

返回 `nodes` 节点的所有上游中磁盘无 `artifact.json` 的节点 id 列表。单调试预检用，CLI 在 `--node` / `--from` / `--from-depth` 前调用，缺失时提前失败并提示先全跑落产物。`nodes` 接 `set`，单值续跑时传 `{from_node}`，按层续跑时传 `runner._target_by_depth(N)`。

### `max_depth` (property)

当前 DAG 最大拓扑深度，`from_depth` 越界校验用。`from_depth` 有效范围 `[0, max_depth]`。

## 执行语义

### 并行调度

同一轮就绪节点 `asyncio.gather` 并行；同步 `run` 用 `asyncio.to_thread` 包。checkpoint 时整 job 暂停（不只该节点）。

### 拓扑深度

`depth(node) = 1 + max(depth(upstream))`，入口 0。Runner 初始化时计算并回填到 `Node.depth`，同层节点 depth 相同。`serial` 同层依次启动按 `nodes` 顺序。

### 产物持久化

`debug` 或 `--out` 模式下，每节点 done/skipped 后把 `run` 返回值序列化到 `job_dir/<run_id>/artifact.json`。下次启动时加载复用，已完成节点被 `_ready_nodes` 自然跳过。

详见 [../artifacts.md](../artifacts.md)。

### 动态扩图

节点 `run` 返回 [`FanOut`](FanOut.md) 时，`_expand_fanout` 创建 N 个副本实例、注入 `fanout_payload`/`depth`/`output_dir`，改写边（移除原 base 边，加 `上游→副本`/`副本→下游`），重建邻接表。

## 模块级常量

| 常量 | 值 | 用途 |
|---|---|---|
| `DEFAULT_OUTPUT_ROOT` | `Path("/tmp/easyflow/outputs")` | 默认产物根目录 |
| `DEBUG_OUTPUT_ROOT` | `Path("/tmp/easyflow/debug")` | debug 模式产物根目录（固定，持久化复用） |

## 相关

- [`FlowDefine`](FlowDefine.md) / [`Node`](Node.md) — 加载与执行的输入
- [`JobEvent`](JobEvent.md) — `run()` 产出的事件流
- [`JobState`](JobState.md) — `runner.state` 事件折叠状态
- [`FanOut`](FanOut.md) — 动态扇出
- 调试模式见 [../debug.md](../debug.md)
- 产物与续跑见 [../artifacts.md](../artifacts.md)
