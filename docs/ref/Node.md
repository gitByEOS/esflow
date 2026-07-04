# Node

## 模块

`esflow.node` — `from esflow import Node`

## 职责

节点基类。子类设 `id`/`title`/`checkpoint` 类属性，实现 `run`，按需 override `accept`/`deliver`。

`Node` 既是用户继承的**定义基类**，又是运行时**实例**：loader/runner 实例化 `Node` 子类，注入 `replica_id`/`index`/`depth`/`output_dir`/`fanout_payload` 等运行时字段。副本就是同一 `Node` 类的多个实例，没有中间层抽象。

## 类定义

```python
class Node:
    id: str = ""
    title: str = ""
    checkpoint: Checkpoint = Checkpoint.NONE
    index: int = 0
    replica_id: str = ""
    depth: int = 0
    output_dir: Path = Path()
    fanout_payload: Any = None

    def accept(self, ctx: DepthScope) -> bool: ...
    def deliver(self, artifact: Any) -> bool: ...
    def run(self, ctx: DepthScope) -> Any: ...
```

### 类属性（子类设置）

| 属性 | 类型 | 默认 | 用途 |
|---|---|---|---|
| `id` | `str` | `""` | 节点 base id，loader 校验非空且唯一 |
| `title` | `str` | `""` | 展示标题，事件 `detail` 回退到 `id` |
| `checkpoint` | [`Checkpoint`](Checkpoint.md) | `NONE` | 暂停点:`TO_HUMAN` 表示 `run` 完成后暂停等人确认;`TO_AGENT` 表示不调 `run`,交给外部 agent 写产物 |

### 运行时字段（框架注入）

| 字段 | 类型 | 注入时机 | 用途 |
|---|---|---|---|
| `replica_id` | `str` | loader/runner 实例化 | 运行实例 id（普通节点 = base id，副本 = `worker#2`） |
| `index` | `int` | loader/runner 实例化 | 副本序号（非副本 = 0） |
| `depth` | `int` | Runner 初始化 | 拓扑深度（入口 0），同层节点 depth 相同 |
| `output_dir` | `Path` | 节点就绪执行时 | 产物目录，节点把大文件写到这里 |
| `fanout_payload` | `Any` | 动态扇出展开时 | 动态扇出载荷（仅 dynamic base 副本有） |

## 方法

### `run(self, ctx: DepthScope) -> Any`

子类**必须实现**，返回 artifact（通常是 `dict`）。可返回 [`FanOut`](FanOut.md) 触发动态扇出。

```python
class Fetch(Node):
    id = "fetch"

    def run(self, ctx) -> dict:
        return {"items": [1, 2, 3]}
```

写文件产物到 `self.output_dir`，`run` 返回值登记路径供下游读取：

```python
class Export(Node):
    id = "export"

    def run(self, ctx) -> dict:
        text = ctx.get("ocr")["text"]
        path = self.output_dir / "result.txt"
        path.write_text(text + "\n", encoding="utf-8")
        return {"out_path": str(path), "chars": len(text)}
```

### `accept(self, ctx: DepthScope) -> bool`

**接手确认**，`run` 前校验前置条件。默认 `True`，子类按需 override。

- 返回 `False` 不是错误，而是"不接手"：框架 emit `skipped`，artifact 置 `None`，下游可推进
- 抛异常视为 error，emit `error` 事件

```python
def accept(self, ctx) -> bool:
    return Path(ctx.get("fetch")["video"]).exists()
```

### `deliver(self, artifact: Any) -> bool`

**脱手确认**，`run` 后校验产物。默认 `True`，子类按需 override。

- 返回 `False` 表示产物不合格，流程进入 error
- 抛异常视为 error

```python
def deliver(self, artifact) -> bool:
    return Path(artifact["srt"]).exists()
```

## 行为语义

| 调用 | 返回 `False` 的后果 | 抛异常的后果 |
|---|---|---|
| `accept` | **skip**(合法路径):artifact 置 `None`,下游推进,emit `skipped` | error:emit `error`,job 停止 |
| `deliver` | **error**(产物不合格):emit `error`,job 停止 | error:emit `error`,job 停止 |

关键差异:**`accept` 返回 `False` 是合法跳过**(`accept`/`deliver` 返回 False 后果截然不同 —— skip vs error),用于 fallback 兜底链;**`deliver` 返回 `False` 是错误停止**,用于产物校验失败。命名"接手确认"/"脱手确认"掩盖了关键差异,写节点时要留意。

- `run` 返回 `FanOut` → 不产 artifact，框架展开 N 个副本并改图（详见 [`FanOut`](FanOut.md)）
- 同步 `run` 由框架用 `asyncio.to_thread` 包起来并行，节点不需要写 `async`
- `checkpoint = Checkpoint.TO_HUMAN` 的节点 `run` 完成后 emit `checkpoint` 事件，整 job 暂停等 [`Runner.resume()`](Runner.md#resume) / `retry()` / `abort()`；`Checkpoint.TO_AGENT` 不调 `run`，交给外部 agent 写产物（详见 [`Checkpoint`](Checkpoint.md)）

## 副本与扇出

副本节点分两种取数据方式：

**静态副本**（`replicas` 展开）——按 `self.index` 切上游 list：

```python
class Worker(Node):
    id = "worker"

    def run(self, ctx) -> dict:
        items = ctx.get("fetch")["items"]      # 上游产物 list
        chunk = items[self.index]              # 静态副本按序号切自己那份
        return {"result": do(chunk)}
```

**动态副本**（`FanOut` 展开）——按 `self.fanout_payload` 取框架注入的载荷：

```python
class Worker(Node):
    id = "worker"

    def run(self, ctx) -> dict:
        chunk = self.fanout_payload            # 动态扇出载荷
        return {"result": do(chunk)}
```

扇入节点用 `ctx.upstream_ids()` 或 `ctx.gather("worker")` 收集，详见 [`DepthScope`](DepthScope.md)。

## 相关

- [`DepthScope`](DepthScope.md) — `run`/`accept` 收到的上下文协议
- [`Checkpoint`](Checkpoint.md) — 暂停点枚举
- [`FanOut`](FanOut.md) — 动态扇出指令
- [`FlowDefine`](FlowDefine.md) — 在 `flow.py` 里把 `Node` 串成 DAG
- [`Runner`](Runner.md) — 实例化并执行 `Node`
- 产物落盘约定见 [../artifacts.md](../artifacts.md)
