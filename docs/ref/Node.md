# Node

`esflow.node` — `from esflow import Node`

节点基类。子类设 `id`/`title`/`checkpoint` 类属性,实现 `run`,按需 override `accept`/`deliver`。

`Node` 既是用户继承的**定义基类**,又是运行时**实例**:loader/runner 实例化子类,注入 `replica_id`/`index`/`depth`/`output_dir`/`fanout_payload`。副本就是同一类的多个实例,无中间层。

## 核心差异(写节点前必看)

| 调用 | 返回 `False` | 抛异常 |
|---|---|---|
| `accept` | **合法跳过**:artifact 置 `None`,下游推进,emit `skipped` | error,job 停止 |
| `deliver`(普通节点) | **错误停止**:emit `error`,job 停止 | error,job 停止 |
| `deliver`(`TO_AGENT` 节点) | **checkpoint**:agent 还没写,让 agent 写,job 暂停退出 | error,job 停止 |

`accept` 返回 `False` 是 fallback 兜底链的合法路径;`deliver` 返回 `False` 是产物校验失败。`TO_AGENT` 节点 `deliver` False 语义不同——agent 还没写,不是写错。

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

### 类属性(子类设置)

| 属性 | 类型 | 默认 | 用途 |
|---|---|---|---|
| `id` | `str` | `""` | base id,loader 校验非空且唯一 |
| `title` | `str` | `""` | 展示标题,事件 `detail` 回退到 `id` |
| `checkpoint` | [`Checkpoint`](Checkpoint.md) | `NONE` | `TO_HUMAN`:`run` 后暂停等人确认;`TO_AGENT`:不调 `run`,交外部 agent 写产物 |

### 运行时字段(框架注入)

| 字段 | 注入时机 | 用途 |
|---|---|---|
| `replica_id` | loader/runner 实例化 | 运行实例 id(普通节点 = base id,副本 = `worker#2`) |
| `index` | loader/runner 实例化 | 副本序号(非副本 = 0) |
| `depth` | Runner 初始化 | 拓扑深度(入口 0) |
| `output_dir` | 节点就绪时 | 产物目录,默认 `Path()`,框架 fallback 到 `job_dir/<run_id>`;`TO_AGENT` 节点可在 `accept` 设为业务目录,框架尊重不覆盖 |
| `fanout_payload` | 动态扇出展开时 | 动态扇出载荷(仅 dynamic base 副本有) |

## 方法

### `run(self, ctx) -> Any`

子类**必须实现**,返回 artifact(通常 `dict`)。可返回 [`FanOut`](FanOut.md) 触发动态扇出。同步 `run` 由框架 `asyncio.to_thread` 包起来并行,节点不写 `async`。

```python
class Export(Node):
    id = "export"

    def run(self, ctx) -> dict:
        text = ctx.get("ocr")["text"]
        path = self.output_dir / "result.txt"
        path.write_text(text + "\n", encoding="utf-8")
        return {"out_path": str(path), "chars": len(text)}
```

### `accept(self, ctx) -> bool`

**接手确认**,`run` 前校验前置。默认 `True`。

```python
def accept(self, ctx) -> bool:
    return Path(ctx.get("fetch")["video"]).exists()
```

### `deliver(self, artifact) -> bool`

**脱手确认**,`run` 后校验产物。默认 `True`。

```python
def deliver(self, artifact) -> bool:
    return Path(artifact["srt"]).exists()
```

## 副本与扇出

**静态副本**(`replicas` 展开)——按 `self.index` 切上游 list:

```python
class Worker(Node):
    id = "worker"

    def run(self, ctx) -> dict:
        chunk = ctx.get("fetch")["items"][self.index]
        return {"result": do(chunk)}
```

**动态副本**(`FanOut` 展开)——按 `self.fanout_payload` 取载荷:

```python
class Worker(Node):
    id = "worker"

    def run(self, ctx) -> dict:
        return {"result": do(self.fanout_payload)}
```

扇入用 `ctx.gather("worker")` 或 `ctx.upstream_ids()`,详见 [`DepthScope`](DepthScope.md)。

## 相关

- [`DepthScope`](DepthScope.md) — `run`/`accept` 收到的上下文
- [`Checkpoint`](Checkpoint.md) — 暂停点枚举
- [`FanOut`](FanOut.md) — 动态扇出指令
- [`FlowDefine`](FlowDefine.md) — 在 `flow.py` 里串成 DAG
- [`Runner`](Runner.md) — 实例化并执行
- 产物落盘见 [../artifacts.md](../artifacts.md)
