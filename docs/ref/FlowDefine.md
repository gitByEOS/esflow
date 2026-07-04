# FlowDefine / flow / edge / Edge

## 模块

`esflow.flow` — `from esflow import flow, edge, Edge, FlowDefine`

## 职责

声明 DAG：节点列表、边、静态并行副本、动态扇出 base、同层依次启动集合。`@flow` 装饰器把一个普通类标记为 `FlowDefine`，`edge()` 声明边。

## flow 装饰器

```python
def flow(id: str, title: str = "") -> Callable
```

把带 `nodes`/`edges`/`replicas`/`dynamic`/`serial` 类属性的类转成 `FlowDefine` 实例。

```python
from esflow import flow, edge

@flow(id="my_flow", title="我的流程")
class MyFlow:
    nodes = ["fetch", "process", "export"]
    edges = [
        edge("fetch", "process"),
        edge("process", "export"),
    ]
```

类体属性映射：

| 类属性 | 类型 | 默认 | 用途 |
|---|---|---|---|
| `nodes` | `list[str]` | `[]` | 节点 base id 列表（含动态 base，但动态 base 不预实例化） |
| `edges` | `list[Edge]` | `[]` | 静态边，动态 base 的边运行时由 runner 扩展 |
| `replicas` | `dict[str, int]` | `{}` | 静态并行度，loader 期展开为 `base#0..N-1` |
| `dynamic` | `set[str]` | `set()` | 动态扇出 base 集合，运行时由 [`FanOut`](FanOut.md) 创建，loader 不预实例化 |
| `serial` | `set[str]` | `set()` | 同层依次启动的 base 集合，runner 同轮只启动 `nodes` 顺序最靠前的那个 |

## edge 函数

```python
def edge(from_: Any, to: Any) -> Edge
```

声明一条边。参数可传 id 字符串或 [`Node`](Node.md) 实例（取其 `id`）。

```python
edge("fetch", "process")
edge(fetch_node, process_node)   # 等价
```

## Edge

```python
@dataclass(frozen=True)
class Edge:
    from_: str
    to: str
```

一条 DAG 边。`from_`/`to` 是 base id（静态副本 base 由 loader 扇出，动态 base 运行时扇出）。frozen 不可变。

## FlowDefine

```python
@dataclass
class FlowDefine:
    id: str
    title: str
    nodes: list[str] = field(default_factory=list)
    edges: list[Edge] = field(default_factory=list)
    replicas: dict[str, int] = field(default_factory=dict)
    dynamic: set[str] = field(default_factory=set)
    serial: set[str] = field(default_factory=set)
```

`@flow` 装饰器的产出。一般不直接构造，由装饰器生成；loader 加载后会返回**展开静态副本后**的 `FlowDefine`（`replicas` 清空，`nodes`/`edges`/`serial` 替换为展开后的副本 id）。

### 字段语义

#### nodes

base id 列表。loader 展开后：普通 base 保留原 id；静态副本 base 替换为 `{base#i for i in range(n)}`（不含 base 本身，edges 中 base 由 loader 扇出/扇入展开）；动态 base 保留 base id 参与无环校验。

#### edges

`Edge` 列表。静态副本 base 的边在 loader 期扇出/扇入展开；动态 base 的边运行时由 runner 改写（详见 [`Runner`](Runner.md)）。

展开示例(`replicas={"worker": 3}`):

```text
edge("fetch", "worker")   →  edge("fetch", "worker#0"), edge("fetch", "worker#1"), edge("fetch", "worker#2")   # 扇出
edge("worker", "merge")   →  edge("worker#0", "merge"), edge("worker#1", "merge"), edge("worker#2", "merge")   # 扇入
```

#### replicas

静态并行度。`replicas = {"worker": 5}` → loader 展开成 `worker#0..worker#4` 五个 [`Node`](Node.md) 实例，边自动扇出/扇入。

```python
@flow(id="fanout")
class FanoutFlow:
    nodes = ["fetch", "worker", "merge"]
    edges = [edge("fetch", "worker"), edge("worker", "merge")]
    replicas = {"worker": 5}
```

#### dynamic

动态扇出 base 集合。loader 不实例化这些 base，运行时由某节点 `run` 返回 [`FanOut`](FanOut.md) 创建副本。

```python
@flow(id="dyn")
class DynFlow:
    nodes = ["ingest", "split", "worker", "merge"]
    edges = [
        edge("ingest", "split"),
        edge("split", "worker"),
        edge("worker", "merge"),
    ]
    dynamic = {"worker"}
```

`replicas` 与 `dynamic` 不相交：同一 base 不能同时静态副本和动态扇出，loader 抛 [`FlowLoadError`](FlowLoadError.md)。

## 调度策略：serial

`serial` 与 `replicas`/`dynamic` 不在同一抽象层：后两者决定"有几个副本"，`serial` 决定"同层多就绪节点怎么排队启动"，是调度策略。

**作用对象是 base**:`serial = {"worker_a", "worker_b"}` 约束的是 base id。若某 base 同时声明 `replicas` + `serial`(如 `replicas={"worker": 3}` + `serial={"worker"}`),loader 展开后 `serial` 变成 `{worker#0, worker#1, worker#2}`,副本按 `index` 顺序串行启动——但通常静态副本要的是并行,不应与 `serial` 组合;`serial` 主要用于跨 base 同层兜底链。

同轮就绪节点中属于 `serial` 的，runner 只启动 `nodes` 声明顺序最靠前的那个，其他 serial 节点等下一轮；非 serial 节点照常并行。用于 **fallback 兜底链**：多源抓取（ssr 失败回退 wechat，再回退 bili）。

```python
@flow(id="fallback")
class FallbackFlow:
    nodes = ["decide", "worker_a", "worker_b", "merge"]
    edges = [
        edge("decide", "worker_a"),
        edge("decide", "worker_b"),
        edge("worker_a", "merge"),
        edge("worker_b", "merge"),
    ]
    serial = {"worker_a", "worker_b"}   # 同层依次启动,worker_a 优先
```

### fallback 怎么写

`worker_a` 和 `worker_b` 都从 `decide` 拿输入，但 `worker_b` 只在 `worker_a` 没出活时才接手。关键点：

- `worker_a.accept` 检查自己能不能干（输入是否合自己胃口）；不能干返回 `False`，框架 emit `skipped`，artifact 置 `None`
- `worker_b.accept` 用 `ctx.layer(self.depth)` 拿同层前序，看 `worker_a` 是否被 skip；被 skip 才接手
- `merge` 用 `ctx.upstream_ids()` 拿到实际跑成功的那个上游产物（被 skip 的为 `None`，过滤掉即可）

```python
class WorkerA(Node):
    id = "worker_a"

    def accept(self, ctx) -> bool:
        # 输入合 ssr 口味才接手
        return ctx.get("decide")["source"] == "ssr"

    def run(self, ctx) -> dict:
        return {"result": fetch_ssr(ctx.get("decide"))}


class WorkerB(Node):
    id = "worker_b"

    def accept(self, ctx) -> bool:
        # worker_a 被 skip 才接手;worker_a 成功了就不重复跑
        prev = ctx.layer(self.depth)        # 同层前序产物,skip 的为 None
        has_a = any(p is not None for p in prev)
        return not has_a

    def run(self, ctx) -> dict:
        return {"result": fetch_wechat(ctx.get("decide"))}


class Merge(Node):
    id = "merge"

    def run(self, ctx) -> dict:
        # 只有一个 worker 跑成功,另一个被 skip 为 None,过滤掉
        results = [ctx.get(uid) for uid in ctx.upstream_ids() if ctx.get(uid) is not None]
        return {"final": results[0]["result"]}
```

非 fallback 场景（普通同层并行）不要用 `serial`，节点会顺序跑失去并行收益。

### ctx 收集 API 选择指引

示例里同时用了 `layer`/`upstream_ids`/`gather`,三者各有适用场景:

| API | 适用场景 | 示例 |
|---|---|---|
| `ctx.gather("worker")` | 收集**同 base** 动态/静态副本的所有产物(按 index 排序) | 扇入 `merge` 收 `worker#0..N-1` 的产物 |
| `ctx.upstream_ids()` + None 过滤 | 跨 base 兜底链,拿所有上游中实际成功的产物 | fallback `merge` 收 `worker_a` 或 `worker_b` 谁成功 |
| `ctx.layer(self.depth)` | 拿同层前序产物(含被 skip 的 None),用于判断前序是否成功 | fallback `worker_b.accept` 看 `worker_a` 是否被 skip |

不要混用:`gather` 要求副本同 base(前缀匹配 `worker#`);跨 base 用 `gather` 会取不到。

## 校验

loader (`load_flow`) 在加载时校验：

- `flow.py` 里有且仅有一个 `@flow`
- `nodes/*.py` 每个有且仅有一个 `Node` 子类（`id` 非空）
- `flow.nodes` 的每个 base id 都能在 `nodes/` 里找到对应 `Node` 子类
- `replicas` / `dynamic` / `serial` 的 base 必须在 `nodes` 里
- `replicas` 与 `dynamic` 不相交
- 展开静态副本后的 DAG（动态 base 以 base id 参与）无环

任一失败抛 [`FlowLoadError`](FlowLoadError.md)。

## 相关

- [`Node`](Node.md) — `nodes` 引用的节点定义
- [`FanOut`](FanOut.md) — `dynamic` base 的运行时展开
- [`Runner`](Runner.md) — 加载并执行 `FlowDefine`
- [`FlowLoadError`](FlowLoadError.md) — 加载/校验失败异常
