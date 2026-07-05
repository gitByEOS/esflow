# FlowDefine / flow / edge / Edge

`esflow.flow` — `from esflow import flow, edge, Edge, FlowDefine`

声明 DAG:节点列表、边、静态并行副本、动态扇出 base、同层依次启动集合。

## flow 装饰器

```python
def flow(id: str, title: str = "") -> Callable
```

把带 `nodes`/`edges`/`replicas`/`dynamic`/`serial` 类属性的类转成 `FlowDefine`。

```python
from esflow import flow, edge

@flow(id="my_flow", title="我的流程")
class MyFlow:
    nodes = ["fetch", "process", "export"]
    edges = [edge("fetch", "process"), edge("process", "export")]
```

### 类属性

| 属性 | 类型 | 默认 | 用途 |
|---|---|---|---|
| `nodes` | `list[str]` | `[]` | base id 列表(含动态 base,动态 base 不预实例化) |
| `edges` | `list[Edge]` | `[]` | 静态边,动态 base 的边运行时由 runner 扩展 |
| `replicas` | `dict[str, int]` | `{}` | 静态并行度,loader 期展开为 `base#0..N-1` |
| `dynamic` | `set[str]` | `set()` | 动态扇出 base 集合,运行时由 [`FanOut`](FanOut.md) 创建 |
| `serial` | `set[str]` | `set()` | 同层依次启动的 base 集合,runner 同轮只启动 `nodes` 顺序最靠前的那个 |

`@flow` 产出 `FlowDefine` dataclass,一般不直接构造。loader 加载后返回**展开静态副本后**的 `FlowDefine`(`replicas` 清空,`nodes`/`edges`/`serial` 替换为展开后的副本 id)。

## edge / Edge

```python
def edge(from_: Any, to: Any) -> Edge
```

声明一条边。参数可传 id 字符串或 [`Node`](Node.md) 实例(取其 `id`)。`Edge` 是 frozen dataclass,字段 `from_: str` / `to: str`。

```python
edge("fetch", "process")
edge(fetch_node, process_node)   # 等价
```

静态副本 base 的边在 loader 期扇出/扇入展开:

```text
replicas={"worker": 3}
edge("fetch", "worker") → edge("fetch", "worker#0"), ..., edge("fetch", "worker#2")   # 扇出
edge("worker", "merge") → edge("worker#0", "merge"), ..., edge("worker#2", "merge")    # 扇入
```

## replicas:静态并行

`replicas = {"worker": 5}` → loader 展开成 `worker#0..worker#4` 五个 [`Node`](Node.md) 实例,边自动扇出/扇入。

```python
@flow(id="fanout")
class FanoutFlow:
    nodes = ["fetch", "worker", "merge"]
    edges = [edge("fetch", "worker"), edge("worker", "merge")]
    replicas = {"worker": 5}
```

## dynamic:动态扇出

loader 不实例化 dynamic base,运行时由某节点 `run` 返回 [`FanOut`](FanOut.md) 创建副本。`replicas` 与 `dynamic` 不相交,同一 base 不能同时静态副本和动态扇出。

```python
@flow(id="dyn")
class DynFlow:
    nodes = ["ingest", "split", "worker", "merge"]
    edges = [edge("ingest", "split"), edge("split", "worker"), edge("worker", "merge")]
    dynamic = {"worker"}
```

## serial:fallback 兜底链

`serial` 是调度策略(不是副本机制):同轮就绪节点中属于 `serial` 的,runner 只启动 `nodes` 顺序最靠前的那个,其他等下一轮。用于**多源兜底**:ssr 失败回退 wechat,再回退 bili。

```python
@flow(id="fallback")
class FallbackFlow:
    nodes = ["decide", "worker_a", "worker_b", "merge"]
    edges = [
        edge("decide", "worker_a"), edge("decide", "worker_b"),
        edge("worker_a", "merge"), edge("worker_b", "merge"),
    ]
    serial = {"worker_a", "worker_b"}   # 同层依次,worker_a 优先
```

fallback 写法要点:

- `worker_a.accept` 检查输入是否合自己胃口,不合返回 `False`(emit `skipped`,artifact 置 None)
- `worker_b.accept` 用 `ctx.layer(self.depth)` 拿同层前序,看 `worker_a` 是否被 skip,被 skip 才接手
- `merge` 用 `ctx.upstream_ids()` + None 过滤,拿实际跑成功的那个上游产物

普通同层并行**不要**用 `serial`,节点会失去并行收益。

### ctx 收集 API 选择

| API | 适用场景 |
|---|---|
| `ctx.gather("worker")` | 收集**同 base** 副本产物(按 index 排序),要求副本前缀 `worker#` |
| `ctx.upstream_ids()` + None 过滤 | 跨 base 兜底链,拿所有上游中实际成功的产物 |
| `ctx.layer(self.depth)` | 拿同层前序产物(含被 skip 的 None),用于判断前序是否成功 |

## 校验

loader(`load_flow`)加载时校验:

- `flow.py` 有且仅有一个 `@flow`
- `nodes/*.py` 每个有且仅有一个 `Node` 子类(`id` 非空)
- `flow.nodes` 每个 base id 都能在 `nodes/` 找到对应 `Node` 子类
- `replicas` / `dynamic` / `serial` 的 base 必须在 `nodes` 里
- `replicas` 与 `dynamic` 不相交
- 展开静态副本后的 DAG(动态 base 以 base id 参与)无环

任一失败抛 [`FlowLoadError`](FlowLoadError.md)。

## 相关

- [`Node`](Node.md) — `nodes` 引用的节点定义
- [`FanOut`](FanOut.md) — `dynamic` base 的运行时展开
- [`Runner`](Runner.md) — 加载并执行
- [`FlowLoadError`](FlowLoadError.md) — 加载/校验失败
