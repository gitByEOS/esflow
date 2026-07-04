# DepthScope

## 模块

`esflow.node` — `from esflow import DepthScope`

## 职责

运行时注入给 `Node.run` / `Node.accept` 的上下文协议（`ctx` 参数）。提供三类能力：

- 取上游节点产物（`get` / `upstream_ids`）
- 收集动态扇出副本产物（`gather`）
- 按拓扑深度访问同层/上游层产物（`layer`）

`ctx` 表达的是 **depth 作用域**：同 depth 的所有副本共享同一份上游产物视图，没有 per-run 私有状态。副本私有数据（动态扇出载荷）通过 [`Node.fanout_payload`](Node.md#运行时字段) 访问，不进 `ctx`。

运行时实现是 `esflow.runner` 内部的 `_Ctx` 类，用户代码只依赖 `DepthScope` 协议。

## 协议定义

```python
class DepthScope(Protocol):
    def get(self, upstream_id: str) -> Any: ...
    def upstream_ids(self) -> list[str]: ...
    def gather(self, base_id: str) -> list[Any]: ...
    def layer(self, depth: int) -> list[Any]: ...
```

## 方法

### `get(upstream_id: str) -> Any`

取上游节点已完成的 artifact。框架保证 `run`/`accept` 被调用时上游已就绪，因此正常路径不会取到"未完成"的节点。三种上游状态对应行为：

| 上游状态 | `ctx.get` 行为 |
|---|---|
| 已完成（`run` 返回 artifact） | 返回该 artifact |
| 被 `accept` 跳过（skip） | 返回 `None`，下游可据此走兜底链 |
| 未完成（不应发生） | 抛 `KeyError`，视为框架 bug 或节点实现 bug |

```python
def run(self, ctx) -> dict:
    ocr = ctx.get("ocr")           # 上游 ocr 节点的 artifact
    text = ocr["text"] if ocr else ""   # ocr 被 skip 时 ocr 为 None
    return {"text": text}
```

### `upstream_ids() -> list[str]`

所有已完成上游的 node id 列表（含被 `accept` 跳过的 id，对应 artifact 为 `None`）。扇入节点枚举各副本用,跨 base 兜底链用 `upstream_ids()` + None 过滤拿实际成功的产物。

```python
def run(self, ctx) -> dict:
    results = [ctx.get(sid) for sid in ctx.upstream_ids()]
    return {"all": results}
```

### `gather(base_id: str) -> list[Any]`

收集**同 base** 动态/静态副本的所有副本产物，按 `index` 排序返回。比 `upstream_ids()` + 前缀过滤更直观。跨 base 收集(如 fallback `worker_a`/`worker_b`)用 `upstream_ids()` + None 过滤,`gather` 取不到不同 base 的产物。

```python
class Merge(Node):
    id = "merge"

    def run(self, ctx) -> dict:
        results = ctx.gather("worker")   # [worker#0 artifact, worker#1 artifact, ...]
        return {"all": results}
```

### `layer(depth: int) -> list[Any]`

该拓扑深度的所有已完成节点产物 list，按 `nodes` 声明顺序排列（含动态副本展开顺序）。skip 节点 artifact 为 `None`，一并返回便于枚举同层前序。

当前节点通常还未完成，所以 `layer(self.depth)` 实际是同层**已跑完的前序**；跨层拿上游用 `layer(self.depth - 1)`。

```python
def run(self, ctx) -> dict:
    prev = ctx.layer(self.depth)         # 同层前序产物
    return {"prev": prev}
```

## 相关

- [`Node`](Node.md) — `run`/`accept` 的 `ctx` 参数就是 `DepthScope`；副本私有数据走 `Node.fanout_payload`
- [`FanOut`](FanOut.md) — 动态扇出指令
- [`Runner`](Runner.md) — `_Ctx` 的宿主,内部实现
