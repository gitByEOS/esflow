# DepthScope

## 模块

`easyflow.node` — `from easyflow import DepthScope`

## 职责

运行时注入给 `Node.run` / `Node.accept` 的上下文协议（`ctx` 参数）。表达 **depth 作用域**：同 depth 的所有副本共享同一份上游产物视图，只有 `fanout_payload` 是 per-run 私有。`ctx` 不是单个节点的私有上下文。

## 协议定义

```python
class DepthScope(Protocol):
    def get(self, upstream_id: str) -> Any: ...
    def upstream_ids(self) -> list[str]: ...
    def gather(self, base_id: str) -> list[Any]: ...
    def layer(self, depth: int) -> list[Any]: ...

    fanout_payload: Any
```

运行时实现是 `easyflow.runner._Ctx`（内部类），用户代码只依赖 `DepthScope` 协议。

## 方法

### `get(upstream_id: str) -> Any`

取上游节点已完成的 artifact。未完成抛 `KeyError`。

上游被 [`Node.accept`](Node.md#accept) 跳过时返回 `None`，下游可据此判断走兜底链。

```python
def run(self, ctx) -> dict:
    ocr = ctx.get("ocr")           # 上游 ocr 节点的 artifact
    text = ocr["text"] if ocr else ""
    return {"text": text}
```

### `upstream_ids() -> list[str]`

所有已完成上游的 node id 列表（含副本 `worker#0`/`worker#1`/...）。扇入节点枚举各副本用。

```python
def run(self, ctx) -> dict:
    results = [ctx.get(sid) for sid in ctx.upstream_ids()]
    return {"all": results}
```

### `gather(base_id: str) -> list[Any]`

收集某动态 base 的所有副本产物，按 `index` 排序返回。语义化收集方式，比 `upstream_ids()` + 前缀过滤更直观。

```python
class Merge(Node):
    id = "merge"

    def run(self, ctx) -> dict:
        results = ctx.gather("worker")   # [worker#0 artifact, worker#1 artifact, ...]
        return {"all": results}
```

### `layer(depth: int) -> list[Any]`

该拓扑深度的所有已完成节点产物 list，按声明顺序（含动态副本展开顺序）。skip 节点 artifact 为 `None`，一并返回便于枚举同层前序。

- 同层 fallback：`ctx.layer(self.depth)` 拿前序
- 跨层拿上游：`ctx.layer(self.depth - 1)`

```python
def run(self, ctx) -> dict:
    prev = ctx.layer(self.depth)         # 同层前序产物
    return {"prev": prev}
```

## 字段

### `fanout_payload: Any`

动态扇出载荷，框架注入的第 i 份数据。仅 dynamic base 副本有，普通节点为 `None`。

由 [`FanOut`](FanOut.md) 的 `payload[i]` 注入到第 i 个副本，详见 [`FanOut`](FanOut.md)。

## 相关

- [`Node`](Node.md) — `run`/`accept` 的 `ctx` 参数就是 `DepthScope`
- [`FanOut`](FanOut.md) — `fanout_payload` 的来源
- [`Runner`](Runner.md) — `_Ctx` 实现
