# FanOut

## 模块

`easyflow.node` — `from easyflow import FanOut`

## 职责

动态扇出指令。节点 [`Node.run`](Node.md#run) 返回它，框架运行时展开 N 个副本并改写边。

## 定义

```python
@dataclass
class FanOut:
    base: str
    payload: list[Any]

    @property
    def n(self) -> int:
        return len(self.payload)
```

| 字段 | 类型 | 约束 |
|---|---|---|
| `base` | `str` | 必须在 `flow.py` 的 `dynamic` 集合里声明，否则 runner 抛 `RuntimeError` |
| `payload` | `list[Any]` | 必须是 list；`n = len(payload)`，第 i 个副本拿 `payload[i]` |

## 行为

1. 节点 `run` 返回 `FanOut(base="worker", payload=[...])`
2. 当前节点不产 artifact，直接转 done
3. [`Runner`](Runner.md) 运行时创建 N 个副本 `worker#0` .. `worker#N-1`
4. 改写边：移除原 `base` 的边，加 `上游 → 副本` / `副本 → 下游`
5. 副本继承 `base` 的拓扑深度，`fanout_payload` 注入到 `Node.fanout_payload`

## 用法

声明 flow 时把 `worker` 放进 `dynamic` 集合（loader 不预实例化）：

```python
from easyflow import flow, edge

@flow(id="dyn")
class DynFlow:
    nodes = ["ingest", "split", "worker", "merge"]
    edges = [
        edge("ingest", "split"),
        edge("split", "worker"),
        edge("worker", "merge"),
    ]
    dynamic = {"worker"}   # worker 由 FanOut 运行时实例化
```

`split` 节点返回 `FanOut`：

```python
from easyflow import Node, FanOut


class Split(Node):
    id = "split"

    def run(self, ctx) -> FanOut:
        tasks = ctx.get("ingest")["tasks"]
        return FanOut(base="worker", payload=tasks)   # n=len(tasks)


class Worker(Node):
    id = "worker"

    def run(self, ctx) -> dict:
        task = ctx.fanout_payload        # 框架注入第 i 份任务
        return {"result": do(task)}


class Merge(Node):
    id = "merge"

    def run(self, ctx) -> dict:
        results = ctx.gather("worker")   # 收集所有副本产物
        return {"all": results}
```

## 静态 replicas vs 动态 FanOut

- 副本数**加载时已知** → 用 [`FlowDefine.replicas`](FlowDefine.md#replicas)（静态副本，loader 期展开）
- 副本数**依赖运行时产物** → 用 `dynamic` + `FanOut`
- 同一 flow 可混用：部分节点 `replicas`，部分节点 `dynamic`
- 同一 base 不能同时声明 `replicas` 和 `dynamic`，loader 会抛 `FlowLoadError`

## 限制

- 动态副本节点（`split` / `worker#i`）不挂 [`Checkpoint`](Checkpoint.md)，扇入节点（`merge`）可以
- `--node` 单调试暂不支持指定动态副本内部（展开前不存在），可指定到 `split` 级
- 动态扇出的运行时图暂不持久化，不承诺从动态副本内部续跑

## 相关

- [`Node`](Node.md#run) — `run` 返回 `FanOut` 触发扇出
- [`DepthScope.fanout_payload`](DepthScope.md#fanout_payload) — 副本取载荷
- [`DepthScope.gather`](DepthScope.md#gather) — 扇入节点收集副本产物
- [`FlowDefine.dynamic`](FlowDefine.md#dynamic) — 声明动态扇出 base
- [`Runner`](Runner.md) — `_expand_fanout` 实现动态扩图
