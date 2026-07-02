"""Flow 定义:@flow 装饰器、edge()、FlowDefine。

flow.py 声明 DAG 的边、静态并行度、动态扇出 base:

    from easyflow import flow, edge

    # 静态副本
    @flow(id="fanout")
    class FanoutFlow:
        nodes = ["fetch", "worker", "merge"]
        edges = [edge("fetch", "worker"), edge("worker", "merge")]
        replicas = {"worker": 5}   # worker 展开 5 个静态副本

    # 动态扇出:split run 时返回 FanOut 展开 worker
    @flow(id="dyn")
    class DynFlow:
        nodes = ["ingest", "split", "worker", "merge"]
        edges = [edge("ingest","split"), edge("split","worker"), edge("worker","merge")]
        dynamic = {"worker"}   # worker 由 FanOut 运行时实例化,loader 不预创建

replicas 的 base loader 期展开为 base#0..N-1,边扇出/扇入。
dynamic 的 base loader 不实例化,运行时由某节点 run 返回 FanOut 创建副本。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

from .step import StepDefine


@dataclass(frozen=True)
class Edge:
    """一条边。from_/to 是 base id(静态副本 base 由 loader 扇出,动态 base 运行时扇出)。"""

    from_: str
    to: str


def edge(from_: Any, to: Any) -> Edge:
    """声明一条边。参数可传 id 字符串或 StepDefine 对象(取其 id)。"""

    def as_id(x: Any) -> str:
        if isinstance(x, StepDefine):
            return x.id
        return str(x)

    return Edge(from_=as_id(from_), to=as_id(to))


@dataclass
class FlowDefine:
    """Flow 定义。

    nodes: base id 列表(含动态 base,但动态 base 不预实例化)。
    edges: Edge 列表(静态边,动态 base 的边运行时由 runner 扩展)。
    replicas: 静态并行度,loader 期展开。
    dynamic: 动态扇出 base 集合,运行时由 FanOut 创建,loader 不预实例化。
    """

    id: str
    title: str
    nodes: list[str] = field(default_factory=list)
    edges: list[Edge] = field(default_factory=list)
    replicas: dict[str, int] = field(default_factory=dict)
    dynamic: set[str] = field(default_factory=set)


def flow(id: str, title: str = "") -> Callable:
    """装饰器:把带 nodes/edges/replicas/dynamic 类属性的类标记为 FlowDefine。"""

    def decorator(cls: type) -> FlowDefine:
        nodes = list(getattr(cls, "nodes", []))
        edges = list(getattr(cls, "edges", []))
        replicas = dict(getattr(cls, "replicas", {}))
        dynamic = set(getattr(cls, "dynamic", set()))
        return FlowDefine(
            id=id,
            title=title or id,
            nodes=nodes,
            edges=edges,
            replicas=replicas,
            dynamic=dynamic,
        )

    return decorator
