"""节点定义:Node 基类、StepContext、Checkpoint 枚举、FanOut 动态扇出指令。

每个节点是一个 .py 文件,文件内定义 Node 子类:

    from easyflow import Node, StepContext, Checkpoint

    class GenSrt(Node):
        id = "gen_srt"
        checkpoint = Checkpoint.AFTER

        def accept(self, ctx) -> bool:          # 接手确认,可选
            return Path(ctx.get("fetch")["video"]).exists()

        def deliver(self, artifact) -> bool:    # 脱手确认,可选
            return Path(artifact["srt"]).exists()

        def run(self, ctx) -> dict:             # 必须实现
            ...
            return {"srt": srt_path}

accept/deliver 默认返回 True,子类按需 override。
副本运行时 index/replica_id 由 runner 注入实例。

动态扇出:节点 run 返回 FanOut,框架运行时展开副本:

    class Split(Node):
        id = "split"
        def run(self, ctx) -> FanOut:
            tasks = ctx.get("ingest")["tasks"]
            return FanOut(base="worker", payload=tasks)  # n=len(payload)

    class Worker(Node):
        id = "worker"
        def run(self, ctx) -> dict:
            task = ctx.fanout_payload        # 框架注入第 i 份任务
            return {"result": do(task)}

    class Merge(Node):
        def run(self, ctx) -> dict:
            results = ctx.gather("worker")   # 语义化收集所有副本产物
            return {"all": results}
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Protocol


class Checkpoint(str, Enum):
    """暂停点。run 之后暂停,等外部 resume/retry。"""

    NONE = "none"
    AFTER = "after"  # run 完成后暂停,展示 artifact 等确认


@dataclass
class FanOut:
    """动态扇出指令:节点 run 返回它,框架展开 N 个副本。

    payload 必须是 list,n = len(payload),第 i 个副本拿 payload[i]。
    base 必须在 flow.py 的 dynamic 集合里声明(loader 不预实例化)。
    """

    base: str
    payload: list[Any]

    @property
    def n(self) -> int:
        return len(self.payload)


class StepContext(Protocol):
    """运行时注入给 Node.run/accept 的上下文。

    ctx.get(upstream_id) 取上游节点已完成的 artifact。
    ctx.upstream_ids() 取所有已完成的上游 node id(扇入节点枚举各副本用)。
    ctx.gather(base_id) 收集某动态 base 的所有副本产物(按 index 排序)。
    动态副本额外有 ctx.fanout_payload(框架注入的第 i 份数据)。
    副本节点额外有 index(副本序号)和 replica_id(实例 id)。
    """

    def get(self, upstream_id: str) -> Any: ...

    def upstream_ids(self) -> list[str]: ...

    def gather(self, base_id: str) -> list[Any]: ...

    fanout_payload: Any


class Node:
    """节点基类。子类设 id/title/checkpoint 类属性,实现 run,按需 override accept/deliver。"""

    id: str = ""
    title: str = ""
    checkpoint: Checkpoint = Checkpoint.NONE
    # 副本运行时由 loader 注入(非副本节点 index=0, replica_id=id)
    index: int = 0
    replica_id: str = ""

    def accept(self, ctx: StepContext) -> bool:
        """接手确认:run 前校验前置条件。默认通过。"""
        return True

    def deliver(self, artifact: Any) -> bool:
        """脱手确认:run 后校验产物。默认通过。"""
        return True

    def run(self, ctx: StepContext) -> Any:
        """子类必须实现,返回 artifact。"""
        raise NotImplementedError(f"{type(self).__name__} 未实现 run")


@dataclass
class StepDefine:
    """节点运行时定义:由 loader 从 Node 子类实例化产出。"""

    id: str
    title: str
    checkpoint: Checkpoint
    node: Node


def _instantiate(node_cls: type[Node], replica_id: str, index: int) -> StepDefine:
    """实例化一个 Node 子类为 StepDefine,注入副本信息。"""
    node = node_cls()
    node.replica_id = replica_id
    node.index = index
    return StepDefine(
        id=replica_id,
        title=node.title or node_cls.id,
        checkpoint=node.checkpoint,
        node=node,
    )
