"""节点定义:Node 基类、DepthScope 上下文协议、Checkpoint 枚举、FanOut 动态扇出指令。

每个节点是一个 .py 文件,文件内定义 Node 子类:

    from esflow import Node, DepthScope, Checkpoint

    class GenSrt(Node):
        id = "gen_srt"
        checkpoint = Checkpoint.TO_HUMAN

        def accept(self, ctx) -> bool:          # 接手确认,可选;返回 False → 跳过本节点
            return Path(ctx.get("fetch")["video"]).exists()

        def deliver(self, artifact) -> bool:    # 脱手确认,可选;返回 False → emit error
            return Path(artifact["srt"]).exists()

        def run(self, ctx) -> dict:             # 必须实现
            ...
            return {"srt": srt_path}

TO_AGENT(agent 介入):节点不实现 run,设 checkpoint=Checkpoint.TO_AGENT,
框架就绪时 emit checkpoint 退出进程,产物由外部 agent 写入 output_dir:

    class AgentSummary(Node):
        id = "agent_summary"
        checkpoint = Checkpoint.TO_AGENT
        def deliver(self, artifact) -> bool:    # 校验 agent 写的产物文件
            return "summary.txt" in artifact.get("files", [])

agent 链路:esflow run --out <path> → 跑到 TO_AGENT 退出(exit 2)→
agent 写产物文件到 <path>/<node>/ → esflow run --resume <path> →
框架扫文件构造 artifact={"output_dir", "files"} + deliver 校验 + 跑下游。

accept 返回 False 不是错误,而是"不接手":框架 emit skipped,artifact 置 None,
下游可推进,通过 ctx.get 拿到 None 知道上游被跳过。deliver 失败才是错误。
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
            task = self.fanout_payload       # 框架注入第 i 份任务
            return {"result": do(task)}

    class Merge(Node):
        def run(self, ctx) -> dict:
            results = ctx.gather("worker")   # 语义化收集所有副本产物
            return {"all": results}

Node 既是用户继承的定义基类,又是运行时实例:loader/runner 实例化 Node 子类,
注入 replica_id/index/depth/output_dir 等运行时字段。副本就是同一 Node 类的多个实例,
不需要中间层抽象。
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Protocol


class Checkpoint(str, Enum):
    """暂停点。维度:暂停后交给谁处理。

    NONE:     不暂停
    TO_HUMAN: run 完成后暂停,展示 artifact 等人机确认(stdin/POST resume/retry/abort)
    TO_AGENT: 不调 run,就绪即暂停退出进程,产物由外部 agent 写入 output_dir,
              --resume 时框架扫文件构造 artifact + deliver 校验
    """

    NONE = "none"
    TO_HUMAN = "to_human"
    TO_AGENT = "to_agent"


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


class DepthScope(Protocol):
    """运行时注入给 Node.run/accept 的 depth 作用域。

    同 depth 的所有副本共享同一份上游产物视图(_artifacts 全局引用),
    没有 per-run 私有状态。ctx 因此表达的是 depth 作用域,不是单个节点的
    私有上下文。副本私有数据(动态扇出载荷)通过 Node.fanout_payload 访问。

    ctx.get(upstream_id) 取上游节点已完成的 artifact。
    ctx.upstream_ids() 取所有已完成的上游 node id(扇入节点枚举各副本用)。
    ctx.gather(base_id) 收集某动态 base 的所有副本产物(按 index 排序)。
    ctx.layer(depth) 取该拓扑深度的所有已完成节点产物 list(按声明顺序,skip 的为 None),
        同层 fallback 用 ctx.layer(self.depth) 拿前序,跨层拿上游用 ctx.layer(self.depth - 1)。
    动态副本额外有 self.fanout_payload(框架注入的第 i 份数据)。
    副本节点额外有 index(副本序号)和 replica_id(实例 id)。
    """

    def get(self, upstream_id: str) -> Any: ...

    def upstream_ids(self) -> list[str]: ...

    def gather(self, base_id: str) -> list[Any]: ...

    def layer(self, depth: int) -> list[Any]: ...


class Node:
    """节点基类。子类设 id/title/checkpoint 类属性,实现 run,按需 override accept/deliver。

    既是用户继承的定义基类,又是运行时实例:loader/runner 实例化后注入运行时字段。
    """

    id: str = ""
    title: str = ""
    checkpoint: Checkpoint = Checkpoint.NONE
    # 副本运行时由 loader 注入(非副本节点 index=0, replica_id=id)
    index: int = 0
    replica_id: str = ""
    # 拓扑深度(从入口节点 0 起),框架注入;同层节点 depth 相同,serial 同层依次启动
    depth: int = 0
    # 产物目录,框架注入;节点把大文件写到这里,run 返回的 dict 登记文件路径供下游读取
    output_dir: Path = Path()
    # 动态扇出载荷,框架注入第 i 份任务(仅 dynamic base 副本有)
    fanout_payload: Any = None
    # 节点入参,Runner.load(node_args=...) 注入;skill 节点统一从 self.kwargs 读 CLI 参数
    # 首跑写入 job metadata,resume 自动继承;输入非产物,不进 artifact.json
    # 默认 None,_instantiate 后保证是独立 dict(避免类属性可变默认值共享陷阱)
    kwargs: dict[str, Any] | None = None

    def accept(self, ctx: DepthScope) -> bool:
        """接手确认:run 前校验前置条件。返回 False → 跳过本节点(emit skipped,artifact None)。默认通过。"""
        return True

    def deliver(self, artifact: Any) -> bool:
        """脱手确认:run 后校验产物。默认通过。"""
        return True

    def run(self, ctx: DepthScope) -> Any:
        """子类必须实现,返回 artifact。"""
        raise NotImplementedError(f"{type(self).__name__} 未实现 run")


def _instantiate(
    node_cls: type[Node], replica_id: str, index: int, depth: int = 0
) -> Node:
    """实例化一个 Node 子类为运行时实例,注入副本信息与拓扑深度。"""
    node = node_cls()
    node.replica_id = replica_id
    node.index = index
    node.depth = depth
    node.kwargs = {}
    return node
