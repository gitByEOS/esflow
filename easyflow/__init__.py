"""easyflow:轻量 Python DAG workflow 框架。

主用法(库式):

    from easyflow import Runner, Node, DepthScope, Checkpoint

    runner = Runner.load("./my_flow")
    async for event in runner.run():
        ...
    # 单调试:只跑指定副本及其上游
    async for event in runner.run(only={"worker#2"}):
        ...

调试便捷入口:

    $ easyflow run ./my_flow
    $ easyflow run ./my_flow --node worker#2   # 单跑副本
    $ easyflow view ./my_flow                  # Web 调试界面
"""

from .event import (
    JobEvent,
    trace,
    delta,
    checkpoint,
    final,
    error,
    end,
    easyflow_event,
)
from .node import Node, DepthScope, Checkpoint, FanOut
from .flow import flow, edge, Edge, FlowDefine
from .state import JobState, RunState, NodeStatus, apply_event
from .loader import load_flow, FlowLoadError
from .runner import Runner

__all__ = [
    "JobEvent",
    "trace",
    "delta",
    "checkpoint",
    "final",
    "error",
    "end",
    "easyflow_event",
    "Node",
    "DepthScope",
    "Checkpoint",
    "FanOut",
    "flow",
    "edge",
    "Edge",
    "FlowDefine",
    "JobState",
    "RunState",
    "NodeStatus",
    "apply_event",
    "load_flow",
    "FlowLoadError",
    "Runner",
]
