"""easyflow:轻量 Python DAG workflow 框架。

主用法(库式):

    from easyflow import Runner, Node, StepContext, Checkpoint

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
    WorkflowJobEvent,
    trace,
    delta,
    checkpoint,
    final,
    error,
    end,
    easyflow_event,
)
from .step import Node, StepContext, Checkpoint, StepDefine, FanOut
from .flow import flow, edge, Edge, FlowDefine
from .state import JobState, StepState, apply_event
from .loader import load_flow, FlowLoadError
from .runner import Runner

__all__ = [
    "WorkflowJobEvent",
    "trace",
    "delta",
    "checkpoint",
    "final",
    "error",
    "end",
    "easyflow_event",
    "Node",
    "StepContext",
    "Checkpoint",
    "StepDefine",
    "FanOut",
    "flow",
    "edge",
    "Edge",
    "FlowDefine",
    "JobState",
    "StepState",
    "apply_event",
    "load_flow",
    "FlowLoadError",
    "Runner",
]
