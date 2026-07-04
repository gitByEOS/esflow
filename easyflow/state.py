"""内存 state:event 折叠成可读状态。借鉴当前 TS 项目 job-reducer,但不落库。

Runner 维护一个 JobState,每来一个 event 调 apply_event 更新。
view/CLI 订阅 state 渲染进度。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from .event import JobEvent


class NodeStatus(str, Enum):
    """节点运行状态。继承 str 让 == "done" 与 dict key 兼容现有字符串用法。"""

    IDLE = "idle"
    QUEUED = "queued"
    RUNNING = "running"
    PAUSED = "paused"
    DONE = "done"
    ERROR = "error"
    SKIPPED = "skipped"


class JobStatus(str, Enum):
    """job 整体状态:NodeStatus 的运行子集,不含节点级 idle/queued/skipped。"""

    RUNNING = "running"
    PAUSED = "paused"
    DONE = "done"
    ERROR = "error"


@dataclass
class RunState:
    """单次节点运行的事件折叠状态(per-replica)。"""

    run_id: str
    status: NodeStatus = NodeStatus.IDLE
    artifact: Any | None = None
    detail: str = ""
    text: str = ""  # delta 累积


@dataclass
class JobState:
    flow_id: str
    runs: dict[str, RunState] = field(default_factory=dict)
    status: JobStatus = JobStatus.RUNNING
    finished: bool = False


def apply_event(state: JobState, event: JobEvent) -> JobState:
    """把 event 折进 state,返回同一 state(就地更新)。"""

    if event.type == "end":
        state.finished = True
        if state.status != JobStatus.ERROR:
            state.status = JobStatus.DONE
        return state

    if event.run_id and event.run_id not in state.runs:
        state.runs[event.run_id] = RunState(run_id=event.run_id)

    if event.type == "trace":
        if event.run_id:
            s = state.runs[event.run_id]
            # event.status 是 TraceStatus Literal,值域与 NodeStatus 重叠,直接转枚举
            s.status = NodeStatus(event.status) if event.status else s.status
            if event.detail:
                s.detail = event.detail
    elif event.type == "delta":
        if event.run_id:
            s = state.runs[event.run_id]
            s.text += event.text or ""
    elif event.type == "checkpoint":
        if event.run_id:
            s = state.runs[event.run_id]
            s.status = NodeStatus.PAUSED
            s.artifact = event.artifact
            state.status = JobStatus.PAUSED
    elif event.type == "final":
        if event.run_id:
            s = state.runs[event.run_id]
            s.artifact = event.artifact
            s.status = NodeStatus.DONE
    elif event.type == "error":
        if event.run_id:
            s = state.runs[event.run_id]
            s.status = NodeStatus.ERROR
        state.status = JobStatus.ERROR

    return state
