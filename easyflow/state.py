"""内存 state:event 折叠成可读状态。借鉴当前 TS 项目 job-reducer,但不落库。

Runner 维护一个 JobState,每来一个 event 调 apply_event 更新。
view/CLI 订阅 state 渲染进度。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .event import WorkflowJobEvent


@dataclass
class StepState:
    step_id: str
    status: str = "idle"  # idle / queued / running / paused / done / error / skipped
    artifact: Any | None = None
    detail: str = ""
    text: str = ""  # delta 累积


@dataclass
class JobState:
    flow_id: str
    steps: dict[str, StepState] = field(default_factory=dict)
    status: str = "running"  # running / paused / done / error
    finished: bool = False


def apply_event(state: JobState, event: WorkflowJobEvent) -> JobState:
    """把 event 折进 state,返回同一 state(就地更新)。"""

    if event.type == "end":
        state.finished = True
        if state.status != "error":
            state.status = "done"
        return state

    if event.step_id and event.step_id not in state.steps:
        state.steps[event.step_id] = StepState(step_id=event.step_id)

    if event.type == "trace":
        if event.step_id:
            s = state.steps[event.step_id]
            s.status = event.status or s.status
            if event.detail:
                s.detail = event.detail
    elif event.type == "delta":
        if event.step_id:
            s = state.steps[event.step_id]
            s.text += event.text or ""
    elif event.type == "checkpoint":
        if event.step_id:
            s = state.steps[event.step_id]
            s.status = "paused"
            s.artifact = event.artifact
            state.status = "paused"
    elif event.type == "final":
        if event.step_id:
            s = state.steps[event.step_id]
            s.artifact = event.artifact
            s.status = "done"
    elif event.type == "error":
        if event.step_id:
            s = state.steps[event.step_id]
            s.status = "error"
        state.status = "error"

    return state
