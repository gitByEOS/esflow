"""WorkflowJobEvent 协议:Runner 产出的唯一事件流,借鉴当前 TS 项目 event.ts。

事件类型:
- trace:      节点状态变更(queued/running/done/error/skipped)
- delta:      节点产出增量文本
- checkpoint: 节点跑到暂停点,等外部确认(resume/retry)
- final:      节点最终 artifact
- error:      错误
- end:        job 结束
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal


TraceStatus = Literal["queued", "running", "done", "error", "skipped"]


@dataclass
class WorkflowJobEvent:
    """统一事件信封。type 决定哪些字段有效。"""

    type: Literal["trace", "delta", "checkpoint", "final", "error", "end"]
    at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    # 公共可选字段
    step_id: str | None = None
    # trace
    status: TraceStatus | None = None
    detail: str | None = None
    # delta
    text: str | None = None
    # checkpoint / final
    artifact: Any | None = None
    # error
    message: str | None = None


# 构造便捷函数

def trace(step_id: str, status: TraceStatus, detail: str = "") -> WorkflowJobEvent:
    return WorkflowJobEvent(type="trace", step_id=step_id, status=status, detail=detail)


def delta(step_id: str, text: str) -> WorkflowJobEvent:
    return WorkflowJobEvent(type="delta", step_id=step_id, text=text)


def checkpoint(step_id: str, artifact: Any) -> WorkflowJobEvent:
    return WorkflowJobEvent(type="checkpoint", step_id=step_id, artifact=artifact)


def final(step_id: str, artifact: Any) -> WorkflowJobEvent:
    return WorkflowJobEvent(type="final", step_id=step_id, artifact=artifact)


def error(step_id: str | None, message: str) -> WorkflowJobEvent:
    return WorkflowJobEvent(type="error", step_id=step_id, message=message)


def end() -> WorkflowJobEvent:
    return WorkflowJobEvent(type="end")
