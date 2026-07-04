"""JobEvent 协议:Runner 产出的唯一事件流,借鉴当前 TS 项目 event.ts。

事件类型:
- trace:      节点状态变更(queued/running/done/error/skipped)
- delta:      节点产出增量文本
- checkpoint: 节点跑到暂停点,等外部确认(resume/retry)
- final:      节点最终 artifact
- error:      错误
- end:        job 结束
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal


TraceStatus = Literal["queued", "running", "done", "error", "skipped"]


@dataclass
class JobEvent:
    """统一事件信封。type 决定哪些字段有效。"""

    type: Literal["trace", "delta", "checkpoint", "final", "error", "end"]
    at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    # 公共可选字段:run_id 标识一次节点运行(普通节点 = base id,副本 = replica_id)
    run_id: str | None = None
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

def trace(run_id: str, status: TraceStatus, detail: str = "") -> JobEvent:
    return JobEvent(type="trace", run_id=run_id, status=status, detail=detail)


def delta(run_id: str, text: str) -> JobEvent:
    return JobEvent(type="delta", run_id=run_id, text=text)


def checkpoint(run_id: str, artifact: Any) -> JobEvent:
    return JobEvent(type="checkpoint", run_id=run_id, artifact=artifact)


def final(run_id: str, artifact: Any) -> JobEvent:
    return JobEvent(type="final", run_id=run_id, artifact=artifact)


def error(run_id: str | None, message: str) -> JobEvent:
    return JobEvent(type="error", run_id=run_id, message=message)


def end() -> JobEvent:
    return JobEvent(type="end")


def easyflow_event(event: JobEvent) -> None:
    """按事件类型打印一行,cli / skill run.py / view 共用的统一消费入口。

    checkpoint 只打印 artifact,不打印交互提示(交互由调用方自行处理)。
    """
    if event.type == "trace":
        print(f"[{event.run_id}] {event.status}: {event.detail}")
    elif event.type == "delta":
        print(f"[{event.run_id}] {event.text}", end="")
    elif event.type == "checkpoint":
        print(f"\n[checkpoint] {event.run_id} artifact:")
        print(json.dumps(event.artifact, ensure_ascii=False, indent=2, default=str))
    elif event.type == "final":
        print(f"[{event.run_id}] artifact: {event.artifact}")
    elif event.type == "error":
        print(f"[error] {event.run_id}: {event.message}", file=sys.stderr)
    elif event.type == "end":
        print("[end]")
